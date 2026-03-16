use super::traits::{Tool, ToolResult};
use super::MealInstamartSyncTool;
use crate::config::Config;
use crate::meal::store::{ContextBundle, ContextQuery, MealStore};
use crate::meal::turn_context;
use crate::security::SecurityPolicy;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use tracing::{info, warn};

const INSTAMART_AUTO_SYNC_INTERVAL_SECS: i64 = 6 * 60 * 60;
const INSTAMART_AUTO_SYNC_FAILURE_BACKOFF_SECS: i64 = 20 * 60;
const INSTAMART_AUTO_SYNC_LOOKBACK_DAYS: usize = 14;
const INSTAMART_AUTO_SYNC_MAX_ORDERS: usize = 25;
const INSTAMART_AUTO_SYNC_MAX_ITEMS: usize = 50;

/// Meal context retrieval for recommendation grounding.
pub struct MealContextReadTool {
    store: MealStore,
    instamart_sync: MealInstamartSyncTool,
}

impl MealContextReadTool {
    pub fn new(config: Arc<Config>, security: Arc<SecurityPolicy>) -> Self {
        Self {
            store: MealStore::new(config.clone()),
            instamart_sync: MealInstamartSyncTool::new(config, security),
        }
    }

    fn infer_channel_from_source_message_id(source_message_id: &str) -> Option<&'static str> {
        if source_message_id.starts_with("telegram_") {
            Some("telegram")
        } else if source_message_id.starts_with("discord_") {
            Some("discord")
        } else if source_message_id.starts_with("slack_") {
            Some("slack")
        } else {
            None
        }
    }

    fn is_pantry_relevant_turn(&self) -> bool {
        let Some(source_message_id) = turn_context::current_turn_source_message_id() else {
            return false;
        };
        let Some(channel) = Self::infer_channel_from_source_message_id(source_message_id.as_str())
        else {
            return false;
        };

        if let Ok(Some(gate)) = self.store.control_gate_event_by_message_id(
            channel,
            source_message_id.as_str(),
            "reply",
        ) {
            let intent = gate
                .intent
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            let reason = gate.reason_code.to_ascii_lowercase();
            if intent == "meal"
                || intent == "feedback"
                || reason.contains("meal")
                || reason.contains("feedback")
            {
                return true;
            }
        }

        if let Ok(Some(ingress)) = self
            .store
            .ingress_event_by_message_id(channel, source_message_id.as_str())
        {
            let (cleaned, image_refs) =
                crate::multimodal::parse_image_markers(ingress.content.as_str());
            if !image_refs.is_empty() {
                return true;
            }
            let text = cleaned.to_ascii_lowercase();
            return [
                "meal",
                "eat",
                "cook",
                "breakfast",
                "lunch",
                "dinner",
                "snack",
                "fridge",
                "pantry",
                "ingredients",
            ]
            .iter()
            .any(|term| text.contains(term));
        }

        false
    }

    async fn resolve_candidate_users(
        &self,
        household_id: &str,
        active_user_ids: &[String],
    ) -> Vec<String> {
        let mut users = Vec::new();
        for user in active_user_ids {
            let normalized = user.trim();
            if normalized.is_empty() {
                continue;
            }
            if !users.iter().any(|existing| existing == normalized) {
                users.push(normalized.to_string());
            }
        }
        if let Ok(Some(user)) = self.store.latest_ingress_sender(household_id) {
            if !user.trim().is_empty() && !users.iter().any(|existing| existing == &user) {
                users.push(user);
            }
        }
        if let Ok(connected) = self.instamart_sync.connected_users(household_id).await {
            for user in connected {
                if !users.iter().any(|existing| existing == &user) {
                    users.push(user);
                }
            }
        }
        users
    }

    async fn maybe_auto_sync_instamart(
        &self,
        household_id: &str,
        active_user_ids: &[String],
        context: &ContextBundle,
    ) -> bool {
        let pantry_relevant_turn = self.is_pantry_relevant_turn();
        let pantry_weak_context = context.pantry_snapshot.is_empty();
        if !pantry_relevant_turn && !pantry_weak_context {
            return false;
        }

        let candidate_users = self
            .resolve_candidate_users(household_id, active_user_ids)
            .await;
        if candidate_users.is_empty() {
            return false;
        }

        let now = chrono::Utc::now().timestamp();
        let mut failures = Vec::new();
        for user_id in candidate_users {
            let stale = match self.store.pantry_sync_state(
                household_id,
                "instamart_order",
                Some(user_id.as_str()),
            ) {
                Ok(Some(state)) => {
                    let failure_backoff_active = state.last_error.is_some()
                        && state.last_attempt_at.is_some_and(|last_attempt| {
                            now.saturating_sub(last_attempt)
                                < INSTAMART_AUTO_SYNC_FAILURE_BACKOFF_SECS
                        });
                    if failure_backoff_active {
                        false
                    } else {
                        state.last_success_at.map_or(true, |last| {
                            now.saturating_sub(last) >= INSTAMART_AUTO_SYNC_INTERVAL_SECS
                        })
                    }
                }
                Ok(None) => true,
                Err(error) => {
                    warn!(
                        component = "meal_context_read",
                        household_id = household_id,
                        user_id = user_id,
                        error = %error,
                        "failed reading pantry sync state; treating as stale"
                    );
                    true
                }
            };
            if !stale {
                continue;
            }
            let result = self
                .instamart_sync
                .execute(json!({
                    "household_id": household_id,
                    "user_id": user_id.as_str(),
                    "lookback_days": INSTAMART_AUTO_SYNC_LOOKBACK_DAYS,
                    "max_orders": INSTAMART_AUTO_SYNC_MAX_ORDERS,
                    "max_items": INSTAMART_AUTO_SYNC_MAX_ITEMS,
                    "persist_pantry": true
                }))
                .await;

            match result {
                Ok(tool_result) if tool_result.success => {
                    info!(
                        component = "meal_context_read",
                        household_id = household_id,
                        user_id = user_id,
                        "instamart auto-sync applied"
                    );
                    return true;
                }
                Ok(tool_result) => {
                    let error = tool_result.error.unwrap_or_default();
                    let normalized = error.to_ascii_lowercase();
                    if normalized.contains("no connected instamart account") {
                        continue;
                    }
                    failures.push(format!("{user_id}: {error}"));
                    warn!(
                        component = "meal_context_read",
                        household_id = household_id,
                        user_id = user_id,
                        "instamart auto-sync skipped: {}",
                        error
                    );
                }
                Err(error) => {
                    failures.push(format!("{user_id}: {error}"));
                    warn!(
                        component = "meal_context_read",
                        household_id = household_id,
                        user_id = user_id,
                        "instamart auto-sync failed: {}",
                        error
                    );
                }
            }
        }
        if !failures.is_empty() {
            warn!(
                component = "meal_context_read",
                household_id = household_id,
                failure_count = failures.len(),
                "instamart auto-sync exhausted candidates without success: {}",
                failures.join(" | ")
            );
        }
        false
    }
}

#[async_trait]
impl Tool for MealContextReadTool {
    fn name(&self) -> &str {
        "meal_context_read"
    }

    fn description(&self) -> &str {
        "Retrieve structured household meal context: preferences, recent meals, pantry hints, feedback, and recent conversation ingress. May auto-refresh stale Instamart pantry history for connected users."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "household_id": { "type": "string" },
                "active_user_ids": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "focus_terms": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "limits": {
                    "type": "object",
                    "properties": {
                        "max_preferences": {"type": "integer"},
                        "max_episodes": {"type": "integer"},
                        "max_pantry_items": {"type": "integer"},
                        "max_feedback": {"type": "integer"},
                        "max_ingress_window": {"type": "integer"}
                    }
                }
            }
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult> {
        if !self.store.enabled() {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "Meal agent is disabled. Enable `[meal_agent].enabled = true` in config."
                        .to_string(),
                ),
            });
        }

        let mut query: ContextQuery = match serde_json::from_value(args) {
            Ok(value) => value,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments for meal_context_read: {error}")),
                });
            }
        };
        if query.household_id.trim().is_empty() {
            query.household_id = self.store.household_id().to_string();
        }

        let active_user_ids = query.active_user_ids.clone();
        let mut context = match self.store.context_get(query.clone()).await {
            Ok(context) => context,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(error.to_string()),
                });
            }
        };

        if self
            .maybe_auto_sync_instamart(query.household_id.as_str(), &active_user_ids, &context)
            .await
        {
            context = match self.store.context_get(query).await {
                Ok(refreshed) => refreshed,
                Err(error) => {
                    return Ok(ToolResult {
                        success: false,
                        output: String::new(),
                        error: Some(error.to_string()),
                    });
                }
            };
        }

        match serde_json::to_string_pretty(&context) {
            Ok(context) => Ok(ToolResult {
                success: true,
                output: context,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(error.to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::SecurityPolicy;
    use serde_json::json;
    use tempfile::TempDir;

    async fn test_config(tmp: &TempDir) -> Arc<Config> {
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        tokio::fs::create_dir_all(&config.workspace_dir)
            .await
            .expect("workspace dir should be created");
        config.meal_agent.enabled = true;
        config.meal_agent.household_id = "household_test".to_string();
        config.meal_agent.db_path = tmp
            .path()
            .join("meal-agent-test.db")
            .to_string_lossy()
            .to_string();
        Arc::new(config)
    }

    fn security_for(cfg: &Arc<Config>) -> Arc<SecurityPolicy> {
        Arc::new(SecurityPolicy::from_config(
            &cfg.autonomy,
            &cfg.workspace_dir,
        ))
    }

    #[tokio::test]
    async fn rejects_invalid_arguments() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealContextReadTool::new(cfg.clone(), security_for(&cfg));

        let result = tool
            .execute(json!("invalid"))
            .await
            .expect("tool call should return ToolResult");
        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("Invalid arguments for meal_context_read"));
    }

    #[tokio::test]
    async fn accepts_empty_object_args() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealContextReadTool::new(cfg.clone(), security_for(&cfg));

        let result = tool
            .execute(json!({}))
            .await
            .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);
    }

    #[tokio::test]
    async fn accepts_partial_limits_object() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealContextReadTool::new(cfg.clone(), security_for(&cfg));

        let result = tool
            .execute(json!({
                "limits": {
                    "max_preferences": 20,
                    "max_episodes": 10
                }
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);
    }
}
