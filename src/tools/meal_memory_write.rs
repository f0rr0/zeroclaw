use super::traits::{Tool, ToolResult};
use crate::config::Config;
use crate::meal::store::{CommitRequest, MealStore};
use crate::meal::turn_context;
use crate::security::SecurityPolicy;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// Mutating meal-state commit tool with idempotent writes.
pub struct MealMemoryWriteTool {
    security: Arc<SecurityPolicy>,
    store: MealStore,
}

impl MealMemoryWriteTool {
    pub fn new(config: Arc<Config>, security: Arc<SecurityPolicy>) -> Self {
        Self {
            security,
            store: MealStore::new(config),
        }
    }
}

#[async_trait]
impl Tool for MealMemoryWriteTool {
    fn name(&self) -> &str {
        "meal_memory_write"
    }

    fn description(&self) -> &str {
        "Apply meal decision mutations transactionally (preferences, pantry, episodes, feedback, nudge intents) with idempotency safeguards."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "household_id": { "type": "string" },
                "meal_date_local": { "type": "string" },
                "meal_slot": {
                    "type": "string",
                    "enum": ["breakfast", "lunch", "dinner", "snack", "unslotted"]
                },
                "slot_instance": { "type": "integer", "minimum": 1 },
                "operations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "op": {
                                "type": "string",
                                "enum": [
                                    "record_preference",
                                    "record_pantry",
                                    "record_episode",
                                    "record_feedback",
                                    "schedule_feedback_nudge",
                                    "cancel_feedback_nudge"
                                ]
                            },
                            "user_id": { "type": "string" },
                            "key": { "type": "string" },
                            "value": { "type": "string" },
                            "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                            "source": { "type": "string" },
                            "item_name": { "type": "string" },
                            "quantity_hint": { "type": "string" },
                            "freshness_profile": {
                                "type": "string",
                                "enum": ["high", "medium", "low", "very_low"]
                            },
                            "observed_at": {
                                "type": "integer",
                                "description": "Unix timestamp (seconds) when item was observed/ordered."
                            },
                            "meal_name": { "type": "string" },
                            "tags": { "type": "array", "items": { "type": "string" } },
                            "decided_by": { "type": "string" },
                            "meal_anchor_key": { "type": "string" },
                            "rating": { "type": "number", "minimum": 0.0, "maximum": 5.0 },
                            "feedback_text": { "type": "string" },
                            "text": {
                                "type": "string",
                                "description": "Deprecated alias for feedback_text."
                            },
                            "decision_case_id": { "type": "string" },
                            "nudge_type": { "type": "string" },
                            "run_at": { "type": "string" },
                            "prompt": { "type": "string" },
                            "channel": { "type": "string" },
                            "target": { "type": "string" },
                            "best_effort": { "type": "boolean" }
                        },
                        "required": ["op"]
                    }
                },
                "client_idempotency_key": { "type": "string" },
                "source_message_id": { "type": "string" }
            },
            "required": ["operations"]
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

        if !self.security.can_act() {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "Security policy: read-only mode, cannot commit meal mutations.".to_string(),
                ),
            });
        }
        if !self.security.record_action() {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some("Rate limit exceeded: action budget exhausted.".to_string()),
            });
        }

        let mut request: CommitRequest = match serde_json::from_value(args) {
            Ok(value) => value,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments for meal_memory_write: {error}")),
                });
            }
        };
        let Some(source_message_id) = turn_context::current_turn_source_message_id() else {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "meal_memory_write requires runtime turn context; source_message_id is server-managed."
                        .to_string(),
                ),
            });
        };
        // Never trust model-provided source IDs; bind commits to server turn context.
        request.source_message_id = source_message_id;
        if request.household_id.trim().is_empty() {
            request.household_id = self.store.household_id().to_string();
        }

        match self.store.commit(request) {
            Ok(response) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&serde_json::json!({ "commit": response }))?,
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
    use crate::meal::turn_context;
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

    fn test_security(cfg: &Config) -> Arc<SecurityPolicy> {
        Arc::new(SecurityPolicy::from_config(
            &cfg.autonomy,
            &cfg.workspace_dir,
        ))
    }

    #[tokio::test]
    async fn rejects_commit_without_turn_context() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealMemoryWriteTool::new(cfg.clone(), test_security(cfg.as_ref()));

        let result = tool
            .execute(json!({
                "household_id": "household_test",
                "meal_date_local": "2026-02-13",
                "meal_slot": "lunch",
                "slot_instance": 1,
                "source_message_id": "forged",
                "operations": [{
                    "op": "record_episode",
                    "meal_name": "chicken stir fry",
                    "tags": ["chicken"]
                }]
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("requires runtime turn context"));
    }

    #[tokio::test]
    async fn binds_source_message_id_from_server_turn_context() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealMemoryWriteTool::new(cfg.clone(), test_security(cfg.as_ref()));

        let result = turn_context::with_turn_source_message_id(
            "telegram_-1001_42".to_string(),
            tool.execute(json!({
                "household_id": "household_test",
                "meal_date_local": "2026-02-13",
                "meal_slot": "lunch",
                "slot_instance": 1,
                "source_message_id": "forged_by_model",
                "operations": [{
                    "op": "record_episode",
                    "meal_name": "chicken stir fry",
                    "tags": ["chicken", "capsicum"]
                }]
            })),
        )
        .await
        .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);

        let ctx = MealStore::new(cfg.clone())
            .context_get(crate::meal::store::ContextQuery {
                household_id: "household_test".to_string(),
                ..crate::meal::store::ContextQuery::default()
            })
            .await
            .expect("context read");

        assert_eq!(ctx.recent_meal_episodes.len(), 1);
        assert_eq!(
            ctx.recent_meal_episodes[0].source_message_id.as_deref(),
            Some("telegram_-1001_42")
        );
    }
}
