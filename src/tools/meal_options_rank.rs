use super::traits::{Tool, ToolResult};
use crate::config::Config;
use crate::meal::store::{
    ContextBundle, MealStore, RecommendRequest, RecommendationCandidateInput,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Deterministic recommendation scorer over meal context.
pub struct MealOptionsRankTool {
    store: MealStore,
}

impl MealOptionsRankTool {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            store: MealStore::new(config),
        }
    }

    fn normalize_candidates(raw: Option<&Value>) -> Result<Vec<RecommendationCandidateInput>> {
        let Some(value) = raw else {
            return Ok(Vec::new());
        };
        let Some(items) = value.as_array() else {
            anyhow::bail!("candidate_set must be an array");
        };

        let mut candidates = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if let Some(name) = item.as_str() {
                if name.trim().is_empty() {
                    anyhow::bail!("candidate_set[{index}] meal name must not be empty");
                }
                candidates.push(RecommendationCandidateInput {
                    meal_name: name.trim().to_string(),
                    tags: Vec::new(),
                });
                continue;
            }
            if let Some(obj) = item.as_object() {
                let name = obj
                    .get("meal_name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "candidate_set[{index}] object requires non-empty `meal_name`"
                        )
                    })?;
                let tags = match obj.get("tags") {
                    Some(Value::Array(arr)) => arr
                        .iter()
                        .map(|value| {
                            value.as_str().map(ToString::to_string).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "candidate_set[{index}].tags must contain strings only"
                                )
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    Some(_) => {
                        anyhow::bail!("candidate_set[{index}].tags must be an array of strings")
                    }
                    None => Vec::new(),
                };
                candidates.push(RecommendationCandidateInput {
                    meal_name: name.to_string(),
                    tags,
                });
                continue;
            }
            anyhow::bail!("candidate_set[{index}] must be either a meal name string or an object");
        }
        Ok(candidates)
    }
}

#[derive(Debug, Deserialize)]
struct RecommendArgs {
    #[serde(default)]
    household_id: String,
    #[serde(default)]
    context_bundle: Option<ContextBundle>,
    #[serde(default)]
    mode_hint: Option<String>,
    #[serde(default)]
    candidate_set: Option<Value>,
}

#[async_trait]
impl Tool for MealOptionsRankTool {
    fn name(&self) -> &str {
        "meal_options_rank"
    }

    fn description(&self) -> &str {
        "Rank meal options against household meal context (preferences, pantry, recency, and feedback)."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "household_id": { "type": "string" },
                "context_bundle": { "type": "object" },
                "candidate_set": {
                    "type": "array",
                    "items": {
                        "oneOf": [
                            { "type": "string" },
                            {
                                "type": "object",
                                "properties": {
                                    "meal_name": { "type": "string" },
                                    "tags": {
                                        "type": "array",
                                        "items": { "type": "string" }
                                    }
                                },
                                "required": ["meal_name"]
                            }
                        ]
                    }
                },
                "mode_hint": {
                    "type": "string",
                    "enum": ["cook", "self_cook", "no_cook"]
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

        let parsed: RecommendArgs = match serde_json::from_value(args) {
            Ok(value) => value,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments for meal_options_rank: {error}")),
                });
            }
        };
        let mut request = RecommendRequest {
            household_id: parsed.household_id,
            context_bundle: parsed.context_bundle,
            candidate_set: Vec::new(),
            mode_hint: parsed.mode_hint,
        };
        if request.household_id.trim().is_empty() {
            request.household_id = self.store.household_id().to_string();
        }
        request.candidate_set = match Self::normalize_candidates(parsed.candidate_set.as_ref()) {
            Ok(candidates) => candidates,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Invalid arguments for meal_options_rank: {error}")),
                });
            }
        };

        match self.store.recommend(request).await {
            Ok(response) => Ok(ToolResult {
                success: true,
                output: serde_json::to_string_pretty(&response)?,
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

    #[tokio::test]
    async fn rejects_invalid_top_level_arguments() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealOptionsRankTool::new(cfg);

        let result = tool
            .execute(json!("invalid"))
            .await
            .expect("tool call should return ToolResult");
        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("Invalid arguments for meal_options_rank"));
    }

    #[tokio::test]
    async fn rejects_invalid_candidate_set_shape() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealOptionsRankTool::new(cfg);

        let result = tool
            .execute(json!({
                "household_id": "household_test",
                "candidate_set": {"meal_name": "not-an-array"}
            }))
            .await
            .expect("tool call should return ToolResult");
        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("candidate_set must be an array"));
    }
}
