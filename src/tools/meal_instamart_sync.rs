use super::traits::{Tool, ToolResult};
use crate::auth::oauth_common::url_encode;
use crate::auth::profiles::AuthProfilesStore;
use crate::config::schema::{McpServerConfig, McpTransport};
use crate::config::Config;
use crate::meal::store::{CommitRequest, MealStore};
use crate::meal::turn_context;
use crate::security::SecurityPolicy;
use crate::tools::mcp_client::McpServer;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

const SWIGGY_ACCOUNT_PROVIDER: &str = "swiggy-instamart-mcp";
const SWIGGY_INSTAMART_MCP_URL: &str = "https://mcp.swiggy.com/im";
const INSTAMART_SYNC_SOURCE: &str = "instamart_order";
const META_HOUSEHOLD_ID: &str = "household_id";
const META_USER_ID: &str = "user_id";
const MAX_RAW_RESULT_CHARS: usize = 6000;

#[derive(Debug, Deserialize)]
#[serde(default)]
struct OrdersSyncArgs {
    household_id: String,
    user_id: String,
    max_orders: usize,
    max_items: usize,
    lookback_days: usize,
    persist_pantry: bool,
}

impl Default for OrdersSyncArgs {
    fn default() -> Self {
        Self {
            household_id: String::new(),
            user_id: String::new(),
            max_orders: 25,
            max_items: 40,
            lookback_days: 14,
            persist_pantry: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PantryCandidate {
    item_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quantity_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    freshness_profile: Option<String>,
}

#[derive(Debug, Clone)]
struct OrderToolSelection {
    name: String,
    schema: Value,
}

pub struct MealInstamartSyncTool {
    security: Arc<SecurityPolicy>,
    store: MealStore,
    profiles: AuthProfilesStore,
}

impl MealInstamartSyncTool {
    pub fn new(config: Arc<Config>, security: Arc<SecurityPolicy>) -> Self {
        let state_dir = crate::auth::state_dir_from_config(config.as_ref());
        Self {
            security,
            store: MealStore::new(config.clone()),
            profiles: AuthProfilesStore::new(&state_dir, config.secrets.encrypt),
        }
    }

    fn account_profile_name(household_id: &str, user_id: &str) -> String {
        format!(
            "h={}&u={}",
            url_encode(household_id.trim()),
            url_encode(user_id.trim())
        )
    }

    fn resolve_user(&self, _household_id: &str, provided_user: &str) -> Option<String> {
        let user = provided_user.trim();
        if !user.is_empty() {
            return Some(user.to_string());
        }
        None
    }

    pub async fn connected_users(&self, household_id: &str) -> Result<Vec<String>> {
        let data = self.profiles.load().await?;
        let mut users = HashSet::new();
        for profile in data.profiles.values() {
            if profile.provider != SWIGGY_ACCOUNT_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            let Some(user_id) = profile.metadata.get(META_USER_ID) else {
                continue;
            };
            let normalized = user_id.trim();
            if normalized.is_empty() {
                continue;
            }
            users.insert(normalized.to_string());
        }
        let mut out = users.into_iter().collect::<Vec<_>>();
        out.sort();
        Ok(out)
    }

    async fn load_access_token(&self, household_id: &str, user_id: &str) -> Result<String> {
        let data = self.profiles.load().await?;
        let preferred_profile_id = format!(
            "{}:{}",
            SWIGGY_ACCOUNT_PROVIDER,
            Self::account_profile_name(household_id, user_id)
        );
        if let Some(profile) = data.profiles.get(&preferred_profile_id) {
            if let Some(token_set) = &profile.token_set {
                let token = token_set.access_token.trim();
                if !token.is_empty() {
                    return Ok(token.to_string());
                }
            }
        }

        // Backstop: scan metadata in case profile naming changes in the future.
        for profile in data.profiles.values() {
            if profile.provider != SWIGGY_ACCOUNT_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            if profile.metadata.get(META_USER_ID).map(String::as_str) != Some(user_id) {
                continue;
            }
            if let Some(token_set) = &profile.token_set {
                let token = token_set.access_token.trim();
                if !token.is_empty() {
                    return Ok(token.to_string());
                }
            }
        }

        anyhow::bail!(
            "No connected Instamart account found for `{user_id}`. Run `/instamart connect` first."
        );
    }

    async fn connect_mcp(&self, access_token: &str) -> Result<McpServer> {
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", access_token.trim()),
        );
        headers.insert(
            "mcp-protocol-version".to_string(),
            crate::tools::mcp_protocol::MCP_PROTOCOL_VERSION.to_string(),
        );
        let config = McpServerConfig {
            name: "swiggy_instamart_runtime".to_string(),
            transport: McpTransport::Http,
            url: Some(SWIGGY_INSTAMART_MCP_URL.to_string()),
            command: String::new(),
            args: Vec::new(),
            env: HashMap::new(),
            headers,
            tool_timeout_secs: Some(60),
        };
        McpServer::connect(config).await
    }

    fn order_tool_score(name: &str, description: Option<&str>) -> i32 {
        let hay = format!(
            "{} {}",
            name.to_ascii_lowercase(),
            description.unwrap_or_default().to_ascii_lowercase()
        );
        let mut score = 0;
        if hay.contains("order") {
            score += 7;
        }
        if hay.contains("history") {
            score += 6;
        }
        if hay.contains("recent") {
            score += 3;
        }
        if hay.contains("instamart") {
            score += 2;
        }
        if hay.contains("list") || hay.contains("get") || hay.contains("fetch") {
            score += 1;
        }
        score
    }

    async fn select_order_tool(&self, server: &McpServer) -> Result<OrderToolSelection> {
        let tools = server.tools().await;
        let mut ranked = Vec::new();
        for tool in tools {
            let score = Self::order_tool_score(&tool.name, tool.description.as_deref());
            if score > 0 {
                ranked.push((score, tool.name, tool.input_schema));
            }
        }
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let Some((_, name, schema)) = ranked.into_iter().next() else {
            anyhow::bail!("No Instamart order-history MCP tool is available for this account.");
        };
        Ok(OrderToolSelection { name, schema })
    }

    fn build_candidate_args(schema: &Value, max_orders: usize, lookback_days: usize) -> Vec<Value> {
        let mut candidates = Vec::new();
        candidates.push(serde_json::json!({
            "limit": max_orders,
            "lookback_days": lookback_days
        }));
        candidates.push(serde_json::json!({ "limit": max_orders }));
        candidates.push(serde_json::json!({ "max_results": max_orders }));
        candidates.push(serde_json::json!({ "count": max_orders }));

        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            let mut generated = Map::new();
            for key in properties.keys() {
                let key_lower = key.to_ascii_lowercase();
                if key_lower.contains("limit")
                    || key_lower.contains("count")
                    || key_lower.contains("max")
                    || key_lower.contains("page_size")
                {
                    generated.insert(key.clone(), Value::from(max_orders as u64));
                    continue;
                }
                if key_lower.contains("lookback")
                    || key_lower.contains("days")
                    || key_lower.contains("window")
                {
                    generated.insert(key.clone(), Value::from(lookback_days as u64));
                    continue;
                }
                if key_lower.contains("include") || key_lower.contains("detail") {
                    generated.insert(key.clone(), Value::Bool(true));
                    continue;
                }
                if key_lower == "page" || key_lower == "offset" {
                    generated.insert(key.clone(), Value::from(0_u64));
                }
            }
            if !generated.is_empty() {
                candidates.push(Value::Object(generated));
            }
        }

        candidates.push(serde_json::json!({}));

        // De-duplicate while preserving order.
        let mut deduped = Vec::new();
        let mut seen = HashSet::new();
        for value in candidates {
            let key = serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string());
            if seen.insert(key) {
                deduped.push(value);
            }
        }
        deduped
    }

    async fn call_order_tool(
        &self,
        server: &McpServer,
        selection: &OrderToolSelection,
        max_orders: usize,
        lookback_days: usize,
    ) -> Result<(Value, Value)> {
        let mut last_error: Option<anyhow::Error> = None;
        for args in Self::build_candidate_args(&selection.schema, max_orders, lookback_days) {
            match server
                .call_tool(selection.name.as_str(), args.clone())
                .await
            {
                Ok(value) => return Ok((value, args)),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to call order-history MCP tool")))
    }

    fn parse_timestamp_from_value(value: &Value) -> Option<i64> {
        match value {
            Value::Number(number) => number.as_i64().map(|raw| {
                if raw > 1_000_000_000_000 {
                    raw / 1000
                } else {
                    raw
                }
            }),
            Value::String(s) => Self::parse_timestamp_string(s),
            _ => None,
        }
    }

    fn parse_timestamp_string(input: &str) -> Option<i64> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(raw) = trimmed.parse::<i64>() {
            return Some(if raw > 1_000_000_000_000 {
                raw / 1000
            } else {
                raw
            });
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
            return Some(dt.timestamp());
        }
        if let Ok(dt) = DateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%z") {
            return Some(dt.timestamp());
        }
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
            return Some(naive.and_utc().timestamp());
        }
        if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
            return date
                .and_hms_opt(0, 0, 0)
                .map(|naive| naive.and_utc().timestamp());
        }
        None
    }

    fn extract_object_timestamp(map: &Map<String, Value>) -> Option<i64> {
        let exact_keys = [
            "ordered_at",
            "order_time",
            "created_at",
            "delivered_at",
            "updated_at",
            "timestamp",
            "time",
            "date",
        ];
        for key in exact_keys {
            if let Some(value) = map.get(key) {
                if let Some(parsed) = Self::parse_timestamp_from_value(value) {
                    return Some(parsed);
                }
            }
        }

        // Swiggy payloads can use camelCase variants such as `createdAt`,
        // `orderDate`, `orderDateTime`, or `deliveryTime`.
        for (key, value) in map {
            let key_lc = key.to_ascii_lowercase();
            let is_candidate = key_lc.contains("timestamp")
                || key_lc.contains("date")
                || key_lc.contains("time")
                || key_lc.contains("created")
                || key_lc.contains("ordered")
                || key_lc.contains("delivered")
                || key_lc.contains("updated");
            if !is_candidate {
                continue;
            }
            if let Some(parsed) = Self::parse_timestamp_from_value(value) {
                return Some(parsed);
            }
            if let Some(obj) = value.as_object() {
                if let Some(parsed) = obj
                    .get("seconds")
                    .and_then(Self::parse_timestamp_from_value)
                    .or_else(|| obj.get("epoch").and_then(Self::parse_timestamp_from_value))
                    .or_else(|| obj.get("ms").and_then(Self::parse_timestamp_from_value))
                    .or_else(|| obj.get("millis").and_then(Self::parse_timestamp_from_value))
                {
                    return Some(parsed);
                }
            }
        }

        None
    }

    fn looks_like_item_context(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        lower.contains("item")
            || lower.contains("product")
            || lower.contains("line")
            || lower.contains("cart")
    }

    fn extract_item_name(map: &Map<String, Value>) -> Option<String> {
        let keys = ["item_name", "name", "product_name", "title", "variant_name"];
        for key in keys {
            let Some(value) = map.get(key) else {
                continue;
            };
            let Some(name) = value.as_str() else {
                continue;
            };
            let trimmed = name.trim();
            if trimmed.len() < 2 || trimmed.len() > 120 {
                continue;
            }
            if trimmed.eq_ignore_ascii_case("order") || trimmed.eq_ignore_ascii_case("instamart") {
                continue;
            }
            return Some(trimmed.to_string());
        }
        None
    }

    fn extract_quantity_hint(map: &Map<String, Value>) -> Option<String> {
        let keys = ["quantity", "qty", "count", "size", "weight", "unit"];
        for key in keys {
            let Some(value) = map.get(key) else {
                continue;
            };
            let hint = match value {
                Value::String(s) => s.trim().to_string(),
                Value::Number(n) => n.to_string(),
                _ => continue,
            };
            if !hint.is_empty() {
                return Some(hint);
            }
        }
        None
    }

    fn infer_freshness_profile(item_name: &str) -> Option<String> {
        let n = item_name.to_ascii_lowercase();
        if [
            "spinach",
            "coriander",
            "mint",
            "lettuce",
            "mushroom",
            "tomato",
            "cucumber",
            "banana",
            "berries",
            "strawberry",
            "blueberry",
            "watermelon",
            "paneer",
            "milk",
            "curd",
            "yogurt",
            "chicken",
            "fish",
            "prawn",
            "seafood",
            "bread",
        ]
        .iter()
        .any(|token| n.contains(token))
        {
            return Some("high".to_string());
        }
        if [
            "onion", "potato", "garlic", "carrot", "cabbage", "capsicum", "apple", "orange", "egg",
            "tofu",
        ]
        .iter()
        .any(|token| n.contains(token))
        {
            return Some("medium".to_string());
        }
        if ["sauce", "ketchup", "mayo", "chutney", "paste"]
            .iter()
            .any(|token| n.contains(token))
        {
            return Some("low".to_string());
        }
        if [
            "rice", "dal", "flour", "atta", "salt", "sugar", "spice", "masala", "oil",
        ]
        .iter()
        .any(|token| n.contains(token))
        {
            return Some("very_low".to_string());
        }
        None
    }

    fn is_non_food_item(item_name: &str) -> bool {
        let n = item_name.to_ascii_lowercase();
        [
            "razor",
            "tissue",
            "foil",
            "cloth",
            "cleaner",
            "detergent",
            "soap",
            "shampoo",
            "toothpaste",
            "deodorant",
            "sanitary",
            "napkin",
            "battery",
            "bulb",
            "trash bag",
            "garbage bag",
            "mop",
            "sponge",
            "air freshener",
            "insect",
            "repellent",
            "diaper",
        ]
        .iter()
        .any(|token| n.contains(token))
    }

    fn collect_embedded_json(value: &Value, out: &mut Vec<Value>) {
        match value {
            Value::Object(map) => {
                if let Some(content) = map.get("content").and_then(Value::as_array) {
                    for entry in content {
                        if let Some(text) = entry.get("text").and_then(Value::as_str) {
                            let trimmed = text.trim();
                            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                                if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
                                    out.push(parsed);
                                }
                            }
                        }
                    }
                }
                for child in map.values() {
                    Self::collect_embedded_json(child, out);
                }
            }
            Value::Array(items) => {
                for child in items {
                    Self::collect_embedded_json(child, out);
                }
            }
            _ => {}
        }
    }

    fn walk_candidates(
        value: &Value,
        parent_ts: Option<i64>,
        item_context: bool,
        out: &mut Vec<PantryCandidate>,
    ) {
        match value {
            Value::Object(map) => {
                let ts = Self::extract_object_timestamp(map).or(parent_ts);
                let has_item_hint = map.keys().any(|k| Self::looks_like_item_context(k));
                if item_context || has_item_hint {
                    if let Some(name) = Self::extract_item_name(map) {
                        out.push(PantryCandidate {
                            freshness_profile: Self::infer_freshness_profile(&name),
                            item_name: name,
                            quantity_hint: Self::extract_quantity_hint(map),
                            observed_at: ts,
                        });
                    }
                }
                for (key, child) in map {
                    let next_item_context =
                        item_context || has_item_hint || Self::looks_like_item_context(key);
                    Self::walk_candidates(child, ts, next_item_context, out);
                }
            }
            Value::Array(items) => {
                for child in items {
                    Self::walk_candidates(child, parent_ts, item_context, out);
                }
            }
            _ => {}
        }
    }

    fn extract_candidates(raw_result: &Value, max_items: usize) -> Vec<PantryCandidate> {
        let mut roots = vec![raw_result.clone()];
        Self::collect_embedded_json(raw_result, &mut roots);

        let mut extracted = Vec::new();
        for root in roots {
            Self::walk_candidates(&root, None, false, &mut extracted);
        }

        // Most recent first.
        extracted.sort_by_key(|item| std::cmp::Reverse(item.observed_at.unwrap_or(0)));

        let mut deduped = Vec::new();
        let mut seen = HashSet::new();
        for item in extracted {
            let norm = item.item_name.trim().to_ascii_lowercase();
            if norm.is_empty() || Self::is_non_food_item(item.item_name.as_str()) {
                continue;
            }
            if !seen.insert(norm) {
                continue;
            }
            deduped.push(item);
            if deduped.len() >= max_items {
                break;
            }
        }
        deduped
    }

    fn build_idempotency_key(user_id: &str, candidates: &[PantryCandidate]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(user_id.as_bytes());
        for item in candidates {
            hasher.update(item.item_name.to_ascii_lowercase().as_bytes());
            hasher.update(b"|");
            if let Some(ts) = item.observed_at {
                hasher.update(ts.to_string().as_bytes());
            }
            hasher.update(b"|");
            if let Some(quantity_hint) = &item.quantity_hint {
                hasher.update(quantity_hint.as_bytes());
            }
            hasher.update(b";");
        }
        let digest = hasher.finalize();
        format!("instamart-sync-{:x}", digest)
    }

    fn persist_candidates(
        &self,
        household_id: &str,
        candidates: &[PantryCandidate],
        user_id: &str,
    ) -> Result<Value> {
        if candidates.is_empty() {
            return Ok(serde_json::json!({"commit_status":"skipped_no_candidates"}));
        }
        let Some(source_message_id) = turn_context::current_turn_source_message_id() else {
            anyhow::bail!(
                "meal_instamart_sync requires runtime turn context to persist pantry hints."
            );
        };

        let operations: Vec<Value> = candidates
            .iter()
            .map(|item| {
                let mut op = BTreeMap::new();
                op.insert("op".to_string(), Value::String("record_pantry".to_string()));
                op.insert(
                    "item_name".to_string(),
                    Value::String(item.item_name.clone()),
                );
                op.insert(
                    "source".to_string(),
                    Value::String("instamart_order".to_string()),
                );
                if let Some(quantity_hint) = &item.quantity_hint {
                    op.insert(
                        "quantity_hint".to_string(),
                        Value::String(quantity_hint.clone()),
                    );
                }
                if let Some(observed_at) = item.observed_at {
                    op.insert("observed_at".to_string(), Value::from(observed_at));
                }
                if let Some(freshness_profile) = &item.freshness_profile {
                    op.insert(
                        "freshness_profile".to_string(),
                        Value::String(freshness_profile.clone()),
                    );
                }
                Value::Object(op.into_iter().collect())
            })
            .collect();

        let request = CommitRequest {
            household_id: household_id.to_string(),
            meal_date_local: Local::now().format("%Y-%m-%d").to_string(),
            meal_slot: "unslotted".to_string(),
            slot_instance: 1,
            operations,
            client_idempotency_key: Some(Self::build_idempotency_key(user_id, candidates)),
            source_message_id,
        };

        let commit = self.store.commit(request)?;
        Ok(serde_json::to_value(commit)?)
    }

    fn persist_sync_attempt(&self, household_id: &str, user_id: &str) {
        if let Err(error) = self.store.upsert_pantry_sync_state(
            household_id,
            INSTAMART_SYNC_SOURCE,
            Some(user_id),
            None,
            Some(chrono::Utc::now().timestamp()),
            None,
            None,
        ) {
            tracing::warn!(
                component = "meal_instamart_sync",
                household_id = household_id,
                user_id = user_id,
                error = %error,
                "failed to persist Instamart sync attempt state"
            );
        }
    }

    fn persist_sync_failure(&self, household_id: &str, user_id: &str, error_message: &str) {
        if let Err(error) = self.store.upsert_pantry_sync_state(
            household_id,
            INSTAMART_SYNC_SOURCE,
            Some(user_id),
            None,
            Some(chrono::Utc::now().timestamp()),
            None,
            Some(error_message),
        ) {
            tracing::warn!(
                component = "meal_instamart_sync",
                household_id = household_id,
                user_id = user_id,
                error = %error,
                "failed to persist Instamart sync failure state"
            );
        }
    }

    fn persist_sync_success(
        &self,
        household_id: &str,
        user_id: &str,
        call_args: &Value,
        candidates: &[PantryCandidate],
    ) {
        let max_observed_at = candidates.iter().filter_map(|item| item.observed_at).max();
        let cursor = serde_json::json!({
            "synced_at": chrono::Utc::now().timestamp(),
            "max_observed_at": max_observed_at,
            "args": call_args,
            "candidate_count": candidates.len()
        })
        .to_string();
        if let Err(error) = self.store.upsert_pantry_sync_state(
            household_id,
            INSTAMART_SYNC_SOURCE,
            Some(user_id),
            Some(chrono::Utc::now().timestamp()),
            Some(chrono::Utc::now().timestamp()),
            Some(cursor.as_str()),
            None,
        ) {
            tracing::warn!(
                component = "meal_instamart_sync",
                household_id = household_id,
                user_id = user_id,
                error = %error,
                "failed to persist Instamart sync success state"
            );
        }
    }
}

#[async_trait]
impl Tool for MealInstamartSyncTool {
    fn name(&self) -> &str {
        "meal_instamart_sync"
    }

    fn description(&self) -> &str {
        "Fetch recent Instamart order history for the active user via MCP, extract pantry candidates, and optionally persist pantry hints for meal recommendations."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "household_id": { "type": "string" },
                "user_id": { "type": "string" },
                "max_orders": { "type": "integer", "minimum": 1, "maximum": 100 },
                "max_items": { "type": "integer", "minimum": 1, "maximum": 100 },
                "lookback_days": { "type": "integer", "minimum": 1, "maximum": 180 },
                "persist_pantry": { "type": "boolean" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ToolResult> {
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

        let mut parsed: OrdersSyncArgs = match serde_json::from_value(args) {
            Ok(value) => value,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!(
                        "Invalid arguments for meal_instamart_sync: {error}"
                    )),
                });
            }
        };

        if parsed.household_id.trim().is_empty() {
            parsed.household_id = self.store.household_id().to_string();
        }
        parsed.max_orders = parsed.max_orders.clamp(1, 100);
        parsed.max_items = parsed.max_items.clamp(1, 100);
        parsed.lookback_days = parsed.lookback_days.clamp(1, 180);

        let household_id = parsed.household_id.trim();
        let Some(user_id) = self.resolve_user(household_id, parsed.user_id.as_str()) else {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some("Could not infer user identity. Provide `user_id`.".to_string()),
            });
        };
        self.persist_sync_attempt(household_id, user_id.as_str());

        let access_token = match self.load_access_token(household_id, user_id.as_str()).await {
            Ok(value) => value,
            Err(error) => {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    error.to_string().as_str(),
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(error.to_string()),
                });
            }
        };

        let server = match self.connect_mcp(access_token.as_str()).await {
            Ok(server) => server,
            Err(error) => {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    format!("Failed to connect to Instamart MCP: {error}").as_str(),
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!("Failed to connect to Instamart MCP: {error}")),
                });
            }
        };

        let available_tools: Vec<String> = server
            .tools()
            .await
            .into_iter()
            .map(|tool| tool.name)
            .collect();

        let selection = match self.select_order_tool(&server).await {
            Ok(selection) => selection,
            Err(error) => {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    format!("{error}").as_str(),
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!(
                        "{error}. Available Instamart MCP tools: {}",
                        available_tools.join(", ")
                    )),
                });
            }
        };

        let (raw_result, call_args) = match self
            .call_order_tool(&server, &selection, parsed.max_orders, parsed.lookback_days)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    format!(
                        "Failed to read Instamart order history via tool `{}`: {error}",
                        selection.name
                    )
                    .as_str(),
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!(
                        "Failed to read Instamart order history via tool `{}`: {error}",
                        selection.name
                    )),
                });
            }
        };

        let candidates = Self::extract_candidates(&raw_result, parsed.max_items);

        let commit = if parsed.persist_pantry {
            if !self.security.can_act() {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    "Security policy: read-only mode, cannot persist pantry hints.",
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(
                        "Security policy: read-only mode, cannot persist pantry hints.".to_string(),
                    ),
                });
            }
            if !self.security.record_action() {
                self.persist_sync_failure(
                    household_id,
                    user_id.as_str(),
                    "Rate limit exceeded: action budget exhausted.",
                );
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some("Rate limit exceeded: action budget exhausted.".to_string()),
                });
            }
            match self.persist_candidates(household_id, &candidates, user_id.as_str()) {
                Ok(commit) => Some(commit),
                Err(error) => {
                    self.persist_sync_failure(
                        household_id,
                        user_id.as_str(),
                        format!("Failed to persist pantry candidates: {error}").as_str(),
                    );
                    return Ok(ToolResult {
                        success: false,
                        output: String::new(),
                        error: Some(format!("Failed to persist pantry candidates: {error}")),
                    });
                }
            }
        } else {
            None
        };
        self.persist_sync_success(household_id, user_id.as_str(), &call_args, &candidates);

        let mut raw_preview =
            serde_json::to_string(&raw_result).unwrap_or_else(|_| "null".to_string());
        if raw_preview.len() > MAX_RAW_RESULT_CHARS {
            raw_preview.truncate(MAX_RAW_RESULT_CHARS);
            raw_preview.push_str("...<truncated>");
        }

        let mut output = serde_json::json!({
            "user_id": user_id,
            "selected_order_tool": selection.name,
            "available_instamart_tools": available_tools,
            "order_tool_args_used": call_args,
            "persist_pantry": parsed.persist_pantry,
            "pantry_candidates_count": candidates.len(),
            "pantry_candidates": candidates,
            "raw_result_preview": raw_preview,
        });
        if let Some(commit_value) = commit {
            output["commit"] = commit_value;
        }

        let mut summary = String::new();
        let _ = writeln!(
            summary,
            "Instamart sync completed for `{}`. Extracted {} pantry candidate(s).",
            user_id,
            candidates.len()
        );
        let _ = write!(summary, "{}", serde_json::to_string_pretty(&output)?);

        Ok(ToolResult {
            success: true,
            output: summary,
            error: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::security::SecurityPolicy;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn extract_candidates_from_embedded_text_json() {
        let payload = serde_json::json!({
            "content": [
                {
                    "type": "text",
                    "text": "{\"orders\":[{\"ordered_at\":\"2026-03-15T10:00:00Z\",\"items\":[{\"item_name\":\"Tomato\",\"quantity\":2},{\"item_name\":\"Soy Sauce\",\"quantity\":\"1 bottle\"}]}]}"
                }
            ]
        });
        let candidates = MealInstamartSyncTool::extract_candidates(&payload, 10);
        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .any(|c| c.item_name.eq_ignore_ascii_case("Tomato")));
        assert!(candidates
            .iter()
            .any(|c| c.item_name.eq_ignore_ascii_case("Soy Sauce")));
    }

    #[test]
    fn order_tool_scoring_prefers_order_history() {
        let top = MealInstamartSyncTool::order_tool_score(
            "get_order_history",
            Some("Fetch recent Instamart orders"),
        );
        let low = MealInstamartSyncTool::order_tool_score("search_catalog", Some("Search"));
        assert!(top > low);
    }

    #[test]
    fn extract_candidates_uses_order_date_from_camel_case_key() {
        let payload = serde_json::json!({
            "orders": [
                {
                    "orderDateTime": "2026-03-11T10:30:00Z",
                    "items": [
                        {"name":"Watermelon","quantity":"1"}
                    ]
                }
            ]
        });
        let candidates = MealInstamartSyncTool::extract_candidates(&payload, 10);
        let ts = candidates
            .iter()
            .find(|c| c.item_name.eq_ignore_ascii_case("Watermelon"))
            .and_then(|c| c.observed_at)
            .expect("watermelon observed_at should be extracted");
        assert_eq!(ts, 1_773_225_000);
    }

    #[test]
    fn extract_candidates_filters_non_food_items() {
        let payload = serde_json::json!({
            "orders": [
                {
                    "created_at": "2026-03-12T10:00:00Z",
                    "items": [
                        {"item_name":"Kitchen Tissue Roll","quantity":"1"},
                        {"item_name":"Tomato","quantity":"2"}
                    ]
                }
            ]
        });
        let candidates = MealInstamartSyncTool::extract_candidates(&payload, 10);
        assert!(candidates
            .iter()
            .any(|c| c.item_name.eq_ignore_ascii_case("Tomato")));
        assert!(!candidates
            .iter()
            .any(|c| c.item_name.to_ascii_lowercase().contains("tissue")));
    }

    #[tokio::test]
    async fn execute_without_user_id_is_rejected() {
        let tmp = TempDir::new().expect("tempdir");
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
        let config = Arc::new(config);
        let security = Arc::new(SecurityPolicy::from_config(
            &config.autonomy,
            &config.workspace_dir,
        ));
        let tool = MealInstamartSyncTool::new(config, security);

        let result = tool
            .execute(json!({
                "household_id": "household_test"
            }))
            .await
            .expect("tool call should return ToolResult");
        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("Could not infer user identity"));
    }
}
