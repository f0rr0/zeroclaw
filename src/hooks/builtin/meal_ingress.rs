use async_trait::async_trait;
use chrono::{Local, TimeZone};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::channels::traits::ChannelMessage;
use crate::config::Config;
use crate::hooks::traits::{HookHandler, HookResult};
use crate::meal::distillation::{
    inline_ingress_event, resolve_distillation_model, run_distillation_batch,
};
use crate::meal::store::{CommitRequest, MealStore};
use crate::providers::{self, Provider, ProviderRuntimeOptions};

/// Persists ambient context, selectively allows inbound turns, and guards outbound sends.
///
/// Behavior in non-mention Telegram groups:
/// 1. Persist every inbound message into meal ingress.
/// 2. Use an LLM-based reply gate to decide `respond` vs `ingest_only`.
/// 3. Only allow outbound sends for turns explicitly permitted by the gate.
pub struct MealIngressHook {
    config: Arc<Config>,
    store: MealStore,
    provider: Mutex<Option<Arc<dyn Provider>>>,
    vision_provider: Mutex<Option<Arc<dyn Provider>>>,
    bot_username: Mutex<Option<String>>,
}

fn elapsed_millis_u64(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl MealIngressHook {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            store: MealStore::new(config.clone()),
            config,
            provider: Mutex::new(None),
            vision_provider: Mutex::new(None),
            bot_username: Mutex::new(None),
        }
    }

    fn should_gate_message(&self, message: &ChannelMessage) -> bool {
        if !self.config.meal_agent.reply_gate_enabled {
            return false;
        }
        if message.channel != "telegram" {
            return false;
        }
        // Telegram groups/supergroups have negative chat IDs.
        let chat_id = message
            .reply_target
            .split_once(':')
            .map_or(message.reply_target.as_str(), |(chat, _)| chat);
        chat_id.starts_with('-')
    }

    async fn get_bot_username(&self) -> Option<String> {
        if let Some(username) = self.bot_username.lock().clone() {
            return Some(username);
        }
        let telegram = self.config.channels_config.telegram.as_ref()?;
        let url = format!("https://api.telegram.org/bot{}/getMe", telegram.bot_token);
        let resp = reqwest::Client::new().get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        let username = json
            .get("result")
            .and_then(|result| result.get("username"))
            .and_then(serde_json::Value::as_str)?
            .to_string();
        *self.bot_username.lock() = Some(username.clone());
        Some(username)
    }

    fn contains_bot_mention(content: &str, bot_username: &str) -> bool {
        let normalized_bot = format!(
            "@{}",
            bot_username.trim_start_matches('@').to_ascii_lowercase()
        );
        content.split_whitespace().any(|token| {
            let normalized = token
                .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '@' && c != '_')
                .to_ascii_lowercase();
            normalized == normalized_bot
        })
    }

    fn is_reply_to_bot_quote(content: &str, bot_username: &str) -> bool {
        let prefix = format!(
            "> @{}:",
            bot_username.trim_start_matches('@').to_ascii_lowercase()
        );
        content
            .lines()
            .next()
            .map(|line| {
                line.trim()
                    .to_ascii_lowercase()
                    .starts_with(prefix.as_str())
            })
            .unwrap_or(false)
    }

    async fn is_direct_address(&self, message: &ChannelMessage) -> bool {
        if let Some(bot_username) = self.get_bot_username().await {
            return Self::contains_bot_mention(message.content.as_str(), bot_username.as_str())
                || Self::is_reply_to_bot_quote(message.content.as_str(), bot_username.as_str());
        }
        false
    }

    fn persist_reply_gate_decision(
        &self,
        message: &ChannelMessage,
        decision: &str,
        confidence: Option<f64>,
        intent: Option<&str>,
        reason_code: &str,
        model: Option<&str>,
    ) {
        if let Err(error) = self.store.record_reply_gate_decision(
            message,
            decision,
            confidence,
            intent,
            reason_code,
            model,
        ) {
            tracing::warn!(
                hook = "meal-ingress",
                message_id = %message.id,
                error = %error,
                "failed to persist meal reply-gate decision"
            );
        }
    }

    fn persist_distill_gate_decision(
        &self,
        message: &ChannelMessage,
        decision: &str,
        confidence: Option<f64>,
        intent: Option<&str>,
        reason_code: &str,
        model: Option<&str>,
    ) {
        if let Err(error) = self.store.record_distill_gate_decision(
            message,
            decision,
            confidence,
            intent,
            reason_code,
            model,
        ) {
            tracing::warn!(
                hook = "meal-ingress",
                message_id = %message.id,
                error = %error,
                "failed to persist meal distill-gate decision"
            );
        }
    }

    fn get_or_create_provider(&self) -> anyhow::Result<Arc<dyn Provider>> {
        if let Some(existing) = self.provider.lock().clone() {
            return Ok(existing);
        }

        let provider_name = self
            .config
            .default_provider
            .clone()
            .unwrap_or_else(|| "openrouter".to_string());
        let options = ProviderRuntimeOptions {
            auth_profile_override: None,
            provider_api_url: self.config.api_url.clone(),
            zeroclaw_dir: self
                .config
                .config_path
                .parent()
                .map(|p| p.to_path_buf())
                .or_else(|| Some(PathBuf::from(".zeroclaw"))),
            secrets_encrypt: self.config.secrets.encrypt,
            reasoning_enabled: self.config.runtime.reasoning_enabled,
            provider_timeout_secs: Some(self.config.provider_timeout_secs),
            extra_headers: self.config.extra_headers.clone(),
            api_path: self.config.api_path.clone(),
        };
        let created = providers::create_provider_with_options(
            provider_name.as_str(),
            self.config.api_key.as_deref(),
            &options,
        )?;
        let created: Arc<dyn Provider> = Arc::from(created);
        *self.provider.lock() = Some(created.clone());
        Ok(created)
    }

    fn get_or_create_vision_provider(&self) -> anyhow::Result<Arc<dyn Provider>> {
        if let Some(existing) = self.vision_provider.lock().clone() {
            return Ok(existing);
        }

        let primary = self.get_or_create_provider()?;
        if primary.supports_vision() {
            *self.vision_provider.lock() = Some(primary.clone());
            return Ok(primary);
        }

        let options = ProviderRuntimeOptions {
            auth_profile_override: None,
            provider_api_url: self.config.api_url.clone(),
            zeroclaw_dir: self
                .config
                .config_path
                .parent()
                .map(|p| p.to_path_buf())
                .or_else(|| Some(PathBuf::from(".zeroclaw"))),
            secrets_encrypt: self.config.secrets.encrypt,
            reasoning_enabled: self.config.runtime.reasoning_enabled,
            provider_timeout_secs: Some(self.config.provider_timeout_secs),
            extra_headers: self.config.extra_headers.clone(),
            api_path: self.config.api_path.clone(),
        };
        let fallback = providers::create_provider_with_options(
            "openai-codex",
            self.config.api_key.as_deref(),
            &options,
        )?;
        let fallback: Arc<dyn Provider> = Arc::from(fallback);
        if !fallback.supports_vision() {
            anyhow::bail!("no vision-capable provider available for pantry image extraction");
        }
        *self.vision_provider.lock() = Some(fallback.clone());
        Ok(fallback)
    }

    fn spawn_inline_pantry_image_ingestion(&self, message: &ChannelMessage) {
        if message.content.trim_start().starts_with('/') {
            return;
        }
        if let Err(error) = self.store.enqueue_pantry_image_ingest(message) {
            tracing::warn!(
                hook = "meal-ingress",
                message_id = %message.id,
                error = %error,
                "failed to enqueue pantry image ingestion job"
            );
            return;
        }

        let provider = match self.get_or_create_vision_provider() {
            Ok(provider) => provider,
            Err(error) => {
                tracing::warn!(
                    hook = "meal-ingress",
                    message_id = %message.id,
                    error = %error,
                    "pantry image ingestion skipped: no vision provider"
                );
                return;
            }
        };
        let store = self.store.clone();
        let model = self
            .config
            .meal_agent
            .pantry_image_model
            .clone()
            .or_else(|| self.config.default_model.clone())
            .unwrap_or_else(|| "gpt-5".to_string());
        let multimodal_config = self.config.multimodal.clone();

        tokio::spawn(async move {
            let jobs = match store.claim_pending_pantry_image_jobs(4, 3) {
                Ok(jobs) => jobs,
                Err(error) => {
                    tracing::warn!(
                        hook = "meal-ingress",
                        error = %error,
                        "failed to claim pending pantry image ingestion jobs"
                    );
                    return;
                }
            };
            if jobs.is_empty() {
                return;
            }
            for job in jobs {
                let ingress = match store.ingress_event_by_message_id(
                    job.channel.as_str(),
                    job.source_message_id.as_str(),
                ) {
                    Ok(Some(event)) => event,
                    Ok(None) => {
                        let _ = store.mark_pantry_image_job_failed(
                            job.channel.as_str(),
                            job.source_message_id.as_str(),
                            Some("ingress_event_not_found"),
                        );
                        continue;
                    }
                    Err(error) => {
                        let _ = store.mark_pantry_image_job_pending(
                            job.channel.as_str(),
                            job.source_message_id.as_str(),
                            Some(format!("ingress_lookup_failed:{error}").as_str()),
                        );
                        continue;
                    }
                };
                let image_message = ChannelMessage {
                    id: ingress.message_id.clone(),
                    sender: ingress.sender.clone(),
                    reply_target: String::new(),
                    content: ingress.content.clone(),
                    channel: ingress.channel.clone(),
                    timestamp: u64::try_from(ingress.timestamp).unwrap_or_default(),
                    thread_ts: ingress.thread_ts.clone(),
                };
                match process_pantry_image_message(
                    &store,
                    provider.clone(),
                    model.as_str(),
                    &image_message,
                    &multimodal_config,
                )
                .await
                {
                    Ok(_) => {
                        let _ = store.mark_pantry_image_job_done(
                            job.channel.as_str(),
                            job.source_message_id.as_str(),
                        );
                    }
                    Err(error) => {
                        let should_fail = job.attempt_count.saturating_add(1) >= 3;
                        if should_fail {
                            let _ = store.mark_pantry_image_job_failed(
                                job.channel.as_str(),
                                job.source_message_id.as_str(),
                                Some(error.to_string().as_str()),
                            );
                        } else {
                            let _ = store.mark_pantry_image_job_pending(
                                job.channel.as_str(),
                                job.source_message_id.as_str(),
                                Some(error.to_string().as_str()),
                            );
                        }
                    }
                }
            }
        });
    }

    fn spawn_inline_distillation(&self, message: &ChannelMessage) {
        if message.content.trim().is_empty() || message.content.trim_start().starts_with('/') {
            return;
        }

        let provider = match self.get_or_create_provider() {
            Ok(provider) => provider,
            Err(error) => {
                tracing::warn!(
                    hook = "meal-ingress",
                    message_id = %message.id,
                    error = %error,
                    "inline distillation skipped: provider unavailable"
                );
                return;
            }
        };

        let store = self.store.clone();
        let message = message.clone();
        let model = resolve_distillation_model(self.config.as_ref());
        let min_confidence = self.config.meal_agent.distillation_min_confidence;

        tokio::spawn(async move {
            let ingress_created_at = store
                .ingress_created_at(store.household_id(), message.id.as_str())
                .ok()
                .flatten()
                .unwrap_or_else(|| chrono::Utc::now().timestamp());
            let event = inline_ingress_event(&message, ingress_created_at);
            match run_distillation_batch(
                provider,
                &store,
                model.as_str(),
                min_confidence,
                vec![event],
            )
            .await
            {
                Ok(outcome) => {
                    if outcome.applied_preferences > 0
                        || outcome.applied_episodes > 0
                        || outcome.applied_unresolved > 0
                    {
                        tracing::info!(
                            hook = "meal-ingress",
                            message_id = %message.id,
                            extracted_preferences = outcome.extracted_preferences,
                            applied_preferences = outcome.applied_preferences,
                            extracted_episodes = outcome.extracted_episodes,
                            applied_episodes = outcome.applied_episodes,
                            extracted_unresolved = outcome.extracted_unresolved,
                            applied_unresolved = outcome.applied_unresolved,
                            "inline distillation applied"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        hook = "meal-ingress",
                        message_id = %message.id,
                        error = %error,
                        "inline distillation failed"
                    );
                }
            }
        });
    }

    async fn classify_reply_decision(
        &self,
        message: &ChannelMessage,
    ) -> anyhow::Result<ReplyGateDecision> {
        let provider = self.get_or_create_provider()?;
        let provider_name = self.config.default_provider.as_deref().unwrap_or("unknown");
        let model = self
            .config
            .meal_agent
            .reply_gate_model
            .clone()
            .or_else(|| self.config.default_model.clone())
            .unwrap_or_else(|| "gpt-4.1-mini".to_string());
        let started = Instant::now();
        tracing::info!(
            component = "reply_gate",
            phase = "start",
            channel = %message.channel,
            message_id = %message.id,
            provider = provider_name,
            model = %model,
            "reply gate classification start"
        );

        let current_message_content = sanitize_control_plane_content(message.content.as_str());
        let recent_window = self
            .store
            .recent_ingress_window(
                self.store.household_id(),
                self.config.meal_agent.reply_gate_window_messages,
            )?
            .into_iter()
            .map(|event| {
                let sanitized = sanitize_control_plane_content(event.content.as_str());
                serde_json::json!({
                    "message_id": event.message_id,
                    "sender": event.sender,
                    "channel": event.channel,
                    "thread_ts": event.thread_ts,
                    "timestamp": event.timestamp,
                    "content_text": sanitized.text,
                    "image_count": sanitized.image_count
                })
            })
            .collect::<Vec<_>>();

        let classifier_system_prompt = "You are ReplyGateClassifier for a household assistant in a group chat.
Decide if the assistant should reply now or only ingest context.
Return JSON only with fields:
decision: \"respond\" | \"ingest_only\"
confidence: number (0..1)
intent: \"meal\" | \"feedback\" | \"clarification\" | \"general\" | \"other\"
reason_code: \"direct_request\" | \"meal_signal\" | \"feedback_signal\" | \"ambient_context\" | \"offtopic\" | \"uncertain\"
distill_decision: \"distill\" | \"skip\"
distill_confidence: number (0..1)
distill_reason_code: \"meal_fact_signal\" | \"feedback_fact_signal\" | \"no_meal_fact\" | \"uncertain\"
Policy:
1) Reply when the message appears to be directed to the assistant (question/request), including non-meal asks.
2) Reply when meal planning/feedback needs active assistance.
3) For ambient human-to-human context, choose ingest_only.
4) Distill when the message likely contains durable meal facts (preferences, constraints, what someone ate, pantry cues).
5) For chatter without durable meal facts, choose distill_decision=skip.
6) If uncertain, choose ingest_only with confidence <= 0.5 and distill_decision=skip.
Input note:
- `current_message.content_text` has image markers stripped.
- `current_message.image_count` indicates how many images were attached.";

        let user_payload = serde_json::json!({
            "current_message": {
                "id": message.id,
                "sender": message.sender,
                "channel": message.channel,
                "reply_target": message.reply_target,
                "thread_ts": message.thread_ts,
                "content_text": current_message_content.text,
                "image_count": current_message_content.image_count
            },
            "recent_window": recent_window,
            "household_id": self.store.household_id()
        });

        let raw = match provider
            .chat_with_system(
                Some(classifier_system_prompt),
                &user_payload.to_string(),
                model.as_str(),
                tool_model_temperature(model.as_str()),
            )
            .await
        {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(
                    component = "reply_gate",
                    phase = "result",
                    status = "error",
                    channel = %message.channel,
                    message_id = %message.id,
                    provider = provider_name,
                    model = %model,
                    duration_ms = elapsed_millis_u64(started),
                    error = %error,
                    "reply gate classification failed"
                );
                return Err(error);
            }
        };

        let Some(parsed) = parse_reply_gate_json(&raw) else {
            tracing::warn!(
                component = "reply_gate",
                phase = "result",
                status = "error",
                channel = %message.channel,
                message_id = %message.id,
                provider = provider_name,
                model = %model,
                duration_ms = elapsed_millis_u64(started),
                "reply gate classifier returned invalid JSON shape"
            );
            return Err(anyhow::anyhow!(
                "Reply gate classifier returned non-JSON or invalid shape: {raw}"
            ));
        };

        tracing::info!(
            component = "reply_gate",
            phase = "result",
            status = "ok",
            channel = %message.channel,
            message_id = %message.id,
            provider = provider_name,
            model = %model,
            duration_ms = elapsed_millis_u64(started),
            decision = %parsed.decision,
            confidence = parsed.confidence,
            intent = %parsed.intent,
            reason_code = %parsed.reason_code,
            distill = parsed.should_distill(),
            distill_confidence = parsed.distill_confidence.unwrap_or_default(),
            "reply gate classification completed"
        );

        Ok(parsed)
    }
}

struct SanitizedControlContent {
    text: String,
    image_count: usize,
}

fn sanitize_control_plane_content(content: &str) -> SanitizedControlContent {
    let (cleaned, image_refs) = crate::multimodal::parse_image_markers(content);
    let cleaned = cleaned.trim().to_string();
    let text = if cleaned.is_empty() && !image_refs.is_empty() {
        "[image attachment]".to_string()
    } else {
        cleaned
    };
    SanitizedControlContent {
        text,
        image_count: image_refs.len(),
    }
}

async fn normalize_image_markers_for_tool_call(
    content: &str,
    multimodal_config: &crate::config::MultimodalConfig,
) -> anyhow::Result<String> {
    let messages = vec![crate::providers::ChatMessage::user(content.to_string())];
    let prepared =
        crate::multimodal::prepare_messages_for_provider(messages.as_slice(), multimodal_config)
            .await?;
    Ok(prepared
        .messages
        .first()
        .map(|message| message.content.clone())
        .unwrap_or_else(|| content.to_string()))
}

async fn process_pantry_image_message(
    store: &MealStore,
    provider: Arc<dyn Provider>,
    model: &str,
    message: &ChannelMessage,
    multimodal_config: &crate::config::MultimodalConfig,
) -> anyhow::Result<usize> {
    let normalized_content =
        normalize_image_markers_for_tool_call(message.content.as_str(), multimodal_config).await?;
    let (_, image_refs) = crate::multimodal::parse_image_markers(normalized_content.as_str());
    if image_refs.is_empty() {
        return Ok(0);
    }
    let system_prompt = "You are PantryImageExtractor for a household meal assistant.
Return strict JSON only:
{
  \"image_kind\": \"pantry_photo\"|\"order_screenshot\"|\"other\",
  \"confidence\": 0.0,
  \"reason\": \"...\",
  \"merchant\": \"...|null\",
  \"order_id\": \"...|null\",
  \"order_timestamp_unix\": 0|null,
  \"items\": [
    {
      \"item_name\": \"...\",
      \"quantity_hint\": \"...|null\",
      \"freshness_profile\": \"high|medium|low|very_low|unknown\",
      \"observed_at_unix\": 0|null,
      \"confidence\": 0.0,
      \"is_food\": true|false
    }
  ]
}
Rules:
- Set image_kind=pantry_photo for fridge/pantry/shelf photos.
- Set image_kind=order_screenshot for grocery order history/order-detail screenshots from any app.
- Set image_kind=other for unrelated images.
- Extract only visible or clearly implied food items; do not invent unseen items.
- For order_screenshot, extract merchant/order_id/order_timestamp_unix when visible; otherwise null.
- For order_screenshot, set each item's observed_at_unix from the visible order date when possible.
- Set is_food=false for non-food products (detergent, soap, tissue, etc).
- Use unknown freshness_profile when unsure.";

    let user_payload = format!(
        "Message content with image markers:\n{}\n\nSender: {}\nChannel: {}\nMessage ID: {}",
        normalized_content, message.sender, message.channel, message.id
    );
    let raw = provider
        .chat_with_system(
            Some(system_prompt),
            user_payload.as_str(),
            model,
            tool_model_temperature(model),
        )
        .await?;
    let parsed = parse_pantry_image_json(raw.as_str())
        .ok_or_else(|| anyhow::anyhow!("pantry image extraction returned non-JSON output"))?;
    let image_kind = parsed.image_kind();
    if !parsed.is_pantry_relevant_kind() || parsed.confidence < 0.55 {
        tracing::info!(
            hook = "meal-ingress",
            message_id = %message.id,
            confidence = parsed.confidence,
            image_kind = image_kind.as_str(),
            reason = parsed.reason.as_deref().unwrap_or(""),
            "pantry image judged non-pantry or low-confidence; skipping commit"
        );
        return Ok(0);
    }

    let now = chrono::Utc::now().timestamp();
    let fallback_observed_at =
        i64::try_from(message.timestamp).unwrap_or_else(|_| chrono::Utc::now().timestamp());
    let source = if image_kind == "order_screenshot" {
        "order_screenshot"
    } else {
        "pantry_photo"
    };
    let order_timestamp = parsed
        .order_timestamp_unix
        .filter(|value| *value > 0)
        .map(|value| value.min(now));
    let order_id_key = parsed
        .order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let base_metadata = serde_json::json!({
        "image_kind": image_kind,
        "merchant": parsed.merchant,
        "order_id": parsed.order_id,
        "order_timestamp_unix": order_timestamp,
        "extractor": "pantry_image_v2"
    });
    let mut operations = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for item in parsed.items {
        let canonical = item.item_name.trim().to_ascii_lowercase();
        if canonical.is_empty() || !item.is_food || item.confidence < 0.55 {
            continue;
        }
        let observed_at = item
            .observed_at_unix
            .filter(|value| *value > 0)
            .map(|value| value.min(now))
            .or(order_timestamp)
            .unwrap_or(fallback_observed_at);
        let dedupe_key = format!("{canonical}|{source}|{observed_at}|{order_id_key}");
        if !seen.insert(dedupe_key) {
            continue;
        }
        let mut op = serde_json::Map::new();
        op.insert(
            "op".to_string(),
            serde_json::Value::String("record_pantry".to_string()),
        );
        op.insert(
            "item_name".to_string(),
            serde_json::Value::String(item.item_name.trim().to_string()),
        );
        op.insert(
            "source".to_string(),
            serde_json::Value::String(source.to_string()),
        );
        op.insert(
            "source_user_id".to_string(),
            serde_json::Value::String(message.sender.clone()),
        );
        op.insert(
            "confidence".to_string(),
            serde_json::Value::from(item.confidence.clamp(0.0, 1.0)),
        );
        op.insert(
            "observed_at".to_string(),
            serde_json::Value::from(observed_at),
        );
        op.insert("metadata".to_string(), base_metadata.clone());
        if let Some(quantity_hint) = item
            .quantity_hint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            op.insert(
                "quantity_hint".to_string(),
                serde_json::Value::String(quantity_hint.to_string()),
            );
        }
        if let Some(profile) = item
            .freshness_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            op.insert(
                "freshness_profile".to_string(),
                serde_json::Value::String(profile.to_string()),
            );
        }
        operations.push(serde_json::Value::Object(op));
        if operations.len() >= 30 {
            break;
        }
    }
    if operations.is_empty() {
        return Ok(0);
    }

    let meal_date_local = Local
        .timestamp_opt(fallback_observed_at, 0)
        .single()
        .unwrap_or_else(Local::now)
        .format("%Y-%m-%d")
        .to_string();
    let commit_request = CommitRequest {
        household_id: store.household_id().to_string(),
        meal_date_local,
        meal_slot: "unslotted".to_string(),
        slot_instance: 1,
        operations,
        client_idempotency_key: Some(format!("pantry-image:{}", message.id)),
        source_message_id: format!("{}:pantry_image_extract", message.id),
    };
    let response = store.commit(commit_request)?;
    tracing::info!(
        hook = "meal-ingress",
        message_id = %message.id,
        commit_status = %response.commit_status,
        op_count = response.operation_results.len(),
        "pantry image ingestion commit applied"
    );
    Ok(response.operation_results.len())
}

#[async_trait]
impl HookHandler for MealIngressHook {
    fn name(&self) -> &str {
        "meal-ingress"
    }

    fn priority(&self) -> i32 {
        90
    }

    async fn on_message_received(&self, message: ChannelMessage) -> HookResult<ChannelMessage> {
        if !self.store.enabled() {
            return HookResult::Continue(message);
        }

        if let Err(error) = self.store.record_ingress(&message) {
            tracing::warn!(
                hook = "meal-ingress",
                message_id = %message.id,
                channel = %message.channel,
                error = %error,
                "failed to persist meal ingress message"
            );
        }

        if !self.should_gate_message(&message) {
            self.spawn_inline_pantry_image_ingestion(&message);
            self.spawn_inline_distillation(&message);
            self.persist_reply_gate_decision(
                &message,
                "respond",
                Some(1.0),
                Some("other"),
                "non_gated_channel",
                None,
            );
            self.persist_distill_gate_decision(
                &message,
                "distill",
                Some(1.0),
                Some("other"),
                "non_gated_channel",
                None,
            );
            return HookResult::Continue(message);
        }

        if message.content.trim_start().starts_with('/')
            || crate::channels::parse_runtime_command(
                message.channel.as_str(),
                message.content.as_str(),
            )
            .is_some()
        {
            self.spawn_inline_pantry_image_ingestion(&message);
            self.persist_reply_gate_decision(
                &message,
                "respond",
                Some(1.0),
                Some("other"),
                "runtime_command",
                None,
            );
            self.persist_distill_gate_decision(
                &message,
                "skip",
                Some(1.0),
                Some("other"),
                "runtime_command",
                None,
            );
            return HookResult::Continue(message);
        }

        if self.is_direct_address(&message).await {
            self.spawn_inline_pantry_image_ingestion(&message);
            self.spawn_inline_distillation(&message);
            self.persist_reply_gate_decision(
                &message,
                "respond",
                Some(1.0),
                Some("clarification"),
                "direct_address",
                None,
            );
            self.persist_distill_gate_decision(
                &message,
                "distill",
                Some(1.0),
                Some("clarification"),
                "direct_address",
                None,
            );
            return HookResult::Continue(message);
        }

        match self.classify_reply_decision(&message).await {
            Ok(decision) => {
                let allow = decision.decision == "respond"
                    && decision.confidence >= self.config.meal_agent.reply_gate_min_confidence;
                self.persist_reply_gate_decision(
                    &message,
                    if allow { "respond" } else { "ingest_only" },
                    Some(decision.confidence),
                    Some(decision.intent.as_str()),
                    decision.reason_code.as_str(),
                    self.config
                        .meal_agent
                        .reply_gate_model
                        .as_deref()
                        .or(self.config.default_model.as_deref()),
                );
                let distill_decision = if decision.should_distill() {
                    "distill"
                } else {
                    "skip"
                };
                self.persist_distill_gate_decision(
                    &message,
                    distill_decision,
                    decision.distill_confidence,
                    Some(decision.intent.as_str()),
                    decision
                        .distill_reason_code
                        .as_deref()
                        .unwrap_or(decision.reason_code.as_str()),
                    self.config
                        .meal_agent
                        .reply_gate_model
                        .as_deref()
                        .or(self.config.default_model.as_deref()),
                );
                tracing::info!(
                    hook = "meal-ingress",
                    message_id = %message.id,
                    decision = %decision.decision,
                    confidence = decision.confidence,
                    intent = %decision.intent,
                    reason_code = %decision.reason_code,
                    distill = decision.should_distill(),
                    distill_confidence = decision.distill_confidence.unwrap_or_default(),
                    allow,
                    "meal reply-gate decision"
                );
                self.spawn_inline_pantry_image_ingestion(&message);
                if decision.should_distill() {
                    self.spawn_inline_distillation(&message);
                }
                if allow {
                    HookResult::Continue(message)
                } else {
                    HookResult::Cancel("meal reply-gate: ingest_only".to_string())
                }
            }
            Err(error) => {
                tracing::warn!(
                    hook = "meal-ingress",
                    message_id = %message.id,
                    error = %error,
                    "meal reply-gate classifier failed; fail-closed to ingest_only"
                );
                self.persist_reply_gate_decision(
                    &message,
                    "ingest_only",
                    None,
                    Some("other"),
                    "classifier_failure",
                    self.config
                        .meal_agent
                        .reply_gate_model
                        .as_deref()
                        .or(self.config.default_model.as_deref()),
                );
                self.spawn_inline_pantry_image_ingestion(&message);
                self.persist_distill_gate_decision(
                    &message,
                    "skip",
                    None,
                    Some("other"),
                    "classifier_failure_fallback",
                    self.config
                        .meal_agent
                        .reply_gate_model
                        .as_deref()
                        .or(self.config.default_model.as_deref()),
                );
                HookResult::Cancel("meal reply-gate: classifier_failure".to_string())
            }
        }
    }

    async fn on_message_sending(
        &self,
        source_message_id: Option<String>,
        channel: String,
        recipient: String,
        content: String,
    ) -> HookResult<(String, String, String)> {
        if !self.store.enabled() {
            return HookResult::Continue((channel, recipient, content));
        }

        if !self.config.meal_agent.reply_gate_enabled {
            return HookResult::Continue((channel, recipient, content));
        }

        if channel != "telegram" {
            return HookResult::Continue((channel, recipient, content));
        }

        let chat_id = recipient
            .split_once(':')
            .map_or(recipient.as_str(), |(chat, _)| chat);
        if !chat_id.starts_with('-') {
            return HookResult::Continue((channel, recipient, content));
        }

        let Some(message_id) = source_message_id else {
            return HookResult::Cancel(
                "meal outbound guard: missing source_message_id".to_string(),
            );
        };

        match self
            .store
            .reply_gate_allows(channel.as_str(), message_id.as_str())
        {
            Ok(true) => HookResult::Continue((channel, recipient, content)),
            Ok(false) => {
                HookResult::Cancel("meal outbound guard: no permit for source message".to_string())
            }
            Err(error) => {
                tracing::warn!(
                    hook = "meal-ingress",
                    source_message_id = %message_id,
                    channel = %channel,
                    error = %error,
                    "meal outbound guard lookup failed; blocking send"
                );
                HookResult::Cancel("meal outbound guard: lookup failure".to_string())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ReplyGateDecision {
    decision: String,
    confidence: f64,
    intent: String,
    reason_code: String,
    #[serde(default = "default_distill_decision")]
    distill_decision: String,
    #[serde(default)]
    distill_confidence: Option<f64>,
    #[serde(default)]
    distill_reason_code: Option<String>,
}

impl ReplyGateDecision {
    fn should_distill(&self) -> bool {
        self.distill_decision.eq_ignore_ascii_case("distill")
    }
}

fn parse_reply_gate_json(raw: &str) -> Option<ReplyGateDecision> {
    serde_json::from_str::<ReplyGateDecision>(raw)
        .ok()
        .or_else(|| {
            let start = raw.find('{')?;
            let end = raw.rfind('}')?;
            if end < start {
                return None;
            }
            serde_json::from_str::<ReplyGateDecision>(&raw[start..=end]).ok()
        })
}

fn default_distill_decision() -> String {
    "skip".to_string()
}

fn tool_model_temperature(model: &str) -> f64 {
    let normalized = model
        .rsplit('/')
        .next()
        .unwrap_or(model)
        .trim()
        .to_ascii_lowercase();
    if normalized.starts_with("gpt-5") {
        1.0
    } else {
        0.0
    }
}

#[derive(Debug, Deserialize)]
struct PantryImageExtraction {
    #[serde(default)]
    image_kind: Option<String>,
    #[serde(default)]
    is_pantry_relevant: Option<bool>,
    confidence: f64,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    merchant: Option<String>,
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    order_timestamp_unix: Option<i64>,
    #[serde(default)]
    items: Vec<PantryImageItem>,
}

impl PantryImageExtraction {
    fn image_kind(&self) -> String {
        if let Some(kind) = self
            .image_kind
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let normalized = kind.to_ascii_lowercase();
            if normalized == "order_screenshot"
                || (normalized.contains("order") && normalized.contains("screen"))
            {
                return "order_screenshot".to_string();
            }
            if normalized == "pantry_photo"
                || normalized.contains("pantry")
                || normalized.contains("fridge")
                || normalized.contains("shelf")
                || normalized.contains("photo")
            {
                return "pantry_photo".to_string();
            }
            if normalized == "other" {
                return "other".to_string();
            }
        }
        if self.is_pantry_relevant.unwrap_or(false) {
            return "pantry_photo".to_string();
        }
        "other".to_string()
    }

    fn is_pantry_relevant_kind(&self) -> bool {
        matches!(
            self.image_kind().as_str(),
            "pantry_photo" | "order_screenshot"
        )
    }
}

fn default_is_food_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct PantryImageItem {
    item_name: String,
    #[serde(default)]
    quantity_hint: Option<String>,
    #[serde(default)]
    freshness_profile: Option<String>,
    #[serde(default)]
    observed_at_unix: Option<i64>,
    confidence: f64,
    #[serde(default = "default_is_food_true")]
    is_food: bool,
}

fn parse_pantry_image_json(raw: &str) -> Option<PantryImageExtraction> {
    serde_json::from_str::<PantryImageExtraction>(raw)
        .ok()
        .or_else(|| {
            let start = raw.find('{')?;
            let end = raw.rfind('}')?;
            if end < start {
                return None;
            }
            serde_json::from_str::<PantryImageExtraction>(&raw[start..=end]).ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_hook(reply_gate_enabled: bool) -> MealIngressHook {
        let mut config = Config::default();
        config.meal_agent.enabled = true;
        config.meal_agent.reply_gate_enabled = reply_gate_enabled;
        config.meal_agent.db_path = format!(
            "state/meal/test-{}.db",
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        MealIngressHook::new(Arc::new(config))
    }

    fn group_message(message_id: &str, content: &str) -> ChannelMessage {
        ChannelMessage {
            id: message_id.to_string(),
            sender: "alice".to_string(),
            reply_target: "-10012345".to_string(),
            content: content.to_string(),
            channel: "telegram".to_string(),
            timestamp: 0,
            thread_ts: None,
        }
    }

    #[tokio::test]
    async fn reply_gate_disabled_allows_inbound_and_outbound_once() {
        let hook = build_test_hook(false);
        let inbound = group_message("telegram_-10012345_1", "hello");

        let allowed = hook.on_message_received(inbound).await;
        assert!(matches!(allowed, HookResult::Continue(_)));

        let outbound = hook
            .on_message_sending(
                Some("telegram_-10012345_1".to_string()),
                "telegram".to_string(),
                "-10012345".to_string(),
                "reply".to_string(),
            )
            .await;
        assert!(matches!(outbound, HookResult::Continue(_)));
    }

    #[tokio::test]
    async fn outbound_guard_blocks_without_gate_decision() {
        let hook = build_test_hook(true);

        let outbound = hook
            .on_message_sending(
                Some("telegram_-10012345_2".to_string()),
                "telegram".to_string(),
                "-10012345".to_string(),
                "reply".to_string(),
            )
            .await;
        assert!(matches!(outbound, HookResult::Cancel(_)));
    }

    #[tokio::test]
    async fn explicit_bot_tag_bypasses_classifier_and_allows_reply() {
        let hook = build_test_hook(true);
        *hook.bot_username.lock() = Some("mealbot".to_string());
        let inbound = group_message("telegram_-10012345_3", "@mealbot recommend dinner");

        let allowed = hook.on_message_received(inbound).await;
        assert!(matches!(allowed, HookResult::Continue(_)));

        let outbound = hook
            .on_message_sending(
                Some("telegram_-10012345_3".to_string()),
                "telegram".to_string(),
                "-10012345".to_string(),
                "reply".to_string(),
            )
            .await;
        assert!(matches!(outbound, HookResult::Continue(_)));
    }

    #[test]
    fn sanitize_control_plane_content_strips_image_marker_and_keeps_text() {
        let content = "[IMAGE:/tmp/pantry.jpg]\n\nWhat can I cook with this?";
        let sanitized = sanitize_control_plane_content(content);
        assert_eq!(sanitized.image_count, 1);
        assert_eq!(sanitized.text, "What can I cook with this?");
        assert!(!sanitized.text.contains("[IMAGE:"));
    }

    #[test]
    fn sanitize_control_plane_content_uses_placeholder_for_image_only() {
        let sanitized = sanitize_control_plane_content("[IMAGE:/tmp/pantry.jpg]");
        assert_eq!(sanitized.image_count, 1);
        assert_eq!(sanitized.text, "[image attachment]");
    }

    #[tokio::test]
    async fn normalize_image_markers_for_tool_call_converts_local_paths_to_data_uri() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image_path = temp.path().join("sample.png");
        std::fs::write(
            &image_path,
            [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'],
        )
        .expect("write image");

        let input = format!(
            "[IMAGE:{}]\n\nI also ordered these",
            image_path.to_string_lossy()
        );
        let normalized = normalize_image_markers_for_tool_call(
            input.as_str(),
            &crate::config::MultimodalConfig::default(),
        )
        .await
        .expect("normalize image markers");
        let (cleaned, refs) = crate::multimodal::parse_image_markers(normalized.as_str());

        assert_eq!(cleaned, "I also ordered these");
        assert_eq!(refs.len(), 1);
        assert!(refs[0].starts_with("data:image/png;base64,"));
    }

    #[test]
    fn parse_pantry_image_json_supports_order_screenshot_schema() {
        let raw = r#"{
          "image_kind":"order_screenshot",
          "confidence":0.91,
          "reason":"order ui",
          "merchant":"Instamart",
          "order_id":"ORD-12",
          "order_timestamp_unix":1773601000,
          "items":[
            {
              "item_name":"Tomato",
              "quantity_hint":"1 kg",
              "freshness_profile":"high",
              "observed_at_unix":1773601000,
              "confidence":0.88,
              "is_food":true
            }
          ]
        }"#;
        let parsed = parse_pantry_image_json(raw).expect("must parse new schema");
        assert_eq!(parsed.image_kind(), "order_screenshot");
        assert!(parsed.is_pantry_relevant_kind());
        assert_eq!(parsed.items.len(), 1);
    }

    #[test]
    fn parse_pantry_image_json_backwards_compatible_old_schema() {
        let raw = r#"{
          "is_pantry_relevant": true,
          "confidence": 0.84,
          "reason": "fridge shelf",
          "items":[{"item_name":"Onion","confidence":0.9}]
        }"#;
        let parsed = parse_pantry_image_json(raw).expect("must parse old schema");
        assert_eq!(parsed.image_kind(), "pantry_photo");
        assert!(parsed.is_pantry_relevant_kind());
        assert!(
            parsed.items[0].is_food,
            "is_food defaults true when omitted to avoid dropping valid items"
        );
    }

    #[tokio::test]
    async fn missing_bot_username_does_not_treat_any_at_prefix_as_direct_address() {
        let hook = build_test_hook(true);
        let inbound = group_message("telegram_-10012345_4", "@alice should we eat poha?");
        let direct = hook.is_direct_address(&inbound).await;
        assert!(
            !direct,
            "without bot username, generic @mentions should not bypass classifier"
        );
    }
}
