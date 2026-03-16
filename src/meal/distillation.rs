use anyhow::{Context, Result};
use chrono::{Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::channels::traits::ChannelMessage;
use crate::config::Config;
use crate::meal::identity_resolution::resolve_distilled_user_id;
use crate::meal::store::{
    DistillationIngressEvent, EpisodeEvidenceRecord, MealStore, PreferenceEvidenceRecord,
    UnresolvedClaimRecord,
};
use crate::providers::Provider;
use crate::util::truncate_with_ellipsis;

const DISTILLER_SYSTEM_PROMPT: &str = "You are MealDistiller for a household meal assistant.
Extract durable food-preference and meal-episode evidence from chat messages.
Return strict JSON:
{
  \"evidence\":[{\"source_message_id\":\"...\",\"user_id\":\"...\",\"pref_key\":\"...\",\"pref_value\":\"...\",\"confidence\":0.0,\"evidence_text\":\"...\"}],
  \"episodes\":[{\"source_message_id\":\"...\",\"user_id\":\"...\",\"meal_name\":\"...\",\"meal_slot\":\"breakfast|lunch|dinner|snack|unslotted\",\"meal_date_local\":\"YYYY-MM-DD\",\"tags\":[\"...\"],\"confidence\":0.0,\"evidence_text\":\"...\"}],
  \"unresolved\":[{\"source_message_id\":\"...\",\"claim_kind\":\"preference_identity|episode_identity\",\"subject_ref\":\"...\",\"question\":\"...\",\"claim_text\":\"...\"}]
}
Rules:
- Only include explicit or high-confidence preferences relevant to food/meals.
- Include episode entries when users state what they ate/had (e.g. \"I had poha for breakfast\", \"we ate chicken stir fry\").
- Ignore uncertain/noisy chatter.
- Use concise stable keys like: likes, dislikes, spice_level, garnish_preference, ingredient_preference, dietary_preference.
- Resolve `user_id` against `known_user_ids` and message `sender` identities from input.
- Prefer exact known IDs. If a message uses a natural-language name that maps to a known participant, emit the canonical known ID.
- If user identity is ambiguous, do not emit evidence for that item.
- If user identity is ambiguous but claim is meal-relevant, add one unresolved item with a concise clarification question.
- confidence must be in [0,1].
Input note:
- Use `messages[].content_text` as the textual content.
- `messages[].image_count` only indicates image attachment count.";

const DEFAULT_KNOWN_PARTICIPANTS_LIMIT: u32 = 24;

#[derive(Debug, Clone, Default)]
pub struct DistillationRunOutcome {
    pub extracted_preferences: usize,
    pub extracted_episodes: usize,
    pub extracted_unresolved: usize,
    pub applied_preferences: usize,
    pub applied_episodes: usize,
    pub applied_unresolved: usize,
}

#[derive(Debug, Deserialize)]
struct DistillerOutput {
    #[serde(default)]
    evidence: Vec<PreferenceEvidenceRecord>,
    #[serde(default)]
    episodes: Vec<EpisodeEvidenceRecord>,
    #[serde(default)]
    unresolved: Vec<UnresolvedClaimRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DistillerMessageInput {
    pub message_id: String,
    pub sender: String,
    pub content_text: String,
    pub image_count: usize,
    pub channel: String,
    pub thread_ts: Option<String>,
    pub created_at: i64,
}

#[derive(Debug)]
pub(crate) struct SanitizedDistillationContent {
    pub text: String,
    pub image_count: usize,
}

pub fn resolve_distillation_model(config: &Config) -> String {
    config
        .meal_agent
        .distillation_model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .or_else(|| config.default_model.clone())
        .unwrap_or_else(|| "gpt-4.1-mini".to_string())
}

pub fn model_temperature(model: &str) -> f64 {
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

pub(crate) fn sanitize_message_content(content: &str) -> SanitizedDistillationContent {
    let (cleaned, image_refs) = crate::multimodal::parse_image_markers(content);
    let cleaned = cleaned.trim().to_string();
    let text = if cleaned.is_empty() && !image_refs.is_empty() {
        "[image attachment]".to_string()
    } else {
        cleaned
    };
    SanitizedDistillationContent {
        text,
        image_count: image_refs.len(),
    }
}

pub(crate) fn distiller_message_input(message: DistillationIngressEvent) -> DistillerMessageInput {
    let sanitized = sanitize_message_content(message.content.as_str());
    DistillerMessageInput {
        message_id: message.message_id,
        sender: message.sender,
        content_text: sanitized.text,
        image_count: sanitized.image_count,
        channel: message.channel,
        thread_ts: message.thread_ts,
        created_at: message.created_at,
    }
}

pub fn inline_ingress_event(
    message: &ChannelMessage,
    ingress_created_at: i64,
) -> DistillationIngressEvent {
    DistillationIngressEvent {
        message_id: message.id.clone(),
        sender: message.sender.clone(),
        content: message.content.clone(),
        channel: message.channel.clone(),
        thread_ts: message.thread_ts.clone(),
        created_at: ingress_created_at.max(0),
    }
}

pub async fn run_distillation_batch(
    provider: Arc<dyn Provider>,
    store: &MealStore,
    model: &str,
    min_confidence: f64,
    ingress_events: Vec<DistillationIngressEvent>,
) -> Result<DistillationRunOutcome> {
    if ingress_events.is_empty() {
        return Ok(DistillationRunOutcome::default());
    }

    let known_participants = store
        .known_participants(store.household_id(), DEFAULT_KNOWN_PARTICIPANTS_LIMIT)
        .unwrap_or_default();
    let cursor_created_at = ingress_events
        .iter()
        .map(|row| row.created_at)
        .max()
        .unwrap_or_default();
    let created_at_by_message_id: HashMap<String, i64> = ingress_events
        .iter()
        .map(|row| (row.message_id.clone(), row.created_at))
        .collect();
    let messages = ingress_events
        .into_iter()
        .map(distiller_message_input)
        .collect::<Vec<_>>();
    let sender_by_message_id: HashMap<String, String> = messages
        .iter()
        .map(|message| (message.message_id.clone(), message.sender.clone()))
        .collect();

    let payload = serde_json::json!({
        "household_id": store.household_id(),
        "known_user_ids": known_participants.clone(),
        "messages": messages
    });
    let payload_json = payload.to_string();
    let raw = provider
        .chat_with_system(
            Some(DISTILLER_SYSTEM_PROMPT),
            payload_json.as_str(),
            model,
            model_temperature(model),
        )
        .await
        .context("distillation model call failed")?;
    let parsed = parse_distiller_output(raw.as_str()).ok_or_else(|| {
        anyhow::anyhow!(
            "distillation response parse failed (preview={})",
            truncate_with_ellipsis(raw.as_str(), 240)
        )
    })?;

    let accepted_preferences: Vec<PreferenceEvidenceRecord> = parsed
        .evidence
        .into_iter()
        .filter_map(|mut entry| {
            if entry.confidence < min_confidence
                || entry.pref_key.trim().is_empty()
                || entry.pref_value.trim().is_empty()
                || entry.source_message_id.trim().is_empty()
            {
                return None;
            }
            entry.source_message_id = entry.source_message_id.trim().to_string();
            let source_sender = sender_by_message_id
                .get(entry.source_message_id.as_str())
                .map(String::as_str);
            let resolved_user = resolve_distilled_user_id(
                entry.user_id.as_str(),
                source_sender,
                known_participants.as_slice(),
            )?;
            entry.user_id = resolved_user;
            Some(entry)
        })
        .collect();

    let accepted_episodes: Vec<EpisodeEvidenceRecord> = parsed
        .episodes
        .into_iter()
        .filter_map(|mut entry| {
            if entry.confidence < min_confidence
                || entry.source_message_id.trim().is_empty()
                || entry.user_id.trim().is_empty()
                || entry.meal_name.trim().is_empty()
            {
                return None;
            }
            let source_message_id = entry.source_message_id.trim().to_string();
            entry.meal_date_local = Some(
                created_at_by_message_id
                    .get(source_message_id.as_str())
                    .map(|created_at| local_date_from_unix(*created_at))
                    .unwrap_or_else(|| Local::now().format("%Y-%m-%d").to_string()),
            );
            entry.meal_slot = Some(normalize_episode_slot(entry.meal_slot.as_deref()));
            entry.source_message_id = source_message_id;
            let source_sender = sender_by_message_id
                .get(entry.source_message_id.as_str())
                .map(String::as_str);
            entry.user_id = resolve_distilled_user_id(
                entry.user_id.trim(),
                source_sender,
                known_participants.as_slice(),
            )?;
            entry.meal_name = entry.meal_name.trim().to_string();
            entry.tags = entry
                .tags
                .into_iter()
                .map(|tag| tag.trim().to_string())
                .filter(|tag| !tag.is_empty())
                .collect();
            Some(entry)
        })
        .collect();

    let unresolved_claims: Vec<UnresolvedClaimRecord> = parsed
        .unresolved
        .into_iter()
        .filter_map(|mut claim| {
            if claim.source_message_id.trim().is_empty()
                || claim.claim_kind.trim().is_empty()
                || claim.subject_ref.trim().is_empty()
                || claim.question.trim().is_empty()
            {
                return None;
            }
            let source_sender = sender_by_message_id
                .get(claim.source_message_id.trim())
                .map(String::as_str);
            if resolve_distilled_user_id(
                claim.subject_ref.as_str(),
                source_sender,
                known_participants.as_slice(),
            )
            .is_some()
            {
                return None;
            }
            claim.source_message_id = claim.source_message_id.trim().to_string();
            claim.claim_kind = claim.claim_kind.trim().to_ascii_lowercase();
            claim.subject_ref = claim.subject_ref.trim().to_string();
            claim.question = claim.question.trim().to_string();
            claim.claim_text = claim
                .claim_text
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string);
            Some(claim)
        })
        .collect();

    let pref_result =
        store.apply_preference_evidence(accepted_preferences.as_slice(), cursor_created_at);
    let episode_result =
        store.apply_episode_evidence(accepted_episodes.as_slice(), cursor_created_at);
    let unresolved_result = store.apply_unresolved_claims(unresolved_claims.as_slice());

    let mut errors = Vec::new();
    let applied_preferences = match pref_result {
        Ok(count) => count,
        Err(error) => {
            errors.push(format!("apply_preference_evidence: {error}"));
            0
        }
    };
    let applied_episodes = match episode_result {
        Ok(count) => count,
        Err(error) => {
            errors.push(format!("apply_episode_evidence: {error}"));
            0
        }
    };
    let applied_unresolved = match unresolved_result {
        Ok(count) => count,
        Err(error) => {
            errors.push(format!("apply_unresolved_claims: {error}"));
            0
        }
    };
    if !errors.is_empty() {
        anyhow::bail!("distillation apply failed: {}", errors.join("; "));
    }

    Ok(DistillationRunOutcome {
        extracted_preferences: accepted_preferences.len(),
        extracted_episodes: accepted_episodes.len(),
        extracted_unresolved: unresolved_claims.len(),
        applied_preferences,
        applied_episodes,
        applied_unresolved,
    })
}

fn parse_distiller_output(raw: &str) -> Option<DistillerOutput> {
    serde_json::from_str::<DistillerOutput>(raw)
        .ok()
        .or_else(|| {
            let start = raw.find('{')?;
            let end = raw.rfind('}')?;
            if end < start {
                return None;
            }
            serde_json::from_str::<DistillerOutput>(&raw[start..=end]).ok()
        })
}

fn normalize_episode_slot(slot: Option<&str>) -> String {
    let value = slot
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .unwrap_or("unslotted")
        .to_ascii_lowercase();
    match value.as_str() {
        "breakfast" | "lunch" | "dinner" | "snack" | "unslotted" => value,
        _ => "unslotted".to_string(),
    }
}

fn local_date_from_unix(created_at: i64) -> String {
    Utc.timestamp_opt(created_at, 0)
        .single()
        .map(|dt| dt.with_timezone(&Local).format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| Local::now().format("%Y-%m-%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distiller_message_input_strips_image_marker_and_keeps_caption_text() {
        let message = DistillationIngressEvent {
            message_id: "telegram_-100_1".to_string(),
            sender: "alice".to_string(),
            content: "[IMAGE:/tmp/pantry.jpg]\n\nCan we cook something with this?".to_string(),
            channel: "telegram".to_string(),
            thread_ts: None,
            created_at: 123,
        };
        let input = distiller_message_input(message);
        assert_eq!(input.image_count, 1);
        assert_eq!(input.content_text, "Can we cook something with this?");
        assert!(!input.content_text.contains("[IMAGE:"));
    }

    #[test]
    fn distiller_message_input_uses_placeholder_for_image_only_content() {
        let message = DistillationIngressEvent {
            message_id: "telegram_-100_2".to_string(),
            sender: "alice".to_string(),
            content: "[IMAGE:/tmp/pantry.jpg]".to_string(),
            channel: "telegram".to_string(),
            thread_ts: None,
            created_at: 124,
        };
        let input = distiller_message_input(message);
        assert_eq!(input.image_count, 1);
        assert_eq!(input.content_text, "[image attachment]");
    }
}
