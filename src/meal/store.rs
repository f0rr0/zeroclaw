use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use crate::channels::traits::ChannelMessage;
use crate::config::Config;
use crate::cron::{self, CronJobPatch, DeliveryConfig, Schedule, SessionTarget};
use crate::memory::embeddings::{create_embedding_provider, EmbeddingProvider};
use crate::memory::vector;

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

const PANTRY_MAX_SCORE_CONTRIBUTION: f64 = 4.0;
const PANTRY_LIKELY_AVAILABLE_THRESHOLD: f64 = 0.60;
const PANTRY_UNCERTAIN_THRESHOLD: f64 = 0.25;
const UNRESOLVED_CLAIM_TTL_SECS: i64 = 3 * 24 * 60 * 60; // 72h
const MAX_CONTEXT_PREFERENCES: u32 = 500;
const MAX_CONTEXT_EPISODES: u32 = 200;
const MAX_CONTEXT_PANTRY_ITEMS: u32 = 200;
const MAX_CONTEXT_FEEDBACK: u32 = 120;
const MAX_CONTEXT_INGRESS_WINDOW: u32 = 200;

fn outbox_backoff_secs(attempt_count: u32) -> i64 {
    let capped_attempt = attempt_count.min(6);
    let factor = 1_i64 << capped_attempt;
    (5_i64.saturating_mul(factor)).min(300)
}

fn normalize_pantry_freshness_profile(profile: Option<&str>) -> Option<String> {
    let normalized = profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    match normalized.as_deref() {
        Some("high" | "medium" | "low" | "very_low" | "unknown") => normalized,
        _ => None,
    }
}

fn normalize_pantry_item_canonical(item_name: &str) -> Option<String> {
    let canonical = item_name
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if canonical.is_empty() {
        None
    } else {
        Some(canonical)
    }
}

fn pantry_profile_class(profile: Option<&str>) -> &'static str {
    match profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("high") => "high",
        Some("medium") => "medium",
        Some("low") => "low",
        Some("very_low") => "very_low",
        _ => "unknown",
    }
}

fn pantry_profile_half_life_days(profile: Option<&str>) -> f64 {
    match pantry_profile_class(profile) {
        "high" => 1.5,
        "medium" => 5.0,
        "low" => 20.0,
        "very_low" => 90.0,
        _ => 3.0,
    }
}

fn pantry_profile_hard_cutoff_days(profile: Option<&str>) -> f64 {
    match pantry_profile_class(profile) {
        "high" => 7.0,
        "medium" => 21.0,
        "low" => 90.0,
        "very_low" => 365.0,
        _ => 14.0,
    }
}

fn pantry_profile_hard_cutoff_secs(profile: Option<&str>) -> i64 {
    match pantry_profile_class(profile) {
        "high" => 7 * 86_400,
        "medium" => 21 * 86_400,
        "low" => 90 * 86_400,
        "very_low" => 365 * 86_400,
        _ => 14 * 86_400,
    }
}

fn pantry_freshness_weight(updated_at: i64, now_ts: i64, freshness_profile: Option<&str>) -> f64 {
    let age_secs = now_ts.saturating_sub(updated_at).max(0);
    let age_days = (age_secs as f64) / 86_400.0;
    let hard_cutoff_days = pantry_profile_hard_cutoff_days(freshness_profile);
    if age_days > hard_cutoff_days {
        return 0.0;
    }
    let half_life_days = pantry_profile_half_life_days(freshness_profile).max(0.1);
    // Exponential half-life decay: exp(-ln(2) * age / half_life)
    let decay = (-std::f64::consts::LN_2 * age_days / half_life_days).exp();
    decay.clamp(0.0, 1.0)
}

fn pantry_source_weight(source: Option<&str>) -> f64 {
    let source = source
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if source.is_empty() {
        return 0.85;
    }
    if source.contains("photo")
        || source.contains("image")
        || source.contains("fridge")
        || source.contains("shelf")
    {
        return 1.0;
    }
    if source.contains("manual") || source.contains("chat") || source.contains("text") {
        return 0.9;
    }
    if source.contains("instamart") || source.contains("swiggy") || source.contains("order") {
        // inferred availability from past orders is significantly weaker than direct observation
        return 0.55;
    }
    0.70
}

fn pantry_observation_hash(
    household_id: &str,
    item_canonical: &str,
    source: &str,
    source_user_id: Option<&str>,
    source_message_id: Option<&str>,
    observed_at: i64,
    quantity_hint: Option<&str>,
    freshness_profile: Option<&str>,
    confidence: f64,
) -> String {
    sha256_hex(
        format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{:.4}",
            household_id.trim(),
            item_canonical.trim(),
            source.trim(),
            source_user_id.unwrap_or_default().trim(),
            source_message_id.unwrap_or_default().trim(),
            observed_at,
            quantity_hint.unwrap_or_default().trim(),
            freshness_profile.unwrap_or_default().trim(),
            confidence.clamp(0.0, 1.0)
        )
        .as_str(),
    )
}

fn pantry_item_confidence(item: &PantryHint, now_ts: i64) -> f64 {
    pantry_freshness_weight(item.updated_at, now_ts, item.freshness_profile.as_deref())
        * pantry_source_weight(item.source.as_deref())
}

fn tokenize_meal_text(input: &str) -> HashSet<String> {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| token.len() >= 3)
        .filter(|token| {
            !matches!(
                token.as_str(),
                "and"
                    | "with"
                    | "the"
                    | "for"
                    | "from"
                    | "that"
                    | "this"
                    | "into"
                    | "over"
                    | "under"
                    | "meal"
                    | "dish"
                    | "fresh"
                    | "style"
                    | "recipe"
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreferenceKind {
    Positive,
    Negative,
    Constraint,
    Scalar,
}

fn normalize_pref_key(key: &str) -> String {
    key.trim().to_ascii_lowercase()
}

fn preference_kind_for_key(key: &str) -> PreferenceKind {
    let key = normalize_pref_key(key);
    if matches!(key.as_str(), "spice_level") {
        return PreferenceKind::Scalar;
    }
    if matches!(
        key.as_str(),
        "allergy" | "dietary_restriction" | "dietary_preference" | "must_avoid"
    ) || key.starts_with("no_")
    {
        return PreferenceKind::Constraint;
    }
    if matches!(
        key.as_str(),
        "dislikes" | "avoid" | "ingredient_avoidance" | "dont_eat"
    ) {
        return PreferenceKind::Negative;
    }
    PreferenceKind::Positive
}

fn preference_value_matches(
    searchable: &str,
    searchable_tokens: &HashSet<String>,
    pref_value: &str,
) -> bool {
    let value_lc = pref_value.trim().to_ascii_lowercase();
    if value_lc.is_empty() {
        return false;
    }
    if searchable.contains(value_lc.as_str()) {
        return true;
    }
    let value_tokens = tokenize_meal_text(value_lc.as_str());
    !value_tokens.is_empty()
        && value_tokens
            .iter()
            .any(|token| searchable_tokens.contains(token))
}

fn has_any_token(searchable_tokens: &HashSet<String>, tokens: &[&str]) -> bool {
    tokens
        .iter()
        .any(|token| searchable_tokens.contains(&token.to_ascii_lowercase()))
}

fn violates_dietary_restriction(searchable_tokens: &HashSet<String>, pref_value: &str) -> bool {
    let value = pref_value.trim().to_ascii_lowercase();
    if value.is_empty() {
        return false;
    }
    let non_veg_tokens = [
        "chicken", "mutton", "lamb", "beef", "pork", "fish", "shrimp", "prawn", "egg", "eggs",
    ];
    if value.contains("vegetarian") || value == "veg" || value.contains("jain") {
        return has_any_token(searchable_tokens, &non_veg_tokens);
    }
    if value.contains("vegan") {
        let vegan_block = [
            "chicken", "mutton", "lamb", "beef", "pork", "fish", "shrimp", "prawn", "egg", "eggs",
            "paneer", "cheese", "yogurt", "curd", "milk", "ghee", "butter",
        ];
        return has_any_token(searchable_tokens, &vegan_block);
    }
    if value.contains("eggless") {
        return has_any_token(searchable_tokens, &["egg", "eggs"]);
    }
    false
}

fn scalar_preference_delta(searchable_tokens: &HashSet<String>, key: &str, value: &str) -> f64 {
    let key = normalize_pref_key(key);
    if key != "spice_level" {
        return 0.0;
    }
    let value = value.trim().to_ascii_lowercase();
    let spicy_tokens = ["spicy", "hot", "fiery", "chili", "chilli"];
    let mild_tokens = ["mild", "bland", "light"];
    if value.contains("spicy") || value.contains("hot") {
        if has_any_token(searchable_tokens, &spicy_tokens) {
            return 1.0;
        }
        if has_any_token(searchable_tokens, &mild_tokens) {
            return -1.0;
        }
        return 0.0;
    }
    if value.contains("mild") || value.contains("bland") {
        if has_any_token(searchable_tokens, &spicy_tokens) {
            return -2.0;
        }
        if has_any_token(searchable_tokens, &mild_tokens) {
            return 1.0;
        }
        return 0.0;
    }
    0.0
}

fn resolve_meal_db_path(config: &Config) -> PathBuf {
    let configured = PathBuf::from(config.meal_agent.db_path.trim());
    if configured.is_absolute() {
        configured
    } else {
        config.workspace_dir.join(configured)
    }
}

fn ingress_doc_ref(channel: &str, message_id: &str) -> String {
    format!("{channel}:{message_id}")
}

fn initialized_meal_dbs() -> &'static StdMutex<HashSet<PathBuf>> {
    static INITIALIZED: OnceLock<StdMutex<HashSet<PathBuf>>> = OnceLock::new();
    INITIALIZED.get_or_init(|| StdMutex::new(HashSet::new()))
}

fn escape_fts_term(term: &str) -> String {
    term.replace('"', "\"\"")
}

fn build_fts_query(terms: &[String]) -> Option<String> {
    let normalized: Vec<String> = terms
        .iter()
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{}\"", escape_fts_term(&t)))
        .collect();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.join(" OR "))
    }
}

fn ensure_search_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meal_search_docs (
            household_id TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            doc_ref TEXT NOT NULL,
            content TEXT NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, doc_type, doc_ref)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_search_docs_household_type
            ON meal_search_docs (household_id, doc_type, updated_at DESC);

        CREATE VIRTUAL TABLE IF NOT EXISTS meal_search_fts
        USING fts5(
            household_id UNINDEXED,
            doc_type UNINDEXED,
            doc_ref UNINDEXED,
            content,
            tokenize = 'unicode61 remove_diacritics 2'
        );",
    )?;
    Ok(())
}

fn upsert_search_doc(
    conn: &Connection,
    household_id: &str,
    doc_type: &str,
    doc_ref: &str,
    content: &str,
) -> Result<()> {
    ensure_search_schema(conn)?;
    conn.execute(
        "INSERT INTO meal_search_docs (
            household_id, doc_type, doc_ref, content, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(household_id, doc_type, doc_ref) DO UPDATE SET
            content=excluded.content,
            updated_at=excluded.updated_at",
        params![household_id, doc_type, doc_ref, content, unix_now()],
    )?;
    conn.execute(
        "DELETE FROM meal_search_fts
         WHERE household_id=?1 AND doc_type=?2 AND doc_ref=?3",
        params![household_id, doc_type, doc_ref],
    )?;
    conn.execute(
        "INSERT INTO meal_search_fts (household_id, doc_type, doc_ref, content)
         VALUES (?1, ?2, ?3, ?4)",
        params![household_id, doc_type, doc_ref, content],
    )?;
    Ok(())
}

fn search_doc_refs(
    conn: &Connection,
    household_id: &str,
    doc_type: &str,
    fts_query: &str,
    limit: u32,
) -> Result<HashMap<String, f64>> {
    let mut out = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT doc_ref, bm25(meal_search_fts) as score
         FROM meal_search_fts
         WHERE household_id=?1 AND doc_type=?2 AND meal_search_fts MATCH ?3
         ORDER BY score ASC
         LIMIT ?4",
    )?;
    let rows = stmt.query_map(
        params![household_id, doc_type, fts_query, i64::from(limit.max(1))],
        |row| {
            let doc_ref: String = row.get(0)?;
            let score: f64 = row.get(1)?;
            Ok((doc_ref, score))
        },
    )?;
    for row in rows {
        let (doc_ref, score) = row?;
        // FTS5 bm25 returns lower-is-better values (often negative). Convert
        // to a higher-is-better score for hybrid fusion.
        out.insert(doc_ref, (-score).max(0.0));
    }
    Ok(out)
}

fn delete_search_doc(
    conn: &Connection,
    household_id: &str,
    doc_type: &str,
    doc_ref: &str,
) -> Result<()> {
    ensure_search_schema(conn)?;
    conn.execute(
        "DELETE FROM meal_search_docs
         WHERE household_id=?1 AND doc_type=?2 AND doc_ref=?3",
        params![household_id, doc_type, doc_ref],
    )?;
    conn.execute(
        "DELETE FROM meal_search_fts
         WHERE household_id=?1 AND doc_type=?2 AND doc_ref=?3",
        params![household_id, doc_type, doc_ref],
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
struct PantryObservationRow {
    item_name: String,
    source: String,
    source_user_id: Option<String>,
    source_message_id: Option<String>,
    quantity_hint: Option<String>,
    freshness_profile: Option<String>,
    observed_at: i64,
    confidence: f64,
}

#[derive(Debug, Clone)]
struct PantryStateRow {
    item_canonical: String,
    display_name: String,
    state: String,
    availability_score: f64,
    perishability_class: Option<String>,
    last_observed_at: i64,
    source: Option<String>,
    quantity_hint: Option<String>,
    freshness_profile: Option<String>,
}

fn pantry_item_matches_searchable(
    item_name: &str,
    searchable: &str,
    searchable_tokens: &HashSet<String>,
) -> bool {
    let item_lc = item_name.to_ascii_lowercase();
    let item_tokens = tokenize_meal_text(item_lc.as_str());
    let direct_match = searchable.contains(item_lc.as_str());
    let token_match = !item_tokens.is_empty()
        && item_tokens
            .iter()
            .any(|token| searchable_tokens.contains(token));
    direct_match || token_match
}

fn pantry_usage_hits_after_observed(
    conn: &Connection,
    household_id: &str,
    item_canonical: &str,
    observed_after: i64,
) -> Result<u32> {
    let mut stmt = conn.prepare(
        "SELECT meal_name, tags_json
         FROM meal_episodes
         WHERE household_id=?1 AND created_at>=?2
         ORDER BY created_at DESC
         LIMIT 80",
    )?;
    let rows = stmt.query_map(params![household_id, observed_after], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut hits = 0_u32;
    for row in rows {
        let (meal_name, tags_json) = row?;
        let tags = serde_json::from_str::<Vec<String>>(&tags_json).unwrap_or_default();
        let searchable = format!(
            "{} {}",
            meal_name.to_ascii_lowercase(),
            tags.join(" ").to_ascii_lowercase()
        );
        let searchable_tokens = tokenize_meal_text(searchable.as_str());
        if pantry_item_matches_searchable(item_canonical, searchable.as_str(), &searchable_tokens) {
            hits = hits.saturating_add(1);
            if hits >= 3 {
                break;
            }
        }
    }
    Ok(hits)
}

fn recompute_pantry_state_for_items(
    conn: &Connection,
    household_id: &str,
    items: &HashSet<String>,
    now_ts: i64,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    for item_canonical in items {
        let mut obs_stmt = conn.prepare(
            "SELECT item_name, source, source_user_id, source_message_id, quantity_hint,
                    freshness_profile, observed_at, confidence
             FROM meal_pantry_observations
             WHERE household_id=?1 AND item_canonical=?2
             ORDER BY observed_at DESC, id DESC
             LIMIT 64",
        )?;
        let obs_rows = obs_stmt.query_map(params![household_id, item_canonical], |row| {
            Ok(PantryObservationRow {
                item_name: row.get(0)?,
                source: row.get(1)?,
                source_user_id: row.get(2)?,
                source_message_id: row.get(3)?,
                quantity_hint: row.get(4)?,
                freshness_profile: row.get(5)?,
                observed_at: row.get(6)?,
                confidence: row.get(7)?,
            })
        })?;

        let mut observations = Vec::new();
        for row in obs_rows {
            observations.push(row?);
        }

        if observations.is_empty() {
            conn.execute(
                "DELETE FROM meal_pantry_state
                 WHERE household_id=?1 AND item_canonical=?2",
                params![household_id, item_canonical],
            )?;
            conn.execute(
                "DELETE FROM meal_pantry_hints
                 WHERE household_id=?1 AND item_name=?2",
                params![household_id, item_canonical],
            )?;
            delete_search_doc(conn, household_id, "pantry", item_canonical.as_str())?;
            continue;
        }

        let latest = &observations[0];
        let display_name = latest.item_name.trim();
        let display_name = if display_name.is_empty() {
            item_canonical.as_str()
        } else {
            display_name
        };
        let perishability_class =
            pantry_profile_class(latest.freshness_profile.as_deref()).to_string();
        let last_observed_at = latest.observed_at.max(0);

        let mut source_mix: HashMap<String, u32> = HashMap::new();
        let mut confidence_product = 1.0_f64;
        for obs in &observations {
            let source_key = obs.source.trim().to_ascii_lowercase();
            *source_mix.entry(source_key.clone()).or_insert(0) += 1;
            let obs_score = pantry_source_weight(Some(source_key.as_str()))
                * pantry_freshness_weight(
                    obs.observed_at,
                    now_ts,
                    obs.freshness_profile.as_deref(),
                )
                * obs.confidence.clamp(0.0, 1.0);
            confidence_product *= 1.0 - obs_score.clamp(0.0, 1.0);
        }
        let availability_raw = (1.0 - confidence_product).clamp(0.0, 1.0);
        let usage_hits = pantry_usage_hits_after_observed(
            conn,
            household_id,
            item_canonical.as_str(),
            last_observed_at,
        )?;
        let usage_decay = (-0.30_f64 * f64::from(usage_hits)).exp();
        let mut availability_score = (availability_raw * usage_decay).clamp(0.0, 1.0);

        let age_days = (now_ts.saturating_sub(last_observed_at).max(0) as f64) / 86_400.0;
        let hard_cutoff_days = pantry_profile_hard_cutoff_days(latest.freshness_profile.as_deref());
        if age_days > hard_cutoff_days {
            availability_score = 0.0;
        }

        let state = if availability_score >= PANTRY_LIKELY_AVAILABLE_THRESHOLD {
            "likely_available"
        } else if availability_score >= PANTRY_UNCERTAIN_THRESHOLD {
            "uncertain"
        } else {
            "likely_unavailable"
        };

        let expected_expiry_at = last_observed_at.saturating_add(pantry_profile_hard_cutoff_secs(
            latest.freshness_profile.as_deref(),
        ));
        let source_mix_json = serde_json::to_string(&source_mix)?;
        let confidence_breakdown_json = serde_json::json!({
            "availability_raw": availability_raw,
            "usage_hits_after_last_observation": usage_hits,
            "usage_decay": usage_decay,
            "age_days": age_days,
            "hard_cutoff_days": hard_cutoff_days
        })
        .to_string();

        conn.execute(
            "INSERT INTO meal_pantry_state (
                household_id, item_canonical, display_name, state, availability_score,
                perishability_class, last_observed_at, expected_expiry_at, source_mix_json,
                confidence_breakdown_json, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(household_id, item_canonical) DO UPDATE SET
                display_name=excluded.display_name,
                state=excluded.state,
                availability_score=excluded.availability_score,
                perishability_class=excluded.perishability_class,
                last_observed_at=excluded.last_observed_at,
                expected_expiry_at=excluded.expected_expiry_at,
                source_mix_json=excluded.source_mix_json,
                confidence_breakdown_json=excluded.confidence_breakdown_json,
                updated_at=excluded.updated_at",
            params![
                household_id,
                item_canonical,
                display_name,
                state,
                availability_score,
                perishability_class,
                last_observed_at,
                expected_expiry_at,
                source_mix_json,
                confidence_breakdown_json,
                now_ts
            ],
        )?;

        if state == "likely_unavailable" {
            conn.execute(
                "DELETE FROM meal_pantry_hints
                 WHERE household_id=?1 AND item_name=?2",
                params![household_id, item_canonical],
            )?;
            delete_search_doc(conn, household_id, "pantry", item_canonical.as_str())?;
            continue;
        }

        conn.execute(
            "INSERT INTO meal_pantry_hints (
                household_id, item_name, quantity_hint, source, freshness_profile, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(household_id, item_name) DO UPDATE SET
                quantity_hint=excluded.quantity_hint,
                source=excluded.source,
                freshness_profile=excluded.freshness_profile,
                updated_at=excluded.updated_at",
            params![
                household_id,
                item_canonical,
                latest.quantity_hint.clone(),
                Some(latest.source.clone()),
                latest.freshness_profile.clone(),
                last_observed_at
            ],
        )?;
        let pantry_search_content = format!(
            "{} {} {} {}",
            display_name,
            latest.quantity_hint.as_deref().unwrap_or_default(),
            latest.source,
            state
        );
        upsert_search_doc(
            conn,
            household_id,
            "pantry",
            item_canonical.as_str(),
            pantry_search_content.as_str(),
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct MealStore {
    config: Arc<Config>,
    db_path: PathBuf,
}

impl MealStore {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            db_path: resolve_meal_db_path(config.as_ref()),
            config,
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.meal_agent.enabled
    }

    pub fn household_id(&self) -> &str {
        self.config.meal_agent.household_id.as_str()
    }

    pub fn db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    fn resolve_household(&self, requested_household: &str) -> Result<String> {
        let configured = self.household_id().trim();
        let requested = requested_household.trim();
        if !requested.is_empty() && requested != configured {
            anyhow::bail!(
                "meal_agent is configured for a single household (`{configured}`); received `{requested}`"
            );
        }
        Ok(configured.to_string())
    }

    fn open_conn(&self) -> Result<Connection> {
        if let Some(parent) = self.db_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create meal-agent DB directory {}",
                    parent.display()
                )
            })?;
        }
        let conn = Connection::open(&self.db_path).with_context(|| {
            format!("Failed to open meal-agent DB at {}", self.db_path.display())
        })?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;",
        )?;
        let mut initialized = initialized_meal_dbs()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !initialized.contains(&self.db_path) {
            ensure_schema(&conn)?;
            initialized.insert(self.db_path.clone());
        }
        Ok(conn)
    }

    fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T>,
    {
        let mut conn = self.open_conn()?;
        f(&mut conn)
    }

    fn resolved_embedding_route(&self) -> Option<(String, String, usize, Option<String>)> {
        let memory = &self.config.memory;
        let fallback = (
            memory.embedding_provider.trim().to_string(),
            memory.embedding_model.trim().to_string(),
            memory.embedding_dimensions,
            None,
        );
        let Some(hint) = memory
            .embedding_model
            .strip_prefix("hint:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Some(fallback);
        };
        let route = self
            .config
            .embedding_routes
            .iter()
            .find(|route| route.hint.trim() == hint)?;
        let dims = route.dimensions.unwrap_or(memory.embedding_dimensions);
        Some((
            route.provider.trim().to_string(),
            route.model.trim().to_string(),
            dims,
            route.api_key.clone(),
        ))
    }

    fn provider_specific_api_key(provider: &str) -> Option<String> {
        let env_var = match provider {
            "openai" => "OPENAI_API_KEY",
            "openrouter" => "OPENROUTER_API_KEY",
            "cohere" => "COHERE_API_KEY",
            _ => return None,
        };
        std::env::var(env_var)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    fn create_semantic_embedder(&self) -> Option<Box<dyn EmbeddingProvider>> {
        let (provider, model, dimensions, route_api_key) = self.resolved_embedding_route()?;
        if provider.is_empty() || provider.eq_ignore_ascii_case("none") || model.is_empty() {
            return None;
        }
        let api_key = route_api_key
            .and_then(|k| {
                let trimmed = k.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            })
            .or_else(|| Self::provider_specific_api_key(provider.as_str()))
            .or_else(|| self.config.api_key.clone());
        Some(create_embedding_provider(
            provider.as_str(),
            api_key.as_deref(),
            model.as_str(),
            dimensions,
        ))
    }

    async fn hybrid_rank_docs(
        &self,
        embedder: &dyn EmbeddingProvider,
        query_text: &str,
        docs: &[(String, String)],
        keyword_hits: &HashMap<String, f64>,
        limit: usize,
    ) -> Result<HashMap<String, f64>> {
        if docs.is_empty() || query_text.trim().is_empty() || limit == 0 {
            return Ok(HashMap::new());
        }

        let mut batch: Vec<&str> = Vec::with_capacity(docs.len() + 1);
        batch.push(query_text);
        batch.extend(docs.iter().map(|(_, text)| text.as_str()));

        let embeddings = embedder
            .embed(&batch)
            .await
            .context("semantic retrieval embedding call failed")?;
        if embeddings.len() != batch.len() {
            anyhow::bail!(
                "semantic retrieval embedding count mismatch: expected {}, got {}",
                batch.len(),
                embeddings.len()
            );
        }
        let query_embedding = &embeddings[0];

        let mut vector_results = Vec::new();
        for (idx, (doc_ref, _)) in docs.iter().enumerate() {
            let sim = vector::cosine_similarity(
                query_embedding.as_slice(),
                embeddings[idx + 1].as_slice(),
            );
            if sim > 0.0 {
                vector_results.push((doc_ref.clone(), sim));
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let keyword_results: Vec<(String, f32)> = keyword_hits
            .iter()
            .map(|(id, score)| (id.clone(), *score as f32))
            .collect();
        #[allow(clippy::cast_possible_truncation)]
        let vector_weight = self.config.memory.vector_weight as f32;
        #[allow(clippy::cast_possible_truncation)]
        let keyword_weight = self.config.memory.keyword_weight as f32;

        let merged = vector::hybrid_merge(
            vector_results.as_slice(),
            keyword_results.as_slice(),
            vector_weight,
            keyword_weight,
            limit,
        );

        #[allow(clippy::cast_possible_truncation)]
        let min_relevance = self.config.memory.min_relevance_score as f32;
        let mut ranked = HashMap::new();
        for item in merged {
            if item.final_score >= min_relevance {
                ranked.insert(item.id, f64::from(item.final_score));
            }
        }
        Ok(ranked)
    }

    pub fn record_ingress(&self, message: &ChannelMessage) -> Result<()> {
        let household_id = self.household_id().to_string();
        let retention_days = i64::from(self.config.meal_agent.ingress_retention_days.max(1));
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO meal_ingress_events (
                    household_id, message_id, channel, sender, reply_target, thread_ts, content, timestamp, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(household_id, channel, message_id) DO UPDATE SET
                    sender=excluded.sender,
                    reply_target=excluded.reply_target,
                    thread_ts=excluded.thread_ts,
                    content=excluded.content,
                    timestamp=excluded.timestamp,
                    created_at=excluded.created_at",
                params![
                    household_id,
                    message.id,
                    message.channel,
                    message.sender,
                    message.reply_target,
                    message.thread_ts,
                    message.content,
                    message.timestamp as i64,
                    unix_now(),
                ],
            )?;
            let ingress_ref = ingress_doc_ref(message.channel.as_str(), message.id.as_str());
            let ingress_search_content = format!("{} {}", message.sender, message.content);
            upsert_search_doc(
                conn,
                household_id.as_str(),
                "ingress",
                ingress_ref.as_str(),
                ingress_search_content.as_str(),
            )?;

            conn.execute(
                "DELETE FROM meal_ingress_events
                 WHERE household_id=?1 AND created_at < ?2",
                params![self.household_id(), unix_now() - (retention_days * 86_400)],
            )?;
            conn.execute(
                "DELETE FROM meal_search_docs
                 WHERE household_id=?1
                   AND doc_type='ingress'
                   AND doc_ref NOT IN (
                     SELECT channel || ':' || message_id
                     FROM meal_ingress_events
                     WHERE household_id=?1
                   )",
                params![self.household_id()],
            )?;
            conn.execute(
                "DELETE FROM meal_search_fts
                 WHERE household_id=?1
                   AND doc_type='ingress'
                   AND doc_ref NOT IN (
                     SELECT channel || ':' || message_id
                     FROM meal_ingress_events
                     WHERE household_id=?1
                   )",
                params![self.household_id()],
            )?;

            Ok(())
        })
    }

    pub fn latest_ingress_sender(&self, requested_household: &str) -> Result<Option<String>> {
        let household = self.resolve_household(requested_household)?;
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT sender
                 FROM meal_ingress_events
                 WHERE household_id=?1
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1",
                params![household],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn ingress_created_at(
        &self,
        requested_household: &str,
        message_id: &str,
    ) -> Result<Option<i64>> {
        let household = self.resolve_household(requested_household)?;
        let normalized_message_id = message_id.trim();
        if normalized_message_id.is_empty() {
            return Ok(None);
        }
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT created_at
                 FROM meal_ingress_events
                 WHERE household_id=?1 AND message_id=?2
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1",
                params![household, normalized_message_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn known_participants(&self, requested_household: &str, limit: u32) -> Result<Vec<String>> {
        let household = self.resolve_household(requested_household)?;
        let max_rows = i64::from(limit.clamp(1, 128));
        self.with_conn(|conn| {
            let mut participants = Vec::new();
            let mut seen = HashSet::new();

            let mut ingress_stmt = conn.prepare(
                "SELECT sender, MAX(created_at) AS last_seen
                 FROM meal_ingress_events
                 WHERE household_id=?1
                 GROUP BY sender
                 ORDER BY last_seen DESC
                 LIMIT ?2",
            )?;
            let ingress_rows = ingress_stmt
                .query_map(params![household, max_rows], |row| row.get::<_, String>(0))?;
            for row in ingress_rows {
                let sender = row?;
                let normalized = sender.trim();
                if normalized.is_empty() || normalized.eq_ignore_ascii_case("unknown") {
                    continue;
                }
                let dedupe_key = normalized.to_ascii_lowercase();
                if seen.insert(dedupe_key) {
                    participants.push(normalized.to_string());
                }
            }

            let mut pref_stmt = conn.prepare(
                "SELECT user_id, MAX(updated_at) AS last_seen
                 FROM meal_preferences
                 WHERE household_id=?1
                 GROUP BY user_id
                 ORDER BY last_seen DESC
                 LIMIT ?2",
            )?;
            let pref_rows =
                pref_stmt.query_map(params![household, max_rows], |row| row.get::<_, String>(0))?;
            for row in pref_rows {
                let user_id = row?;
                let normalized = user_id.trim();
                if normalized.is_empty() || normalized.eq_ignore_ascii_case("unknown") {
                    continue;
                }
                let dedupe_key = normalized.to_ascii_lowercase();
                if seen.insert(dedupe_key) {
                    participants.push(normalized.to_string());
                }
            }

            Ok(participants)
        })
    }

    pub fn record_control_gate_decision(
        &self,
        message: &ChannelMessage,
        gate_kind: &str,
        decision: &str,
        confidence: Option<f64>,
        intent: Option<&str>,
        reason_code: &str,
        model: Option<&str>,
    ) -> Result<()> {
        let normalized_gate_kind = gate_kind.trim().to_ascii_lowercase();
        if normalized_gate_kind.is_empty() {
            anyhow::bail!("control gate kind must not be empty");
        }
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO meal_control_gate_events (
                    household_id, channel, source_message_id, gate_kind, decision, confidence,
                    intent, reason_code, model, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(household_id, channel, source_message_id, gate_kind) DO UPDATE SET
                    decision=excluded.decision,
                    confidence=excluded.confidence,
                    intent=excluded.intent,
                    reason_code=excluded.reason_code,
                    model=excluded.model,
                    created_at=excluded.created_at",
                params![
                    self.household_id(),
                    message.channel,
                    message.id,
                    normalized_gate_kind,
                    decision,
                    confidence,
                    intent,
                    reason_code,
                    model,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_reply_gate_decision(
        &self,
        message: &ChannelMessage,
        decision: &str,
        confidence: Option<f64>,
        intent: Option<&str>,
        reason_code: &str,
        model: Option<&str>,
    ) -> Result<()> {
        self.record_control_gate_decision(
            message,
            "reply",
            decision,
            confidence,
            intent,
            reason_code,
            model,
        )
    }

    pub fn record_distill_gate_decision(
        &self,
        message: &ChannelMessage,
        decision: &str,
        confidence: Option<f64>,
        intent: Option<&str>,
        reason_code: &str,
        model: Option<&str>,
    ) -> Result<()> {
        self.record_control_gate_decision(
            message,
            "distill",
            decision,
            confidence,
            intent,
            reason_code,
            model,
        )
    }

    pub fn reply_gate_allows(&self, channel: &str, source_message_id: &str) -> Result<bool> {
        self.with_conn(|conn| {
            let decision: Option<String> = conn
                .query_row(
                    "SELECT decision
                     FROM meal_control_gate_events
                     WHERE household_id=?1 AND channel=?2 AND source_message_id=?3
                       AND gate_kind='reply'",
                    params![self.household_id(), channel, source_message_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(decision
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("respond"))
                .unwrap_or(false))
        })
    }

    pub fn apply_unresolved_claims(&self, claims: &[UnresolvedClaimRecord]) -> Result<usize> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut applied = 0usize;
            for claim in claims {
                let source_message_id = claim.source_message_id.trim();
                let claim_kind = claim.claim_kind.trim().to_ascii_lowercase();
                let subject_ref = claim.subject_ref.trim();
                let question = claim.question.trim();
                if source_message_id.is_empty()
                    || claim_kind.is_empty()
                    || subject_ref.is_empty()
                    || question.is_empty()
                {
                    continue;
                }
                let claim_text = claim
                    .claim_text
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                tx.execute(
                    "INSERT INTO meal_unresolved_claims (
                        household_id, source_message_id, claim_kind, subject_ref,
                        claim_text, question, status, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', ?7, ?7)
                     ON CONFLICT(household_id, source_message_id, claim_kind, subject_ref, question)
                     DO UPDATE SET
                        claim_text=COALESCE(excluded.claim_text, meal_unresolved_claims.claim_text),
                        status='open',
                        updated_at=excluded.updated_at",
                    params![
                        self.household_id(),
                        source_message_id,
                        claim_kind,
                        subject_ref,
                        claim_text,
                        question,
                        unix_now()
                    ],
                )?;
                applied += 1;
            }
            tx.commit()?;
            Ok(applied)
        })
    }

    pub fn next_distillation_window(&self, limit: u32) -> Result<Vec<DistillationIngressEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.with_conn(|conn| {
            let cursor: i64 = conn
                .query_row(
                    "SELECT last_ingress_created_at
                     FROM meal_distiller_state
                     WHERE household_id=?1",
                    params![self.household_id()],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            let mut stmt = conn.prepare(
                "SELECT message_id, sender, content, channel, thread_ts, created_at
                 FROM meal_ingress_events
                 WHERE household_id=?1 AND created_at>?2
                 ORDER BY created_at ASC
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![self.household_id(), cursor, i64::from(limit)],
                |row| {
                    Ok(DistillationIngressEvent {
                        message_id: row.get(0)?,
                        sender: row.get(1)?,
                        content: row.get(2)?,
                        channel: row.get(3)?,
                        thread_ts: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn apply_preference_evidence(
        &self,
        evidence: &[PreferenceEvidenceRecord],
        cursor_created_at: i64,
    ) -> Result<usize> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut applied = 0usize;
            for item in evidence {
                let user_id = item.user_id.trim();
                let pref_key = item.pref_key.trim();
                let pref_value = item.pref_value.trim();
                let source_message_id = item.source_message_id.trim();
                if user_id.is_empty()
                    || pref_key.is_empty()
                    || pref_value.is_empty()
                    || source_message_id.is_empty()
                {
                    continue;
                }
                let confidence = item.confidence.clamp(0.0, 1.0);
                if confidence <= 0.0 {
                    continue;
                }
                tx.execute(
                    "INSERT INTO meal_preference_evidence (
                        household_id, user_id, pref_key, pref_value, confidence,
                        evidence_text, source_message_id, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(household_id, source_message_id, user_id, pref_key, pref_value)
                     DO UPDATE SET
                        confidence=MAX(meal_preference_evidence.confidence, excluded.confidence),
                        evidence_text=excluded.evidence_text,
                        created_at=excluded.created_at",
                    params![
                        self.household_id(),
                        user_id,
                        pref_key,
                        pref_value,
                        confidence,
                        item.evidence_text.as_deref(),
                        source_message_id,
                        unix_now()
                    ],
                )?;
                tx.execute(
                    "INSERT INTO meal_preferences (
                        household_id, user_id, pref_key, pref_value, confidence, source, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(household_id, user_id, pref_key, pref_value) DO UPDATE SET
                        confidence=MAX(meal_preferences.confidence, excluded.confidence),
                        source=CASE
                            WHEN excluded.confidence >= meal_preferences.confidence THEN excluded.source
                            ELSE meal_preferences.source
                        END,
                        updated_at=excluded.updated_at",
                    params![
                        self.household_id(),
                        user_id,
                        pref_key,
                        pref_value,
                        confidence,
                        format!("distiller:{source_message_id}"),
                        unix_now()
                    ],
                )?;
                let pref_doc_ref = format!("{user_id}:{pref_key}:{}", sha256_hex(pref_value));
                let pref_search_content = format!("{user_id} {pref_key} {pref_value}");
                upsert_search_doc(
                    &tx,
                    self.household_id(),
                    "preference",
                    pref_doc_ref.as_str(),
                    pref_search_content.as_str(),
                )?;
                applied += 1;
            }
            tx.execute(
                "INSERT INTO meal_distiller_state (
                    household_id, last_ingress_created_at, last_run_at
                 ) VALUES (?1, ?2, ?3)
                 ON CONFLICT(household_id) DO UPDATE SET
                    last_ingress_created_at=MAX(meal_distiller_state.last_ingress_created_at, excluded.last_ingress_created_at),
                    last_run_at=excluded.last_run_at",
                params![self.household_id(), cursor_created_at.max(0), unix_now()],
            )?;
            tx.commit()?;
            Ok(applied)
        })
    }

    pub fn apply_episode_evidence(
        &self,
        evidence: &[EpisodeEvidenceRecord],
        cursor_created_at: i64,
    ) -> Result<usize> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut applied = 0usize;
            for item in evidence {
                let source_message_id = item.source_message_id.trim();
                let user_id = item.user_id.trim();
                let meal_name = item.meal_name.trim();
                if source_message_id.is_empty() || user_id.is_empty() || meal_name.is_empty() {
                    continue;
                }
                let confidence = item.confidence.clamp(0.0, 1.0);
                if confidence <= 0.0 {
                    continue;
                }
                let slot = item
                    .meal_slot
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("unslotted");
                let meal_date_local = item
                    .meal_date_local
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string);
                let tags: Vec<String> = item
                    .tags
                    .iter()
                    .map(|tag| tag.trim())
                    .filter(|tag| !tag.is_empty())
                    .map(ToString::to_string)
                    .collect();
                let episode_fingerprint = sha256_hex(
                    format!(
                        "{}|{}|{}",
                        meal_name.to_ascii_lowercase(),
                        slot,
                        meal_date_local.as_deref().unwrap_or_default()
                    )
                    .as_str(),
                );
                let anchor_key = format!(
                    "{}:ingress:{}:{}",
                    self.household_id(),
                    source_message_id,
                    &episode_fingerprint[..12]
                );

                tx.execute(
                    "INSERT INTO meal_episodes (
                        household_id, meal_anchor_key, meal_name, slot, meal_date_local, tags_json, decided_by, source_message_id, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(household_id, meal_anchor_key) DO UPDATE SET
                        meal_name=excluded.meal_name,
                        slot=excluded.slot,
                        meal_date_local=excluded.meal_date_local,
                        tags_json=excluded.tags_json,
                        decided_by=excluded.decided_by,
                        source_message_id=excluded.source_message_id,
                        created_at=excluded.created_at",
                    params![
                        self.household_id(),
                        anchor_key,
                        meal_name,
                        slot,
                        meal_date_local,
                        serde_json::to_string(&tags)?,
                        user_id,
                        source_message_id,
                        unix_now()
                    ],
                )?;
                let episode_search_content = format!(
                    "{} {} {} {} {}",
                    meal_name,
                    tags.join(" "),
                    slot,
                    meal_date_local.as_deref().unwrap_or_default(),
                    user_id
                );
                upsert_search_doc(
                    &tx,
                    self.household_id(),
                    "episode",
                    anchor_key.as_str(),
                    episode_search_content.as_str(),
                )?;
                applied += 1;
            }
            tx.execute(
                "INSERT INTO meal_distiller_state (
                    household_id, last_ingress_created_at, last_run_at
                 ) VALUES (?1, ?2, ?3)
                 ON CONFLICT(household_id) DO UPDATE SET
                    last_ingress_created_at=MAX(meal_distiller_state.last_ingress_created_at, excluded.last_ingress_created_at),
                    last_run_at=excluded.last_run_at",
                params![self.household_id(), cursor_created_at.max(0), unix_now()],
            )?;
            tx.commit()?;
            Ok(applied)
        })
    }

    pub fn recent_ingress_window(
        &self,
        household_id: &str,
        limit: u32,
    ) -> Result<Vec<IngressEvent>> {
        let household = self.resolve_household(household_id)?;
        let clamped_limit = limit.clamp(1, MAX_CONTEXT_INGRESS_WINDOW);
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT message_id, sender, content, timestamp, channel, thread_ts
                 FROM meal_ingress_events
                 WHERE household_id=?1
                 ORDER BY created_at DESC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![household, i64::from(clamped_limit)], |row| {
                Ok(IngressEvent {
                    message_id: row.get(0)?,
                    sender: row.get(1)?,
                    content: row.get(2)?,
                    timestamp: row.get(3)?,
                    channel: row.get(4)?,
                    thread_ts: row.get(5)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn ingress_event_by_message_id(
        &self,
        channel: &str,
        source_message_id: &str,
    ) -> Result<Option<IngressEvent>> {
        if channel.trim().is_empty() || source_message_id.trim().is_empty() {
            return Ok(None);
        }
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT message_id, sender, content, timestamp, channel, thread_ts
                 FROM meal_ingress_events
                 WHERE household_id=?1 AND channel=?2 AND message_id=?3
                 LIMIT 1",
                params![
                    self.household_id(),
                    channel.trim(),
                    source_message_id.trim()
                ],
                |row| {
                    Ok(IngressEvent {
                        message_id: row.get(0)?,
                        sender: row.get(1)?,
                        content: row.get(2)?,
                        timestamp: row.get(3)?,
                        channel: row.get(4)?,
                        thread_ts: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn control_gate_event_by_message_id(
        &self,
        channel: &str,
        source_message_id: &str,
        gate_kind: &str,
    ) -> Result<Option<ControlGateEvent>> {
        if channel.trim().is_empty() || source_message_id.trim().is_empty() {
            return Ok(None);
        }
        let gate_kind = gate_kind
            .trim()
            .to_ascii_lowercase()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>();
        if gate_kind.is_empty() {
            return Ok(None);
        }
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT gate_kind, decision, confidence, intent, reason_code, model, created_at
                 FROM meal_control_gate_events
                 WHERE household_id=?1 AND channel=?2 AND source_message_id=?3 AND gate_kind=?4
                 LIMIT 1",
                params![
                    self.household_id(),
                    channel.trim(),
                    source_message_id.trim(),
                    gate_kind
                ],
                |row| {
                    Ok(ControlGateEvent {
                        gate_kind: row.get(0)?,
                        decision: row.get(1)?,
                        confidence: row.get(2)?,
                        intent: row.get(3)?,
                        reason_code: row.get(4)?,
                        model: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    fn normalize_sync_state_user_id(source_user_id: Option<&str>) -> String {
        source_user_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("")
            .to_string()
    }

    pub fn pantry_sync_state(
        &self,
        household_id: &str,
        source: &str,
        source_user_id: Option<&str>,
    ) -> Result<Option<PantrySyncState>> {
        let household = self.resolve_household(household_id)?;
        let source = source.trim().to_ascii_lowercase();
        if source.is_empty() {
            return Ok(None);
        }
        let source_user_id = Self::normalize_sync_state_user_id(source_user_id);
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT household_id, source, source_user_id, last_success_at, last_attempt_at,
                        last_cursor_json, last_error, updated_at
                 FROM meal_pantry_sync_state
                 WHERE household_id=?1 AND source=?2 AND source_user_id=?3
                 LIMIT 1",
                params![household, source, source_user_id],
                |row| {
                    Ok(PantrySyncState {
                        household_id: row.get(0)?,
                        source: row.get(1)?,
                        source_user_id: row.get(2)?,
                        last_success_at: row.get(3)?,
                        last_attempt_at: row.get(4)?,
                        last_cursor_json: row.get(5)?,
                        last_error: row.get(6)?,
                        updated_at: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn pantry_sync_last_success_any(
        &self,
        household_id: &str,
        source: &str,
    ) -> Result<Option<i64>> {
        let household = self.resolve_household(household_id)?;
        let source = source.trim().to_ascii_lowercase();
        if source.is_empty() {
            return Ok(None);
        }
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT MAX(last_success_at)
                 FROM meal_pantry_sync_state
                 WHERE household_id=?1 AND source=?2",
                params![household, source],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(Into::into)
        })
    }

    pub fn upsert_pantry_sync_state(
        &self,
        household_id: &str,
        source: &str,
        source_user_id: Option<&str>,
        last_success_at: Option<i64>,
        last_attempt_at: Option<i64>,
        last_cursor_json: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<()> {
        let household = self.resolve_household(household_id)?;
        let source = source.trim().to_ascii_lowercase();
        if source.is_empty() {
            anyhow::bail!("source must not be empty");
        }
        let source_user_id = Self::normalize_sync_state_user_id(source_user_id);
        let now = unix_now();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO meal_pantry_sync_state (
                    household_id, source, source_user_id, last_success_at, last_attempt_at,
                    last_cursor_json, last_error, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(household_id, source, source_user_id) DO UPDATE SET
                    last_success_at=CASE
                        WHEN excluded.last_success_at IS NULL THEN meal_pantry_sync_state.last_success_at
                        ELSE excluded.last_success_at
                    END,
                    last_attempt_at=excluded.last_attempt_at,
                    last_cursor_json=COALESCE(excluded.last_cursor_json, meal_pantry_sync_state.last_cursor_json),
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at",
                params![
                    household,
                    source,
                    source_user_id,
                    last_success_at,
                    last_attempt_at.unwrap_or(now),
                    last_cursor_json,
                    last_error,
                    now
                ],
            )?;
            Ok(())
        })
    }

    pub fn enqueue_pantry_image_ingest(&self, message: &ChannelMessage) -> Result<()> {
        let (_, image_refs) = crate::multimodal::parse_image_markers(message.content.as_str());
        if image_refs.is_empty() {
            return Ok(());
        }
        let household_id = self.household_id().to_string();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO meal_pantry_image_ingest_state (
                    household_id, channel, source_message_id, status, attempt_count,
                    last_error, processed_at, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, 'pending', 0, NULL, NULL, ?4, ?4)
                 ON CONFLICT(household_id, channel, source_message_id) DO NOTHING",
                params![household_id, message.channel, message.id, unix_now()],
            )?;
            Ok(())
        })
    }

    pub fn claim_pending_pantry_image_jobs(
        &self,
        limit: u32,
        max_attempts: u32,
    ) -> Result<Vec<PantryImageIngestJob>> {
        let household_id = self.household_id().to_string();
        let clamped_limit = limit.clamp(1, 32);
        let clamped_max_attempts = max_attempts.max(1);
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let jobs = {
                let mut stmt = tx.prepare(
                    "SELECT channel, source_message_id, attempt_count
                     FROM meal_pantry_image_ingest_state
                     WHERE household_id=?1
                       AND status='pending'
                       AND attempt_count<?2
                     ORDER BY updated_at ASC
                     LIMIT ?3",
                )?;
                let rows = stmt.query_map(
                    params![
                        household_id,
                        i64::from(clamped_max_attempts),
                        i64::from(clamped_limit)
                    ],
                    |row| {
                        let attempt_count: i64 = row.get(2)?;
                        Ok(PantryImageIngestJob {
                            channel: row.get(0)?,
                            source_message_id: row.get(1)?,
                            attempt_count: u32::try_from(attempt_count.max(0)).unwrap_or(u32::MAX),
                        })
                    },
                )?;
                let mut jobs = Vec::new();
                for row in rows {
                    jobs.push(row?);
                }
                jobs
            };
            let now = unix_now();
            for job in &jobs {
                tx.execute(
                    "UPDATE meal_pantry_image_ingest_state
                     SET status='processing',
                         attempt_count=attempt_count+1,
                         updated_at=?4
                     WHERE household_id=?1 AND channel=?2 AND source_message_id=?3",
                    params![household_id, job.channel, job.source_message_id, now],
                )?;
            }
            tx.commit()?;
            Ok(jobs)
        })
    }

    pub fn mark_pantry_image_job_pending(
        &self,
        channel: &str,
        source_message_id: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let household_id = self.household_id().to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_pantry_image_ingest_state
                 SET status='pending',
                     last_error=?4,
                     updated_at=?5
                 WHERE household_id=?1 AND channel=?2 AND source_message_id=?3",
                params![
                    household_id,
                    channel.trim(),
                    source_message_id.trim(),
                    error,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    pub fn mark_pantry_image_job_done(&self, channel: &str, source_message_id: &str) -> Result<()> {
        let household_id = self.household_id().to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_pantry_image_ingest_state
                 SET status='done',
                     last_error=NULL,
                     processed_at=?4,
                     updated_at=?4
                 WHERE household_id=?1 AND channel=?2 AND source_message_id=?3",
                params![
                    household_id,
                    channel.trim(),
                    source_message_id.trim(),
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    pub fn mark_pantry_image_job_failed(
        &self,
        channel: &str,
        source_message_id: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let household_id = self.household_id().to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_pantry_image_ingest_state
                 SET status='failed',
                     last_error=?4,
                     processed_at=?5,
                     updated_at=?5
                 WHERE household_id=?1 AND channel=?2 AND source_message_id=?3",
                params![
                    household_id,
                    channel.trim(),
                    source_message_id.trim(),
                    error,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    pub async fn context_get(&self, query: ContextQuery) -> Result<ContextBundle> {
        let mut query = query;
        query.limits.max_preferences = query
            .limits
            .max_preferences
            .clamp(1, MAX_CONTEXT_PREFERENCES);
        query.limits.max_episodes = query.limits.max_episodes.clamp(1, MAX_CONTEXT_EPISODES);
        query.limits.max_pantry_items = query
            .limits
            .max_pantry_items
            .clamp(1, MAX_CONTEXT_PANTRY_ITEMS);
        query.limits.max_feedback = query.limits.max_feedback.clamp(1, MAX_CONTEXT_FEEDBACK);
        query.limits.max_ingress_window = query
            .limits
            .max_ingress_window
            .clamp(1, MAX_CONTEXT_INGRESS_WINDOW);
        let household = self.resolve_household(query.household_id.as_str())?;
        let active_users: HashSet<String> = query
            .active_user_ids
            .iter()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .collect();
        let focus_terms: Vec<String> = query
            .focus_terms
            .iter()
            .map(|v| v.to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .collect();
        let fts_query = build_fts_query(&focus_terms);
        let (
            preferences_by_user,
            episode_candidates,
            pantry_snapshot,
            pantry_uncertain,
            pantry_unavailable_recent,
            pantry_metrics,
            recent_feedback,
            open_decision_state,
            ingress_candidates,
            episode_keyword_hits,
            ingress_keyword_hits,
            unresolved_questions,
        ) = {
            let conn = self.open_conn()?;

            let episode_keyword_hits = if let Some(ref q) = fts_query {
                search_doc_refs(
                    &conn,
                    household.as_str(),
                    "episode",
                    q,
                    query.limits.max_episodes.saturating_mul(8),
                )?
            } else {
                HashMap::new()
            };
            let ingress_keyword_hits = if let Some(ref q) = fts_query {
                search_doc_refs(
                    &conn,
                    household.as_str(),
                    "ingress",
                    q,
                    query.limits.max_ingress_window.saturating_mul(8),
                )?
            } else {
                HashMap::new()
            };

            let mut preferences_by_user: HashMap<String, Vec<UserPreference>> = HashMap::new();
            let mut pref_stmt = conn.prepare(
                "SELECT user_id, pref_key, pref_value, confidence, updated_at
                 FROM meal_preferences
                 WHERE household_id=?1
                 ORDER BY updated_at DESC
                 LIMIT ?2",
            )?;
            let pref_rows = pref_stmt.query_map(
                params![household, i64::from(query.limits.max_preferences)],
                |row| {
                    Ok(UserPreference {
                        user_id: row.get(0)?,
                        key: row.get(1)?,
                        value: row.get(2)?,
                        confidence: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                },
            )?;
            for pref in pref_rows {
                let pref = pref?;
                if !active_users.is_empty() && !active_users.contains(pref.user_id.as_str()) {
                    continue;
                }
                preferences_by_user
                    .entry(pref.user_id.clone())
                    .or_default()
                    .push(pref);
            }

            let mut episode_candidates = Vec::new();
            let mut episode_stmt = conn.prepare(
                "SELECT meal_anchor_key, meal_name, slot, meal_date_local, tags_json, decided_by, source_message_id, created_at
                 FROM meal_episodes
                 WHERE household_id=?1
                 ORDER BY created_at DESC
                 LIMIT ?2",
            )?;
            let episode_scan_limit = if focus_terms.is_empty() {
                query.limits.max_episodes
            } else {
                query.limits.max_episodes.saturating_mul(8)
            };
            let episode_rows = episode_stmt.query_map(
                params![household, i64::from(episode_scan_limit.max(1))],
                |row| {
                    let tags_raw: String = row.get(4)?;
                    let tags = serde_json::from_str::<Vec<String>>(&tags_raw).unwrap_or_default();
                    Ok(MealEpisode {
                        meal_anchor_key: row.get(0)?,
                        meal_name: row.get(1)?,
                        slot: row.get(2)?,
                        meal_date_local: row.get(3)?,
                        tags,
                        decided_by: row.get(5)?,
                        source_message_id: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                },
            )?;
            for episode in episode_rows {
                episode_candidates.push(episode?);
            }

            let mut pantry_snapshot = Vec::new();
            let mut pantry_uncertain = Vec::new();
            let mut pantry_unavailable_recent = Vec::new();
            let mut pantry_state_stmt = conn.prepare(
                "SELECT
                    s.item_canonical,
                    s.display_name,
                    s.state,
                    s.availability_score,
                    s.perishability_class,
                    s.last_observed_at,
                    (SELECT o.source
                     FROM meal_pantry_observations o
                     WHERE o.household_id=s.household_id AND o.item_canonical=s.item_canonical
                     ORDER BY o.observed_at DESC, o.id DESC
                     LIMIT 1) AS source,
                    (SELECT o.quantity_hint
                     FROM meal_pantry_observations o
                     WHERE o.household_id=s.household_id AND o.item_canonical=s.item_canonical
                     ORDER BY o.observed_at DESC, o.id DESC
                     LIMIT 1) AS quantity_hint,
                    (SELECT o.freshness_profile
                     FROM meal_pantry_observations o
                     WHERE o.household_id=s.household_id AND o.item_canonical=s.item_canonical
                     ORDER BY o.observed_at DESC, o.id DESC
                     LIMIT 1) AS freshness_profile
                 FROM meal_pantry_state s
                 WHERE s.household_id=?1
                 ORDER BY
                    CASE s.state
                      WHEN 'likely_available' THEN 0
                      WHEN 'uncertain' THEN 1
                      ELSE 2
                    END,
                    s.availability_score DESC,
                    s.updated_at DESC
                 LIMIT ?2",
            )?;
            let pantry_scan_limit = query.limits.max_pantry_items.saturating_mul(3).max(12);
            let pantry_rows = pantry_state_stmt.query_map(
                params![household, i64::from(pantry_scan_limit)],
                |row| {
                    Ok(PantryStateRow {
                        item_canonical: row.get(0)?,
                        display_name: row.get(1)?,
                        state: row.get(2)?,
                        availability_score: row.get(3)?,
                        perishability_class: row.get(4)?,
                        last_observed_at: row.get(5)?,
                        source: row.get(6)?,
                        quantity_hint: row.get(7)?,
                        freshness_profile: row.get(8)?,
                    })
                },
            )?;
            let mut pantry_likely_count = 0usize;
            let mut pantry_uncertain_count = 0usize;
            let mut pantry_unavailable_count = 0usize;
            let mut pantry_avg_likely_score = 0.0_f64;
            for row in pantry_rows {
                let row = row?;
                let hint = PantryHint {
                    item_name: if row.display_name.trim().is_empty() {
                        row.item_canonical.clone()
                    } else {
                        row.display_name.clone()
                    },
                    quantity_hint: row.quantity_hint.clone(),
                    source: row.source.clone(),
                    freshness_profile: row.freshness_profile.clone().or(row.perishability_class),
                    updated_at: row.last_observed_at,
                };
                match row.state.as_str() {
                    "likely_available" => {
                        if pantry_snapshot.len() < query.limits.max_pantry_items as usize {
                            pantry_snapshot.push(hint);
                            pantry_avg_likely_score += row.availability_score;
                        }
                        pantry_likely_count += 1;
                    }
                    "uncertain" => {
                        if pantry_uncertain.len() < query.limits.max_pantry_items as usize {
                            pantry_uncertain.push(hint);
                        }
                        pantry_uncertain_count += 1;
                    }
                    _ => {
                        if pantry_unavailable_recent.len() < 20 {
                            pantry_unavailable_recent.push(hint);
                        }
                        pantry_unavailable_count += 1;
                    }
                }
            }
            let pantry_metrics = serde_json::json!({
                "counts": {
                    "likely_available": pantry_likely_count,
                    "uncertain": pantry_uncertain_count,
                    "likely_unavailable": pantry_unavailable_count
                },
                "avg_likely_available_score": if pantry_snapshot.is_empty() {
                    0.0
                } else {
                    pantry_avg_likely_score / pantry_snapshot.len() as f64
                }
            });

            let mut recent_feedback = Vec::new();
            let mut feedback_stmt = conn.prepare(
                "SELECT meal_anchor_key, user_id, rating, feedback_text, created_at
                 FROM meal_feedback_signals
                 WHERE household_id=?1
                 ORDER BY created_at DESC
                 LIMIT ?2",
            )?;
            let feedback_rows = feedback_stmt.query_map(
                params![household, i64::from(query.limits.max_feedback)],
                |row| {
                    Ok(MealFeedback {
                        meal_anchor_key: row.get(0)?,
                        user_id: row.get(1)?,
                        rating: row.get(2)?,
                        feedback_text: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                },
            )?;
            for row in feedback_rows {
                recent_feedback.push(row?);
            }

            let open_decision_state: Option<OpenDecisionState> = conn
                .query_row(
                    "SELECT decision_case_id, meal_anchor_key, case_seq, status, updated_at
                     FROM meal_decision_states
                     WHERE household_id=?1 AND status='open'
                     ORDER BY updated_at DESC
                     LIMIT 1",
                    params![household],
                    |row| {
                        Ok(OpenDecisionState {
                            decision_case_id: row.get(0)?,
                            meal_anchor_key: row.get(1)?,
                            case_seq: row.get(2)?,
                            status: row.get(3)?,
                            updated_at: row.get(4)?,
                        })
                    },
                )
                .optional()?;

            let mut ingress_candidates = Vec::new();
            let mut ingress_stmt = conn.prepare(
                "SELECT message_id, sender, content, timestamp, channel, thread_ts
                 FROM meal_ingress_events
                 WHERE household_id=?1
                 ORDER BY created_at DESC
                 LIMIT ?2",
            )?;
            let ingress_scan_limit = if focus_terms.is_empty() {
                query.limits.max_ingress_window
            } else {
                query.limits.max_ingress_window.saturating_mul(8)
            };
            let ingress_rows = ingress_stmt.query_map(
                params![household, i64::from(ingress_scan_limit.max(1))],
                |row| {
                    Ok(IngressEvent {
                        message_id: row.get(0)?,
                        sender: row.get(1)?,
                        content: row.get(2)?,
                        timestamp: row.get(3)?,
                        channel: row.get(4)?,
                        thread_ts: row.get(5)?,
                    })
                },
            )?;
            for row in ingress_rows {
                ingress_candidates.push(row?);
            }

            let unresolved_cutoff = unix_now().saturating_sub(UNRESOLVED_CLAIM_TTL_SECS);
            let mut unresolved_questions = Vec::new();
            let mut unresolved_stmt = conn.prepare(
                "SELECT question
                 FROM meal_unresolved_claims
                 WHERE household_id=?1 AND status='open' AND created_at>=?2
                 ORDER BY created_at DESC
                 LIMIT 3",
            )?;
            let unresolved_rows = unresolved_stmt
                .query_map(params![household, unresolved_cutoff], |row| {
                    row.get::<_, String>(0)
                })?;
            for row in unresolved_rows {
                let question = row?;
                let normalized = question.trim();
                if normalized.is_empty() {
                    continue;
                }
                unresolved_questions.push(normalized.to_string());
            }

            Ok::<_, anyhow::Error>((
                preferences_by_user,
                episode_candidates,
                pantry_snapshot,
                pantry_uncertain,
                pantry_unavailable_recent,
                pantry_metrics,
                recent_feedback,
                open_decision_state,
                ingress_candidates,
                episode_keyword_hits,
                ingress_keyword_hits,
                unresolved_questions,
            ))?
        };

        let mut retrieval_strategy = if focus_terms.is_empty() {
            "recency_only".to_string()
        } else {
            "fts5_bm25".to_string()
        };
        let mut semantic_enabled = false;
        let mut semantic_applied = false;
        let mut episode_scores = episode_keyword_hits.clone();
        let mut ingress_scores = ingress_keyword_hits.clone();
        let mut semantic_episode_hits = 0usize;
        let mut semantic_ingress_hits = 0usize;

        if !focus_terms.is_empty() {
            if let Some(embedder) = self.create_semantic_embedder() {
                semantic_enabled = true;
                let query_text = focus_terms.join(" ");
                let episode_docs: Vec<(String, String)> = episode_candidates
                    .iter()
                    .map(|episode| {
                        (
                            episode.meal_anchor_key.clone(),
                            format!(
                                "{} {} {} {}",
                                episode.meal_name,
                                episode.tags.join(" "),
                                episode.slot.clone().unwrap_or_default(),
                                episode.meal_date_local.clone().unwrap_or_default()
                            ),
                        )
                    })
                    .collect();
                let ingress_docs: Vec<(String, String)> = ingress_candidates
                    .iter()
                    .map(|event| {
                        (
                            event.message_id.clone(),
                            format!("{} {}", event.sender, event.content),
                        )
                    })
                    .collect();

                match self
                    .hybrid_rank_docs(
                        embedder.as_ref(),
                        query_text.as_str(),
                        episode_docs.as_slice(),
                        &episode_keyword_hits,
                        query.limits.max_episodes.max(1) as usize,
                    )
                    .await
                {
                    Ok(scores) => {
                        semantic_episode_hits = scores.len();
                        episode_scores = scores;
                        semantic_applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            component = "meal_context_read",
                            household = %household,
                            error = %error,
                            "semantic hybrid ranking failed for episodes; falling back to bm25"
                        );
                    }
                }

                match self
                    .hybrid_rank_docs(
                        embedder.as_ref(),
                        query_text.as_str(),
                        ingress_docs.as_slice(),
                        &ingress_keyword_hits,
                        query.limits.max_ingress_window.max(1) as usize,
                    )
                    .await
                {
                    Ok(scores) => {
                        semantic_ingress_hits = scores.len();
                        ingress_scores = scores;
                        semantic_applied = true;
                    }
                    Err(error) => {
                        tracing::warn!(
                            component = "meal_context_read",
                            household = %household,
                            error = %error,
                            "semantic hybrid ranking failed for ingress; falling back to bm25"
                        );
                    }
                }
            }
        }

        if semantic_applied {
            retrieval_strategy = "hybrid_semantic_bm25".to_string();
        }

        let recent_meal_episodes = if focus_terms.is_empty() {
            episode_candidates
                .into_iter()
                .take(query.limits.max_episodes as usize)
                .collect::<Vec<_>>()
        } else {
            let mut scored: Vec<(f64, MealEpisode)> = episode_candidates
                .into_iter()
                .filter_map(|episode| {
                    episode_scores
                        .get(episode.meal_anchor_key.as_str())
                        .copied()
                        .map(|score| (score, episode))
                })
                .collect();
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
            scored
                .into_iter()
                .take(query.limits.max_episodes as usize)
                .map(|(_, episode)| episode)
                .collect::<Vec<_>>()
        };

        let recent_ingress_window = if focus_terms.is_empty() {
            ingress_candidates
                .into_iter()
                .take(query.limits.max_ingress_window as usize)
                .collect::<Vec<_>>()
        } else {
            let mut scored: Vec<(f64, IngressEvent)> = ingress_candidates
                .into_iter()
                .filter_map(|event| {
                    let ref_key =
                        ingress_doc_ref(event.channel.as_str(), event.message_id.as_str());
                    ingress_scores
                        .get(ref_key.as_str())
                        .copied()
                        .map(|score| (score, event))
                })
                .collect();
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
            scored
                .into_iter()
                .take(query.limits.max_ingress_window as usize)
                .map(|(_, event)| event)
                .collect::<Vec<_>>()
        };

        let mut grounding_refs = Vec::new();
        grounding_refs.extend(
            recent_meal_episodes
                .iter()
                .map(|episode| format!("episode:{}", episode.meal_anchor_key)),
        );
        grounding_refs.extend(
            recent_feedback.iter().map(|feedback| {
                format!("feedback:{}:{}", feedback.meal_anchor_key, feedback.user_id)
            }),
        );
        let retrieval_metrics = serde_json::json!({
            "strategy": retrieval_strategy,
            "focus_terms": focus_terms,
            "fts_query": fts_query,
            "semantic": {
                "enabled": semantic_enabled,
                "applied": semantic_applied
            },
            "match_counts": {
                "episodes_keyword": episode_keyword_hits.len(),
                "episodes_semantic": semantic_episode_hits,
                "ingress_keyword": ingress_keyword_hits.len(),
                "ingress_semantic": semantic_ingress_hits
            },
            "returned_counts": {
                "preferences": preferences_by_user.values().map(|v| v.len()).sum::<usize>(),
                "episodes": recent_meal_episodes.len(),
                "pantry_likely_available": pantry_snapshot.len(),
                "pantry_uncertain": pantry_uncertain.len(),
                "pantry_likely_unavailable_recent": pantry_unavailable_recent.len(),
                "feedback": recent_feedback.len(),
                "ingress": recent_ingress_window.len(),
                "unresolved_questions": unresolved_questions.len()
            }
        });

        Ok(ContextBundle {
            household_id: household,
            preferences_by_user,
            recent_meal_episodes,
            pantry_snapshot,
            pantry_uncertain,
            pantry_unavailable_recent,
            pantry_metrics,
            recent_feedback,
            open_decision_state,
            recent_ingress_window,
            grounding_refs,
            retrieval_metrics,
            unresolved_questions,
        })
    }

    pub async fn recommend(&self, mut request: RecommendRequest) -> Result<RecommendResponse> {
        request.household_id = self.resolve_household(request.household_id.as_str())?;
        let context = if let Some(bundle) = request.context_bundle.clone() {
            bundle
        } else {
            self.context_get(ContextQuery {
                household_id: request.household_id.clone(),
                ..ContextQuery::default()
            })
            .await?
        };

        let mut candidates = request.candidate_set;
        if candidates.is_empty() {
            let mut seen = HashSet::new();
            for episode in &context.recent_meal_episodes {
                let key = episode.meal_name.to_ascii_lowercase();
                if seen.insert(key) {
                    candidates.push(RecommendationCandidateInput {
                        meal_name: episode.meal_name.clone(),
                        tags: episode.tags.clone(),
                    });
                }
            }
        }
        if candidates.is_empty() {
            let context_counts = serde_json::json!({
                "preferences": context.preferences_by_user.values().map(|v| v.len()).sum::<usize>(),
                "episodes": context.recent_meal_episodes.len(),
                "pantry_likely_available": context.pantry_snapshot.len(),
                "pantry_uncertain": context.pantry_uncertain.len(),
                "pantry_likely_unavailable_recent": context.pantry_unavailable_recent.len(),
                "feedback": context.recent_feedback.len(),
                "ingress": context.recent_ingress_window.len(),
                "unresolved_questions": context.unresolved_questions.len()
            });
            return Ok(RecommendResponse {
                ranked_candidates: Vec::new(),
                score_breakdown: HashMap::new(),
                fairness_metrics: serde_json::json!({
                    "min_user_fit": 0.0,
                    "avg_user_fit": 0.0
                }),
                grounding_metrics: serde_json::json!({
                    "context_counts": context_counts,
                    "grounding_ref_count": context.grounding_refs.len(),
                    "retrieval": context.retrieval_metrics
                }),
                grounding_refs: context.grounding_refs,
                warnings: vec![
                    "No recommendation candidates available.".to_string(),
                    "Generate candidate_set first (e.g., with contextual brainstorming or web_search_tool/web_fetch) and call meal_options_rank again."
                        .to_string(),
                ],
            });
        }

        let mut feedback_by_meal: HashMap<String, Vec<f64>> = HashMap::new();
        let meal_name_by_anchor: HashMap<String, String> = context
            .recent_meal_episodes
            .iter()
            .map(|e| (e.meal_anchor_key.clone(), e.meal_name.to_ascii_lowercase()))
            .collect();
        for feedback in &context.recent_feedback {
            if let Some(meal_name) = meal_name_by_anchor.get(&feedback.meal_anchor_key) {
                if let Some(rating) = feedback.rating {
                    feedback_by_meal
                        .entry(meal_name.clone())
                        .or_default()
                        .push(rating);
                }
            }
        }

        let recent_names: Vec<String> = context
            .recent_meal_episodes
            .iter()
            .take(3)
            .map(|e| e.meal_name.to_ascii_lowercase())
            .collect();

        let mut ranked = Vec::new();
        let mut score_breakdown = HashMap::new();
        let now_ts = unix_now();
        let pantry_likely_total = context.pantry_snapshot.len();
        let pantry_uncertain_total = context.pantry_uncertain.len();
        let pantry_unavailable_total = context.pantry_unavailable_recent.len();
        let pantry_stale_count = context
            .pantry_snapshot
            .iter()
            .filter(|item| {
                pantry_freshness_weight(item.updated_at, now_ts, item.freshness_profile.as_deref())
                    < 0.35
            })
            .count();
        let pantry_recent_count = context
            .pantry_snapshot
            .iter()
            .filter(|item| {
                pantry_freshness_weight(item.updated_at, now_ts, item.freshness_profile.as_deref())
                    >= 0.75
            })
            .count();
        let mut warnings = Vec::new();
        if pantry_likely_total > 0 && pantry_stale_count > 0 {
            warnings.push(format!(
                "{pantry_stale_count}/{pantry_likely_total} likely-available pantry items are stale; treat pantry assumptions as tentative."
            ));
        }
        if pantry_likely_total > 0 && pantry_recent_count == 0 {
            warnings.push(
                "No recent pantry updates found; include fallback options that do not depend on pantry availability."
                    .to_string(),
            );
        }
        if pantry_uncertain_total > 0 {
            warnings.push(format!(
                "{pantry_uncertain_total} pantry item(s) are uncertain; ask one quick confirmation for critical ingredients."
            ));
        }
        if pantry_likely_total == 0 && pantry_unavailable_total > 0 {
            warnings.push(
                "Recent pantry evidence suggests many items may be unavailable; include robust no-pantry fallback options."
                    .to_string(),
            );
        }
        let mut blocked_candidates = Vec::new();
        for candidate in candidates {
            let lc_name = candidate.meal_name.to_ascii_lowercase();
            let tag_text = candidate.tags.join(" ").to_ascii_lowercase();
            let searchable = format!("{lc_name} {tag_text}");
            let searchable_tokens = tokenize_meal_text(searchable.as_str());

            let mut positive_fit = 0.0;
            let mut negative_penalty = 0.0;
            let mut scalar_fit = 0.0;
            let mut per_user_fit = Vec::new();
            let mut constraint_reasons = Vec::new();
            for prefs in context.preferences_by_user.values() {
                let mut user_fit = 0.0;
                for pref in prefs {
                    let kind = preference_kind_for_key(pref.key.as_str());
                    let weight = (1.0_f64).max(pref.confidence);
                    match kind {
                        PreferenceKind::Positive => {
                            if preference_value_matches(
                                searchable.as_str(),
                                &searchable_tokens,
                                pref.value.as_str(),
                            ) {
                                positive_fit += weight;
                                user_fit += weight;
                            }
                        }
                        PreferenceKind::Negative => {
                            if preference_value_matches(
                                searchable.as_str(),
                                &searchable_tokens,
                                pref.value.as_str(),
                            ) {
                                let penalty = 1.25 * weight;
                                negative_penalty += penalty;
                                user_fit -= penalty;
                            }
                        }
                        PreferenceKind::Constraint => {
                            let direct_violation = preference_value_matches(
                                searchable.as_str(),
                                &searchable_tokens,
                                pref.value.as_str(),
                            );
                            let dietary_violation = violates_dietary_restriction(
                                &searchable_tokens,
                                pref.value.as_str(),
                            );
                            if direct_violation || dietary_violation {
                                constraint_reasons.push(format!("{}={}", pref.key, pref.value));
                            }
                        }
                        PreferenceKind::Scalar => {
                            let delta = scalar_preference_delta(
                                &searchable_tokens,
                                pref.key.as_str(),
                                pref.value.as_str(),
                            ) * weight;
                            if delta != 0.0 {
                                scalar_fit += delta;
                                user_fit += delta;
                            }
                        }
                    }
                }
                per_user_fit.push(user_fit);
            }
            let constraint_blocked = !constraint_reasons.is_empty();

            let mut pantry_score = 0.0;
            let mut pantry_match_count = 0usize;
            let mut pantry_low_confidence_matches = 0usize;
            let mut pantry_match_preview = Vec::new();
            for item in &context.pantry_snapshot {
                let item_lc = item.item_name.to_ascii_lowercase();
                let item_tokens = tokenize_meal_text(item_lc.as_str());
                let direct_match = searchable.contains(item_lc.as_str());
                let token_match = !item_tokens.is_empty()
                    && item_tokens.iter().any(|t| searchable_tokens.contains(t));
                if !direct_match && !token_match {
                    continue;
                }
                let confidence = pantry_item_confidence(item, now_ts);
                pantry_score += 1.2 * confidence;
                pantry_match_count += 1;
                if confidence < 0.45 {
                    pantry_low_confidence_matches += 1;
                }
                if pantry_match_preview.len() < 4 {
                    pantry_match_preview.push(format!("{}@{confidence:.2}", item.item_name));
                }
            }
            pantry_score = pantry_score.min(PANTRY_MAX_SCORE_CONTRIBUTION);

            let repeat_penalty = if recent_names.iter().any(|n| n == &lc_name) {
                -2.0
            } else {
                0.0
            };

            let feedback_bonus = feedback_by_meal
                .get(&lc_name)
                .map(|ratings| ratings.iter().sum::<f64>() / ratings.len() as f64)
                .unwrap_or(0.0);

            let total_score = positive_fit + scalar_fit - negative_penalty
                + pantry_score
                + repeat_penalty
                + feedback_bonus;
            let min_fit = per_user_fit.iter().copied().fold(f64::INFINITY, f64::min);
            let avg_fit = if per_user_fit.is_empty() {
                0.0
            } else {
                per_user_fit.iter().sum::<f64>() / per_user_fit.len() as f64
            };
            let min_fit = if min_fit.is_infinite() { 0.0 } else { min_fit };

            score_breakdown.insert(
                candidate.meal_name.clone(),
                serde_json::json!({
                    "positive_fit": positive_fit,
                    "negative_penalty": negative_penalty,
                    "scalar_fit": scalar_fit,
                    "constraint_blocked": constraint_blocked,
                    "constraint_reasons": constraint_reasons,
                    "pantry_fit": pantry_score,
                    "pantry_match_count": pantry_match_count,
                    "pantry_low_confidence_matches": pantry_low_confidence_matches,
                    "pantry_match_preview": pantry_match_preview,
                    "repeat_penalty": repeat_penalty,
                    "feedback_bonus": feedback_bonus,
                    "min_user_fit": min_fit,
                    "sum_user_fit": per_user_fit.iter().sum::<f64>(),
                    "total": total_score,
                }),
            );

            if constraint_blocked {
                blocked_candidates.push(candidate.meal_name.clone());
                continue;
            }

            let pantry_reason = if pantry_match_count == 0 {
                "pantry_matches=0".to_string()
            } else {
                format!(
                    "pantry_matches={pantry_match_count} low_conf={pantry_low_confidence_matches}"
                )
            };
            ranked.push(RankedCandidate {
                meal_name: candidate.meal_name,
                tags: candidate.tags,
                score: total_score,
                fairness_floor: min_fit,
                avg_user_fit: avg_fit,
                reasons: vec![
                    format!("positive_fit={positive_fit:.2}"),
                    format!("negative_penalty={negative_penalty:.2}"),
                    format!("scalar_fit={scalar_fit:.2}"),
                    format!("pantry_fit={pantry_score:.2}"),
                    pantry_reason,
                    format!("feedback_bonus={feedback_bonus:.2}"),
                ],
            });
        }
        ranked.sort_by(|a, b| {
            b.fairness_floor
                .total_cmp(&a.fairness_floor)
                .then_with(|| b.score.total_cmp(&a.score))
                .then_with(|| a.meal_name.cmp(&b.meal_name))
        });
        if !blocked_candidates.is_empty() {
            warnings.push(format!(
                "{} candidate(s) blocked by hard constraints: {}",
                blocked_candidates.len(),
                blocked_candidates.join(", ")
            ));
        }
        let min_user_fit = ranked
            .iter()
            .map(|c| c.fairness_floor)
            .fold(f64::INFINITY, f64::min);
        let avg_user_fit = if ranked.is_empty() {
            0.0
        } else {
            ranked.iter().map(|c| c.avg_user_fit).sum::<f64>() / ranked.len() as f64
        };
        let context_counts = serde_json::json!({
            "preferences": context.preferences_by_user.values().map(|v| v.len()).sum::<usize>(),
            "episodes": context.recent_meal_episodes.len(),
            "pantry_likely_available": context.pantry_snapshot.len(),
            "pantry_uncertain": context.pantry_uncertain.len(),
            "pantry_likely_unavailable_recent": context.pantry_unavailable_recent.len(),
            "feedback": context.recent_feedback.len(),
            "ingress": context.recent_ingress_window.len(),
            "unresolved_questions": context.unresolved_questions.len()
        });

        Ok(RecommendResponse {
            ranked_candidates: ranked,
            score_breakdown,
            fairness_metrics: serde_json::json!({
                "min_user_fit": if min_user_fit.is_infinite() { 0.0 } else { min_user_fit },
                "avg_user_fit": avg_user_fit
            }),
            grounding_metrics: serde_json::json!({
                "context_counts": context_counts,
                "grounding_ref_count": context.grounding_refs.len(),
                "retrieval": context.retrieval_metrics
            }),
            grounding_refs: context.grounding_refs,
            warnings,
        })
    }

    pub fn commit(&self, mut request: CommitRequest) -> Result<CommitResponse> {
        request.household_id = self.resolve_household(request.household_id.as_str())?;
        if request.source_message_id.trim().is_empty() {
            anyhow::bail!("source_message_id must be provided");
        }
        if request.operations.is_empty() {
            anyhow::bail!("operations must not be empty");
        }
        if request.meal_date_local.trim().is_empty() {
            request.meal_date_local = chrono::Local::now().format("%Y-%m-%d").to_string();
        }
        if request.meal_slot.trim().is_empty() {
            request.meal_slot = "unslotted".to_string();
        }

        self.with_conn(|conn| {
            let anchor_key = format!(
                "{}:{}:{}:{}",
                request.household_id,
                request.meal_date_local,
                request.meal_slot,
                request.slot_instance
            );
            let commit_kind = "meal_memory_write";
            let op_fingerprint = sha256_hex(&canonical_json(&Value::Array(request.operations.clone())));
            let mut request_for_hash = request.clone();
            request_for_hash.client_idempotency_key = None;
            let request_hash =
                sha256_hex(&canonical_json(&serde_json::to_value(&request_for_hash)?));
            let idempotency_scope = request
                .client_idempotency_key
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(request.source_message_id.as_str());
            let effective_idempotency_key = sha256_hex(&format!(
                "{}:{}:{}:{}",
                request.household_id,
                anchor_key,
                op_fingerprint,
                idempotency_scope
            ));

            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let existing_fence: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT request_hash, response_json
                     FROM meal_memory_write_fence
                     WHERE household_id=?1 AND source_message_id=?2 AND commit_kind=?3",
                    params![
                        request.household_id,
                        request.source_message_id,
                        commit_kind
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((fence_request_hash, fence_response_json)) = existing_fence {
                if fence_request_hash != request_hash {
                    return Ok(CommitResponse {
                        commit_status: "conflict".to_string(),
                        effective_idempotency_key: effective_idempotency_key.clone(),
                        meal_anchor_key: anchor_key.clone(),
                        decision_case_id: None,
                        operation_results: Vec::new(),
                        warnings: vec![
                            "source_message_id already used by a different commit payload."
                                .to_string(),
                        ],
                    });
                }

                if let Some(response_json) = fence_response_json {
                    let mut prior: CommitResponse = serde_json::from_str(&response_json)
                        .unwrap_or_else(|_| CommitResponse::default_noop());
                    prior.commit_status = "noop_duplicate".to_string();
                    prior.effective_idempotency_key = effective_idempotency_key.clone();
                    prior.meal_anchor_key = anchor_key.clone();
                    return Ok(prior);
                }

                return Ok(CommitResponse {
                    commit_status: "in_progress_retry".to_string(),
                    effective_idempotency_key: effective_idempotency_key.clone(),
                    meal_anchor_key: anchor_key.clone(),
                    decision_case_id: None,
                    operation_results: Vec::new(),
                    warnings: vec![
                        "Commit for this source_message_id is already in progress; retry."
                            .to_string(),
                    ],
                });
            }
            tx.execute(
                "INSERT INTO meal_memory_write_fence (
                    household_id, source_message_id, commit_kind, request_hash, response_json,
                    status, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, NULL, 'in_progress', ?5, ?5)",
                params![
                    request.household_id,
                    request.source_message_id,
                    commit_kind,
                    request_hash,
                    unix_now()
                ],
            )?;

            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT request_hash, response_json
                     FROM meal_idempotency_keys
                     WHERE household_id=?1 AND tool_name='meal_memory_write' AND idempotency_key=?2",
                    params![request.household_id, effective_idempotency_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if let Some((existing_hash, response_json)) = existing {
                if existing_hash != request_hash {
                    return Ok(CommitResponse {
                        commit_status: "idempotency_mismatch".to_string(),
                        effective_idempotency_key,
                        meal_anchor_key: anchor_key,
                        decision_case_id: None,
                        operation_results: Vec::new(),
                        warnings: vec![
                            "Existing idempotency key has different payload hash.".to_string()
                        ],
                    });
                }
                let mut prior: CommitResponse = serde_json::from_str(&response_json)
                    .unwrap_or_else(|_| CommitResponse::default_noop());
                prior.commit_status = "noop_duplicate".to_string();
                prior.effective_idempotency_key = effective_idempotency_key;
                prior.meal_anchor_key = anchor_key;
                return Ok(prior);
            }

            let (decision_case_id, _case_seq) = if let Some(existing) = tx
                .query_row(
                    "SELECT decision_case_id, case_seq
                     FROM meal_decision_states
                     WHERE household_id=?1 AND meal_anchor_key=?2 AND status='open'
                     ORDER BY case_seq DESC
                     LIMIT 1",
                    params![request.household_id, anchor_key],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?
            {
                existing
            } else {
                let next_case_seq: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(case_seq), 0) + 1
                     FROM meal_decision_states
                     WHERE household_id=?1 AND meal_anchor_key=?2",
                    params![request.household_id, anchor_key],
                    |row| row.get(0),
                )?;
                let next_case_id = format!("{anchor_key}:v{next_case_seq}");
                tx.execute(
                    "INSERT INTO meal_decision_states (
                        household_id, decision_case_id, meal_anchor_key, case_seq, status,
                        opened_at, closed_at, last_event_seq, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, 'open', ?5, NULL, 0, ?5)",
                    params![
                        request.household_id,
                        next_case_id,
                        anchor_key,
                        next_case_seq,
                        unix_now()
                    ],
                )?;
                (next_case_id, next_case_seq)
            };
            let mut operation_results = Vec::new();
            let mut touched_pantry_items: HashSet<String> = HashSet::new();
            for operation in &request.operations {
                let op_name = operation
                    .get("op")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("operation missing `op` field"))?;
                match op_name {
                    "record_preference" => {
                        let user_id = operation
                            .get("user_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_preference.user_id is required"))?;
                        let key = operation
                            .get("key")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_preference.key is required"))?;
                        let value = operation
                            .get("value")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_preference.value is required"))?;
                        let confidence = operation
                            .get("confidence")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.8);
                        let source = operation.get("source").and_then(Value::as_str);
                        tx.execute(
                            "INSERT INTO meal_preferences (
                                household_id, user_id, pref_key, pref_value, confidence, source, updated_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                             ON CONFLICT(household_id, user_id, pref_key, pref_value) DO UPDATE SET
                                confidence=MAX(meal_preferences.confidence, excluded.confidence),
                                source=CASE
                                    WHEN excluded.confidence >= meal_preferences.confidence THEN excluded.source
                                    ELSE meal_preferences.source
                                END,
                                updated_at=excluded.updated_at",
                            params![
                                request.household_id,
                                user_id.trim(),
                                key.trim(),
                                value.trim(),
                                confidence,
                                source,
                                unix_now()
                            ],
                        )?;
                        let pref_doc_ref = format!(
                            "{}:{}:{}",
                            user_id.trim(),
                            key.trim(),
                            sha256_hex(value.trim())
                        );
                        let pref_search_content =
                            format!("{} {} {}", user_id.trim(), key.trim(), value.trim());
                        upsert_search_doc(
                            &tx,
                            request.household_id.as_str(),
                            "preference",
                            pref_doc_ref.as_str(),
                            pref_search_content.as_str(),
                        )?;
                        operation_results.push(format!("record_preference:{user_id}:{key}"));
                    }
                    "record_pantry" => {
                        let item_name = operation
                            .get("item_name")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_pantry.item_name is required"))?;
                        let item_canonical =
                            normalize_pantry_item_canonical(item_name).ok_or_else(|| {
                                anyhow::anyhow!("record_pantry.item_name produced empty canonical key")
                            })?;
                        let quantity_hint = operation
                            .get("quantity_hint")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string);
                        let source = operation
                            .get("source")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .unwrap_or("manual_chat")
                            .to_ascii_lowercase();
                        let source_user_id = operation
                            .get("source_user_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string)
                            .or_else(|| {
                                operation
                                    .get("user_id")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(ToString::to_string)
                            });
                        let freshness_profile = normalize_pantry_freshness_profile(
                            operation.get("freshness_profile").and_then(Value::as_str),
                        );
                        let confidence = operation
                            .get("confidence")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.70)
                            .clamp(0.0, 1.0);
                        let observed_at = operation
                            .get("observed_at")
                            .and_then(Value::as_i64)
                            .filter(|value| *value > 0)
                            .map(|value| value.min(unix_now()))
                            .unwrap_or_else(unix_now);
                        let observation_hash = pantry_observation_hash(
                            request.household_id.as_str(),
                            item_canonical.as_str(),
                            source.as_str(),
                            source_user_id.as_deref(),
                            Some(request.source_message_id.as_str()),
                            observed_at,
                            quantity_hint.as_deref(),
                            freshness_profile.as_deref(),
                            confidence,
                        );
                        let metadata_json = operation
                            .get("metadata")
                            .and_then(Value::as_object)
                            .map(|_| operation.get("metadata").cloned().unwrap_or(Value::Null))
                            .map(|value| value.to_string());
                        tx.execute(
                            "INSERT INTO meal_pantry_observations (
                                household_id, item_name, item_canonical, source, source_user_id,
                                source_message_id, quantity_hint, freshness_profile, observed_at,
                                confidence, metadata_json, observation_hash, created_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                             ON CONFLICT(household_id, observation_hash) DO NOTHING",
                            params![
                                request.household_id,
                                item_name.trim(),
                                item_canonical,
                                source,
                                source_user_id,
                                request.source_message_id,
                                quantity_hint,
                                freshness_profile,
                                observed_at,
                                confidence,
                                metadata_json,
                                observation_hash,
                                unix_now()
                            ],
                        )?;
                        touched_pantry_items.insert(
                            normalize_pantry_item_canonical(item_name)
                                .unwrap_or_else(|| item_name.trim().to_ascii_lowercase()),
                        );
                        operation_results.push(format!(
                            "record_pantry:{item_name}:{}",
                            freshness_profile
                                .as_deref()
                                .unwrap_or("unknown")
                        ));
                    }
                    "record_episode" => {
                        let meal_name = operation
                            .get("meal_name")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_episode.meal_name is required"))?;
                        let decided_by = operation.get("decided_by").and_then(Value::as_str);
                        let tags = operation
                            .get("tags")
                            .and_then(Value::as_array)
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(Value::as_str)
                                    .map(ToString::to_string)
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        tx.execute(
                            "INSERT INTO meal_episodes (
                                household_id, meal_anchor_key, meal_name, slot, meal_date_local, tags_json, decided_by, source_message_id, created_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                             ON CONFLICT(household_id, meal_anchor_key) DO UPDATE SET
                                meal_name=excluded.meal_name,
                                slot=excluded.slot,
                                meal_date_local=excluded.meal_date_local,
                                tags_json=excluded.tags_json,
                                decided_by=excluded.decided_by,
                                source_message_id=excluded.source_message_id",
                            params![
                                request.household_id,
                                anchor_key,
                                meal_name.trim(),
                                request.meal_slot,
                                request.meal_date_local,
                                serde_json::to_string(&tags)?,
                                decided_by,
                                request.source_message_id,
                                unix_now()
                            ],
                        )?;
                        let episode_search_content = format!(
                            "{} {} {} {}",
                            meal_name.trim(),
                            tags.join(" "),
                            request.meal_slot,
                            request.meal_date_local
                        );
                        upsert_search_doc(
                            &tx,
                            request.household_id.as_str(),
                            "episode",
                            anchor_key.as_str(),
                            episode_search_content.as_str(),
                        )?;
                        operation_results.push(format!("record_episode:{meal_name}"));
                    }
                    "record_feedback" => {
                        let user_id = operation
                            .get("user_id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("record_feedback.user_id is required"))?;
                        let meal_anchor_key = operation
                            .get("meal_anchor_key")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                            .unwrap_or_else(|| anchor_key.clone());
                        let rating = operation.get("rating").and_then(Value::as_f64);
                        let feedback_text = operation
                            .get("feedback_text")
                            .and_then(Value::as_str)
                            .or_else(|| operation.get("text").and_then(Value::as_str));
                        tx.execute(
                            "INSERT INTO meal_feedback_signals (
                                household_id, meal_anchor_key, user_id, rating, feedback_text, created_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![
                                request.household_id,
                                meal_anchor_key,
                                user_id.trim(),
                                rating,
                                feedback_text,
                                unix_now()
                            ],
                        )?;
                        let feedback_row_id = tx.last_insert_rowid();
                        let feedback_doc_ref = format!("feedback:{feedback_row_id}");
                        let feedback_search_content = format!(
                            "{} {} {}",
                            meal_anchor_key,
                            user_id.trim(),
                            feedback_text.unwrap_or_default()
                        );
                        upsert_search_doc(
                            &tx,
                            request.household_id.as_str(),
                            "feedback",
                            feedback_doc_ref.as_str(),
                            feedback_search_content.as_str(),
                        )?;
                        if operation.get("feedback_text").is_none()
                            && operation.get("text").is_some()
                        {
                            operation_results.push(format!(
                                "record_feedback:{user_id}:alias_text_deprecated"
                            ));
                        } else {
                            operation_results.push(format!("record_feedback:{user_id}"));
                        }
                    }
                    "schedule_feedback_nudge" => {
                        let run_at = operation
                            .get("run_at")
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow::anyhow!("schedule_feedback_nudge.run_at is required"))?;
                        let run_at = DateTime::parse_from_rfc3339(run_at)
                            .with_context(|| {
                                format!(
                                    "Invalid RFC3339 schedule_feedback_nudge.run_at value: {run_at}"
                                )
                            })?
                            .with_timezone(&Utc)
                            .to_rfc3339();
                        let prompt = operation
                            .get("prompt")
                            .and_then(Value::as_str)
                            .unwrap_or("Ask for brief meal feedback from participants.")
                            .trim()
                            .to_string();
                        if prompt.is_empty() {
                            anyhow::bail!("schedule_feedback_nudge.prompt must not be empty");
                        }
                        let channel = operation
                            .get("channel")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                anyhow::anyhow!("schedule_feedback_nudge.channel is required")
                            })?
                            .trim()
                            .to_ascii_lowercase();
                        let target = operation
                            .get("target")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                anyhow::anyhow!("schedule_feedback_nudge.target is required")
                            })?
                            .trim()
                            .to_string();
                        if target.is_empty() {
                            anyhow::bail!("schedule_feedback_nudge.target must not be empty");
                        }
                        let requested_case_id = operation
                            .get("decision_case_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| decision_case_id.clone());
                        let best_effort = operation
                            .get("best_effort")
                            .and_then(Value::as_bool)
                            .unwrap_or(true);

                        tx.execute(
                            "INSERT INTO meal_nudge_jobs (
                                household_id, decision_case_id, nudge_type, run_at, prompt, channel, target,
                                best_effort, desired_state, status, cron_job_id, last_error, updated_at
                             ) VALUES (?1, ?2, 'feedback', ?3, ?4, ?5, ?6, ?7, 'scheduled', 'pending', NULL, NULL, ?8)
                             ON CONFLICT(household_id, decision_case_id, nudge_type) DO UPDATE SET
                                run_at=excluded.run_at,
                                prompt=excluded.prompt,
                                channel=excluded.channel,
                                target=excluded.target,
                                best_effort=excluded.best_effort,
                                desired_state='scheduled',
                                status='pending',
                                last_error=NULL,
                                updated_at=excluded.updated_at",
                            params![
                                request.household_id,
                                requested_case_id,
                                run_at,
                                prompt,
                                channel,
                                target,
                                if best_effort { 1 } else { 0 },
                                unix_now()
                            ],
                        )?;
                        let event_key = format!("nudge:{}:feedback", requested_case_id);
                        let event_payload = serde_json::json!({
                            "decision_case_id": requested_case_id,
                            "nudge_type": "feedback"
                        });
                        tx.execute(
                            "INSERT INTO meal_outbox_events (
                                household_id, event_key, event_type, payload_json, status,
                                attempt_count, next_attempt_at, last_error, created_at, updated_at
                             ) VALUES (?1, ?2, 'nudge_reconcile', ?3, 'pending', 0, ?4, NULL, ?4, ?4)
                             ON CONFLICT(household_id, event_key) DO UPDATE SET
                                payload_json=excluded.payload_json,
                                status='pending',
                                attempt_count=0,
                                next_attempt_at=excluded.next_attempt_at,
                                last_error=NULL,
                                updated_at=excluded.updated_at",
                            params![
                                request.household_id,
                                event_key,
                                event_payload.to_string(),
                                unix_now()
                            ],
                        )?;
                        operation_results.push(format!(
                            "schedule_feedback_nudge:{}",
                            requested_case_id
                        ));
                    }
                    "cancel_feedback_nudge" => {
                        let requested_case_id = operation
                            .get("decision_case_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| decision_case_id.clone());

                        tx.execute(
                            "INSERT INTO meal_nudge_jobs (
                                household_id, decision_case_id, nudge_type, run_at, prompt, channel, target,
                                best_effort, desired_state, status, cron_job_id, last_error, updated_at
                             ) VALUES (?1, ?2, 'feedback', NULL, NULL, NULL, NULL, 1, 'canceled', 'cancel_pending', NULL, NULL, ?3)
                             ON CONFLICT(household_id, decision_case_id, nudge_type) DO UPDATE SET
                                desired_state='canceled',
                                status='cancel_pending',
                                last_error=NULL,
                                updated_at=excluded.updated_at",
                            params![request.household_id, requested_case_id, unix_now()],
                        )?;
                        let event_key = format!("nudge:{}:feedback", requested_case_id);
                        let event_payload = serde_json::json!({
                            "decision_case_id": requested_case_id,
                            "nudge_type": "feedback"
                        });
                        tx.execute(
                            "INSERT INTO meal_outbox_events (
                                household_id, event_key, event_type, payload_json, status,
                                attempt_count, next_attempt_at, last_error, created_at, updated_at
                             ) VALUES (?1, ?2, 'nudge_reconcile', ?3, 'pending', 0, ?4, NULL, ?4, ?4)
                             ON CONFLICT(household_id, event_key) DO UPDATE SET
                                payload_json=excluded.payload_json,
                                status='pending',
                                attempt_count=0,
                                next_attempt_at=excluded.next_attempt_at,
                                last_error=NULL,
                                updated_at=excluded.updated_at",
                            params![
                                request.household_id,
                                event_key,
                                event_payload.to_string(),
                                unix_now()
                            ],
                        )?;
                        operation_results.push(format!("cancel_feedback_nudge:{}", requested_case_id));
                    }
                    other => {
                        anyhow::bail!("Unsupported commit operation `{other}`");
                    }
                }
            }

            if !touched_pantry_items.is_empty() {
                recompute_pantry_state_for_items(
                    &tx,
                    request.household_id.as_str(),
                    &touched_pantry_items,
                    unix_now(),
                )?;
            }

            let response = CommitResponse {
                commit_status: "applied".to_string(),
                effective_idempotency_key: effective_idempotency_key.clone(),
                meal_anchor_key: anchor_key.clone(),
                decision_case_id: Some(decision_case_id.clone()),
                operation_results,
                warnings: Vec::new(),
            };
            let next_event_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(event_seq), 0) + 1
                 FROM meal_decision_events
                 WHERE household_id=?1 AND decision_case_id=?2",
                params![request.household_id, decision_case_id],
                |row| row.get(0),
            )?;
            let event_payload = serde_json::json!({
                "meal_anchor_key": anchor_key.clone(),
                "meal_date_local": request.meal_date_local.clone(),
                "meal_slot": request.meal_slot.clone(),
                "slot_instance": request.slot_instance,
                "operations": request.operations.clone()
            });
            tx.execute(
                "INSERT INTO meal_decision_events (
                    household_id, decision_case_id, event_seq, event_type,
                    source_message_id, payload_json, created_at
                 ) VALUES (?1, ?2, ?3, 'commit_applied', ?4, ?5, ?6)",
                params![
                    request.household_id,
                    decision_case_id,
                    next_event_seq,
                    request.source_message_id,
                    event_payload.to_string(),
                    unix_now()
                ],
            )?;
            let closes_case = request.operations.iter().any(|operation| {
                operation
                    .get("op")
                    .and_then(Value::as_str)
                    .is_some_and(|op| op == "record_episode")
            });
            if closes_case {
                tx.execute(
                    "UPDATE meal_decision_states
                     SET status='closed',
                         closed_at=COALESCE(closed_at, ?3),
                         last_event_seq=?4,
                         updated_at=?3
                     WHERE household_id=?1 AND decision_case_id=?2",
                    params![
                        request.household_id,
                        decision_case_id,
                        unix_now(),
                        next_event_seq
                    ],
                )?;
            } else {
                tx.execute(
                    "UPDATE meal_decision_states
                     SET last_event_seq=?3,
                         updated_at=?4
                     WHERE household_id=?1 AND decision_case_id=?2",
                    params![
                        request.household_id,
                        decision_case_id,
                        next_event_seq,
                        unix_now()
                    ],
                )?;
            }
            let response_json = serde_json::to_string(&response)?;
            tx.execute(
                "INSERT INTO meal_idempotency_keys (
                    household_id, tool_name, idempotency_key, request_hash, response_json, created_at
                 ) VALUES (?1, 'meal_memory_write', ?2, ?3, ?4, ?5)",
                params![
                    request.household_id,
                    effective_idempotency_key,
                    request_hash,
                    response_json,
                    unix_now()
                ],
            )?;
            tx.execute(
                "UPDATE meal_memory_write_fence
                 SET response_json=?4, status='applied', updated_at=?5
                 WHERE household_id=?1 AND source_message_id=?2 AND commit_kind=?3",
                params![
                    request.household_id,
                    request.source_message_id,
                    commit_kind,
                    response_json,
                    unix_now()
                ],
            )?;

            tx.commit()?;
            Ok(response)
        })
    }

    pub fn process_outbox_events(&self, limit: usize) -> Result<OutboxProcessReport> {
        let mut report = OutboxProcessReport::default();
        if limit == 0 {
            return Ok(report);
        }

        let rows = self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, household_id, event_key, payload_json, attempt_count
                 FROM meal_outbox_events
                 WHERE household_id=?1
                   AND status='pending'
                   AND next_attempt_at <= ?2
                 ORDER BY updated_at ASC
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![
                    self.household_id(),
                    unix_now(),
                    i64::try_from(limit).unwrap_or(1)
                ],
                |row| {
                    Ok(OutboxEventRow {
                        id: row.get(0)?,
                        household_id: row.get(1)?,
                        event_key: row.get(2)?,
                        payload_json: row.get(3)?,
                        attempt_count: row.get(4)?,
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })?;

        for row in rows {
            let payload = match serde_json::from_str::<NudgeOutboxPayload>(&row.payload_json) {
                Ok(value) => value,
                Err(error) => {
                    self.mark_outbox_retry(row.id, row.attempt_count, error.to_string().as_str())?;
                    report.failed += 1;
                    report
                        .messages
                        .push(format!("outbox {} parse failed: {}", row.event_key, error));
                    continue;
                }
            };

            let nudge_row = self.load_nudge_job_row(
                row.household_id.as_str(),
                payload.decision_case_id.as_str(),
                payload.nudge_type.as_str(),
            )?;
            let Some(nudge_row) = nudge_row else {
                self.mark_outbox_applied(row.id)?;
                report.applied += 1;
                report.messages.push(format!(
                    "outbox {} no-op (nudge row missing)",
                    row.event_key
                ));
                continue;
            };

            let apply_result =
                if nudge_row.status == "cancel_pending" || nudge_row.desired_state == "canceled" {
                    self.cancel_nudge_job(&nudge_row)
                } else {
                    match self.schedule_or_update_nudge_job(&nudge_row) {
                        Ok(cron_job_id) => self
                            .mark_nudge_scheduled(nudge_row.identity_key(), cron_job_id.as_str()),
                        Err(error) => Err(error),
                    }
                };

            match apply_result {
                Ok(()) => {
                    self.mark_outbox_applied(row.id)?;
                    report.applied += 1;
                    report
                        .messages
                        .push(format!("outbox {} applied", row.event_key));
                }
                Err(error) => {
                    self.mark_nudge_error(nudge_row.identity_key(), error.to_string().as_str())?;
                    self.mark_outbox_retry(row.id, row.attempt_count, error.to_string().as_str())?;
                    report.retried += 1;
                    report.messages.push(format!(
                        "outbox {} retry scheduled: {}",
                        row.event_key, error
                    ));
                }
            }
        }

        Ok(report)
    }

    pub fn reconcile_nudge_jobs(&self, limit: usize) -> Result<NudgeReconcileReport> {
        let mut report = NudgeReconcileReport::default();
        if limit == 0 {
            return Ok(report);
        }
        let rows = self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT household_id, decision_case_id, nudge_type, run_at, prompt, channel, target,
                        best_effort, desired_state, status, cron_job_id
                 FROM meal_nudge_jobs
                 WHERE household_id=?1 AND status IN ('pending', 'cancel_pending')
                 ORDER BY updated_at ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(
                params![self.household_id(), i64::try_from(limit).unwrap_or(1)],
                |row| {
                    Ok(NudgeJobRow {
                        household_id: row.get(0)?,
                        decision_case_id: row.get(1)?,
                        nudge_type: row.get(2)?,
                        run_at: row.get(3)?,
                        prompt: row.get(4)?,
                        channel: row.get(5)?,
                        target: row.get(6)?,
                        best_effort: row.get::<_, i64>(7)? != 0,
                        desired_state: row.get(8)?,
                        status: row.get(9)?,
                        cron_job_id: row.get(10)?,
                    })
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })?;

        if rows.is_empty() {
            return Ok(report);
        }

        if !self.config.cron.enabled {
            for row in rows {
                self.mark_nudge_error(row.identity_key(), "cron subsystem is disabled")?;
                report.failed += 1;
                report.messages.push(format!(
                    "nudge {:?} failed: cron disabled",
                    row.identity_key()
                ));
            }
            return Ok(report);
        }

        for row in rows {
            if row.status == "cancel_pending" || row.desired_state == "canceled" {
                match self.cancel_nudge_job(&row) {
                    Ok(()) => {
                        report.applied += 1;
                        report
                            .messages
                            .push(format!("nudge {:?} canceled", row.identity_key()));
                    }
                    Err(error) => {
                        self.mark_nudge_error(row.identity_key(), error.to_string().as_str())?;
                        report.failed += 1;
                        report.messages.push(format!(
                            "nudge {:?} cancel failed: {}",
                            row.identity_key(),
                            error
                        ));
                    }
                }
                continue;
            }

            match self.schedule_or_update_nudge_job(&row) {
                Ok(cron_job_id) => {
                    self.mark_nudge_scheduled(row.identity_key(), cron_job_id.as_str())?;
                    report.applied += 1;
                    report.messages.push(format!(
                        "nudge {:?} scheduled with cron {}",
                        row.identity_key(),
                        cron_job_id
                    ));
                }
                Err(error) => {
                    self.mark_nudge_error(row.identity_key(), error.to_string().as_str())?;
                    report.failed += 1;
                    report.messages.push(format!(
                        "nudge {:?} scheduling failed: {}",
                        row.identity_key(),
                        error
                    ));
                }
            }
        }

        Ok(report)
    }

    fn load_nudge_job_row(
        &self,
        household_id: &str,
        decision_case_id: &str,
        nudge_type: &str,
    ) -> Result<Option<NudgeJobRow>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT household_id, decision_case_id, nudge_type, run_at, prompt, channel, target,
                        best_effort, desired_state, status, cron_job_id
                 FROM meal_nudge_jobs
                 WHERE household_id=?1 AND decision_case_id=?2 AND nudge_type=?3",
                params![household_id, decision_case_id, nudge_type],
                |row| {
                    Ok(NudgeJobRow {
                        household_id: row.get(0)?,
                        decision_case_id: row.get(1)?,
                        nudge_type: row.get(2)?,
                        run_at: row.get(3)?,
                        prompt: row.get(4)?,
                        channel: row.get(5)?,
                        target: row.get(6)?,
                        best_effort: row.get::<_, i64>(7)? != 0,
                        desired_state: row.get(8)?,
                        status: row.get(9)?,
                        cron_job_id: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(anyhow::Error::from)
        })
    }

    fn find_cron_jobs_by_name(&self, name: &str) -> Result<Vec<cron::CronJob>> {
        let jobs = cron::list_jobs(self.config.as_ref())?;
        Ok(jobs
            .into_iter()
            .filter(|job| job.name.as_deref() == Some(name))
            .collect())
    }

    fn schedule_or_update_nudge_job(&self, row: &NudgeJobRow) -> Result<String> {
        let run_at = row
            .run_at
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing run_at for scheduled nudge"))?;
        let run_at = DateTime::parse_from_rfc3339(run_at)
            .with_context(|| format!("invalid nudge run_at timestamp: {run_at}"))?
            .with_timezone(&Utc);
        let prompt = row
            .prompt
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing prompt for scheduled nudge"))?;
        let channel = row
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing delivery channel for scheduled nudge"))?
            .to_ascii_lowercase();
        let target = row
            .target
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing delivery target for scheduled nudge"))?
            .to_string();
        let name = format!("meal_nudge:{}:{}", row.household_id, row.decision_case_id);
        let schedule = Schedule::At { at: run_at };
        let delivery = DeliveryConfig {
            mode: "announce".to_string(),
            channel: Some(channel),
            to: Some(target),
            best_effort: row.best_effort,
        };

        let patch = CronJobPatch {
            schedule: Some(schedule.clone()),
            prompt: Some(prompt.to_string()),
            name: Some(name.clone()),
            enabled: Some(true),
            delivery: Some(delivery.clone()),
            delete_after_run: Some(true),
            ..CronJobPatch::default()
        };

        if let Some(cron_job_id) = row.cron_job_id.as_deref() {
            if cron::get_job(self.config.as_ref(), cron_job_id).is_ok() {
                let _ = cron::update_job(self.config.as_ref(), cron_job_id, patch)?;
                return Ok(cron_job_id.to_string());
            }
        }

        let matches = self.find_cron_jobs_by_name(name.as_str())?;
        if matches.len() > 1 {
            anyhow::bail!("multiple cron jobs found for deterministic nudge name '{name}'");
        }
        if let Some(existing) = matches.first() {
            let _ = cron::update_job(self.config.as_ref(), existing.id.as_str(), patch)?;
            return Ok(existing.id.clone());
        }

        let created = cron::add_agent_job(
            self.config.as_ref(),
            Some(name),
            schedule,
            prompt,
            SessionTarget::Isolated,
            self.config.default_model.clone(),
            Some(delivery),
            true,
        )?;
        Ok(created.id)
    }

    fn cancel_nudge_job(&self, row: &NudgeJobRow) -> Result<()> {
        if let Some(cron_job_id) = row.cron_job_id.as_deref() {
            if let Err(error) = cron::remove_job(self.config.as_ref(), cron_job_id) {
                tracing::debug!(
                    component = "meal_nudge_reconcile",
                    cron_job_id = %cron_job_id,
                    error = %error,
                    "best-effort remove of mapped nudge cron job failed"
                );
            }
        }
        let name = format!("meal_nudge:{}:{}", row.household_id, row.decision_case_id);
        let matches = self.find_cron_jobs_by_name(name.as_str())?;
        for job in matches {
            if let Err(error) = cron::remove_job(self.config.as_ref(), job.id.as_str()) {
                tracing::debug!(
                    component = "meal_nudge_reconcile",
                    cron_job_id = %job.id,
                    error = %error,
                    "best-effort remove of named nudge cron job failed"
                );
            }
        }
        self.mark_nudge_canceled(row.identity_key())?;
        Ok(())
    }

    fn mark_nudge_scheduled(&self, key: NudgeIdentityKey, cron_job_id: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_nudge_jobs
                 SET status='scheduled',
                     desired_state='scheduled',
                     cron_job_id=?4,
                     last_error=NULL,
                     updated_at=?5
                 WHERE household_id=?1 AND decision_case_id=?2 AND nudge_type=?3",
                params![
                    key.household_id,
                    key.decision_case_id,
                    key.nudge_type,
                    cron_job_id,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    fn mark_nudge_canceled(&self, key: NudgeIdentityKey) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_nudge_jobs
                 SET status='canceled',
                     desired_state='canceled',
                     cron_job_id=NULL,
                     last_error=NULL,
                     updated_at=?4
                 WHERE household_id=?1 AND decision_case_id=?2 AND nudge_type=?3",
                params![
                    key.household_id,
                    key.decision_case_id,
                    key.nudge_type,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    fn mark_nudge_error(&self, key: NudgeIdentityKey, error: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_nudge_jobs
                 SET status='error',
                     last_error=?4,
                     updated_at=?5
                 WHERE household_id=?1 AND decision_case_id=?2 AND nudge_type=?3",
                params![
                    key.household_id,
                    key.decision_case_id,
                    key.nudge_type,
                    error,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }

    fn mark_outbox_applied(&self, outbox_id: i64) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_outbox_events
                 SET status='applied',
                     last_error=NULL,
                     updated_at=?2
                 WHERE id=?1",
                params![outbox_id, unix_now()],
            )?;
            Ok(())
        })
    }

    fn mark_outbox_retry(&self, outbox_id: i64, attempt_count: u32, error: &str) -> Result<()> {
        let next_attempt_count = attempt_count.saturating_add(1);
        let next_attempt_at = unix_now() + outbox_backoff_secs(next_attempt_count);
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE meal_outbox_events
                 SET status='pending',
                     attempt_count=?2,
                     next_attempt_at=?3,
                     last_error=?4,
                     updated_at=?5
                 WHERE id=?1",
                params![
                    outbox_id,
                    i64::from(next_attempt_count),
                    next_attempt_at,
                    error,
                    unix_now()
                ],
            )?;
            Ok(())
        })
    }
}

fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meal_ingress_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            sender TEXT NOT NULL,
            reply_target TEXT NOT NULL,
            thread_ts TEXT,
            content TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_meal_ingress_unique
            ON meal_ingress_events (household_id, channel, message_id);

        CREATE TABLE IF NOT EXISTS meal_preferences (
            household_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            pref_key TEXT NOT NULL,
            pref_value TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.8,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, user_id, pref_key, pref_value)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_preferences_household_user_key_updated
            ON meal_preferences (household_id, user_id, pref_key, updated_at DESC);

        CREATE TABLE IF NOT EXISTS meal_episodes (
            household_id TEXT NOT NULL,
            meal_anchor_key TEXT NOT NULL,
            meal_name TEXT NOT NULL,
            slot TEXT,
            meal_date_local TEXT,
            tags_json TEXT NOT NULL DEFAULT '[]',
            decided_by TEXT,
            source_message_id TEXT,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, meal_anchor_key)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_episodes_household_created
            ON meal_episodes (household_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS meal_feedback_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            meal_anchor_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            rating REAL,
            feedback_text TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_meal_feedback_household_created
            ON meal_feedback_signals (household_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS meal_pantry_hints (
            household_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            quantity_hint TEXT,
            source TEXT,
            freshness_profile TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, item_name)
        );

        CREATE TABLE IF NOT EXISTS meal_idempotency_keys (
            household_id TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, tool_name, idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS meal_memory_write_fence (
            household_id TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            commit_kind TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            response_json TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, source_message_id, commit_kind)
        );

        CREATE TABLE IF NOT EXISTS meal_decision_states (
            household_id TEXT NOT NULL,
            decision_case_id TEXT NOT NULL,
            meal_anchor_key TEXT NOT NULL,
            case_seq INTEGER NOT NULL,
            status TEXT NOT NULL,
            opened_at INTEGER NOT NULL,
            closed_at INTEGER,
            last_event_seq INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, decision_case_id),
            UNIQUE (household_id, meal_anchor_key, case_seq)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_meal_decision_open_per_anchor
            ON meal_decision_states (household_id, meal_anchor_key)
            WHERE status='open';

        CREATE TABLE IF NOT EXISTS meal_decision_events (
            household_id TEXT NOT NULL,
            decision_case_id TEXT NOT NULL,
            event_seq INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, decision_case_id, event_seq)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_decision_events_created
            ON meal_decision_events (household_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS meal_preference_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            pref_key TEXT NOT NULL,
            pref_value TEXT NOT NULL,
            confidence REAL NOT NULL,
            evidence_text TEXT,
            source_message_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE (household_id, source_message_id, user_id, pref_key, pref_value)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_pref_evidence_household_created
            ON meal_preference_evidence (household_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS meal_distiller_state (
            household_id TEXT NOT NULL PRIMARY KEY,
            last_ingress_created_at INTEGER NOT NULL DEFAULT 0,
            last_run_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS meal_nudge_jobs (
            household_id TEXT NOT NULL,
            decision_case_id TEXT NOT NULL,
            nudge_type TEXT NOT NULL,
            run_at TEXT,
            prompt TEXT,
            channel TEXT,
            target TEXT,
            best_effort INTEGER NOT NULL DEFAULT 1,
            desired_state TEXT NOT NULL,
            status TEXT NOT NULL,
            cron_job_id TEXT,
            last_error TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, decision_case_id, nudge_type)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_nudge_jobs_status
            ON meal_nudge_jobs (household_id, status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS meal_outbox_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER NOT NULL,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_meal_outbox_event_key
            ON meal_outbox_events (household_id, event_key);
        CREATE INDEX IF NOT EXISTS idx_meal_outbox_pending
            ON meal_outbox_events (household_id, status, next_attempt_at, updated_at);

        CREATE TABLE IF NOT EXISTS meal_pantry_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            item_canonical TEXT NOT NULL,
            source TEXT NOT NULL,
            source_user_id TEXT,
            source_message_id TEXT,
            quantity_hint TEXT,
            freshness_profile TEXT,
            observed_at INTEGER NOT NULL,
            confidence REAL NOT NULL,
            metadata_json TEXT,
            observation_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(household_id, observation_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_pantry_obs_household_observed
            ON meal_pantry_observations (household_id, observed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_meal_pantry_obs_household_item
            ON meal_pantry_observations (household_id, item_canonical, observed_at DESC);

        CREATE TABLE IF NOT EXISTS meal_pantry_state (
            household_id TEXT NOT NULL,
            item_canonical TEXT NOT NULL,
            display_name TEXT NOT NULL,
            state TEXT NOT NULL,
            availability_score REAL NOT NULL,
            perishability_class TEXT,
            last_observed_at INTEGER NOT NULL,
            expected_expiry_at INTEGER,
            source_mix_json TEXT,
            confidence_breakdown_json TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, item_canonical)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_pantry_state_household_state
            ON meal_pantry_state (household_id, state, availability_score DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS meal_pantry_sync_state (
            household_id TEXT NOT NULL,
            source TEXT NOT NULL,
            source_user_id TEXT,
            last_success_at INTEGER,
            last_attempt_at INTEGER,
            last_cursor_json TEXT,
            last_error TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, source, source_user_id)
        );

        CREATE TABLE IF NOT EXISTS meal_pantry_image_ingest_state (
            household_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            last_error TEXT,
            processed_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, channel, source_message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_pantry_image_ingest_pending
            ON meal_pantry_image_ingest_state (household_id, status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS meal_schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS meal_control_gate_events (
            household_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            gate_kind TEXT NOT NULL,
            decision TEXT NOT NULL,
            confidence REAL,
            intent TEXT,
            reason_code TEXT NOT NULL,
            model TEXT,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, channel, source_message_id, gate_kind)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_control_gate_lookup
            ON meal_control_gate_events (household_id, channel, source_message_id, gate_kind, created_at DESC);

        CREATE TABLE IF NOT EXISTS meal_unresolved_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id TEXT NOT NULL,
            source_message_id TEXT NOT NULL,
            claim_kind TEXT NOT NULL,
            subject_ref TEXT NOT NULL,
            claim_text TEXT,
            question TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (household_id, source_message_id, claim_kind, subject_ref, question)
        );
        CREATE INDEX IF NOT EXISTS idx_meal_unresolved_claims_status
            ON meal_unresolved_claims (household_id, status, created_at DESC);

        ",
    )?;
    // Backward-compatible migration for older DBs created before freshness_profile existed.
    let _ = conn.execute(
        "ALTER TABLE meal_pantry_hints ADD COLUMN freshness_profile TEXT",
        [],
    );
    migrate_meal_preferences_to_multi_value(conn)?;
    migrate_reply_gate_events_to_control_gate(conn)?;
    migrate_pantry_hints_to_observations_state(conn)?;
    ensure_search_schema(conn)?;
    backfill_search_index(conn)?;
    Ok(())
}

fn migrate_meal_preferences_to_multi_value(conn: &Connection) -> Result<()> {
    let table_sql: Option<String> = conn
        .query_row(
            "SELECT sql
             FROM sqlite_master
             WHERE type='table' AND name='meal_preferences'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(definition) = table_sql else {
        return Ok(());
    };
    let normalized = definition.to_ascii_lowercase();
    if !normalized.contains("primary key (household_id, user_id, pref_key)")
        || normalized.contains("primary key (household_id, user_id, pref_key, pref_value)")
    {
        return Ok(());
    }

    conn.execute_batch(
        "BEGIN IMMEDIATE;
         ALTER TABLE meal_preferences RENAME TO meal_preferences_legacy;
         CREATE TABLE meal_preferences (
            household_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            pref_key TEXT NOT NULL,
            pref_value TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.8,
            source TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (household_id, user_id, pref_key, pref_value)
         );
         INSERT INTO meal_preferences (
            household_id, user_id, pref_key, pref_value, confidence, source, updated_at
         )
         SELECT household_id, user_id, pref_key, pref_value, confidence, source, updated_at
         FROM meal_preferences_legacy;
         DROP TABLE meal_preferences_legacy;
         CREATE INDEX IF NOT EXISTS idx_meal_preferences_household_user_key_updated
            ON meal_preferences (household_id, user_id, pref_key, updated_at DESC);
         COMMIT;",
    )?;
    Ok(())
}

fn migrate_reply_gate_events_to_control_gate(conn: &Connection) -> Result<()> {
    let migration_name = "migrate_reply_gate_events_to_control_gate_v1";
    if is_schema_migration_applied(conn, migration_name)? {
        return Ok(());
    }

    let legacy_exists: Option<String> = conn
        .query_row(
            "SELECT name
             FROM sqlite_master
             WHERE type='table' AND name='meal_reply_gate_events'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if legacy_exists.is_none() {
        mark_schema_migration_applied(conn, migration_name)?;
        return Ok(());
    }

    conn.execute_batch(
        "INSERT INTO meal_control_gate_events (
            household_id, channel, source_message_id, gate_kind, decision, confidence, intent,
            reason_code, model, created_at
         )
         SELECT household_id, channel, source_message_id, 'reply', decision, confidence, intent,
                reason_code, model, created_at
         FROM meal_reply_gate_events
         WHERE 1=1
         ON CONFLICT(household_id, channel, source_message_id, gate_kind) DO UPDATE SET
            decision=excluded.decision,
            confidence=excluded.confidence,
            intent=excluded.intent,
            reason_code=excluded.reason_code,
            model=excluded.model,
            created_at=excluded.created_at;",
    )?;
    mark_schema_migration_applied(conn, migration_name)?;
    Ok(())
}

fn migrate_pantry_hints_to_observations_state(conn: &Connection) -> Result<()> {
    let migration_name = "migrate_pantry_hints_to_observations_state_v1";
    if is_schema_migration_applied(conn, migration_name)? {
        return Ok(());
    }

    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let migration_result = (|| -> Result<()> {
        let mut rows_by_household: HashMap<String, HashSet<String>> = HashMap::new();
        let mut stmt = conn.prepare(
            "SELECT household_id, item_name, quantity_hint, source, freshness_profile, updated_at
             FROM meal_pantry_hints",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;

        for row in rows {
            let (household_id, item_name, quantity_hint, source, freshness_profile, updated_at) =
                row?;
            let Some(item_canonical) = normalize_pantry_item_canonical(item_name.as_str()) else {
                continue;
            };
            let source = source
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("legacy_hint")
                .to_ascii_lowercase();
            let observed_at = updated_at.max(1);
            let confidence = pantry_source_weight(Some(source.as_str())).clamp(0.4, 0.95);
            let normalized_freshness =
                normalize_pantry_freshness_profile(freshness_profile.as_deref());
            let observation_hash = pantry_observation_hash(
                household_id.as_str(),
                item_canonical.as_str(),
                source.as_str(),
                None,
                Some("migration:meal_pantry_hints"),
                observed_at,
                quantity_hint.as_deref(),
                normalized_freshness.as_deref(),
                confidence,
            );
            conn.execute(
                "INSERT INTO meal_pantry_observations (
                    household_id, item_name, item_canonical, source, source_user_id,
                    source_message_id, quantity_hint, freshness_profile, observed_at, confidence,
                    metadata_json, observation_hash, created_at
                 ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11)
                 ON CONFLICT(household_id, observation_hash) DO NOTHING",
                params![
                    household_id,
                    item_name.trim(),
                    item_canonical,
                    source,
                    "migration:meal_pantry_hints",
                    quantity_hint,
                    normalized_freshness,
                    observed_at,
                    confidence,
                    observation_hash,
                    unix_now()
                ],
            )?;
            rows_by_household
                .entry(household_id)
                .or_default()
                .insert(item_canonical);
        }

        let now = unix_now();
        for (household, items) in rows_by_household {
            recompute_pantry_state_for_items(conn, household.as_str(), &items, now)?;
        }
        mark_schema_migration_applied(conn, migration_name)?;
        Ok(())
    })();

    match migration_result {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK;");
            Err(error)
        }
    }
}

fn is_schema_migration_applied(conn: &Connection, name: &str) -> Result<bool> {
    let applied: Option<String> = conn
        .query_row(
            "SELECT name
             FROM meal_schema_migrations
             WHERE name=?1",
            params![name],
            |row| row.get(0),
        )
        .optional()?;
    Ok(applied.is_some())
}

fn mark_schema_migration_applied(conn: &Connection, name: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO meal_schema_migrations (name, applied_at)
         VALUES (?1, ?2)
         ON CONFLICT(name) DO UPDATE SET applied_at=excluded.applied_at",
        params![name, unix_now()],
    )?;
    Ok(())
}

fn backfill_search_index(conn: &Connection) -> Result<()> {
    let existing_count: i64 =
        conn.query_row("SELECT COUNT(1) FROM meal_search_docs", [], |row| {
            row.get(0)
        })?;
    if existing_count > 0 {
        return Ok(());
    }

    {
        let mut stmt = conn.prepare(
            "SELECT household_id, user_id, pref_key, pref_value
             FROM meal_preferences",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (household, user_id, key, value) = row?;
            let doc_ref = format!("{user_id}:{key}:{}", sha256_hex(value.as_str()));
            let content = format!("{user_id} {key} {value}");
            upsert_search_doc(
                conn,
                household.as_str(),
                "preference",
                doc_ref.as_str(),
                content.as_str(),
            )?;
        }
    }

    {
        let mut stmt = conn.prepare(
            "SELECT household_id, item_name, quantity_hint, freshness_profile
             FROM meal_pantry_hints",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (household, item_name, quantity_hint, freshness_profile) = row?;
            let content = format!(
                "{} {} {}",
                item_name,
                quantity_hint.unwrap_or_default(),
                freshness_profile.unwrap_or_default()
            );
            upsert_search_doc(
                conn,
                household.as_str(),
                "pantry",
                item_name.as_str(),
                content.as_str(),
            )?;
        }
    }

    {
        let mut stmt = conn.prepare(
            "SELECT household_id, meal_anchor_key, meal_name, tags_json, slot, meal_date_local
             FROM meal_episodes",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        for row in rows {
            let (household, anchor, meal_name, tags_json, slot, meal_date_local) = row?;
            let tags = serde_json::from_str::<Vec<String>>(&tags_json).unwrap_or_default();
            let content = format!(
                "{} {} {} {}",
                meal_name,
                tags.join(" "),
                slot.unwrap_or_default(),
                meal_date_local.unwrap_or_default()
            );
            upsert_search_doc(
                conn,
                household.as_str(),
                "episode",
                anchor.as_str(),
                content.as_str(),
            )?;
        }
    }

    {
        let mut stmt = conn.prepare(
            "SELECT household_id, channel, message_id, sender, content
             FROM meal_ingress_events",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        for row in rows {
            let (household, channel, message_id, sender, content) = row?;
            let body = format!("{sender} {content}");
            let doc_ref = ingress_doc_ref(channel.as_str(), message_id.as_str());
            upsert_search_doc(
                conn,
                household.as_str(),
                "ingress",
                doc_ref.as_str(),
                body.as_str(),
            )?;
        }
    }

    {
        let mut stmt = conn.prepare(
            "SELECT id, household_id, meal_anchor_key, user_id, feedback_text
             FROM meal_feedback_signals",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;
        for row in rows {
            let (id, household, anchor, user_id, feedback_text) = row?;
            let doc_ref = format!("feedback:{id}");
            let content = format!(
                "{} {} {}",
                anchor,
                user_id,
                feedback_text.unwrap_or_default()
            );
            upsert_search_doc(
                conn,
                household.as_str(),
                "feedback",
                doc_ref.as_str(),
                content.as_str(),
            )?;
        }
    }

    Ok(())
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (k, v) in map {
                ordered.insert(k, canonical_json(v));
            }
            let body = ordered
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", escape_json_key(k), v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(values) => {
            let body = values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{body}]")
        }
        _ => value.to_string(),
    }
}

fn escape_json_key(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPreference {
    pub user_id: String,
    pub key: String,
    pub value: String,
    pub confidence: f64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MealEpisode {
    pub meal_anchor_key: String,
    pub meal_name: String,
    pub slot: Option<String>,
    pub meal_date_local: Option<String>,
    pub tags: Vec<String>,
    pub decided_by: Option<String>,
    pub source_message_id: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PantryHint {
    pub item_name: String,
    pub quantity_hint: Option<String>,
    pub source: Option<String>,
    pub freshness_profile: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MealFeedback {
    pub meal_anchor_key: String,
    pub user_id: String,
    pub rating: Option<f64>,
    pub feedback_text: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressEvent {
    pub message_id: String,
    pub sender: String,
    pub content: String,
    pub timestamp: i64,
    pub channel: String,
    pub thread_ts: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlGateEvent {
    pub gate_kind: String,
    pub decision: String,
    pub confidence: Option<f64>,
    pub intent: Option<String>,
    pub reason_code: String,
    pub model: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistillationIngressEvent {
    pub message_id: String,
    pub sender: String,
    pub content: String,
    pub channel: String,
    pub thread_ts: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PantrySyncState {
    pub household_id: String,
    pub source: String,
    pub source_user_id: String,
    pub last_success_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    pub last_cursor_json: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PantryImageIngestJob {
    pub channel: String,
    pub source_message_id: String,
    pub attempt_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreferenceEvidenceRecord {
    pub source_message_id: String,
    pub user_id: String,
    pub pref_key: String,
    pub pref_value: String,
    pub confidence: f64,
    #[serde(default)]
    pub evidence_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeEvidenceRecord {
    pub source_message_id: String,
    pub user_id: String,
    pub meal_name: String,
    pub confidence: f64,
    #[serde(default)]
    pub meal_slot: Option<String>,
    #[serde(default)]
    pub meal_date_local: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub evidence_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnresolvedClaimRecord {
    pub source_message_id: String,
    pub claim_kind: String,
    pub subject_ref: String,
    pub question: String,
    #[serde(default)]
    pub claim_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDecisionState {
    pub decision_case_id: String,
    pub meal_anchor_key: String,
    pub case_seq: i64,
    pub status: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextBundle {
    pub household_id: String,
    pub preferences_by_user: HashMap<String, Vec<UserPreference>>,
    pub recent_meal_episodes: Vec<MealEpisode>,
    pub pantry_snapshot: Vec<PantryHint>,
    #[serde(default)]
    pub pantry_uncertain: Vec<PantryHint>,
    #[serde(default)]
    pub pantry_unavailable_recent: Vec<PantryHint>,
    #[serde(default)]
    pub pantry_metrics: Value,
    pub recent_feedback: Vec<MealFeedback>,
    pub open_decision_state: Option<OpenDecisionState>,
    pub recent_ingress_window: Vec<IngressEvent>,
    pub grounding_refs: Vec<String>,
    pub retrieval_metrics: Value,
    pub unresolved_questions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextLimits {
    pub max_preferences: u32,
    pub max_episodes: u32,
    pub max_pantry_items: u32,
    pub max_feedback: u32,
    pub max_ingress_window: u32,
}

impl Default for ContextLimits {
    fn default() -> Self {
        Self {
            max_preferences: 200,
            max_episodes: 25,
            max_pantry_items: 60,
            max_feedback: 30,
            max_ingress_window: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextQuery {
    #[serde(default)]
    pub household_id: String,
    #[serde(default)]
    pub active_user_ids: Vec<String>,
    #[serde(default)]
    pub focus_terms: Vec<String>,
    #[serde(default)]
    pub limits: ContextLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationCandidateInput {
    pub meal_name: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecommendRequest {
    pub household_id: String,
    #[serde(default)]
    pub context_bundle: Option<ContextBundle>,
    #[serde(default)]
    pub candidate_set: Vec<RecommendationCandidateInput>,
    #[serde(default)]
    pub mode_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedCandidate {
    pub meal_name: String,
    pub tags: Vec<String>,
    pub score: f64,
    pub fairness_floor: f64,
    pub avg_user_fit: f64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendResponse {
    pub ranked_candidates: Vec<RankedCandidate>,
    pub score_breakdown: HashMap<String, Value>,
    pub fairness_metrics: Value,
    pub grounding_metrics: Value,
    pub grounding_refs: Vec<String>,
    pub warnings: Vec<String>,
}

fn default_slot_instance() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub household_id: String,
    pub meal_date_local: String,
    pub meal_slot: String,
    #[serde(default = "default_slot_instance")]
    pub slot_instance: u32,
    #[serde(default)]
    pub operations: Vec<Value>,
    #[serde(default)]
    pub client_idempotency_key: Option<String>,
    #[serde(default)]
    pub source_message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {
    pub commit_status: String,
    pub effective_idempotency_key: String,
    pub meal_anchor_key: String,
    pub decision_case_id: Option<String>,
    pub operation_results: Vec<String>,
    pub warnings: Vec<String>,
}

impl CommitResponse {
    fn default_noop() -> Self {
        Self {
            commit_status: "noop_duplicate".to_string(),
            effective_idempotency_key: String::new(),
            meal_anchor_key: String::new(),
            decision_case_id: None,
            operation_results: Vec::new(),
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NudgeReconcileReport {
    pub applied: usize,
    pub failed: usize,
    pub messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OutboxProcessReport {
    pub applied: usize,
    pub retried: usize,
    pub failed: usize,
    pub messages: Vec<String>,
}

#[derive(Debug, Clone)]
struct NudgeJobRow {
    household_id: String,
    decision_case_id: String,
    nudge_type: String,
    run_at: Option<String>,
    prompt: Option<String>,
    channel: Option<String>,
    target: Option<String>,
    best_effort: bool,
    desired_state: String,
    status: String,
    cron_job_id: Option<String>,
}

#[derive(Debug, Clone)]
struct OutboxEventRow {
    id: i64,
    household_id: String,
    event_key: String,
    payload_json: String,
    attempt_count: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct NudgeOutboxPayload {
    decision_case_id: String,
    nudge_type: String,
}

#[derive(Debug, Clone)]
struct NudgeIdentityKey {
    household_id: String,
    decision_case_id: String,
    nudge_type: String,
}

impl NudgeJobRow {
    fn identity_key(&self) -> NudgeIdentityKey {
        NudgeIdentityKey {
            household_id: self.household_id.clone(),
            decision_case_id: self.decision_case_id.clone(),
            nudge_type: self.nudge_type.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn test_store() -> (TempDir, MealStore) {
        let tmp = TempDir::new().unwrap();
        let mut cfg = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        cfg.meal_agent.enabled = true;
        cfg.meal_agent.household_id = "household_test".to_string();
        cfg.meal_agent.db_path = "state/meal/test.db".to_string();
        let store = MealStore::new(Arc::new(cfg));
        (tmp, store)
    }

    #[tokio::test]
    async fn ingress_is_persisted_and_retrieved_in_context() {
        let (_tmp, store) = test_store();
        store
            .record_ingress(&ChannelMessage {
                id: "telegram_1_1".to_string(),
                sender: "alice".to_string(),
                reply_target: "1".to_string(),
                content: "we should eat chicken tonight".to_string(),
                channel: "telegram".to_string(),
                timestamp: u64::try_from(unix_now())
                    .expect("unix timestamp should be non-negative"),
                thread_ts: None,
            })
            .unwrap();

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(ctx.recent_ingress_window.len(), 1);
        assert!(ctx.recent_ingress_window[0].content.contains("chicken"));
    }

    #[tokio::test]
    async fn context_get_clamps_oversized_limits() {
        let (_tmp, store) = test_store();
        for idx in 0..260 {
            store
                .record_ingress(&ChannelMessage {
                    id: format!("telegram_1_{idx}"),
                    sender: "alice".to_string(),
                    reply_target: "1".to_string(),
                    content: format!("message {idx}"),
                    channel: "telegram".to_string(),
                    timestamp: u64::try_from(unix_now())
                        .expect("unix timestamp should be non-negative"),
                    thread_ts: None,
                })
                .expect("ingress should persist");
        }

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                limits: ContextLimits {
                    max_preferences: u32::MAX,
                    max_episodes: u32::MAX,
                    max_pantry_items: u32::MAX,
                    max_feedback: u32::MAX,
                    max_ingress_window: u32::MAX,
                },
                ..ContextQuery::default()
            })
            .await
            .expect("context should load");
        assert!(
            ctx.recent_ingress_window.len() <= MAX_CONTEXT_INGRESS_WINDOW as usize,
            "ingress window should be clamped"
        );
    }

    #[test]
    fn control_gate_reply_decision_allows_outbound_guard_lookup() {
        let (_tmp, store) = test_store();
        let message = ChannelMessage {
            id: "telegram_-1001_1".to_string(),
            sender: "alice".to_string(),
            reply_target: "-1001".to_string(),
            content: "what should we eat?".to_string(),
            channel: "telegram".to_string(),
            timestamp: u64::try_from(unix_now()).expect("unix timestamp should be non-negative"),
            thread_ts: None,
        };
        store
            .record_reply_gate_decision(
                &message,
                "respond",
                Some(0.92),
                Some("meal"),
                "direct_request",
                Some("gpt-5"),
            )
            .expect("reply decision should persist");
        store
            .record_distill_gate_decision(
                &message,
                "distill",
                Some(0.88),
                Some("meal"),
                "meal_fact_signal",
                Some("gpt-5"),
            )
            .expect("distill decision should persist");

        assert!(
            store
                .reply_gate_allows("telegram", "telegram_-1001_1")
                .expect("lookup should succeed"),
            "reply gate should allow this source message id"
        );
    }

    #[test]
    fn new_schema_does_not_create_legacy_reply_gate_table() {
        let (_tmp, store) = test_store();
        let legacy_exists: Option<String> = store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT name
                     FROM sqlite_master
                     WHERE type='table' AND name='meal_reply_gate_events'",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into)
            })
            .expect("legacy table lookup should succeed");
        assert!(
            legacy_exists.is_none(),
            "new schema should not create meal_reply_gate_events"
        );
    }

    #[test]
    fn legacy_reply_gate_rows_migrate_to_control_gate_table() {
        let tmp = TempDir::new().expect("tempdir should create");
        let db_path = tmp.path().join("legacy-reply-gate.db");
        let conn = Connection::open(&db_path).expect("sqlite connection should open");
        conn.execute_batch(
            "CREATE TABLE meal_reply_gate_events (
                household_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                source_message_id TEXT NOT NULL,
                decision TEXT NOT NULL,
                confidence REAL,
                intent TEXT,
                reason_code TEXT NOT NULL,
                model TEXT,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (household_id, channel, source_message_id)
            );
            INSERT INTO meal_reply_gate_events (
                household_id, channel, source_message_id, decision, confidence,
                intent, reason_code, model, created_at
            ) VALUES (
                'household_test', 'telegram', 'telegram_-1001_88', 'respond',
                0.91, 'meal', 'direct_request', 'gpt-5', 12345
            );",
        )
        .expect("legacy reply-gate fixture should seed");

        ensure_schema(&conn).expect("schema migration should succeed");

        let migrated_decision: Option<String> = conn
            .query_row(
                "SELECT decision
                 FROM meal_control_gate_events
                 WHERE household_id='household_test'
                   AND channel='telegram'
                   AND source_message_id='telegram_-1001_88'
                   AND gate_kind='reply'",
                [],
                |row| row.get(0),
            )
            .optional()
            .expect("migrated decision lookup should succeed");
        assert_eq!(migrated_decision.as_deref(), Some("respond"));

        let migration_marker: Option<String> = conn
            .query_row(
                "SELECT name
                 FROM meal_schema_migrations
                 WHERE name='migrate_reply_gate_events_to_control_gate_v1'",
                [],
                |row| row.get(0),
            )
            .optional()
            .expect("migration marker lookup should succeed");
        assert_eq!(
            migration_marker.as_deref(),
            Some("migrate_reply_gate_events_to_control_gate_v1")
        );
    }

    #[tokio::test]
    async fn latest_ingress_sender_returns_most_recent_sender() {
        let (_tmp, store) = test_store();
        store
            .record_ingress(&ChannelMessage {
                id: "telegram_1_1".to_string(),
                sender: "alice".to_string(),
                reply_target: "1".to_string(),
                content: "first".to_string(),
                channel: "telegram".to_string(),
                timestamp: u64::try_from(unix_now())
                    .expect("unix timestamp should be non-negative"),
                thread_ts: None,
            })
            .expect("first ingress should record");
        store
            .record_ingress(&ChannelMessage {
                id: "telegram_1_2".to_string(),
                sender: "bob".to_string(),
                reply_target: "1".to_string(),
                content: "second".to_string(),
                channel: "telegram".to_string(),
                timestamp: u64::try_from(unix_now())
                    .expect("unix timestamp should be non-negative"),
                thread_ts: None,
            })
            .expect("second ingress should record");

        let latest = store
            .latest_ingress_sender("household_test")
            .expect("latest sender lookup should succeed");
        assert_eq!(latest.as_deref(), Some("bob"));
    }

    #[tokio::test]
    async fn episode_distillation_persists_meal_episode() {
        let (_tmp, store) = test_store();

        let applied = store
            .apply_episode_evidence(
                &[EpisodeEvidenceRecord {
                    source_message_id: "telegram_1_901".to_string(),
                    user_id: "alice".to_string(),
                    meal_name: "poha".to_string(),
                    confidence: 0.91,
                    meal_slot: Some("breakfast".to_string()),
                    meal_date_local: Some("2026-03-15".to_string()),
                    tags: vec!["light".to_string()],
                    evidence_text: Some("I had poha for breakfast".to_string()),
                }],
                unix_now(),
            )
            .expect("episode evidence should apply");
        assert_eq!(applied, 1);

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context should load");

        assert_eq!(ctx.recent_meal_episodes.len(), 1);
        assert_eq!(ctx.recent_meal_episodes[0].meal_name, "poha");
        assert_eq!(
            ctx.recent_meal_episodes[0].slot.as_deref(),
            Some("breakfast")
        );
        assert_eq!(
            ctx.recent_meal_episodes[0].meal_date_local.as_deref(),
            Some("2026-03-15")
        );
    }

    #[tokio::test]
    async fn episode_distillation_keeps_multiple_meals_from_one_message() {
        let (_tmp, store) = test_store();
        let source_message_id = "telegram_1_902";

        let applied = store
            .apply_episode_evidence(
                &[
                    EpisodeEvidenceRecord {
                        source_message_id: source_message_id.to_string(),
                        user_id: "alice".to_string(),
                        meal_name: "khichdi".to_string(),
                        confidence: 0.89,
                        meal_slot: Some("lunch".to_string()),
                        meal_date_local: Some("2026-03-15".to_string()),
                        tags: vec![],
                        evidence_text: None,
                    },
                    EpisodeEvidenceRecord {
                        source_message_id: source_message_id.to_string(),
                        user_id: "alice".to_string(),
                        meal_name: "fruit juice".to_string(),
                        confidence: 0.86,
                        meal_slot: Some("lunch".to_string()),
                        meal_date_local: Some("2026-03-15".to_string()),
                        tags: vec![],
                        evidence_text: None,
                    },
                ],
                unix_now(),
            )
            .expect("episode evidence should apply");
        assert_eq!(applied, 2);

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context should load");
        assert_eq!(ctx.recent_meal_episodes.len(), 2);
    }

    #[test]
    fn commit_is_idempotent_for_same_turn_and_payload() {
        let (_tmp, store) = test_store();
        let request = CommitRequest {
            household_id: "household_test".to_string(),
            meal_date_local: "2026-02-13".to_string(),
            meal_slot: "lunch".to_string(),
            slot_instance: 1,
            operations: vec![serde_json::json!({
                "op": "record_episode",
                "meal_name": "chicken stir fry",
                "tags": ["chicken", "capsicum"]
            })],
            client_idempotency_key: None,
            source_message_id: "telegram_1_77".to_string(),
        };

        let first = store.commit(request.clone()).unwrap();
        assert_eq!(first.commit_status, "applied");

        let second = store.commit(request).unwrap();
        assert_eq!(second.commit_status, "noop_duplicate");
    }

    #[test]
    fn client_idempotency_key_changes_effective_key() {
        let (_tmp, store) = test_store();
        let base_ops = vec![serde_json::json!({
            "op": "record_episode",
            "meal_name": "dal chawal",
            "tags": ["dal", "rice"]
        })];

        let first = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-14".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: base_ops.clone(),
                client_idempotency_key: Some("client-key-a".to_string()),
                source_message_id: "telegram_1_770".to_string(),
            })
            .expect("first commit should succeed");
        let second = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-14".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: base_ops,
                client_idempotency_key: Some("client-key-b".to_string()),
                source_message_id: "telegram_1_771".to_string(),
            })
            .expect("second commit should succeed");

        assert_ne!(
            first.effective_idempotency_key, second.effective_idempotency_key,
            "client idempotency key should influence effective idempotency key"
        );
    }

    #[tokio::test]
    async fn commit_conflicts_for_same_source_message_with_different_payload() {
        let (_tmp, store) = test_store();
        let source_message_id = "telegram_1_88".to_string();

        let first = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-13".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_episode",
                    "meal_name": "chicken stir fry",
                    "tags": ["chicken", "capsicum"]
                })],
                client_idempotency_key: None,
                source_message_id: source_message_id.clone(),
            })
            .unwrap();
        assert_eq!(first.commit_status, "applied");

        let second = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-13".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_episode",
                    "meal_name": "paneer bhurji",
                    "tags": ["paneer"]
                })],
                client_idempotency_key: None,
                source_message_id,
            })
            .unwrap();
        assert_eq!(second.commit_status, "conflict");

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(ctx.recent_meal_episodes.len(), 1);
        assert_eq!(ctx.recent_meal_episodes[0].meal_name, "chicken stir fry");
    }

    #[tokio::test]
    async fn recommend_uses_recent_context() {
        let (_tmp, store) = test_store();
        let _ = store.commit(CommitRequest {
            household_id: "household_test".to_string(),
            meal_date_local: "2026-02-12".to_string(),
            meal_slot: "dinner".to_string(),
            slot_instance: 1,
            operations: vec![
                serde_json::json!({
                    "op": "record_preference",
                    "user_id": "A",
                    "key": "protein_preference",
                    "value": "chicken",
                    "confidence": 0.9
                }),
                serde_json::json!({
                    "op": "record_pantry",
                    "item_name": "capsicum",
                    "quantity_hint": "2",
                    "source": "fridge"
                }),
                serde_json::json!({
                    "op": "record_episode",
                    "meal_name": "chicken curry",
                    "tags": ["chicken", "gravy"]
                }),
            ],
            client_idempotency_key: None,
            source_message_id: "telegram_1_120".to_string(),
        });

        let result = store
            .recommend(RecommendRequest {
                household_id: "household_test".to_string(),
                context_bundle: None,
                candidate_set: vec![
                    RecommendationCandidateInput {
                        meal_name: "chicken stir fry".to_string(),
                        tags: vec!["chicken".to_string(), "capsicum".to_string()],
                    },
                    RecommendationCandidateInput {
                        meal_name: "paneer bhurji".to_string(),
                        tags: vec!["paneer".to_string()],
                    },
                ],
                mode_hint: Some("cook".to_string()),
            })
            .await
            .unwrap();
        assert!(!result.ranked_candidates.is_empty());
        assert_eq!(result.ranked_candidates[0].meal_name, "chicken stir fry");
    }

    #[tokio::test]
    async fn recommend_penalizes_disliked_ingredients() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-03-16".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_preference",
                    "user_id": "alice",
                    "key": "dislikes",
                    "value": "eggs",
                    "confidence": 0.95
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_510".to_string(),
            })
            .expect("dislike preference should persist");

        let result = store
            .recommend(RecommendRequest {
                household_id: "household_test".to_string(),
                context_bundle: None,
                candidate_set: vec![
                    RecommendationCandidateInput {
                        meal_name: "egg bhurji".to_string(),
                        tags: vec!["eggs".to_string()],
                    },
                    RecommendationCandidateInput {
                        meal_name: "paneer bhurji".to_string(),
                        tags: vec!["paneer".to_string()],
                    },
                ],
                mode_hint: Some("cook".to_string()),
            })
            .await
            .expect("recommend should succeed");

        assert!(!result.ranked_candidates.is_empty());
        assert_eq!(result.ranked_candidates[0].meal_name, "paneer bhurji");
    }

    #[tokio::test]
    async fn recommend_blocks_dietary_constraints() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-03-16".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_preference",
                    "user_id": "bob",
                    "key": "dietary_restriction",
                    "value": "vegetarian",
                    "confidence": 0.9
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_511".to_string(),
            })
            .expect("dietary preference should persist");

        let result = store
            .recommend(RecommendRequest {
                household_id: "household_test".to_string(),
                context_bundle: None,
                candidate_set: vec![
                    RecommendationCandidateInput {
                        meal_name: "chicken curry".to_string(),
                        tags: vec!["chicken".to_string(), "gravy".to_string()],
                    },
                    RecommendationCandidateInput {
                        meal_name: "paneer curry".to_string(),
                        tags: vec!["paneer".to_string(), "gravy".to_string()],
                    },
                ],
                mode_hint: Some("cook".to_string()),
            })
            .await
            .expect("recommend should succeed");

        assert!(result
            .ranked_candidates
            .iter()
            .all(|candidate| candidate.meal_name != "chicken curry"));
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("blocked by hard constraints")));
    }

    #[tokio::test]
    async fn feedback_alias_text_maps_to_feedback_text() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-03-16".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_feedback",
                    "user_id": "alice",
                    "meal_anchor_key": "household_test:2026-03-16:dinner:1",
                    "rating": 4.5,
                    "text": "Really liked this one"
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_512".to_string(),
            })
            .expect("feedback write should succeed");

        let context = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context should load");
        assert!(context.recent_feedback.iter().any(|entry| {
            entry
                .feedback_text
                .as_deref()
                .is_some_and(|value| value.contains("Really liked"))
        }));
    }

    #[tokio::test]
    async fn record_preference_keeps_multiple_values_for_same_key() {
        let (_tmp, store) = test_store();
        let source_message_id = "telegram_1_401";

        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-12".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_preference",
                    "user_id": "alice",
                    "key": "likes",
                    "value": "chicken"
                })],
                client_idempotency_key: None,
                source_message_id: source_message_id.to_string(),
            })
            .expect("first preference write");
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-13".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_preference",
                    "user_id": "alice",
                    "key": "likes",
                    "value": "paneer"
                })],
                client_idempotency_key: Some("pref:alice:likes:paneer".to_string()),
                source_message_id: "telegram_1_402".to_string(),
            })
            .expect("second preference write");

        let context = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context read");

        let likes = context
            .preferences_by_user
            .get("alice")
            .expect("alice preferences should exist")
            .iter()
            .filter(|pref| pref.key == "likes")
            .map(|pref| pref.value.as_str())
            .collect::<HashSet<_>>();
        assert!(likes.contains("chicken"));
        assert!(likes.contains("paneer"));
    }

    #[tokio::test]
    async fn unresolved_claims_surface_in_context_bundle() {
        let (_tmp, store) = test_store();
        let applied = store
            .apply_unresolved_claims(&[UnresolvedClaimRecord {
                source_message_id: "telegram_1_777".to_string(),
                claim_kind: "preference_identity".to_string(),
                subject_ref: "shreya".to_string(),
                question: "When you say 'Shreya', do you mean user mimosa0807?".to_string(),
                claim_text: Some("Shreya does not eat eggs".to_string()),
            }])
            .expect("unresolved claims should apply");
        assert_eq!(applied, 1);

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context should load");
        assert!(ctx
            .unresolved_questions
            .iter()
            .any(|question| question.contains("mimosa0807")));
    }

    #[tokio::test]
    async fn recommend_applies_pantry_recency_decay() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-03-15".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_pantry",
                    "item_name": "capsicum",
                    "quantity_hint": "2",
                    "source": "fridge_photo"
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_450".to_string(),
            })
            .expect("record pantry commit should succeed");

        let request = RecommendRequest {
            household_id: "household_test".to_string(),
            context_bundle: None,
            candidate_set: vec![
                RecommendationCandidateInput {
                    meal_name: "capsicum stir fry".to_string(),
                    tags: vec!["capsicum".to_string()],
                },
                RecommendationCandidateInput {
                    meal_name: "plain dal".to_string(),
                    tags: vec!["dal".to_string()],
                },
            ],
            mode_hint: Some("cook".to_string()),
        };

        let fresh = store
            .recommend(request.clone())
            .await
            .expect("fresh recommend");
        let fresh_fit = fresh
            .score_breakdown
            .get("capsicum stir fry")
            .and_then(|v| v.get("pantry_fit"))
            .and_then(Value::as_f64)
            .expect("fresh pantry_fit should be present");
        assert!(
            fresh_fit > 0.0,
            "expected positive pantry fit for fresh pantry hint"
        );

        store
            .with_conn(|conn| {
                let stale_observed_at = unix_now().saturating_sub(12 * 24 * 60 * 60);
                conn.execute(
                    "UPDATE meal_pantry_observations
                     SET observed_at=?1
                     WHERE household_id=?2 AND item_canonical='capsicum'",
                    params![stale_observed_at, "household_test"],
                )?;
                let mut touched = HashSet::new();
                touched.insert("capsicum".to_string());
                recompute_pantry_state_for_items(conn, "household_test", &touched, unix_now())?;
                Ok(())
            })
            .expect("update pantry recency");

        let stale = store.recommend(request).await.expect("stale recommend");
        let stale_fit = stale
            .score_breakdown
            .get("capsicum stir fry")
            .and_then(|v| v.get("pantry_fit"))
            .and_then(Value::as_f64)
            .expect("stale pantry_fit should be present");
        assert!(
            stale_fit < fresh_fit,
            "stale pantry fit should decay (fresh={fresh_fit}, stale={stale_fit})"
        );
        assert!(
            stale
                .warnings
                .iter()
                .any(|msg| {
                    msg.contains("stale")
                        || msg.contains("No recent pantry updates")
                        || msg.contains("many items may be unavailable")
                }),
            "stale pantry warnings should be present: {:?}",
            stale.warnings
        );
    }

    #[test]
    fn pantry_freshness_profile_changes_decay_speed() {
        let now = unix_now();
        let observed = now.saturating_sub(10 * 24 * 60 * 60); // 10 days old
        let high = pantry_freshness_weight(observed, now, Some("high"));
        let medium = pantry_freshness_weight(observed, now, Some("medium"));
        let low = pantry_freshness_weight(observed, now, Some("low"));
        let very_low = pantry_freshness_weight(observed, now, Some("very_low"));
        assert!(
            high < medium && medium < low && low <= very_low,
            "expected decay ordering high < medium < low <= very_low, got high={high} medium={medium} low={low} very_low={very_low}"
        );
    }

    #[tokio::test]
    async fn record_pantry_persists_observed_at_and_freshness_profile() {
        let (_tmp, store) = test_store();
        let observed_at = unix_now().saturating_sub(3 * 24 * 60 * 60);
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-03-15".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_pantry",
                    "item_name": "Tomato Ketchup",
                    "quantity_hint": "1 bottle",
                    "source": "instamart_order",
                    "freshness_profile": "low",
                    "observed_at": observed_at
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_999".to_string(),
            })
            .expect("record pantry commit should succeed");

        let ctx = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                ..ContextQuery::default()
            })
            .await
            .expect("context read");

        let hint = ctx
            .pantry_snapshot
            .iter()
            .chain(ctx.pantry_uncertain.iter())
            .find(|item| item.item_name.eq_ignore_ascii_case("tomato ketchup"))
            .expect("pantry hint should exist in likely or uncertain pantry state");
        assert_eq!(hint.freshness_profile.as_deref(), Some("low"));
        assert_eq!(hint.updated_at, observed_at);
    }

    #[tokio::test]
    async fn context_focus_terms_use_fts_bm25() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-10".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_episode",
                    "meal_name": "chicken stir fry",
                    "tags": ["chicken", "capsicum"]
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_201".to_string(),
            })
            .unwrap();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-11".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "record_episode",
                    "meal_name": "paneer bhurji",
                    "tags": ["paneer"]
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_202".to_string(),
            })
            .unwrap();
        store
            .record_ingress(&ChannelMessage {
                id: "telegram_1_203".to_string(),
                sender: "alice".to_string(),
                reply_target: "1".to_string(),
                content: "lets cook chicken tonight".to_string(),
                channel: "telegram".to_string(),
                timestamp: u64::try_from(unix_now())
                    .expect("unix timestamp should be non-negative"),
                thread_ts: None,
            })
            .unwrap();

        let focused = store
            .context_get(ContextQuery {
                household_id: "household_test".to_string(),
                focus_terms: vec!["chicken".to_string()],
                limits: ContextLimits {
                    max_episodes: 5,
                    max_ingress_window: 5,
                    ..ContextLimits::default()
                },
                ..ContextQuery::default()
            })
            .await
            .unwrap();

        assert_eq!(
            focused.retrieval_metrics["strategy"].as_str(),
            Some("fts5_bm25")
        );
        assert!(!focused.recent_meal_episodes.is_empty());
        assert!(focused.recent_meal_episodes[0]
            .meal_name
            .contains("chicken"));
        assert!(!focused.recent_ingress_window.is_empty());
        assert!(focused.recent_ingress_window[0].content.contains("chicken"));
    }

    #[tokio::test]
    async fn recommend_includes_grounding_metrics() {
        let (_tmp, store) = test_store();
        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-12".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![
                    serde_json::json!({
                        "op": "record_preference",
                        "user_id": "A",
                        "key": "likes",
                        "value": "chicken"
                    }),
                    serde_json::json!({
                        "op": "record_episode",
                        "meal_name": "chicken curry",
                        "tags": ["chicken", "gravy"]
                    }),
                ],
                client_idempotency_key: None,
                source_message_id: "telegram_1_301".to_string(),
            })
            .unwrap();

        let result = store
            .recommend(RecommendRequest {
                household_id: "household_test".to_string(),
                candidate_set: vec![RecommendationCandidateInput {
                    meal_name: "chicken stir fry".to_string(),
                    tags: vec!["chicken".to_string()],
                }],
                ..RecommendRequest::default()
            })
            .await
            .unwrap();

        assert_eq!(result.grounding_metrics["context_counts"]["episodes"], 1);
        assert!(
            result.grounding_metrics["grounding_ref_count"]
                .as_u64()
                .unwrap_or_default()
                >= 1
        );
    }

    #[test]
    fn outbox_worker_schedules_agent_cron_job() {
        let (_tmp, store) = test_store();
        let run_at = (Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
        let commit = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-13".to_string(),
                meal_slot: "dinner".to_string(),
                slot_instance: 1,
                operations: vec![
                    serde_json::json!({
                        "op": "record_episode",
                        "meal_name": "veg pulao",
                        "tags": ["rice"]
                    }),
                    serde_json::json!({
                        "op": "schedule_feedback_nudge",
                        "run_at": run_at,
                        "prompt": "Ask both users for a quick rating for dinner.",
                        "channel": "telegram",
                        "target": "-10012345"
                    }),
                ],
                client_idempotency_key: None,
                source_message_id: "telegram_1_401".to_string(),
            })
            .unwrap();
        let decision_case_id = commit.decision_case_id.unwrap();

        let report = store.process_outbox_events(8).unwrap();
        assert_eq!(report.applied, 1, "{report:?}");
        assert_eq!(report.failed, 0, "{report:?}");

        let name = format!("meal_nudge:household_test:{decision_case_id}");
        let jobs = cron::list_jobs(store.config.as_ref()).unwrap();
        let matched: Vec<_> = jobs
            .into_iter()
            .filter(|job| job.name.as_deref() == Some(name.as_str()))
            .collect();
        assert_eq!(matched.len(), 1, "expected one nudge cron job by name");
        assert_eq!(
            matched[0].delivery.channel.as_deref(),
            Some("telegram"),
            "nudge delivery channel"
        );
    }

    #[test]
    fn outbox_worker_can_cancel_scheduled_job() {
        let (_tmp, store) = test_store();
        let run_at = (Utc::now() + chrono::Duration::minutes(45)).to_rfc3339();
        let commit = store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-14".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "schedule_feedback_nudge",
                    "run_at": run_at,
                    "prompt": "Ask both users for lunch feedback.",
                    "channel": "telegram",
                    "target": "-10012345"
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_402".to_string(),
            })
            .unwrap();
        store.process_outbox_events(8).unwrap();
        let decision_case_id = commit.decision_case_id.unwrap();

        store
            .commit(CommitRequest {
                household_id: "household_test".to_string(),
                meal_date_local: "2026-02-14".to_string(),
                meal_slot: "lunch".to_string(),
                slot_instance: 1,
                operations: vec![serde_json::json!({
                    "op": "cancel_feedback_nudge",
                    "decision_case_id": decision_case_id
                })],
                client_idempotency_key: None,
                source_message_id: "telegram_1_403".to_string(),
            })
            .unwrap();
        let report = store.process_outbox_events(8).unwrap();
        assert_eq!(report.failed, 0, "{report:?}");

        let jobs = cron::list_jobs(store.config.as_ref()).unwrap();
        let still_present = jobs.into_iter().any(|job| {
            job.name
                .as_deref()
                .map(|name| name.starts_with("meal_nudge:household_test:"))
                .unwrap_or(false)
        });
        assert!(!still_present, "expected nudge cron jobs to be removed");
    }
}
