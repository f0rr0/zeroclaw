# Meal Agent Pantry Intelligence Implementation Spec (Code-Rooted)

Status: Proposed (implementation-ready)
Date: 2026-03-16
Owner: Meal Agent
Scope: Pantry correctness for meal suggestions (photo + Swiggy Instamart + chat context)

## 1) Goal

Eliminate stale-perishable leakage in meal suggestions while preserving ZeroClaw's current architecture:

1. Keep meal integration as hooks + tools + meal SQLite store.
2. Keep unmodified core runtime semantics.
3. Make pantry availability a first-class computed state, not raw hint recency.

## 1.1) Architectural Decision: Direct LLM Calls in Meal Subsystem

Direct LLM calls outside the primary conversational turn are accepted for this feature set, and are required for correctness.

Why this is the correct choice in this codebase:

1. The existing meal preference distillation worker already does direct model calls (`run_distillation_batch`), so this pattern is native to current architecture.
2. Passive pantry image ingestion (image-only posts without user text) cannot depend on primary-turn tool calling because many such messages are intentionally `ingest_only`.
3. Instamart normalization/classification is an extraction pipeline concern, not conversational reasoning.

Guardrails (mandatory):

1. Direct calls are only for bounded extraction/classification tasks (no free-form conversation).
2. Strict JSON schema output required.
3. All writes still go through meal transactional write path with idempotency.
4. Failures must fail-safe (no optimistic pantry writes on parse/model failure).

## 2) Current Codebase Baseline (exact seams)

### 2.1 Tools

1. `src/tools/meal_context_get.rs`
   - Auto-sync constants:
     - `INSTAMART_AUTO_SYNC_INTERVAL_SECS = 30 * 60`
     - `INSTAMART_AUTO_SYNC_LOOKBACK_DAYS = 35`
   - Auto-sync runs before returning context.

2. `src/tools/meal_instamart_orders_sync.rs`
   - Extracts pantry candidates from MCP order payload.
   - Uses heuristic token functions:
     - `infer_freshness_profile(...)`
     - `is_non_food_item(...)`
   - Persists with `record_pantry` ops through `CommitRequest`.

3. `src/tools/meal_turn_commit.rs`
   - Mutations are transactional and fenced via `MealStore::commit`.

### 2.2 Store and scoring

1. `src/meal/store.rs`
   - Pantry hint table: `meal_pantry_hints`.
   - `record_pantry` does item-level upsert keyed by `(household_id, item_name)`.
   - Freshness is score-decay only (`pantry_freshness_weight`), with non-zero floor.
   - `context_get` returns pantry rows directly from `meal_pantry_hints`.
   - `recommend` adds pantry score from all matched pantry hints.

### 2.3 Prompt policy

1. `src/channels/mod.rs`
   - Meal policy instructs model to treat pantry as time-sensitive.
   - Pantry extraction from image is currently prompt-driven (no dedicated extraction tool contract).

### 2.4 Hook/gating context

1. `src/hooks/builtin/meal_ingress.rs`
   - Inbound messages are ingested and reply-gated.
   - Image markers are stripped for classifier/distillation control plane text.

## 3) Problem Statement (implementation target)

Observed failures are direct consequences of current code semantics:

1. Old Instamart purchases remain visible in pantry context indefinitely.
2. Perishable items (e.g., chicken, bhindi) remain candidate pantry matches weeks later.
3. No-food rows can remain in pantry state after historical ingestion.
4. Image pantry extraction is non-deterministic because it depends on model behavior under prompt guidance.
5. Recommendation can still assign positive pantry contribution to stale/uncertain items.

## 4) Target Runtime Behavior (single path, non-optional)

1. Pantry availability must be computed from evidence, not assumed from order history recency.
2. Order history and pantry availability must be separated:
   - Order history remains queryable for "when did I buy X?".
   - Active pantry state only includes items with sufficient availability confidence.
3. Meal ranking only uses `likely_available` pantry items for pantry-fit scoring.
4. Uncertain/expired items are surfaced as assumptions, not treated as available facts.
5. Photo ingestion must have a deterministic tool path with structured output.

## 5) Data Model Changes (SQLite, meal DB)

All changes happen in `src/meal/store.rs` schema initialization + migration block.

### 5.1 New table: `meal_pantry_observations`

Append-only evidence table.

```sql
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
```

Notes:

1. `observation_hash` provides idempotency for repeat sync/parse replays.
2. `item_canonical` is normalized lower-case canonical key.

### 5.2 New table: `meal_pantry_state`

Materialized availability state.

```sql
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
```

State enum (enforced in code):

1. `likely_available`
2. `uncertain`
3. `likely_unavailable`

### 5.3 New table: `meal_pantry_sync_state`

Source sync cursor/health.

```sql
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
```

Purpose:

1. Prevent frequent full lookback sync.
2. Enable incremental Instamart sync semantics.

### 5.4 Existing table retained: `meal_pantry_hints`

Retain as compatibility projection only; stop using it as source-of-truth for context and ranking.

Write policy:

1. New writes go to `meal_pantry_observations` and `meal_pantry_state`.
2. `meal_pantry_hints` is updated from state projection code path only (no direct independent writes).

### 5.5 New table: `meal_pantry_image_ingest_state`

Tracks passive image ingestion progress for idempotent background processing.

```sql
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
```

## 6) Pantry Availability Model (implemented in `MealStore`)

### 6.1 Inputs

For each item canonical key, aggregate recent observations from `meal_pantry_observations`:

1. `age_secs` from latest observations.
2. `source` reliability (photo/manual/chat/order).
3. `freshness_profile` / `perishability_class`.
4. corroboration count (multiple distinct recent observations).
5. canonicalization and perishability assignment come from structured classifier outputs (not regex token tables).

### 6.2 Deterministic score

Per-observation score:

```text
obs_score = clamp(source_weight(source) * time_decay(age_days, perishability_class) * confidence, 0.0, 1.0)
```

Item-level aggregation across observations:

```text
availability_raw = 1 - Π(1 - obs_score_i)
```

Consumption-aware adjustment:

```text
availability_score = availability_raw * exp(-0.30 * usage_hits_after_last_observation)
```

Where `usage_hits_after_last_observation` is derived from recent meal episodes/tags mentioning the ingredient canonical name (capped to 3 hits).

### 6.3 Source weights

1. `fridge_photo|pantry_photo|shelf_photo`: `1.00`
2. `manual_chat|explicit_user_confirmation`: `0.90`
3. `instamart_order`: `0.55`
4. unknown source: `0.70`

### 6.4 Time decay and hard cutoffs

Decay:

```text
time_decay = exp(-ln(2) * age_days / half_life_days)
```

Perishability classes:

1. `high`: half-life `1.5d`, hard cutoff `7d`
2. `medium`: half-life `5d`, hard cutoff `21d`
3. `low`: half-life `20d`, hard cutoff `90d`
4. `very_low`: half-life `90d`, hard cutoff `365d`
5. `unknown`: half-life `3d`, hard cutoff `14d`

If `age_days > hard_cutoff`, force `likely_unavailable`.

### 6.5 State thresholds

1. `score >= 0.60` -> `likely_available`
2. `0.25 <= score < 0.60` -> `uncertain`
3. `< 0.25` -> `likely_unavailable`

### 6.6 Storage location

Storage location is optional metadata in v1 (fridge/freezer/pantry/unknown). If unknown, apply conservative cutoffs above.

## 7) Tool Contract Changes

## 7.1 `meal_turn_commit` (`src/tools/meal_turn_commit.rs`)

`record_pantry` op contract tightened (server-side validation in `MealStore::commit`):

Required for `record_pantry`:

1. `item_name`

Optional with server normalization (to avoid brittle tool-call failures):

1. `source` (default: `manual_chat`)
2. `observed_at` (default: current unix timestamp)
3. `freshness_profile` (default: `unknown`, then classifier/refiner may update)
4. `confidence` (default: `0.70`)

Behavior:

1. Write to `meal_pantry_observations` (append with idempotency hash).
2. Accumulate touched canonical items across the whole commit.
3. Trigger one batched synchronous `recompute_pantry_state(household_id, touched_items[])` at end of commit transaction.
3. Continue writing compatibility hint row only during migration window.

## 7.2 `meal_instamart_orders_sync` (`src/tools/meal_instamart_orders_sync.rs`)

### 7.2.1 Extraction path change

Current token heuristics are replaced by LLM-structured normalization for candidate items.

New internal step:

1. Raw MCP payload -> candidate item list (existing structural walk retained).
2. Candidate item list -> LLM normalization call (strict JSON schema):
   - `item_name`
   - `item_canonical`
   - `is_food`
   - `freshness_profile`
   - `confidence`
   - `observed_at`
3. Persist only `is_food=true` with confidence >= threshold.

Implementation seam:

1. Add provider-backed classifier helper in this tool (similar provider construction pattern used in `src/tools/delegate.rs`).
2. Keep MCP retrieval unchanged.

Classifier call contract:

1. Temperature `0.0`.
2. Strict schema JSON only.
3. Dedicated model from `meal_agent.pantry_classifier_model` (fallback: `distillation_model`, then `default_model`).
4. Max 1 retry on parse failure.
5. If classification fails after retry, do not write active pantry observations for that batch; return explicit tool error payload with diagnostics.
6. Classification call must run with bounded candidate count per batch (default 60) and bounded max output tokens.

### 7.2.2 Sync strategy change

1. Use `meal_pantry_sync_state` cursor when available.
2. Reduce default broad lookback from 35d-equivalent behavior to incremental-first.
3. On cursor failure/fallback, bounded lookback is `14 days` (user-approved cold start bound), while preserving any previously known durable pantry state through decay model.
4. Persist `last_attempt_at`, `last_success_at`, and cursor payload in `meal_pantry_sync_state` for deterministic next run behavior.

### 7.2.3 Persist semantics

1. Persist as observations only.
2. Run pantry recompute after successful sync.
3. Always separate purchase-history evidence from availability state (history remains queryable even when state becomes unavailable).

## 7.3 `meal_context_get` (`src/tools/meal_context_get.rs`)

### 7.3.1 Auto-sync trigger policy

Replace current unconditional staleness rule with a gated trigger:

Auto-sync only when all true:

1. Pantry relevance is true by server-side signal:
   - current source message content (resolved from `source_message_id` in turn context) has pantry/ingredient intent, OR
   - current source message has image marker(s), OR
   - latest reply-gate intent for source message is `meal` and user asks a meal recommendation.
2. `meal_pantry_sync_state.last_success_at` older than configured threshold (default 6h).
3. Active pantry state freshness coverage is low (e.g., `likely_available` count below threshold).

Implementation additions in `MealStore`:

1. `ingress_event_by_message_id(channel, message_id)` read helper.
2. `control_gate_event_by_message_id(channel, message_id, gate_kind='reply')` read helper.

### 7.3.2 Returned pantry payload

`ContextBundle` pantry section changes from raw hints to computed state split:

1. `pantry_likely_available[]`
2. `pantry_uncertain[]`
3. `pantry_unavailable_recent[]` (bounded, optional for transparency)
4. `pantry_metrics` (`state_counts`, `staleness_ratio`, `last_sync_at`)

Migration bridge:

1. Keep `pantry_snapshot` field temporarily populated from `pantry_likely_available` for backward compatibility.

## 7.4 Read-path consolidation (no extra read tool)

Do not add a separate `meal_pantry_state_get` tool.

Single read path:

1. Extend `meal_context_get` to return pantry partitions (`likely_available`, `uncertain`, `likely_unavailable` metrics).
2. Keep one canonical context-read tool to avoid tool-surface bloat and orchestration drift.

## 7.5 New tool: `meal_pantry_image_extract`

New vision extraction tool in `src/tools/meal_pantry_image_extract.rs`:

1. Reads current source message content via `source_message_id` and parses image markers.
2. If provider lacks vision capability, returns structured error for assistant to ask text fallback.
3. Uses strict JSON schema extraction from image(s):
   - `item_name`, `item_canonical`, `quantity_hint`, `freshness_profile`, `confidence`.
4. Persists high-confidence observations through internal commit path.
5. Is invoked in two modes:
   - active turn mode (user asks question with image),
   - passive worker mode (image-only ingress without text).

Implementation note:

1. Tool must use existing provider multimodal marker flow (image marker content passed in chat messages) rather than introducing a separate image transport path.

## 8) Recommendation Changes (`src/meal/store.rs`)

In `MealStore::recommend`:

1. Pantry match loop must use `meal_pantry_state` items where `state='likely_available'` only.
2. `uncertain` items:
   - do not add pantry score
   - add candidate warning if they are critical ingredients.
3. `likely_unavailable` items:
   - excluded from pantry score and pantry match count.

Result payload additions:

1. `pantry_likely_matches`
2. `pantry_uncertain_mentions`
3. `pantry_excluded_unavailable_count`

## 9) Prompt/Policy Updates (`src/channels/mod.rs`)

Meal policy block updates:

1. For pantry-aware suggestions, call `meal_context_get` and use its partitioned pantry state (not raw pantry assumptions).
2. If message contains image marker and pantry intent, call `meal_pantry_image_extract` before recommendation.
3. If pantry confidence is weak, call `meal_instamart_orders_sync`, then `meal_context_get` again, then `meal_recommend`.
4. Never present `uncertain`/`likely_unavailable` items as confirmed inventory.

## 10) Scheduling/Workers

No new standalone daemon required.

Reuse existing meal worker scheduling hooks in channel runtime:

1. Add pantry maintenance tick near existing meal background processing path:
   - recompute pantry state periodically (default every 3h)
   - no user-facing message side effects.
2. Add passive pantry image worker:
   - scan recent ingress rows with image markers not yet processed in `meal_pantry_image_ingest_state`
   - call `meal_pantry_image_extract` in passive mode
   - write status (`processed|skipped|retry`) with bounded retry/backoff.
3. Keep nudge/outbox worker unchanged (no pantry nudges in this change set).

## 11) Migration Plan

### 11.1 Schema migration

1. Add new pantry tables.
2. Backfill `meal_pantry_observations` from existing `meal_pantry_hints` (single synthetic observation per hint row).
3. Run one-time `recompute_pantry_state` for household.
4. Initialize `meal_pantry_image_ingest_state` as empty.
5. Keep `meal_pantry_hints` as projection target only (no independent writes).

### 11.2 Cutover

1. Change `context_get` and `recommend` to read from `meal_pantry_state`.
2. Keep compatibility payload field names until prompt update is deployed.

## 12) Observability and Audit

Add logs in these components:

1. `meal_instamart_orders_sync`:
   - extraction_count, classified_food_count, dropped_non_food_count, avg_confidence.
   - classification_failures, fallback_mode_used, cursor_age_secs.
2. `meal_pantry_image_extract`:
   - image_count, extracted_count, commit_count, vision_capability.
   - passive_mode=true|false, parse_failures.
3. `meal_context_get`:
   - auto_sync_trigger_reason, state_counts, freshness_coverage.
4. `recommend`:
   - pantry_likely_matches, pantry_uncertain_mentions, pantry_excluded_unavailable_count.
5. passive image worker:
   - pending_count, processed_count, retry_count, hard_fail_count.

Add DB inspection helper query docs in maintainers runbook (separate doc update).

## 13) Test Plan (exact file targets)

### 13.1 `src/meal/store.rs` tests

Add:

1. `pantry_state_excludes_high_perishable_after_expiry_window`
2. `pantry_state_keeps_very_low_perishable_as_likely_available_longer`
3. `recommend_uses_only_likely_available_for_pantry_fit`
4. `record_pantry_requires_source_observed_at_freshness_confidence`
5. `observation_idempotency_prevents_duplicate_observation_rows`

### 13.2 `src/tools/meal_instamart_orders_sync.rs` tests

Add:

1. `llm_classification_drops_non_food_candidates`
2. `sync_uses_cursor_before_fallback_lookback`
3. `sync_persists_observations_and_recomputes_state`

### 13.3 New tool tests

1. `src/tools/meal_pantry_image_extract.rs`:
   - rejects when no image marker.
   - vision capability error path.
   - structured extraction commit success path.
2. `src/tools/meal_context_get.rs`:
   - returns partitioned pantry state payload.
   - auto-sync trigger uses source-message pantry relevance (not only focus_terms).

### 13.4 Integration tests (`src/channels/mod.rs` + hook path)

Add end-to-end scenarios:

1. Old chicken order (>safe window) does not appear as likely available in meal suggestion.
2. Recent pantry photo updates availability and recommendation uses photo-grounded items.
3. Pantry query response separates likely/uncertain/unavailable buckets.
4. Image-only ingress (no text) is passively ingested and updates pantry state without bot reply spam.

### 13.5 Config validation tests (`src/config/schema.rs`)

Add:

1. `meal_agent_enabled_requires_image_extract_tool_policy_when_passive_worker_enabled`
2. `pantry_threshold_validation_rejects_invalid_ordering`
3. `pantry_classifier_max_items_bounds_are_enforced`

## 14) Config Additions (`src/config/schema.rs`)

Add `[meal_agent]` keys:

1. `pantry_sync_interval_minutes` (default 360)
2. `pantry_recompute_interval_minutes` (default 180)
3. `pantry_likely_threshold` (default 0.60)
4. `pantry_uncertain_threshold` (default 0.25)
5. `pantry_instamart_fallback_lookback_days` (default 14)
6. `pantry_classifier_model` (optional; default `distillation_model` then `default_model`)
7. `pantry_image_worker_interval_seconds` (default 90)
8. `pantry_classifier_max_items` (default 60, max 120)

Validation:

1. thresholds in `[0,1]` and `likely > uncertain`.
2. intervals > 0.
3. classifier_max_items in allowed range.
4. when meal agent is enabled, `meal_pantry_image_extract` must not be excluded and should be auto-approved if passive image ingestion is enabled.

## 15) Compatibility and Risk

### 15.1 Compatibility

1. Existing meal tools remain; one new tool added (`meal_pantry_image_extract`).
2. Existing DB retained; additive migrations only.
3. Existing prompts updated, not removed.

### 15.2 Risks and mitigations

1. Additional LLM cost from classification/extraction
   - Mitigation: only classify delta candidates and cache by `(canonical_name, source)`.
2. Vision provider mismatch
   - Mitigation: explicit capability error + deterministic assistant fallback question.
3. Migration drift
   - Mitigation: migration tests + one-time recompute verification query.
4. Duplicate extraction between active turn and passive worker
   - Mitigation: observation hash idempotency + per-message ingest-state dedupe.

## 16) Definition of Done

All conditions must pass:

1. Stale perishable order items no longer contribute to pantry-fit ranking.
2. Pantry responses are partitioned by confidence state.
3. Photo ingestion path is deterministic and auditable.
4. Instamart sync becomes incremental-first and no longer floods stale pantry assumptions.
5. All new tests pass (`cargo test`), and no clippy/fmt regressions.
