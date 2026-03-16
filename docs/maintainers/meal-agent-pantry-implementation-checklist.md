# Meal Agent Pantry Implementation Checklist

Status: In Progress
Date: 2026-03-16
Spec: `docs/maintainers/meal-agent-pantry-impl-spec.md`

## Phase 1: Store and Schema

- [ ] Add `meal_pantry_observations` table + indexes.
- [ ] Add `meal_pantry_state` table + indexes.
- [ ] Add `meal_pantry_sync_state` table.
- [ ] Add `meal_pantry_image_ingest_state` table.
- [ ] Add migration/backfill from `meal_pantry_hints` to observations.
- [ ] Convert `meal_pantry_hints` to compatibility projection semantics.

## Phase 2: Pantry Model in Store

- [ ] Add deterministic decay model (source weights + half-life + hard cutoffs).
- [ ] Add batched `recompute_pantry_state(...)`.
- [ ] Add read helpers: ingress by message id, control-gate event by message id.
- [ ] Update commit path: `record_pantry` -> observation append + batched recompute.

## Phase 3: Tool Path Updates

- [ ] Update `meal_context_get` auto-sync trigger to server-side pantry relevance.
- [ ] Extend `ContextBundle` to include pantry partitions and metrics.
- [ ] Update `meal_recommend` scoring to use only `likely_available` pantry state.
- [ ] Update `meal_instamart_orders_sync` sync-state cursor semantics + 14d fallback.
- [ ] Add classifier failure policy + bounded batch limits.

## Phase 4: Image Ingestion

- [ ] Add `meal_pantry_image_extract` tool.
- [ ] Add passive pantry image worker in channel runtime.
- [ ] Add idempotent ingest-state updates and retry behavior.

## Phase 5: Config and Prompt

- [ ] Add new `[meal_agent]` pantry config keys + validation.
- [ ] Update meal prompt policy for consolidated read path and image flow.
- [ ] Update meal tool policy checks/auto-approve expectations for new image tool.

## Phase 6: Tests and Validation

- [ ] Add store tests for decay/cutoffs/state partitions and idempotency.
- [ ] Add tool tests for context trigger behavior and image extraction.
- [ ] Add integration tests for image-only passive ingestion and stale-order exclusion.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run targeted `cargo test` suite for changed modules.
