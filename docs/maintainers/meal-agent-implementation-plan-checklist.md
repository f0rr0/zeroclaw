# Meal Agent Implementation Plan And Checklist

Status: In Progress  
Date: 2026-03-15

## Goal

Deliver an end-to-end usable meal agent in ZeroClaw that can:

1. Persist ambient meal-relevant context from chat.
2. Retrieve contextual meal memory for grounded recommendations.
3. Recommend meals using stored preferences, pantry, history, and feedback.
4. Commit meal decisions transactionally with idempotency.

## Scope For This Execution Pass

1. Core config and storage wiring.
2. Meal tools (`meal_context_get`, `meal_recommend`, `meal_turn_commit`).
3. Ambient ingress capture hook.
4. Prompt/tool wiring in channel runtime.
5. Focused tests for the two key scenarios and duplicate commit safety.
6. Non-mention selective reply gate + outbound guard.

## Implemented Checklist

- [x] Add `[meal_agent]` config block with defaults and validation.
- [x] Add meal store module with SQLite schema and bootstrapping.
- [x] Add ingress persistence (`meal_ingress_events`) with retention cleanup.
- [x] Add context retrieval (`meal_context_get`) for preferences/history/pantry/feedback/ingress.
- [x] Add deterministic recommendation ranking (`meal_recommend`).
- [x] Add transactional, idempotent write path (`meal_turn_commit`).
- [x] Add built-in `MealIngressHook` to persist every inbound message when meal mode is enabled.
- [x] Add LLM-based reply gate in `MealIngressHook` for non-mention Telegram group traffic.
- [x] Add outbound guard in hook pipeline to suppress unpermitted responses.
- [x] Extend hook API to pass `source_message_id` into `on_message_sending` for guard enforcement.
- [x] Add richer retrieval using SQLite FTS5/BM25 for focus-term context lookup.
- [x] Surface retrieval/grounding metrics in context and recommendation outputs.
- [x] Register meal tools in tool registry only when `meal_agent.enabled=true`.
- [x] Add meal tool guidance to channel system prompt.
- [x] Wire meal ingress hook into runtime hook registration path.
- [x] Add focused unit tests in meal store:
  - [x] ambient ingress retrieval
  - [x] idempotent commit replay
  - [x] recommendation using stored context
  - [x] FTS/BM25 focus-term retrieval path
  - [x] grounding metrics presence in recommend output

## Validation Checklist

- [x] `cargo check` passes.
- [x] `cargo fmt --all -- --check` passes.
- [x] `cargo clippy --lib --tests -- -D warnings` passes.
- [x] Focused exact tests passed for meal store scenarios.
- [ ] Full project test suite (`cargo test`) pass (pending due runtime cost in this pass).
- [ ] Telegram manual E2E verification in live group.

## Manual E2E Checklist (Telegram)

1. Enable config:
   - `[meal_agent].enabled = true`
   - `[meal_agent].household_id = "household_main"`
2. Start daemon/channel.
3. Send ambient messages:
   - `A: let's eat chicken today`
   - `B: there is cabbage and capsicum in fridge`
4. Ask bot:
   - `@bot recommend`
5. Confirm via behavior:
   - Bot uses `meal_context_get` + `meal_recommend` before recommendation claims.
6. Persist outcome:
   - `meal_turn_commit` call includes episode/pantry/preference/feedback writes.
7. Re-ask next day with same ingredient and verify history-aware suggestion.

## Next Hardening Steps

- [x] Add richer retrieval (FTS/BM25) and grounding metrics.
- [x] Add semantic retrieval via embeddings (hybrid with BM25).
- [ ] Add edited-message ingestion revision handling.
- [x] Add scheduler-backed feedback nudges and reconcile worker.
