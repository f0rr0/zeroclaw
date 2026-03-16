# Meal Agent Remediation Spec (Code-Aligned)

## 1) Purpose

This spec defines the concrete fixes for the current meal-agent implementation issues found in:

- `src/hooks/builtin/meal_ingress.rs`
- `src/channels/mod.rs`
- `src/meal/store.rs`
- `src/tools/meal_context_get.rs`
- `src/tools/meal_turn_commit.rs`
- `src/tools/meal_instamart_auth.rs`
- `src/tools/meal_instamart_orders_sync.rs`

The goal is to fix correctness, remove redundant fallback behavior, and reduce cost/latency while preserving current ZeroClaw runtime architecture (unmodified core runtime model, hooks + tools integration).

## 2) Current Runtime Context (for correctness)

1. Inbound Telegram messages enter `process_channel_message` in `src/channels/mod.rs`.
2. `HookRunner.run_on_message_received` executes before runtime command handling and before the model/tool loop.
3. `MealIngressHook` persists ingress, applies reply gate, and can cancel processing (`ingest_only`).
4. Runtime commands are then handled (`/instamart ...`) and bypass normal LLM flow.
5. Regular turns run `run_tool_call_loop` under `turn_context::with_turn_source_message_id(...)`.
6. Outbound sends are passed through `HookRunner.run_on_message_sending`.
7. `MealIngressHook` outbound guard blocks sends without a matching reply-gate permit row.
8. Meal writes happen through `meal_turn_commit`, which is transactional and fenced on `(household_id, source_message_id, commit_kind)`.

This spec keeps that execution topology and only changes meal-specific logic and contracts.

## 3) Non-Goals

1. No rewrite of ZeroClaw core agent loop, hook runner, or provider architecture.
2. No introduction of closed-source dependencies.
3. No change to single-household partitioning semantics in meal DB.

## 4) Design Invariants (must hold after fixes)

1. Personalized meal claims must be grounded in `meal_context_get`.
2. Meal recommendations must respect preference polarity and hard constraints.
3. Distillation must not corrupt user identity attribution.
4. Mutating meal writes remain transactional and idempotent.
5. Non-mention group mode continues to ingest full context while replying selectively.

## 5) Fix Set

### 5.1 Recommendation Semantics: Polarity, Constraints, Fairness

#### Problem

Current scoring in `MealStore::recommend` uses simple substring match on `pref_value`, always adding positive score. This treats `dislikes` and constraints as positive signals.

#### Required changes

1. Add normalized preference semantics layer inside `src/meal/store.rs`:
   - `PreferenceKind::Positive`
   - `PreferenceKind::Negative`
   - `PreferenceKind::Constraint`
   - `PreferenceKind::Scalar`

2. Add key mapping function:
   - Positive keys: `likes`, `ingredient_preference`, `garnish_preference`, `meal_like`.
   - Negative keys: `dislikes`, `avoid`, `ingredient_avoidance`.
   - Constraint keys: `allergy`, `dietary_restriction`, `no_*` normalized patterns from distiller output.
   - Scalar keys: `spice_level` (and future scalar keys).

3. Recommendation pipeline change:
   - Step A: Build candidate token set from `meal_name + tags`.
   - Step B: Apply hard-constraint filter first.
   - Step C: Score positives and negatives separately.
   - Step D: Compute per-user utility vector.
   - Step E: Rank by lexicographic objective:
     - primary: maximize minimum user utility (`min_user_fit`)
     - secondary: maximize total score
     - tertiary: maximize pantry fit
     - quaternary: deterministic tie-break by candidate name.

4. Update `score_breakdown` payload to include:
   - `positive_fit`
   - `negative_penalty`
   - `constraint_blocked` (bool)
   - `constraint_reasons`
   - `min_user_fit`
   - `sum_user_fit`

5. If all candidates are blocked by constraints, return:
   - empty ranked list
   - warning with blocked reasons
   - no hallucinated fallback candidate.

#### Code seams

1. `src/meal/store.rs` (`recommend`, helper functions near scoring block).
2. `RecommendResponse` structure can remain shape-compatible; extend `score_breakdown` and warnings only.

#### Acceptance criteria

1. A stored `dislikes: eggs` causes egg-containing candidates to be penalized or blocked.
2. Hard restrictions are never top-ranked.
3. Rankings prefer options that avoid starving one participant's utility.

### 5.2 Feedback Contract Mismatch (`text` vs `feedback_text`)

#### Problem

`meal_turn_commit` schema exposes feedback field as `text`, but commit engine reads `feedback_text`.

#### Required changes

1. Canonicalize on `feedback_text`.
2. Keep `text` as backward-compatible alias for one release window:
   - If `feedback_text` missing and `text` present, map `text -> feedback_text`.
3. Update tool schema in `meal_turn_commit`:
   - Include `feedback_text` in schema.
   - Keep `text` documented as deprecated alias.
4. Add deprecation warning string into commit operation result when alias is used.

#### Code seams

1. `src/tools/meal_turn_commit.rs` parameter schema.
2. `src/meal/store.rs` in `record_feedback` operation parsing.

#### Acceptance criteria

1. Both existing callers (`text`) and new callers (`feedback_text`) persist feedback text.
2. DB row `meal_feedback_signals.feedback_text` is always populated when either field is provided.

### 5.3 Identity Attribution Safety (remove "other participant" heuristic)

#### Problem

`resolve_distilled_user_id` in both ingress hook and distiller worker can map unknown names to "the other participant" in 2-user groups.

#### Required changes

1. Remove "by_norm.len() == 2 -> choose other participant" logic completely.
2. Create a single shared resolver module:
   - `src/meal/identity_resolution.rs`
   - expose `resolve_distilled_user_id(...) -> Option<String>`
3. Resolver policy:
   - only exact normalized match to known participant IDs OR exact sender match.
   - otherwise unresolved (`None`).
4. Keep unresolved-claim write path as the only fallback for ambiguous identity.

#### Code seams

1. Delete duplicated resolver implementations in:
   - `src/hooks/builtin/meal_ingress.rs`
   - `src/channels/mod.rs`
2. Import shared resolver in both call sites.

#### Acceptance criteria

1. Ambiguous names never mutate another user's preferences.
2. Ambiguous identity produces unresolved claim only.

### 5.4 Direct Address Fallback Tightening

#### Problem

When bot username lookup fails, `is_direct_address` falls back to `starts_with('@')`, causing false direct-address triggers.

#### Required changes

1. Remove generic `starts_with('@')` fallback.
2. If bot username is unavailable:
   - do not bypass classifier via direct-address short-circuit.
   - proceed to normal reply gate classification path.

#### Code seams

1. `src/hooks/builtin/meal_ingress.rs` (`is_direct_address`).

#### Acceptance criteria

1. Mentioning other users with `@...` does not auto-bypass reply gate.
2. Bot mention bypass still works when username is known.

### 5.5 Distillation Pipeline Consolidation and Cursor Semantics

#### Problem

Inline distillation and periodic distiller duplicate logic and can reprocess the same messages; inline writes update distiller state with cursor `0`.

#### Required changes

1. Introduce shared distillation service module:
   - `src/meal/distillation.rs`
   - shared prompt, parse, validation, identity resolution, evidence normalization.

2. Cursor behavior:
   - add store helper to fetch persisted ingress `created_at` by `message_id`.
   - inline distillation must advance cursor using actual ingress `created_at` for that message.

3. Keep both modes, but make behavior explicit:
   - inline path = low-latency opportunistic
   - worker = durability backstop
   - both use same normalize/apply path from shared module.

4. Reduce over-distillation:
   - `ReplyGateDecision::should_distill()` should not override explicit classifier output with broad intent fallback.
   - If `distill_decision` present, obey it.
   - If parse missing `distill_decision`, default to `skip`.

5. Classifier failure path:
   - fail closed for reply (`ingest_only`) remains.
   - do not force immediate inline distillation on classifier failure.
   - rely on periodic worker for durability.

#### Code seams

1. `src/hooks/builtin/meal_ingress.rs`
2. `src/channels/mod.rs` (worker)
3. `src/meal/store.rs` (cursor helper)
4. new `src/meal/distillation.rs`

#### Acceptance criteria

1. Same message is not repeatedly LLM-distilled by both paths in normal operation.
2. Distillation behavior is deterministic and shared across paths.

### 5.6 Reply Gate Classifier Context Lightweight Query

#### Problem

Reply gate classification currently calls full `context_get`, which pulls more data than needed.

#### Required changes

1. Add a dedicated store query:
   - `recent_ingress_window(household_id, limit) -> Vec<IngressEvent>`
2. `classify_reply_decision` uses that query only.
3. Optional: include open decision state summary via a second tiny query if required by prompt.
4. Remove `context_get` dependency from classifier path.

#### Code seams

1. `src/meal/store.rs` new read method(s).
2. `src/hooks/builtin/meal_ingress.rs` classifier path.

#### Acceptance criteria

1. Classifier DB read count and latency decrease measurably.
2. No regression in gate decision correctness tests.

### 5.7 Instamart Auto-Sync Candidate and Retry Semantics

#### Problem

Auto-sync exits on first non-"no account" error and may never try other connected users.

#### Required changes

1. Candidate user set for auto-sync should include:
   - active users from request
   - latest ingress sender
   - all connected Instamart users in household (from profile metadata)
2. Execution policy:
   - attempt each candidate user once
   - continue after per-user errors
   - success if at least one sync succeeds
   - collect per-user errors and expose concise diagnostics in logs

#### Code seams

1. `src/tools/meal_context_get.rs`
2. shared helper in Instamart tool/profile layer to list connected users by `household_id`.

#### Acceptance criteria

1. One failing account does not block sync attempts for other connected accounts.
2. Context refresh occurs when any connected account succeeds.

### 5.8 Instamart User Resolution Safety

#### Problem

`resolve_user` for Instamart tools falls back to latest ingress sender when `user_id` is omitted, which is unsafe in active groups.

#### Required changes

1. For LLM/tool path:
   - require explicit `user_id` for `connect`, `complete`, `disconnect`, and `orders_sync`.
   - if missing: fail with actionable error.
2. Runtime command path already provides `msg.sender`; keep as-is.
3. `status` command:
   - household scope may omit user
   - personal scope must include explicit user.

#### Code seams

1. `src/tools/meal_instamart_auth.rs`
2. `src/tools/meal_instamart_orders_sync.rs`

#### Acceptance criteria

1. Tool calls cannot silently bind/sync/disconnect the wrong user.

### 5.9 Idempotency API Contract Cleanup

#### Problem

`client_idempotency_key` exists in API but is not used for keying.

#### Required changes

Pick one path and enforce consistently:

Path A (recommended): **activate it**
1. If `client_idempotency_key` is present and non-empty, include it in `effective_idempotency_key` derivation.
2. Keep source-message fence unchanged (still primary anti-duplicate guard per turn).

Path B (if avoiding API drift): **remove it**
1. Remove from schema and struct.
2. Reject unknown field if strict serde mode enabled later.

This spec adopts Path A to avoid breaking existing call sites/tests.

#### Code seams

1. `src/meal/store.rs` idempotency key derivation.
2. `src/tools/meal_turn_commit.rs` schema text update.

#### Acceptance criteria

1. Same payload + same source + different client idempotency key produces different effective key.
2. Same key + same payload remains noop duplicate.

### 5.10 Legacy Reply Gate Table Decommission

#### Problem

Legacy `meal_reply_gate_events` fallback remains in read path and startup migration copy runs every boot.

#### Required changes

1. Add one-time migration marker table:
   - `meal_schema_migrations(name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`
2. Run legacy copy only when marker absent.
3. After successful copy:
   - write marker
   - stop querying legacy table in `reply_gate_allows`.
4. Keep table existence tolerance only in migration code.

#### Code seams

1. `src/meal/store.rs` schema bootstrap + migration function + `reply_gate_allows`.

#### Acceptance criteria

1. Startup no longer performs repeated legacy upsert copy.
2. Runtime reply-gate lookup reads only `meal_control_gate_events`.

### 5.11 Context Limit Clamping

#### Problem

`meal_context_get` accepts unbounded limits that flow directly into DB queries.

#### Required changes

1. Define clamp constants in meal store module:
   - `MAX_PREF_LIMIT`
   - `MAX_EPISODE_LIMIT`
   - `MAX_PANTRY_LIMIT`
   - `MAX_FEEDBACK_LIMIT`
   - `MAX_INGRESS_LIMIT`
2. Apply clamp in one place:
   - `ContextQuery::normalized()` helper OR at start of `context_get`.
3. Maintain backward compatibility:
   - default behavior unchanged for ordinary values.

#### Code seams

1. `src/meal/store.rs` query normalization.
2. `src/tools/meal_context_get.rs` optional schema descriptions for limits.

#### Acceptance criteria

1. Oversized limits no longer trigger large scans.

## 6) Test Plan Additions

## 6.1 Unit tests (must add)

1. Recommendation:
   - dislikes/avoidance penalization test.
   - hard constraint exclusion test.
   - fairness objective tie-break test.

2. Commit:
   - `text` alias persists to `feedback_text`.
   - `feedback_text` canonical path.
   - client idempotency key affects effective key (Path A).

3. Identity:
   - ambiguous two-user name does not map to "other user".
   - unresolved claim persists for ambiguous subject.

4. Reply gate:
   - bot username unavailable + `@otheruser` does not bypass classifier.
   - classifier parse missing `distill_decision` defaults to skip.

5. Distillation:
   - inline path advances cursor for processed ingress event.
   - worker skips already cursor-advanced ingress.

6. Instamart:
   - auto-sync continues after one user failure and succeeds on another.
   - missing `user_id` in LLM path returns actionable error.

7. Migration:
   - legacy copy runs once and sets marker.
   - subsequent startup does not rerun copy and no legacy read fallback is used.

## 6.2 Integration tests (must add)

1. End-to-end message:
   - preference statement -> distill -> context reflects user-scoped preference.
2. "No eggs for user B" + recommendation:
   - recommendations avoid egg options.
3. Group with two users and ambiguous name:
   - no wrong-user mutation, unresolved clarification appears.

## 7) Observability Changes

1. Add structured counters/timers:
   - `meal_reply_gate_classifier_db_ms`
   - `meal_distill_inline_applied`
   - `meal_distill_worker_applied`
   - `meal_distill_cursor_advanced`
   - `meal_instamart_auto_sync_attempted_users`
   - `meal_instamart_auto_sync_success_users`
   - `meal_recommend_constraint_block_count`

2. Add warning log for deprecated feedback alias usage (`text`).

## 8) Rollout Sequence

1. Land shared identity + distillation modules first (behavior-preserving).
2. Land feedback contract compatibility and tests.
3. Land recommendation semantics fix + fairness ranking + tests.
4. Land reply gate/direct-address tightening + lightweight query.
5. Land Instamart candidate/retry and explicit-user changes.
6. Land idempotency key activation.
7. Land migration marker + legacy fallback removal.
8. Land limit clamp + perf tests.

Each step should pass:

1. `cargo fmt --all -- --check`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test` (or targeted suites during iteration, full before merge)

## 9) Expected Outcome

After this remediation set:

1. Meal recommendations become constraint-safe and person-correct.
2. Distillation becomes less redundant and more deterministic.
3. Instamart synchronization becomes robust in multi-user chat.
4. API contracts are coherent and auditable.
5. Reply gate path is cheaper and less noisy while preserving selective response behavior.
