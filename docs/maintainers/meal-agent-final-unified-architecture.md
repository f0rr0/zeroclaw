# Meal Agent Final Unified Architecture (Locked Canonical Spec)

Status: Final v3.1 (Implementation Lock)  
Date: 2026-03-14  
Scope: Single household, single long-running Telegram group, one ZeroClaw daemon (Rust)

## 0) Canonical Contract

This is the single authoritative meal-agent specification.

This version is implementation-locked:

1. One path only.
2. No optional architecture branches.
3. Every behavior maps to concrete ZeroClaw integration seams.
4. Every critical behavior has acceptance tests.

## 1) Source-Verified ZeroClaw Baseline

These are current facts from code and are treated as hard integration constraints:

1. Tool output contract is string-based: `ToolResult { success, output, error }`.
2. Tool calls in one turn can execute in parallel.
3. Channel runtime history is sender-scoped.
4. `on_message_received` hooks run before runtime command handling.
5. Hook system currently registers only built-in hooks from `config.hooks.builtin` wiring.
6. Telegram long polling currently requests only `message` updates.
7. Telegram sender identity currently prefers username when present.
8. Base channel task prompt explicitly tells model: "When the user sends a message, respond naturally."
9. Cron jobs live in a separate SQLite store (`workspace/cron/jobs.db`) and use UUID IDs.

## 2) Hard Limits (Still True)

These cannot be fully eliminated under current runtime model:

1. Prompt-only tool routing cannot guarantee meal tool invocation for every relevant turn.
2. Perfect cross-user reply ordering is impossible under concurrent message dispatch.
3. Strict exactly-once across meal DB and cron DB is impossible; target is idempotent at-least-once.
4. Cancellation cannot guarantee zero side effects for non-cooperative long-running operations.

## 3) Product Goals (Locked)

The system must:

1. Learn durable individual preferences over time.
2. Recommend meals using ambient conversation + pantry + history + feedback.
3. Support group decision making and delegation naturally.
4. Elicit and learn from post-meal feedback.
5. Remain conversational without noisy over-replying.
6. Keep auditable meal decision records.

## 4) Interaction Model (Observe-All, Respond-Selectively)

Tagged-only mode is rejected. `mention_only = false` is mandatory.

Locked behavior:

1. Ingest every allowed inbound group message.
2. Reply only when needed.
3. Use scheduler nudges for proactive interventions.

Why explicit gating is required:

1. In non-mention mode, every allowed group message reaches `process_channel_message`.
2. ZeroClaw default brain prompt is reply-first.
3. Prompt-only "reply only when needed" is not reliable enough for non-noisy group behavior.

Reply policy (required, not optional):

1. `MealIngressGateHook` runs on every inbound message.
2. The hook always writes an ingress event first (idempotent write).
3. Runtime control commands (`/models`, `/model`, `/new`) are detected using a shared parser and always `Continue`.
4. Direct-address signals (`@bot mention`, explicit bot-reply context, active nudge flow) always `Continue`.
5. Non-addressed turns call `ReplyGateClassifier` and return strict JSON:  
   `decision: respond|ingest_only`, `confidence: 0..1`, `intent: meal|feedback|clarification|other`, `reason_code`.
6. Decision rule: `respond` only when `decision=respond` and `confidence >= min_confidence`; else `ingest_only`.
7. Gate model failure is fail-closed (`ingest_only`) unless deterministic direct-address/runtime-command bypass matched.
8. `MealOutboundGuardHook` cancels outbound sends unless the current turn has a task-local gate-allow marker, preventing accidental over-replies.

## 5) Required Core Deltas Against ZeroClaw

These are required code changes, not optional:

1. `src/channels/telegram.rs`
   - parse `edited_message` updates and emit correction ingress events
   - change sender identity to stable numeric Telegram user ID when available
   - keep human-readable sender alias separately for display only
2. `src/channels/mod.rs`
   - register `MealIngressGateHook` and `MealOutboundGuardHook` in hook runner when meal mode enabled
   - expose shared runtime-command parser for both channel path and meal ingress hook
   - set per-turn task-local context usable by tools and hooks (`source_message_id`, `channel`, `reply_target`, `thread_ts`, `gate_decision`)
3. `src/agent/loop_.rs`
   - add task-local turn metadata access for tool argument enrichment
   - inject server turn context into `meal_turn_commit` args before execution
4. `src/tools/mod.rs`
   - register meal tools: `meal_context_get`, `meal_recommend`, `meal_turn_commit`
5. config schema
   - add `[meal_agent]` configuration block and validation hooks
6. startup validation path
   - fail fast when meal mode invariants are violated

## 6) End-to-End Runtime Pipeline (Exact Order)

```text
Telegram update
  -> allowlist + normalization
  -> MealIngressGateHook.on_message_received
       1) write ingress event (idempotent)
       2) runtime command parser (shared with channel runtime)
       3) direct-address detector
       4) non-addressed -> ReplyGateClassifier (`respond|ingest_only`)
       5) persist gate decision + set task-local gate marker
       6) ingest_only => Cancel
       7) respond => Continue
  -> standard ZeroClaw channel runtime (respond path only)
       -> LLM turn
       -> meal_context_get (read)
       -> meal_recommend (read)
       -> meal_turn_commit (single mutating lane)
             -> WriteGuard + CommitService
             -> meal DB state/events/outbox
       -> MealOutboundGuardHook.on_message_sending
            -> verify task-local gate marker
            -> allow send or cancel

OutboxReconciler worker
  -> consume outbox intents
  -> reconcile cron jobs idempotently
  -> finalize mapping + outbox status

PreferenceDistiller worker
  -> consume ambient ingress window
  -> derive high-confidence preference evidence
  -> upsert preference evidence/state transactionally
```

## 7) Components (Locked)

## 7.1 Runtime Components

1. `MealIngressGateHook`
2. `ReplyGateClassifier`
3. `MealOutboundGuardHook`
4. `TurnContextInjector`
5. `ContextService`
6. `RecommendationService`
7. `CommitService`
8. `WriteGuard`
9. `OutboxReconciler`
10. `PreferenceDistiller`

## 7.2 Package Layout

```text
crates/meal-agent/
  src/
    lib.rs
    config.rs
    runtime.rs
    ingress/
      gate_hook.rs
      gate_classifier.rs
      outbound_guard_hook.rs
      ingress_writer.rs
      turn_context.rs
    tools/
      meal_context_get.rs
      meal_recommend.rs
      meal_turn_commit.rs
    services/
      context_service.rs
      recommendation_service.rs
      commit_service.rs
      write_guard.rs
      outbox_reconciler.rs
      preference_distiller.rs
    store/
      sqlite.rs
      migrations.rs
      repos/...
```

## 8) Tool Contracts (Locked)

## 8.1 `meal_context_get` (read)

Input:

1. `household_id`
2. `active_user_ids[]`
3. `focus_terms[]`
4. `now_local`
5. `limits`

Output:

1. `preferences_by_user`
2. `recent_meal_episodes`
3. `pantry_snapshot`
4. `recent_feedback`
5. `open_decision_state`
6. `recent_ingress_window`
7. `grounding_refs[]`
8. `unresolved_questions[]`

## 8.2 `meal_recommend` (read)

Input:

1. `household_id`
2. `context_bundle`
3. `candidate_set[]` (LLM expansion layer only)
4. `mode_hint` (`cook|self_cook|no_cook`)

Output:

1. `ranked_candidates[]`
2. `score_breakdown`
3. `fairness_metrics`
4. `grounding_refs[]`
5. `warnings[]`

## 8.3 `meal_turn_commit` (single mutating tool)

Input:

1. `household_id`
2. `meal_date_local`
3. `meal_slot` (`breakfast|lunch|dinner|snack|unslotted`)
4. `slot_instance` (default `1`)
5. `operations[]`
6. `client_idempotency_key` (metadata only)
7. `source_message_id` (ignored from model; overwritten by server turn context)

Output:

1. `commit_status` (`applied|noop_duplicate|idempotency_mismatch|conflict|in_progress_retry|failed`)
2. `effective_idempotency_key`
3. `meal_anchor_key`
4. `decision_case_id`
5. `operation_results[]`
6. `warnings[]`

## 9) Mutation Safety and Decision Identity (Locked)

## 9.1 Idempotency

1. `meal_anchor_key = "{household_id}:{meal_date_local}:{meal_slot}:{slot_instance}"`.
2. `op_fingerprint = sha256(canonical_json(operations[]))`.
3. `effective_idempotency_key = sha256(household_id + source_message_id + meal_anchor_key + op_fingerprint)`.
4. `request_hash = sha256(canonical_json(payload_without_client_key))`.
5. same idempotency key + different request hash => hard mismatch error.

## 9.2 Turn Write Fence

1. one mutating commit attempt per `(household_id, source_message_id, commit_kind)`.
2. duplicate replay => deterministic `noop_duplicate`.

## 9.3 Decision Identity

1. `decision_case_id = "{meal_anchor_key}:v{case_seq}"`.
2. `case_seq` allocated transactionally per anchor.
3. exactly one open case per meal anchor via partial unique index.

## 10) Data Model (Authoritative Meal DB)

## 10.1 Required Tables

1. `meal_ingress_events`
2. `meal_user_identities`
3. `meal_idempotency_keys`
4. `meal_turn_commit_fence`
5. `meal_decision_states`
6. `meal_decision_events`
7. `meal_episodes`
8. `meal_feedback_signals`
9. `meal_user_preferences`
10. `meal_preference_evidence`
11. `meal_pantry_hints`
12. `meal_lexicon_entries`
13. `meal_kitchen_profile`
14. `meal_nudge_jobs`
15. `meal_outbox_events`
16. `meal_reply_gate_events`
17. `meal_text_index` (FTS5)

## 10.2 Required Constraints

1. `UNIQUE(household_id, channel, ingress_event_id)` on ingress events
2. `UNIQUE(household_id, channel, source_message_id, revision)` on ingress events
3. `UNIQUE(household_id, channel, platform_user_id)` on identities
4. `PRIMARY KEY(household_id, tool_name, effective_idempotency_key)` on idempotency keys
5. `UNIQUE(household_id, source_message_id, commit_kind)` on commit fence
6. `PRIMARY KEY(household_id, decision_case_id)` on decision states
7. `UNIQUE(household_id, meal_anchor_key, case_seq)` on decision states
8. `UNIQUE(household_id, meal_anchor_key) WHERE status='open'` on decision states
9. `UNIQUE(household_id, decision_case_id, event_seq)` on decision events
10. `UNIQUE(household_id, meal_anchor_key)` on episodes
11. `UNIQUE(household_id, decision_case_id)` on nudge mapping
12. `UNIQUE(household_id, channel, source_message_id)` on reply-gate events

## 11) Ingress and Distillation

## 11.1 Ingress Rules

1. Every allowed inbound message becomes ingress.
2. Edits become new revisions, never destructive overwrite.
3. Ingress stores bounded raw content + metadata + attachment refs.
4. Latest revision per source message is used in retrieval.
5. Every inbound message gets one persisted reply-gate decision record.

## 11.2 Preference Distillation Rules

1. Distiller scans recent ingress windows on schedule.
2. Distiller extracts candidate preference facts with confidence.
3. facts above threshold write `meal_preference_evidence`.
4. `meal_user_preferences` is derived from evidence with time decay and contradiction handling.
5. distillation writes are transactional and auditable.

## 12) Retrieval and Recommendation

## 12.1 Context Retrieval

`meal_context_get` merges:

1. structured state
2. ingress recency
3. BM25 over FTS
4. semantic retrieval over evidence corpus

Startup requirement:

1. semantic retrieval enabled only when meal embedding provider is configured; otherwise meal mode startup fails.

## 12.2 Candidate Generation and Ranking

1. deterministic seed set from:
   - historically liked meals for relevant ingredients
   - pantry-fit historical meals
   - novelty-safe alternatives
2. LLM expands seed set, does not replace it.
3. deterministic rerank scores:
   - preference fit
   - group fairness floor
   - pantry fit
   - novelty
   - feasibility
   - fatigue balance
4. hard rejection only for safety constraints.

## 13) Scheduler Saga (Cron Reconciliation)

1. commit writes outbox intent in meal DB tx.
2. reconciler processes intent idempotently.
3. deterministic cron name: `meal_nudge:{household_id}:{decision_case_id}`.
4. schedule reconcile:
   - if mapped job exists => `cron_update`
   - else `cron_list` by name
   - 0 matches => `cron_add`
   - 1 match => adopt + update
   - >1 matches => mark ambiguous failure and alert
5. cancel reconcile:
   - remove mapped ID
   - fallback remove by deterministic name
6. finalize outbox + mapping in meal DB tx.

## 14) Prompt Policy (Locked)

## 14.1 Main Brain Policy

1. Non-meal direct asks can be answered directly.
2. Personalized meal claims require `meal_context_get`.
3. Ranked recommendation requires `meal_recommend`.
4. Mutations require `meal_turn_commit`.
5. Ambiguous low-confidence asks get one concise clarification.
6. no regex/procedural semantic extraction in prompts.

## 14.2 Reply Gate Classifier Prompt Contract

1. Gate call happens inside `MealIngressGateHook`, before the normal LLM turn.
2. Input bundle:
   - current inbound message
   - last `N` ingress turns in same chat/thread
   - open meal decision state summary
   - active nudge window state
3. Output is strict JSON (schema-validated):
   - `decision`: `respond|ingest_only`
   - `confidence`: float in `[0,1]`
   - `intent`: `meal|feedback|clarification|other`
   - `reason_code`: short enum (`direct_meal_ask`, `feedback_signal`, `ambient_context`, `offtopic`, `uncertain`)
4. Classifier prompt explicitly prefers abstention for uncertain ambient chatter.
5. Classifier output parse failure or provider failure => fail-closed `ingest_only` with metric emission.

## 15) Configuration Contract (Locked)

```toml
[meal_agent]
enabled = true
household_id = "household_main"
ingress_retention_days = 14
distillation_interval_minutes = 30
semantic_embeddings_required = true

[meal_agent.reply_gate]
model = "small-fast-model"
min_confidence = 0.72
recent_window_size = 8
unsolicited_cooldown_seconds = 900
fail_closed = true

[cron]
enabled = true

[memory]
auto_save = false

[hooks]
enabled = true

[autonomy]
auto_approve = [
  "meal_context_get",
  "meal_recommend",
  "meal_turn_commit"
]

[channels_config.telegram]
bot_token = "<token>"
allowed_users = ["<telegram_user_id_a>", "<telegram_user_id_b>"]
mention_only = false
interrupt_on_new_message = true
```

## 16) Startup Guardrails (Fail Fast)

Meal mode startup must fail when any check fails:

1. meal tools not registered
2. meal tools hidden by non-CLI exclusions
3. meal tools missing from auto-approve
4. meal ingress hook not registered
5. meal outbound guard hook not registered
6. shared runtime-command parser not wired in ingress hook
7. turn-context injector missing
8. cron disabled while nudges enabled
9. telegram not in observe-all mode (`mention_only` must be `false`)
10. edited-message ingestion path disabled
11. semantic embeddings required but not configured
12. meal DB migration mismatch

## 17) Observability and Audit

Emit:

1. `meal_ingress_write_*`
2. `meal_reply_gate_decision`
3. `meal_reply_gate_marker_*`
4. `meal_outbound_guard_*`
5. `meal_distillation_*`
6. `meal_context_get_*`
7. `meal_recommend_*`
8. `meal_turn_commit_*`
9. `meal_outbox_reconcile_*`

Track:

1. recommendation grounding rate
2. reply-gate false-positive rate
3. reply-gate false-negative rate
4. reply-gate abstain rate
5. unsolicited reply suppression rate
6. idempotency mismatch rate
7. turn-fence duplicate rate
8. decision conflict rate
9. feedback capture rate
10. outbox lag and retry depth

Persist audit:

1. append-only decision events
2. append-only preference evidence
3. ingress event and revision lineage
4. reply-gate decision records
5. idempotency snapshot records
6. commit fence records

## 18) Usage Scenario Correctness

## 18.1 Individual preference memory

Target: "A spicy + onion/chilli side", "B bland + tomato on poha".  
Correctness condition:

1. ambient statements ingressed
2. distiller converts to durable evidence
3. recommendation uses per-user evidence and emits grounded options

## 18.2 Contextual chicken recommendation

Target conversation:

1. A: chicken today
2. B: cabbage/capsicum in fridge
3. A: `@bot recommend`

Correctness condition:

1. first two turns ingressed even untagged
2. third turn responds
3. retrieval includes pantry + chicken history + feedback
4. output references specific past meals/feedback evidence IDs

## 18.3 Non-noisy group behavior

Correctness condition:

1. ambient chat is ingested but reply-gated to silence
2. bot responds only when directly asked or nudge condition triggers
3. runtime commands still work even when gate abstains ambient turns

## 18.4 Concurrent conflict safety

Correctness condition:

1. simultaneous conflicting commits resolve deterministically
2. one commit applies, one returns conflict/noop
3. no duplicate episode rows

## 19) Requirement-to-Code-to-Test Matrix (Mandatory)

1. Observe-all + selective reply  
Code seams: `src/channels/telegram.rs`, `src/channels/mod.rs`, hook registration  
Tests: ambient ingest without reply; direct ask responds; runtime command bypasses gate.
2. Stable user identity  
Code seams: `src/channels/telegram.rs`, identity mapping repo  
Tests: username change still maps same actor.
3. Server context injection  
Code seams: `src/channels/mod.rs`, `src/agent/loop_.rs`, meal tool args  
Tests: commit without model `source_message_id` still writes correct source.
4. Commit idempotency + fence  
Code seams: meal store + commit service  
Tests: duplicate/retry, mismatched payload key, concurrent commit.
5. Distillation durability  
Code seams: distiller service + preference repos  
Tests: ambient preference becomes durable without direct bot invocation.
6. Cron reconciliation idempotency  
Code seams: outbox reconciler + cron tool adapters  
Tests: crash windows, duplicate named jobs, adopt/update/remove behavior.
7. Correction handling  
Code seams: telegram edited message ingestion + revision resolution  
Tests: edited pantry statement updates recommendation context.
8. Semantic retrieval readiness  
Code seams: startup validation + retrieval service  
Tests: startup fail when embeddings required but unavailable.
9. Outbound suppression integrity  
Code seams: meal outbound guard hook + task-local gate context  
Tests: forced model chatter without gate marker is cancelled.

## 20) Implementation Sequence (Locked)

1. add `meal_agent` config schema + validation
2. implement DB migrations and repositories
3. implement telegram identity + edited-message ingestion deltas
4. implement shared runtime-command parser module
5. implement ingress hook + reply gate classifier + decision writes + gate marker set
6. implement outbound guard hook
7. implement turn-context injector
8. implement `meal_turn_commit` with guard/fence
9. implement `meal_context_get` and `meal_recommend`
10. implement outbox reconciler
11. implement preference distiller
12. implement startup guardrails
13. execute matrix tests and scenario stress tests
