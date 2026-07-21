---
'@workflow/world-vercel': minor
'@workflow/world-postgres': minor
'@workflow/world-local': minor
'@workflow/world': minor
'@workflow/core': minor
---

Batch step transitions: fold a sequential inline step transition — completing
step N and creating + starting the lone next inline step N+1 — into ONE durable,
atomic write instead of two serialized event writes (`step_completed` then a
lazy `step_started`). This is not optimistic start: step N+1's body still runs
only after the batch (its durable claim) returns; the win is removing a full
round-trip per step, and the create-claim for N+1 now commits atomically with
N's completion (strictly fewer intermediate crash states).

On by default, with an emergency kill switch: `WORKFLOW_BATCH_TRANSITIONS=0`
(or `false`) restores the exact prior two-write path byte-for-byte, mirroring
the default-on + kill-switch shape of `WORKFLOW_TURBO`. The kill switch is
**temporary** — a burn-in rollback control planned for removal, not a long-term
configurable option.

Every World implements the `events.createBatch` contract, so the batch path runs
uniformly (no silent single-write fallback by World): the Vercel World against
its `/events/batch` endpoint, `world-postgres` in one `drizzle.transaction`
(true all-or-nothing), and `world-local` as a per-run write sequence guarded by
its on-disk exclusive claims. A `stepCreated` signal on the started result marks
the create-claim winner; an already-applied retry or lost race omits it so the
non-committer re-derives from a fresh replay instead of double-running the body.

The transition remains transparent and idempotent end-to-end: a server without
the `/events/batch` endpoint (404/405) falls back to the separate awaited POSTs
for the rest of the invocation, and conflict (409), not-running (410), and
stale-snapshot (412) responses abandon the transition and re-derive from a fresh
replay, so transport failures are always safe to retry.
