---
'@workflow/world-vercel': patch
'@workflow/world': patch
'@workflow/core': patch
---

Add a flag-gated batch step-transition client. When `WORKFLOW_BATCH_TRANSITIONS`
is enabled and the World implements the optional `events.createBatch`, the
sequential inline runtime path folds a step transition — completing step N and
creating + starting the lone next inline step N+1 — into ONE durable, atomic
POST instead of two serialized event writes (`step_completed` then a lazy
`step_started`). This is not optimistic start: step N+1's body still runs only
after the batch (its durable claim) returns; the win is removing a full HTTPS
round-trip per step. Default OFF, and fully transparent — a World without
`createBatch`, or a server without the `/events/batch` endpoint (404/405),
falls back to today's separate awaited POSTs for the rest of the invocation.
Conflicts (409), not-running (410), and stale-snapshot (412) responses abandon
the transition and re-derive from a fresh replay; the batch is idempotent
end-to-end, so transport failures are safe to retry.
