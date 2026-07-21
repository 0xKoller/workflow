import type { CreateEventRequest } from '@workflow/world';

/**
 * Client-side assembly of a v2 **suspension batch** — the ordered event set of
 * one workflow suspension that {@link World.events.createBatch} commits in a
 * single conditional transaction (the analog of Temporal's MapExecuteBatchCAS).
 *
 * This module is the single source of truth for two things the SDK must keep
 * byte-for-byte in agreement with workflow-server's batch endpoint, so a batch
 * the client assembles is never rejected for a shape/size the server derives
 * differently:
 *
 * 1. **Grammar order.** The server validates and classifies the frames with
 *    `classifySuspensionBatch` (workflow-server `lib/data/events.ts`):
 *
 *      suspensionBatch := (step_completed | step_failed)?          # leading outcome (index 0)
 *                         ( wait_created | wait_completed | hook_received
 *                           | (step_created step_started)          # inline born-running step
 *                           | step_created )*                      # queued (pending) step
 *                         (run_completed | run_failed)?            # terminal, MUST be last
 *
 *    with the invariants: ≥1 event; a terminal is the final element with
 *    nothing after it; a `step_started` is *immediately* preceded by a
 *    `step_created` with the same `correlationId`; and each step correlationId
 *    appears in exactly one operation. The middle section is a free
 *    interleaving, so the client picks one stable order.
 *
 * 2. **Item budget.** DynamoDB `TransactWriteItems` caps at 100 items; the
 *    server rejects an over-budget batch with a distinct 400. Each role
 *    contributes a fixed transaction-item count ({@link SUSPENSION_BATCH_ITEM_COST},
 *    mirrored from the server's `opItemCount`), plus one run-fence item. The
 *    client budgets against the same arithmetic so it can split or fall back
 *    *before* the round-trip rather than eat a 400.
 *
 * NOT carried here (the caller keeps these on the single-event path — the
 * server excludes them for physical reasons): `hook_created` / `hook_disposed`
 * (the hook entity + token constraint are written in a fixed region via
 * `hooksHomeDb`, so they cannot join a non-home-region run's transaction) and
 * `attr_set` (a nested-map per-key update that would clobber a concurrent
 * step-writer's attribute write if folded into the fenced run patch).
 *
 * The builder is pure: payload construction (dehydrated inputs, ownership
 * stamps, telemetry) happens at the call site where the run/step context
 * lives; this module only *orders* pre-built {@link CreateEventRequest}s and
 * *counts* their transaction cost.
 */

/**
 * DynamoDB caps `TransactWriteItems` at 100 items. Mirrors
 * `MAX_SUSPENSION_BATCH_ITEMS` in workflow-server `lib/data/events.ts`.
 */
export const MAX_SUSPENSION_BATCH_ITEMS = 100;

/**
 * Transaction items each batch role contributes, mirrored verbatim from the
 * server's `opItemCount` (workflow-server `lib/data/events.ts`). The run-fence
 * item is separate ({@link RUN_FENCE_ITEM_COST}).
 *
 * - leading outcome (`complete` / `fail`): step patch + event row.
 * - inline born-running step (`step-run`): step create + created event + started event.
 * - queued step (`step-pending`): step create + event row.
 * - wait (`wait-create` / `wait-complete`): wait create/patch + event row.
 * - `hook-received`: event row only (run's own region).
 * - terminal (`run-completed` / `run-failed`): event row only — the run patch
 *   is folded into the fence item, so the terminal adds just its event.
 */
export const SUSPENSION_BATCH_ITEM_COST = {
  leadingOutcome: 2,
  inlineStep: 3,
  pendingStep: 2,
  wait: 2,
  hookReceived: 1,
  terminal: 1,
} as const;

/** The fenced run-version item present in every v2 batch transaction. */
export const RUN_FENCE_ITEM_COST = 1;

/**
 * The batchable components of one suspension, already built as
 * {@link CreateEventRequest}s by the caller. Every field is optional; an empty
 * set assembles to an empty batch (the caller must not send a 0-event batch —
 * the server requires ≥1).
 */
export interface SuspensionBatchComponents {
  /**
   * The deferred outcome of the previous inline step: `step_completed` or
   * `step_failed`. Always index 0 when present (the server reads it only at
   * index 0).
   */
  leadingOutcome?: CreateEventRequest;
  /** `hook_received` frames (e.g. abort deliveries) — event-row only, batchable. */
  hookReceiveds?: CreateEventRequest[];
  /**
   * Inline born-running steps as `{ created, started }` pairs. Each pair is
   * emitted adjacently (created immediately followed by started, same
   * correlationId) as the server's classifier requires.
   */
  inlineSteps?: Array<{
    created: CreateEventRequest;
    started: CreateEventRequest;
  }>;
  /** Queued fan-out steps as bare `step_created` frames (no inline start). */
  pendingSteps?: CreateEventRequest[];
  /** `wait_created` / `wait_completed` frames. */
  waits?: CreateEventRequest[];
  /**
   * The terminal run event (`run_completed` / `run_failed`). Emitted last, with
   * nothing after it.
   */
  terminal?: CreateEventRequest;
}

/** The assembled, grammar-ordered batch plus its transaction cost. */
export interface AssembledSuspensionBatch {
  /** Frames in the exact order the server grammar expects. */
  events: CreateEventRequest[];
  /**
   * Worst-case transaction item count, including the run-fence item — the same
   * arithmetic the server applies before it commits. Compare against
   * {@link MAX_SUSPENSION_BATCH_ITEMS}.
   */
  itemCount: number;
  /** Whether {@link itemCount} is within {@link MAX_SUSPENSION_BATCH_ITEMS}. */
  withinItemBudget: boolean;
}

/**
 * Order one suspension's batchable components into the server's grammar and
 * compute the transaction item cost. Pure — no payload construction, no I/O.
 *
 * Middle-section order (a valid grammar instance; the server accepts any
 * interleaving): `hook_received`, then inline born-running pairs, then bare
 * pending creates, then waits. The leading outcome always leads and the
 * terminal always trails.
 */
export function assembleSuspensionBatch(
  components: SuspensionBatchComponents
): AssembledSuspensionBatch {
  const {
    leadingOutcome,
    hookReceiveds = [],
    inlineSteps = [],
    pendingSteps = [],
    waits = [],
    terminal,
  } = components;

  const events: CreateEventRequest[] = [];
  if (leadingOutcome) events.push(leadingOutcome);
  events.push(...hookReceiveds);
  for (const { created, started } of inlineSteps) {
    // Adjacency is load-bearing: the server pairs a step_started only with the
    // step_created immediately before it (same correlationId).
    events.push(created, started);
  }
  events.push(...pendingSteps);
  events.push(...waits);
  if (terminal) events.push(terminal);

  const itemCount =
    RUN_FENCE_ITEM_COST +
    (leadingOutcome ? SUSPENSION_BATCH_ITEM_COST.leadingOutcome : 0) +
    hookReceiveds.length * SUSPENSION_BATCH_ITEM_COST.hookReceived +
    inlineSteps.length * SUSPENSION_BATCH_ITEM_COST.inlineStep +
    pendingSteps.length * SUSPENSION_BATCH_ITEM_COST.pendingStep +
    waits.length * SUSPENSION_BATCH_ITEM_COST.wait +
    (terminal ? SUSPENSION_BATCH_ITEM_COST.terminal : 0);

  return {
    events,
    itemCount,
    withinItemBudget: itemCount <= MAX_SUSPENSION_BATCH_ITEMS,
  };
}
