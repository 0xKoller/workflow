import {
  SPEC_VERSION_CURRENT,
  type Event,
  type WorkflowRun,
} from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { registerStepFunction } from './private.js';
import { setWorld } from './runtime/world.js';
import { workflowEntrypoint } from './runtime.js';
import { dehydrateWorkflowArguments } from './serialization.js';

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn((p: Promise<unknown>) => {
    p.catch(() => {});
  }),
}));

// REGRESSION: attribute-ordering causal safety under WORKFLOW_BATCH_TRANSITIONS.
//
// A workflow that writes an attribute co-suspended with the NEXT step right
// after a deferrable step wedges under the batch flag and eventually run_fails
// via replay-budget timeout, while completing fine with the flag off. Two
// equivalent shapes trigger it:
//   raced:    await Promise.race([setAttributes(...), s3()])
//   un-raced: setAttributes(...);  await s3()   // setAttributes not awaited
// Both produce ONE suspension carrying an attribute event AND a single lazy
// inline step (stepCount === 1, attributeCount === 1).
//
// Root cause (pre-fix): `batchTransitionCandidate` (runtime.ts) judged only
// step/hook/wait counts, so an attr+step suspension looked batchable — which
// SUPPRESSED the pre-`handleSuspension` flush of the deferred completed(N).
// `handleSuspension` then durably wrote attr_set (an ordering inversion: the
// batch endpoint never carries attr_set, and completed(N) was still deferred),
// and the `hasAttributeEvents` replay `continue` restarted the loop before the
// batch commit. The pending-deferral replay view is [...cachedEvents, synthetic]
// with no reload, so cachedEvents never gained the durable attr_set → every
// iteration re-derived the same attr_set → livelock → replay-budget run_failed.
//
// Fix: add `err.attributeCount === 0` to `batchTransitionCandidate`, so an
// attr+step suspension is NOT a batch candidate and hits the existing pre-flush
// (`pendingBatchTransition && !batchTransitionCandidate`). That makes completed(N)
// durable via the single path BEFORE handleSuspension writes attr_set, restoring
// the causal order completed(N) < attr_set that the single-write path always
// produces. These tests assert the FIXED behavior: batch ON now matches the
// flag-off control — run_completed, no livelock, completed(N) before attr_set.
const runs: Record<string, number> = { astep1: 0, astep2: 0, astep3: 0 };
registerStepFunction('astep1', async () => {
  runs.astep1 += 1;
  return 'r1';
});
registerStepFunction('astep2', async () => {
  runs.astep2 += 1;
  return 'r2';
});
registerStepFunction('astep3', async () => {
  runs.astep3 += 1;
  return 'r3';
});

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

// astep1 = normal (first inline step, never deferred)
// astep2 = its completion is DEFERRED (2nd inline step, batch's home)
// astep3 = scheduled in the suspension that ALSO writes the attribute.
const attrRaceWorkflow = `const setAttributes = globalThis[Symbol.for("WORKFLOW_SET_ATTRIBUTES")];
  const s1 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep1");
  const s2 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep2");
  const s3 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep3");
  async function workflow() {
    const a = await s1();
    const b = await s2();
    await Promise.race([
      setAttributes([{ key: "k", value: "v" }]),
      s3(),
    ]);
    return [a, b];
  }${xform('workflow')}`;

// Un-raced variant: setAttributes is called but NOT awaited, then the next step
// is awaited. Same suspension shape (one attribute + one step) via a different
// user-code idiom.
const attrUnracedWorkflow = `const setAttributes = globalThis[Symbol.for("WORKFLOW_SET_ATTRIBUTES")];
  const s1 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep1");
  const s2 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep2");
  const s3 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("astep3");
  async function workflow() {
    const a = await s1();
    const b = await s2();
    setAttributes([{ key: "k", value: "v" }]);
    const c = await s3();
    return [a, b, c];
  }${xform('workflow')}`;

async function driveRun(opts: {
  runId: string;
  withBatch: boolean;
  source: string;
}) {
  const { runId, source } = opts;
  const durable: Event[] = [];
  // Ordered record of every durable write, single-path and batch alike.
  const writeOrder: string[] = [];
  let seq = 0;
  // Hard cap so a livelock cannot hang the test runner.
  let writeCap = 200;
  let attrReattempts = 0;
  const rec = (data: any): Event => {
    if (--writeCap <= 0) throw new Error('write cap exceeded (livelock?)');
    seq += 1;
    const e = {
      eventId: `e-${seq}`,
      runId,
      createdAt: new Date(),
      ...data,
    } as Event;
    durable.push(e);
    writeOrder.push(
      `${data.eventType}${data.correlationId ? `(${data.correlationId})` : ''}`
    );
    return e;
  };

  const runEntity: WorkflowRun = {
    runId,
    workflowName: 'workflow',
    status: 'running',
    input: await dehydrateWorkflowArguments([], runId, undefined, []),
    createdAt: new Date('2024-01-01T00:00:00.000Z'),
    updatedAt: new Date('2024-01-01T00:00:00.000Z'),
    startedAt: new Date('2024-01-01T00:00:00.000Z'),
    deploymentId: 'test-deployment',
  };
  const runningStep = (data: any, input?: unknown) => ({
    runId,
    stepId: data.correlationId,
    stepName: data.eventData?.stepName,
    status: 'running' as const,
    attempt: 1,
    input: input !== undefined ? input : data.eventData?.input,
    startedAt: new Date(),
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  const create = vi.fn(async (_runId: string, data: any) => {
    if (data.eventType === 'run_started') {
      return { run: runEntity, events: [] as Event[] };
    }
    if (data.eventType === 'step_started') {
      const d = data.eventData as { stepName?: string; input?: unknown };
      if (d?.input !== undefined) {
        rec({
          eventType: 'step_created',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: data.correlationId,
          eventData: { stepName: d.stepName, input: d.input },
        });
      }
      return {
        event: rec(data),
        step: runningStep(data),
        ...(d?.input !== undefined ? { stepCreated: true } : {}),
      };
    }
    if (data.eventType === 'step_completed') {
      return {
        event: rec(data),
        step: { ...runningStep(data), status: 'completed' as const },
      };
    }
    if (data.eventType === 'attr_set') {
      // Idempotent by correlationId, like the real world: re-attempting an
      // already-written attr_set is a conflict the suspension handler swallows.
      const existing = durable.find(
        (e) =>
          e.eventType === 'attr_set' && e.correlationId === data.correlationId
      );
      if (existing) {
        attrReattempts += 1;
        // A healthy run writes each attr_set once. Repeated re-attempts of the
        // SAME attr_set mean the replay loop is spinning (never reaching the
        // batch commit, never flushing the deferred completion). Short-circuit
        // the replay-timeout so a regression is observable in ms, not seconds.
        if (attrReattempts >= 3) {
          throw new Error(
            'LIVELOCK: attr_set re-attempted 3x without progress'
          );
        }
        const { EntityConflictError } = await import('@workflow/errors');
        throw new EntityConflictError('attr_set already exists');
      }
      return { event: rec(data) };
    }
    return { event: rec(data) };
  });

  const createBatch = vi.fn(async (_runId: string, events: any[]) => {
    const inputByStep = new Map<string, unknown>();
    for (const e of events) {
      if (e.eventType === 'step_created') {
        inputByStep.set(e.correlationId, e.eventData?.input);
      }
    }
    const results = events.map((data: any) => {
      if (data.eventType === 'step_completed') {
        return {
          event: rec(data),
          step: { ...runningStep(data), status: 'completed' as const },
        };
      }
      if (data.eventType === 'step_created') {
        return { event: rec(data), step: runningStep(data) };
      }
      return {
        event: rec(data),
        step: runningStep(data, inputByStep.get(data.correlationId)),
        stepCreated: true,
      };
    });
    return {
      results,
      events: [...durable],
      cursor: `cursor-${durable.length}`,
      hasMore: false,
    };
  });

  const queued: any[] = [];
  const maxDeliveries = 8;
  const world: any = {
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler: vi.fn(
      (_p: string, handler: (m: unknown, md: unknown) => Promise<unknown>) => {
        return async () => {
          const firstInput = {
            input: await dehydrateWorkflowArguments([], runId, undefined, []),
            deploymentId: 'test-deployment',
            workflowName: 'workflow',
            specVersion: SPEC_VERSION_CURRENT,
            executionContext: {},
          };
          for (let attempt = 1; attempt <= maxDeliveries; attempt++) {
            const before = queued.length;
            let ret: unknown;
            try {
              ret = await handler(
                attempt === 1
                  ? {
                      runId,
                      requestedAt: new Date('2024-01-01T00:00:00.000Z'),
                      runInput: firstInput,
                    }
                  : { runId, requestedAt: new Date() },
                {
                  requestId: 'req_batch',
                  attempt,
                  queueName: '__wkf_workflow_workflow',
                  messageId: `msg_${attempt}`,
                }
              );
            } catch (e) {
              writeOrder.push(`THROW:${(e as Error).message}`);
              break;
            }
            const terminal = durable.some(
              (e) =>
                e.eventType === 'run_completed' ||
                e.eventType === 'run_failed' ||
                e.eventType === 'run_cancelled'
            );
            if (terminal) break;
            const nacked =
              ret !== null &&
              typeof ret === 'object' &&
              'timeoutSeconds' in (ret as Record<string, unknown>);
            const enqueuedContinuation = queued
              .slice(before)
              .some((m) => m && m.stepId === undefined);
            if (!nacked && !enqueuedContinuation) break;
          }
          return new Response(null, { status: 204 });
        };
      }
    ),
    events: {
      create,
      list: vi.fn(async () => ({
        data: [...durable],
        hasMore: false,
        cursor: `cursor-${durable.length}`,
      })),
    },
    runs: { get: vi.fn(async () => runEntity) },
    queue: vi.fn(async (_name: string, message: any) => {
      queued.push(message);
      return { messageId: null };
    }),
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  };
  if (opts.withBatch) world.events.createBatch = createBatch;

  setWorld(world);
  await workflowEntrypoint(source)(new Request('https://example.test'));

  return { durable, writeOrder };
}

// completed(astep2) must be durable and precede attr_set — the causal invariant
// a single-write path always upholds and the batch path must not break.
function assertCausalOrdering(durable: Event[]) {
  const startedCorrelation = (name: string) =>
    durable.find(
      (e) =>
        e.eventType === 'step_started' &&
        (e.eventData as { stepName?: string } | undefined)?.stepName === name
    )?.correlationId;
  const deferredCid = startedCorrelation('astep2');
  const completedIdx = durable.findIndex(
    (e) => e.eventType === 'step_completed' && e.correlationId === deferredCid
  );
  const attrIdx = durable.findIndex((e) => e.eventType === 'attr_set');
  expect(completedIdx).toBeGreaterThanOrEqual(0); // completed(astep2) is durable
  expect(attrIdx).toBeGreaterThanOrEqual(0); // attr_set is durable
  expect(completedIdx).toBeLessThan(attrIdx); // and precedes attr_set
}

// Normalize a write order for control-vs-batch comparison: correlation IDs are
// per-run ULIDs, so map each distinct id to a stable first-appearance token.
// Equal normalized orders means batch ON produced the SAME durable event
// sequence as the flag-off single-write control.
function normalizeOrder(order: string[]): string[] {
  const idMap = new Map<string, string>();
  return order.map((w) => {
    const m = w.match(/^(\w+)\((.+)\)$/);
    if (!m) return w;
    const [, type, id] = m;
    if (!idMap.has(id)) idMap.set(id, `#${idMap.size}`);
    return `${type}(${idMap.get(id)})`;
  });
}

describe('adversarial: attr_set ordering under deferred completion', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;
  const ORIG_TIMEOUT = process.env.WORKFLOW_REPLAY_TIMEOUT_MS;

  beforeEach(() => {
    process.env.WORKFLOW_TURBO = '0';
    // Bound any regression livelock so the test can't hang.
    process.env.WORKFLOW_REPLAY_TIMEOUT_MS = '400';
    runs.astep1 = 0;
    runs.astep2 = 0;
    runs.astep3 = 0;
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    if (ORIG_TIMEOUT === undefined)
      delete process.env.WORKFLOW_REPLAY_TIMEOUT_MS;
    else process.env.WORKFLOW_REPLAY_TIMEOUT_MS = ORIG_TIMEOUT;
  });

  it('FIXED: batch ON produces the SAME event order as the flag-off control (raced: Promise.race([setAttributes, step]))', async () => {
    // Control: the single-write path is the ground-truth ordering.
    process.env.WORKFLOW_BATCH_TRANSITIONS = '0';
    const control = await driveRun({
      runId: 'wrun_attr_raced_ctrl',
      withBatch: false,
      source: attrRaceWorkflow,
    });
    expect(control.durable.some((e) => e.eventType === 'run_completed')).toBe(
      true
    );
    assertCausalOrdering(control.durable);

    // Batch ON must match it — no livelock, no run_failed, identical order.
    runs.astep1 = 0;
    runs.astep2 = 0;
    runs.astep3 = 0;
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const batch = await driveRun({
      runId: 'wrun_attr_raced_batch',
      withBatch: true,
      source: attrRaceWorkflow,
    });

    expect(batch.writeOrder.some((w) => w.startsWith('THROW:LIVELOCK'))).toBe(
      false
    );
    expect(batch.durable.some((e) => e.eventType === 'run_completed')).toBe(
      true
    );
    expect(batch.durable.some((e) => e.eventType === 'run_failed')).toBe(false);
    assertCausalOrdering(batch.durable);
    // The whole point of the fix: batch ON is observationally identical to the
    // control. (Here the race resolves on the attribute side, so astep3 is
    // never scheduled in EITHER run — the equality proves batch didn't diverge.)
    expect(normalizeOrder(batch.writeOrder)).toEqual(
      normalizeOrder(control.writeOrder)
    );
    // astep1/astep2 bodies ran exactly once (no double-execution).
    expect(runs.astep1).toBe(1);
    expect(runs.astep2).toBe(1);
  });

  it('FIXED: batch ON produces the SAME event order as the flag-off control (un-raced: setAttributes not awaited, then await step)', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '0';
    const control = await driveRun({
      runId: 'wrun_attr_unraced_ctrl',
      withBatch: false,
      source: attrUnracedWorkflow,
    });
    expect(control.durable.some((e) => e.eventType === 'run_completed')).toBe(
      true
    );
    assertCausalOrdering(control.durable);
    const controlAstep3 = runs.astep3;

    runs.astep1 = 0;
    runs.astep2 = 0;
    runs.astep3 = 0;
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const batch = await driveRun({
      runId: 'wrun_attr_unraced_batch',
      withBatch: true,
      source: attrUnracedWorkflow,
    });

    expect(batch.writeOrder.some((w) => w.startsWith('THROW:LIVELOCK'))).toBe(
      false
    );
    expect(batch.durable.some((e) => e.eventType === 'run_completed')).toBe(
      true
    );
    expect(batch.durable.some((e) => e.eventType === 'run_failed')).toBe(false);
    assertCausalOrdering(batch.durable);
    expect(normalizeOrder(batch.writeOrder)).toEqual(
      normalizeOrder(control.writeOrder)
    );
    // Un-raced: the step is awaited, so astep3 deterministically runs once in
    // both control and batch.
    expect(controlAstep3).toBe(1);
    expect(runs.astep1).toBe(1);
    expect(runs.astep2).toBe(1);
    expect(runs.astep3).toBe(1);
  });
});
