import {
  type Event,
  SPEC_VERSION_CURRENT,
  type WorkflowRun,
} from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { registerStepFunction } from './private.js';
import { setWorld } from './runtime/world.js';
import { workflowEntrypoint } from './runtime.js';
import { dehydrateWorkflowArguments } from './serialization.js';

// ADVERSARIAL collect-mode suite (p1-correctness). p1-sdk2's
// runtime-batch-fanout.test.ts covers the happy paths and the ALL-unstamped
// already-applied case, but every batch there is the FIRST suspension (no
// leading outcome, so startedBaseIndex === 0) and the only ownership-loss case
// is zero-stamps. This suite gates the three surfaces those tests leave open:
//
//   A. MIXED stamp — a batch response that stamps SOME inline started frames
//      and not others (reachable on world-postgres, whose onConflictDoNothing
//      does not abort the tx; documented at storage.ts ~2270). The all-or-
//      nothing ownership loop must treat ANY unstamped frame as not-owned and
//      run NO body — including the body of a frame that WAS stamped. Tested in
//      BOTH positional orders (first-stamped / second-stamped) so a loop that
//      only checked one end would be caught.
//
//   B. LEADING-OUTCOME OFFSET — a fan-out that follows a deferred step
//      completion, so the batch leads with a step_completed and
//      startedBaseIndex === 1. An off-by-one in the offset would read the
//      leading outcome's result (no stepCreated) as the first inline started,
//      falsely disown, and wedge the run. This is the positional-mapping gate.
//
//   C. LOST-CLAIM CONVERGENCE — after an invocation loses the create-claim
//      (already-applied 200, no stamps → runs no body, nacks), a REDELIVERY of
//      the same message must converge to exactly-once by re-executing the
//      durable born-running steps via owned recovery.
const bodyRuns: Record<string, number> = {
  fadv1: 0,
  fadv2: 0,
  fadv3: 0,
  fadv4: 0,
};
for (const name of Object.keys(bodyRuns)) {
  registerStepFunction(name, async () => {
    bodyRuns[name] += 1;
    return `r-${name}`;
  });
}

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

// Two steps launched together → ONE suspension, two inline born-running pairs,
// no leading outcome (startedBaseIndex === 0).
const twoWayFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv2");
  async function workflow() {
    const [x, y] = await Promise.all([a(), b()]);
    return [x, y];
  }${xform('workflow')}`;

// Two sequential steps THEN a two-way fan-out. The first step stays on the
// single-write path (first inline step never defers); the second defers its
// completion (inlineStepsExecuted >= 1); the fan-out suspension then folds that
// deferred completion in as the batch's LEADING step_completed — the only way
// to produce a collect batch with startedBaseIndex === 1.
const deferThenFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv2");
  const c = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv3");
  const d = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fadv4");
  async function workflow() {
    await a();
    await b();
    const [x, y] = await Promise.all([c(), d()]);
    return [x, y];
  }${xform('workflow')}`;

interface BatchCall {
  events: Array<{ eventType: string; correlationId?: string }>;
  params?: {
    expectedRunVersion?: number;
    batchId?: string;
  };
}

async function driveFanout(opts: {
  runId: string;
  source: string;
  maxDeliveries?: number;
  batchImpl?: (
    events: any[],
    durable: Event[],
    rec: (data: any) => Event,
    runningStep: (data: any, input?: unknown) => any
  ) => Promise<any>;
}) {
  const { runId, source } = opts;
  const durable: Event[] = [];
  let seq = 0;
  const rec = (data: any): Event => {
    seq += 1;
    const e = {
      eventId: `e-${seq}`,
      runId,
      createdAt: new Date(),
      ...data,
    } as Event;
    durable.push(e);
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
    runVersion: 0,
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
      // Bare restart (owned recovery): no input on the wire — the world returns
      // the EXISTING step entity, whose input was persisted at step_created
      // time. Look it up from the durable log so hydration has the real args
      // (a real world does this; without it the recovered body gets no input).
      const persistedInput =
        d?.input !== undefined
          ? d.input
          : durable.find(
              (e) =>
                e.eventType === 'step_created' &&
                e.correlationId === data.correlationId
            )?.eventData?.input;
      return {
        event: rec(data),
        step: runningStep(data, persistedInput),
        ...(d?.input !== undefined ? { stepCreated: true } : {}),
      };
    }
    if (data.eventType === 'step_completed') {
      return {
        event: rec(data),
        step: { ...runningStep(data), status: 'completed' as const },
      };
    }
    return { event: rec(data) };
  });

  const batchCalls: BatchCall[] = [];
  const createBatch = vi.fn(
    async (_runId: string, events: any[], params: any) => {
      batchCalls.push({
        events: events.map((e) => ({
          eventType: e.eventType,
          correlationId: e.correlationId,
        })),
        params,
      });
      if (opts.batchImpl) {
        return opts.batchImpl(events, durable, rec, runningStep);
      }
      // Default happy path: apply positionally, stamp EVERY step_started.
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
        if (data.eventType === 'step_started') {
          return {
            event: rec(data),
            step: runningStep(data, inputByStep.get(data.correlationId)),
            stepCreated: true,
          };
        }
        return { event: rec(data) };
      });
      return {
        results,
        events: [...durable],
        cursor: `cursor-${durable.length}`,
        hasMore: false,
        runVersion: (params.expectedRunVersion ?? 0) + 1,
      };
    }
  );

  const queued: any[] = [];
  const returns: unknown[] = [];
  const maxDeliveries = opts.maxDeliveries ?? 12;
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
            const ret = await handler(
              attempt === 1
                ? {
                    runId,
                    requestedAt: new Date('2024-01-01T00:00:00.000Z'),
                    runInput: firstInput,
                  }
                : { runId, requestedAt: new Date() },
              {
                requestId: 'req_adv',
                attempt,
                queueName: '__wkf_workflow_workflow',
                messageId: 'msg_adv',
              }
            );
            returns.push(ret);
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
      createBatch,
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

  setWorld(world);
  await workflowEntrypoint(source)(new Request('https://example.test'));

  const created = create.mock.calls.map((c) => c[1] as any);
  return { created, batchCalls, createBatch, durable, returns, queued };
}

// A batchImpl that commits durably (records every frame) but stamps
// `stepCreated` on ONLY the step_started frames at the given ordinal positions
// (0-based, in event order among step_started frames). Models world-postgres
// onConflictDoNothing where a subset of the fan-out's step creates lost the
// claim to a concurrent writer. Selecting by ordinal (not correlationId) keeps
// the test independent of the runtime's correlationId format.
function mixedStampBatchImpl(stampedStartedOrdinals: Set<number>) {
  return async (
    events: any[],
    durable: Event[],
    rec: (data: any) => Event,
    runningStep: (data: any, input?: unknown) => any
  ) => {
    const inputByStep = new Map<string, unknown>();
    for (const e of events) {
      if (e.eventType === 'step_created') {
        inputByStep.set(e.correlationId, e.eventData?.input);
      }
    }
    for (const e of events) rec(e);
    let startedOrdinal = 0;
    const results = events.map((data: any) => {
      if (data.eventType === 'step_created') {
        return { step: runningStep(data) };
      }
      if (data.eventType === 'step_started') {
        const won = stampedStartedOrdinals.has(startedOrdinal);
        startedOrdinal += 1;
        return {
          step: runningStep(data, inputByStep.get(data.correlationId)),
          ...(won ? { stepCreated: true } : {}),
        };
      }
      return {};
    });
    return {
      results,
      events: [...durable],
      cursor: `cursor-${durable.length}`,
      hasMore: false,
      runVersion: 1,
    };
  };
}

describe('runtime collect-mode fan-out (adversarial: ownership + offset)', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    process.env.WORKFLOW_TURBO = '0';
    delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    for (const k of Object.keys(bodyRuns)) bodyRuns[k] = 0;
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    setWorld(undefined);
    vi.clearAllMocks();
  });

  // A. MIXED stamp — first inline stamped, second not. The loop populates the
  // first frame's preStarted entry, then breaks on the second → not-owned →
  // reinvoke(0). NEITHER body may run: partial ownership is never honored.
  it('mixed stamp (first inline stamped, second not): runs NO body and nacks', async () => {
    const { batchCalls, created, returns } = await driveFanout({
      runId: 'wrun_adv_mixed_first',
      source: twoWayFanout,
      maxDeliveries: 1,
      batchImpl: mixedStampBatchImpl(new Set([0])),
    });
    // Sanity: the mixed batchImpl saw the two-way fan-out.
    expect(batchCalls).toHaveLength(1);
    // No body ran — not even the stamped one — and no terminal was written.
    expect(bodyRuns.fadv1).toBe(0);
    expect(bodyRuns.fadv2).toBe(0);
    expect(created.some((e) => e.eventType === 'run_completed')).toBe(false);
    expect(created.some((e) => e.eventType === 'run_failed')).toBe(false);
    // The invocation nacked (reinvoke) for a fresh replay.
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  // A. MIXED stamp — SECOND inline stamped, first not. The loop breaks at i=0
  // (first frame unstamped) before touching the second → not-owned. A loop that
  // only inspected the last frame would wrongly claim ownership here.
  it('mixed stamp (second inline stamped, first not): runs NO body and nacks', async () => {
    const { batchCalls, created, returns } = await driveFanout({
      runId: 'wrun_adv_mixed_second',
      source: twoWayFanout,
      maxDeliveries: 1,
      batchImpl: mixedStampBatchImpl(new Set([1])),
    });
    expect(batchCalls).toHaveLength(1);
    expect(bodyRuns.fadv1).toBe(0);
    expect(bodyRuns.fadv2).toBe(0);
    expect(created.some((e) => e.eventType === 'run_completed')).toBe(false);
    expect(created.some((e) => e.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  // B. LEADING-OUTCOME OFFSET — the fan-out follows a deferred completion, so
  // the batch leads with a step_completed and startedBaseIndex === 1. Both
  // fan-out bodies must run exactly once and the run must complete: this only
  // holds if the ownership loop reads the started stamps at index 1+2i+1
  // (skipping the leading outcome), not 0+2i+1.
  it('fan-out after a deferred completion: batch leads with step_completed, both bodies run once', async () => {
    const { batchCalls, durable } = await driveFanout({
      runId: 'wrun_adv_leading',
      source: deferThenFanout,
      maxDeliveries: 6,
    });

    // There is a collect batch whose FIRST frame is the deferred step_completed
    // followed by the two born-running pairs (startedBaseIndex === 1).
    const leadingBatch = batchCalls.find(
      (c) => c.events[0]?.eventType === 'step_completed'
    );
    expect(leadingBatch).toBeDefined();
    expect(leadingBatch!.events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
      'step_created',
      'step_started',
    ]);
    // The completed frame is the SECOND step (fadv2), and the two fan-out steps
    // are fadv3/fadv4 (distinct correlationIds, paired adjacently).
    const ev = leadingBatch!.events;
    expect(ev[1].correlationId).toBe(ev[2].correlationId);
    expect(ev[3].correlationId).toBe(ev[4].correlationId);
    expect(ev[1].correlationId).not.toBe(ev[3].correlationId);
    // Every step body ran exactly once (a + b sequential, c + d fan-out) and
    // the run completed — proving the offset was honored (a wrong offset would
    // disown the fan-out, never run c/d, and never complete).
    expect(bodyRuns.fadv1).toBe(1);
    expect(bodyRuns.fadv2).toBe(1);
    expect(bodyRuns.fadv3).toBe(1);
    expect(bodyRuns.fadv4).toBe(1);
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(true);
  });

  // C. LOST-CLAIM CONVERGENCE — delivery 1 loses the whole claim (already-
  // applied 200, no stamps) and runs no body; the batch's born-running steps
  // are durable and owned by this message. A redelivery of the SAME message
  // must re-execute them via owned recovery, exactly once, and complete.
  it('lost claim then redelivery: converges to exactly-once via owned recovery', async () => {
    let deliveries = 0;
    const { durable, batchCalls } = await driveFanout({
      runId: 'wrun_adv_converge',
      source: twoWayFanout,
      maxDeliveries: 4,
      batchImpl: async (events, durableArr, rec, runningStep) => {
        deliveries += 1;
        const inputByStep = new Map<string, unknown>();
        for (const e of events) {
          if (e.eventType === 'step_created') {
            inputByStep.set(e.correlationId, e.eventData?.input);
          }
        }
        // Delivery 1: commit the born-running steps durably (a concurrent
        // writer's batch) but stamp NONE — this invocation loses the claim.
        // Any later delivery finds the steps already created (empty realized
        // batch) so createBatch is only called once.
        for (const e of events) rec(e);
        const results = events.map((data: any) => {
          if (data.eventType === 'step_created') {
            return { step: runningStep(data) };
          }
          if (data.eventType === 'step_started') {
            return {
              step: runningStep(data, inputByStep.get(data.correlationId)),
            };
          }
          return {};
        });
        return {
          results,
          events: [...durableArr],
          cursor: `cursor-${durableArr.length}`,
          hasMore: false,
          runVersion: 1,
        };
      },
    });

    // The batch was committed exactly once (delivery 1); the redelivery saw the
    // steps already durable and did not re-batch.
    expect(batchCalls).toHaveLength(1);
    expect(deliveries).toBe(1);
    // Convergence: both bodies ran exactly once (via owned recovery on the
    // redelivery) and the run completed.
    expect(bodyRuns.fadv1).toBe(1);
    expect(bodyRuns.fadv2).toBe(1);
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(true);
  });
});
