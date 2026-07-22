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

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn((p: Promise<unknown>) => {
    p.catch(() => {});
  }),
}));

// ADVERSARIAL collect-mode crash-window suite (p1-correctness), added on
// p1-sdk2's offer to gate the fan-out+WAIT shape against the pushed reorder.
//
// The runtime's collect-mode reorder for a step+wait suspension is:
//   1. createBatch commits [step_created, step_started, wait_created] durably
//      (runtime.ts ~2200) and stamps `stepCreated` on the inline started frame,
//   2. the dispatch loop arms the wait continuation via `world.queue`
//      (runtime.ts ~2477, awaited at ~2494),
//   3. the inline step body runs LAST via preStarted (runtime.ts ~2596).
//
// The two existing suites leave the wait shape's redelivery path untested:
// runtime-batch-fanout.test.ts's step+wait case is single-delivery happy path
// (never crashes), and the adversarial lost-claim convergence case
// (runtime-batch-fanout-adversarial.test.ts, C) is step-ONLY (twoWayFanout, no
// wait) so it never asserts the wait continuation survives a redelivery.
//
// This suite gates two crash points that leave the SAME durable residue — a
// born-running step owned by this message plus a durable wait_created, with the
// step body NOT yet run — and asserts the redelivery converges to exactly-once
// (body runs once, via owned recovery) while re-arming the wait:
//
//   1. WON-COMMIT CRASH IN THE ARM WINDOW — delivery 1 wins the batch (all
//      frames stamped), so it OWNS the transition, then the process dies while
//      arming the wait continuation (step 2), BEFORE the inline body runs
//      (step 3). This is exactly the "crash after commit, before enqueue/arm"
//      window team-lead flagged for focused review. The body must NOT have run
//      on delivery 1 (it is ordered after the arm), and the redelivery — the
//      owning message — must re-run it exactly once via owned recovery and arm
//      the wait, WITHOUT re-committing the batch.
//
//   2. LOST-CLAIM CONVERGENCE WITH A WAIT — delivery 1 loses the claim (a
//      concurrent writer committed the same fenced batch first: no stamps), so
//      it abandons pre-dispatch (reinvoke, runtime.ts ~2254) and arms nothing.
//      The wait_created is durable (the winner wrote it); the redelivery must
//      re-derive, arm the wait, and run the body exactly once via owned
//      recovery. This is the wait-bearing generalization of adversarial case C.
const bodyRuns: Record<string, number> = {
  fwc1: 0,
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

// One step plus a 60s sleep launched together → ONE suspension folding
// [step_created, step_started, wait_created] into a single fenced batch. The
// run never completes here (the sleep never elapses in the harness), so every
// assertion is about the step body running EXACTLY once and the wait being
// (re-)armed across redeliveries — not about terminal state.
const stepAndWaitFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fwc1");
  const sleep = globalThis[Symbol.for("WORKFLOW_SLEEP")];
  async function workflow() {
    const [x] = await Promise.all([a(), sleep('60s')]);
    return x;
  }${xform('workflow')}`;

interface BatchCall {
  events: Array<{ eventType: string; correlationId?: string }>;
  params?: {
    expectedRunVersion?: number;
    batchId?: string;
  };
}

async function driveWaitCrash(opts: {
  runId: string;
  maxDeliveries?: number;
  // Throw from `world.queue` when arming the wait continuation (a message with
  // no stepId) on this 1-based delivery number — models the process dying in
  // the reorder's arm window, AFTER the durable batch commit. A real queue
  // redelivers a message whose handler failed, which the delivery loop models.
  failWaitArmOnDelivery?: number;
  // When true, delivery 1's batch stamps NO frame (a concurrent writer won the
  // fenced commit): this invocation loses the claim and abandons pre-dispatch.
  loseClaimOnFirstDelivery?: boolean;
}) {
  const { runId } = opts;
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
      // Bare restart (owned recovery): no input on the wire, so hydrate the
      // step's input from the durable step_created (a real world returns the
      // existing entity, whose input was persisted at create time).
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
  let batchDeliveries = 0;
  const createBatch = vi.fn(
    async (_runId: string, events: any[], params: any) => {
      batchDeliveries += 1;
      batchCalls.push({
        events: events.map((e) => ({
          eventType: e.eventType,
          correlationId: e.correlationId,
        })),
        params,
      });
      const loseClaim =
        opts.loseClaimOnFirstDelivery === true && batchDeliveries === 1;
      const inputByStep = new Map<string, unknown>();
      for (const e of events) {
        if (e.eventType === 'step_created') {
          inputByStep.set(e.correlationId, e.eventData?.input);
        }
      }
      // Commit every frame durably (the batch is atomic and, in the lost-claim
      // case, the concurrent winner's write is what we are echoing).
      for (const e of events) rec(e);
      const results = events.map((data: any) => {
        if (data.eventType === 'step_created') {
          return { step: runningStep(data) };
        }
        if (data.eventType === 'step_started') {
          return {
            step: runningStep(data, inputByStep.get(data.correlationId)),
            // Fresh commit stamps ownership; a lost claim stamps nothing.
            ...(loseClaim ? {} : { stepCreated: true }),
          };
        }
        // wait_created: recorded above, benign result.
        return {};
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
  const deliveriesRun = { count: 0 };
  const crashedDeliveries: number[] = [];
  const maxDeliveries = opts.maxDeliveries ?? 6;
  let currentAttempt = 0;
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
            currentAttempt = attempt;
            deliveriesRun.count += 1;
            const before = queued.length;
            let ret: unknown;
            let threw = false;
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
                  requestId: 'req_wc',
                  attempt,
                  queueName: '__wkf_workflow_workflow',
                  messageId: 'msg_wc',
                }
              );
            } catch {
              // A handler crash (e.g. the injected wait-arm fault) leaves the
              // message un-acked → the queue redelivers it on the next attempt.
              threw = true;
              crashedDeliveries.push(attempt);
            }
            if (!threw) returns.push(ret);
            const terminal = durable.some(
              (e) =>
                e.eventType === 'run_completed' ||
                e.eventType === 'run_failed' ||
                e.eventType === 'run_cancelled'
            );
            if (terminal) break;
            const nacked =
              !threw &&
              ret !== null &&
              typeof ret === 'object' &&
              'timeoutSeconds' in (ret as Record<string, unknown>);
            const enqueuedContinuation = queued
              .slice(before)
              .some((m) => m && m.stepId === undefined);
            // Keep redelivering while the last delivery crashed, nacked, or only
            // armed a continuation (the wait has not resolved) — otherwise stop.
            if (!threw && !nacked && !enqueuedContinuation) break;
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
      // Inject the crash strictly on the wait-continuation arm (no stepId) at
      // the configured delivery — AFTER createBatch has made the frames durable.
      if (
        opts.failWaitArmOnDelivery === currentAttempt &&
        message &&
        message.stepId === undefined
      ) {
        throw new Error('injected crash: process died while arming wait');
      }
      queued.push(message);
      return { messageId: null };
    }),
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  };

  setWorld(world);
  await workflowEntrypoint(stepAndWaitFanout)(
    new Request('https://example.test')
  );

  const created = create.mock.calls.map((c) => c[1] as any);
  return {
    created,
    batchCalls,
    createBatch,
    durable,
    returns,
    queued,
    crashedDeliveries,
    deliveriesRun: deliveriesRun.count,
  };
}

describe('runtime collect-mode step+wait (adversarial: reorder crash-window)', () => {
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

  // 1. WON-COMMIT CRASH IN THE ARM WINDOW. Delivery 1 wins the batch (frames
  // stamped → owns the transition), commits [step_created, step_started,
  // wait_created] durably, then dies arming the wait continuation — before the
  // inline body runs. The redelivery (the owning message) re-runs the body
  // exactly once via owned recovery and arms the wait, without re-batching.
  it('won commit then crash while arming the wait: redelivery runs the body once via owned recovery and arms the wait', async () => {
    const { batchCalls, durable, queued, crashedDeliveries, deliveriesRun } =
      await driveWaitCrash({
        runId: 'wrun_wc_crash',
        failWaitArmOnDelivery: 1,
        maxDeliveries: 6,
      });

    // Delivery 1 committed exactly one batch folding the step pair + the wait,
    // and the redelivery did NOT re-commit (the step + wait were already
    // durable → empty realized batch → normal dispatch / owned recovery).
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_created',
      'step_started',
      'wait_created',
    ]);
    // The fence rode on the batch (first suspension → expectedRunVersion 0).
    expect(batchCalls[0].params?.expectedRunVersion).toBe(0);
    expect(batchCalls[0].params?.batchId).toMatch(/^bat_[0-9A-HJKMNP-TV-Z]+$/);
    // Delivery 1 crashed in the arm window (the injected wait-arm fault fired
    // AFTER the durable batch commit) and a later delivery converged — so the
    // single body run below was reached only through a redelivery, not a clean
    // first pass.
    expect(crashedDeliveries).toContain(1);
    expect(deliveriesRun).toBeGreaterThan(1);
    // Exactly-once across the crash + redelivery: the body ran once total (a
    // double-run — e.g. owned recovery re-running an already-completed body, or
    // a lost deferred completion — would make this 2).
    expect(bodyRuns.fwc1).toBe(1);
    // The run did not complete (still on the 60s sleep) but a wait continuation
    // (a message with no stepId) WAS armed on the redelivery — the wait
    // survived the crash in the arm window.
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(false);
    expect(queued.some((m) => m && m.stepId === undefined)).toBe(true);
  });

  // 2. LOST-CLAIM CONVERGENCE WITH A WAIT. Delivery 1 loses the claim (no
  // stamps → a concurrent writer committed the same fenced batch first),
  // abandons pre-dispatch, and arms nothing. The wait_created is durable; the
  // redelivery re-derives, runs the body once via owned recovery, and arms the
  // wait. Wait-bearing generalization of adversarial case C.
  it('lost claim with a folded wait: redelivery converges to exactly-once and arms the wait', async () => {
    const { batchCalls, durable, queued, deliveriesRun } = await driveWaitCrash(
      {
        runId: 'wrun_wc_lost',
        loseClaimOnFirstDelivery: true,
        maxDeliveries: 6,
      }
    );

    // The batch was committed exactly once (delivery 1's lost claim); the
    // redelivery saw the step + wait already durable and did not re-batch.
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_created',
      'step_started',
      'wait_created',
    ]);
    // Convergence: the body ran exactly once (owned recovery on the redelivery,
    // NOT on the lost-claim delivery 1).
    expect(bodyRuns.fwc1).toBe(1);
    // The run did not complete (still waiting on the 60s sleep) but a wait
    // continuation (a message with no stepId) WAS armed on the converging
    // redelivery — the wait survived the lost claim + redelivery.
    expect(durable.some((e) => e.eventType === 'run_completed')).toBe(false);
    expect(queued.some((m) => m && m.stepId === undefined)).toBe(true);
    // More than one delivery ran (delivery 1 lost, a later one converged).
    expect(deliveriesRun).toBeGreaterThan(1);
  });
});
