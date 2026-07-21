import {
  EntityConflictError,
  RunExpiredError,
  WorkflowWorldError,
} from '@workflow/errors';
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

// Three sequential inline steps. With WORKFLOW_BATCH_TRANSITIONS on, the first
// step stays on the single-write path, the second step's completion is
// deferred, and the second→third transition commits as ONE batch
// [step_completed(s2), step_created(s3), step_started(s3)]. The third step's
// own completion is the run's final step, so it flushes on the single path.
registerStepFunction('bstep1', async () => 'r1');
registerStepFunction('bstep2', async () => 'r2');
registerStepFunction('bstep3', async () => 'r3');

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

const threeStepWorkflow = `const s1 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep1");
  const s2 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep2");
  const s3 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("bstep3");
  async function workflow() {
    const a = await s1();
    const b = await s2();
    const c = await s3();
    return [a, b, c];
  }${xform('workflow')}`;

interface BatchCall {
  events: Array<{ eventType: string; correlationId?: string }>;
  params?: {
    sinceCursor?: string;
    stateUpdatedAt?: number;
    requestId?: string;
  };
}

/**
 * Drive the three-step workflow against a mock World whose durable event log
 * grows as events are created (so the inline replay loop makes progress over
 * its own writes). Turbo is disabled so the non-turbo await-then-run inline
 * path (the batch's home) is exercised.
 *
 * The queue is simulated: any orchestrator continuation the runtime enqueues
 * (a `reinvoke`, i.e. a message with no `stepId`) is redelivered as a fresh
 * non-turbo invocation, up to a bound, so a run that falls back via reinvoke
 * still runs to completion exactly as a real queue would drive it.
 */
async function driveRun(opts: {
  runId: string;
  withBatch: boolean;
  maxDeliveries?: number;
  batchImpl?: (
    events: any[],
    durable: Event[],
    rec: (data: any) => Event,
    runningStep: (data: any, input?: unknown) => any
  ) => Promise<any>;
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
      // Default happy path: apply all events, seed entities (the born-running
      // step carries its input from the folded step_created), return the log as
      // the inline delta.
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
    }
  );

  const queued: any[] = [];
  const returns: unknown[] = [];
  const maxDeliveries = opts.maxDeliveries ?? 12;
  const world: any = {
    specVersion: SPEC_VERSION_CURRENT,
    createQueueHandler: vi.fn(
      (_p: string, handler: (m: unknown, md: unknown) => Promise<unknown>) => {
        // Redelivery loop: deliver the first-delivery message, then keep
        // redelivering any orchestrator continuation the runtime enqueues
        // until the run is terminal or a bound is hit.
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
                requestId: 'req_batch',
                attempt,
                queueName: '__wkf_workflow_workflow',
                messageId: 'msg_batch',
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
            // Redeliver when the invocation asked to be re-run: in non-turbo
            // mode `reinvoke` nacks by returning `{ timeoutSeconds }` (the
            // queue redelivers) rather than self-enqueuing; a broken loop may
            // also enqueue an orchestrator continuation (no stepId).
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
  await workflowEntrypoint(threeStepWorkflow)(
    new Request('https://example.test')
  );

  const created = create.mock.calls.map((c) => c[1] as any);
  return { created, batchCalls, createBatch, durable, returns };
}

const startedFor = (created: any[], stepId: string) =>
  created.filter(
    (d) => d.eventType === 'step_started' && d.correlationId === stepId
  );
const completedFor = (created: any[], stepId: string) =>
  created.filter(
    (d) => d.eventType === 'step_completed' && d.correlationId === stepId
  );
const startedStepIds = (created: any[]) => {
  const ids: string[] = [];
  for (const d of created) {
    if (d.eventType === 'step_started' && !ids.includes(d.correlationId)) {
      ids.push(d.correlationId);
    }
  }
  return ids;
};

describe('runtime batch step transitions', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    // Turbo is mutually exclusive with the (await-then-run) batch path.
    process.env.WORKFLOW_TURBO = '0';
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    setWorld(undefined);
    vi.clearAllMocks();
  });

  it('flag OFF: never calls createBatch and keeps the two-POST-per-step pattern', async () => {
    delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_off',
      withBatch: true,
    });

    expect(createBatch).not.toHaveBeenCalled();
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    // Every step: its own step_started AND its own step_completed via create().
    for (const id of ids) {
      expect(startedFor(created, id)).toHaveLength(1);
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('flag ON: folds the middle transition into one batch [completed, created, started]', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const { created, batchCalls } = await driveRun({
      runId: 'wrun_batch_on',
      withBatch: true,
    });

    // Exactly one batch (the s2 → s3 transition).
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    const [completed, createdEv, started] = batchCalls[0].events;
    // create + start target the same (next) step; completed targets a
    // different (previous) step.
    expect(createdEv.correlationId).toBe(started.correlationId);
    expect(completed.correlationId).not.toBe(started.correlationId);

    const s1 = startedStepIds(created)[0];
    const s2 = completed.correlationId!;
    const s3 = started.correlationId!;

    // s1: single-write path (the first step is never deferred).
    expect(startedFor(created, s1)).toHaveLength(1);
    expect(completedFor(created, s1)).toHaveLength(1);
    // s2: started via create, but its completion came via the batch (deferred).
    expect(startedFor(created, s2)).toHaveLength(1);
    expect(completedFor(created, s2)).toHaveLength(0);
    // s3: started via the batch (NOT a separate step_started create); its
    // completion is the run's final step, flushed on the single path.
    expect(startedFor(created, s3)).toHaveLength(0);
    expect(completedFor(created, s3)).toHaveLength(1);
    // The batch's step_completed rode a sinceCursor for the inline delta.
    expect(typeof batchCalls[0].params?.sinceCursor).toBe('string');
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('world lacks createBatch: falls back to the single-POST path entirely', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const { created } = await driveRun({
      runId: 'wrun_batch_absent',
      withBatch: false,
    });
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(startedFor(created, id)).toHaveLength(1);
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  it('404 endpoint absent: disables batching for the invocation, flushes, and completes via single POSTs', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const { created, createBatch } = await driveRun({
      runId: 'wrun_batch_404',
      withBatch: true,
      batchImpl: async () => {
        throw new WorkflowWorldError('no batch route', { status: 404 });
      },
    });
    // Attempted once; then disabled + reinvoked, and the run completes with
    // every step written via single POSTs.
    expect(createBatch).toHaveBeenCalledTimes(1);
    const ids = startedStepIds(created);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(completedFor(created, id)).toHaveLength(1);
    }
    expect(created.some((d) => d.eventType === 'run_completed')).toBe(true);
  });

  // For 409/410 the batch aborts all-or-nothing: nothing was written, so the
  // runtime must ABANDON the deferred completion and re-derive from a fresh
  // replay (nack), never failing the run and never leaking a partial write.
  // We assert that contract on the failing delivery — a single delivery — since
  // the subsequent re-derivation exercises the ordinary single-POST/owned-
  // recovery paths, which are covered elsewhere.
  it('409 conflict: abandons the deferred completion and nacks without failing the run', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_409',
      withBatch: true,
      maxDeliveries: 1,
      batchImpl: async () => {
        throw new EntityConflictError('batch conflict');
      },
    });
    // The batch was attempted with the full transition shape.
    expect(batchCalls).toHaveLength(1);
    expect(batchCalls[0].events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    const deferredStep = batchCalls[0].events[0].correlationId!;
    // All-or-nothing abandonment: the deferred step_completed was NOT committed
    // via any path in this delivery.
    expect(completedFor(created, deferredStep)).toHaveLength(0);
    // The run was not failed; the invocation nacked (reinvoke) for redelivery.
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });

  it('410 run-not-running: abandons the deferred completion and nacks without failing the run', async () => {
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    const { created, batchCalls, returns } = await driveRun({
      runId: 'wrun_batch_410',
      withBatch: true,
      maxDeliveries: 1,
      batchImpl: async () => {
        throw new RunExpiredError('run not running');
      },
    });
    expect(batchCalls).toHaveLength(1);
    const deferredStep = batchCalls[0].events[0].correlationId!;
    expect(completedFor(created, deferredStep)).toHaveLength(0);
    expect(created.some((d) => d.eventType === 'run_failed')).toBe(false);
    expect(
      returns.some(
        (r) => r !== null && typeof r === 'object' && 'timeoutSeconds' in r
      )
    ).toBe(true);
  });
});
