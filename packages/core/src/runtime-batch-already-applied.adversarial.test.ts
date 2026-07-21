// REGRESSION: exactly-once body execution on a batch "already-applied" 200.
//
// Originally an adversarial repro from the correctness review of PR #3025; it
// caught a double-execution defect where the batch success path ran step N+1's
// BODY on an "already-applied" 200 from createBatch even though the batch did
// NOT win the create-claim on this call (a concurrent/redelivered owner did).
// The single-event lazy-start path gates the body on winning the create-claim
// (a loser gets EntityConflictError -> { type: 'skipped' } and never runs the
// body); the batch path used to seed `preStarted` from `results[last].step`
// UNCONDITIONALLY, ignoring the server's `stepCreated` create-claim signal.
//
// The server sets `stepCreated: true` on the step_started result ONLY on a
// fresh born-running commit (workflow-server lib/data/events.ts:4708) and
// deliberately omits it on the idempotent already-applied 200
// (resolveStepTransitionBatchCancellation, events.ts:4808-4824) — precisely so
// a retrying/losing client neither double-bills nor re-runs the body. The fix
// (runtime.ts) gates preStarted + body execution on `stepCreated === true`;
// on a falsy signal it abandons the deferred completion and reconciles from a
// fresh replay (reinvoke), so body ownership is decided by the existing
// owned-recovery / inline-ownership logic — never run speculatively.
//
// This test asserts the FIXED behavior: step N+1's body does NOT run on an
// already-applied result.

import { SPEC_VERSION_CURRENT } from '@workflow/world';
import type { Event, WorkflowRun } from '@workflow/world';
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

// Distinct step names so this file's module-level registrations don't collide
// with the sibling batch-transition test's `bstep*` registry entries.
const execCount: Record<string, number> = { aa1: 0, aa2: 0, aa3: 0 };
registerStepFunction('aa1', async () => {
  execCount.aa1 += 1;
  return 'r1';
});
registerStepFunction('aa2', async () => {
  execCount.aa2 += 1;
  return 'r2';
});
registerStepFunction('aa3', async () => {
  execCount.aa3 += 1;
  return 'r3';
});

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

const threeStepWorkflow = `const s1 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("aa1");
  const s2 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("aa2");
  const s3 = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("aa3");
  async function workflow() {
    const a = await s1();
    const b = await s2();
    const c = await s3();
    return [a, b, c];
  }${xform('workflow')}`;

describe('runtime batch already-applied (adversarial)', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    process.env.WORKFLOW_TURBO = '0';
    process.env.WORKFLOW_BATCH_TRANSITIONS = '1';
    execCount.aa1 = 0;
    execCount.aa2 = 0;
    execCount.aa3 = 0;
  });
  afterEach(() => {
    if (ORIG_TURBO === undefined) delete process.env.WORKFLOW_TURBO;
    else process.env.WORKFLOW_TURBO = ORIG_TURBO;
    if (ORIG_BATCH === undefined) delete process.env.WORKFLOW_BATCH_TRANSITIONS;
    else process.env.WORKFLOW_BATCH_TRANSITIONS = ORIG_BATCH;
    setWorld(undefined);
    vi.clearAllMocks();
  });

  it('does NOT run step N+1 body on an already-applied 200 (no stepCreated on the started result)', async () => {
    const runId = 'wrun_batch_already_applied';
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

    let batchCalls = 0;
    const createBatch = vi.fn(
      async (_runId: string, events: any[], _params: any) => {
        batchCalls += 1;
        // ALREADY-APPLIED shape: a concurrent/redelivered owner already
        // committed the whole transition. Record its events into the durable
        // log (as the winner would have) and return them as the inline delta,
        // but return per-event results with eventWasCreated:false, NO `event`,
        // and — critically — NO `stepCreated` on the started element. This is
        // exactly what workflow-server's resolveStepTransitionBatchCancellation
        // returns on the idempotent 200 path.
        const inputByStep = new Map<string, unknown>();
        for (const e of events) {
          if (e.eventType === 'step_created') {
            inputByStep.set(e.correlationId, e.eventData?.input);
          }
        }
        for (const e of events) rec(e); // winner's durable writes
        const results = events.map((data: any) => {
          if (data.eventType === 'step_completed') {
            return {
              eventWasCreated: false,
              step: { ...runningStep(data), status: 'completed' as const },
            };
          }
          if (data.eventType === 'step_created') {
            return { eventWasCreated: false, step: runningStep(data) };
          }
          // step_started (the claim). Already-applied => NO stepCreated.
          return {
            eventWasCreated: false,
            step: runningStep(data, inputByStep.get(data.correlationId)),
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
    const world: any = {
      specVersion: SPEC_VERSION_CURRENT,
      createQueueHandler: vi.fn(
        (
          _p: string,
          handler: (m: unknown, md: unknown) => Promise<unknown>
        ) => {
          return async () => {
            const firstInput = {
              input: await dehydrateWorkflowArguments([], runId, undefined, []),
              deploymentId: 'test-deployment',
              workflowName: 'workflow',
              specVersion: SPEC_VERSION_CURRENT,
              executionContext: {},
            };
            for (let attempt = 1; attempt <= 12; attempt++) {
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
                  requestId: 'req_aa',
                  attempt,
                  queueName: '__wkf_workflow_workflow',
                  messageId: 'msg_aa',
                }
              );
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
    await workflowEntrypoint(threeStepWorkflow)(
      new Request('https://example.test')
    );

    // The batch was attempted for the s2 -> s3 transition.
    expect(batchCalls).toBe(1);

    // aa3's BODY must NOT run in this delivery: the batch returned an
    // already-applied result with no stepCreated, so this invocation did not
    // win step 3's create-claim. The runtime gates body execution on the
    // create-claim signal (as the single-event path does) and abandons the
    // deferred completion, leaving the concurrent winner to own the body.
    expect(execCount.aa3).toBe(0);
  });
});
