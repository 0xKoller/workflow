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

// ADVERSARIAL REPRO (p1-correctness): does the collect-mode entry gate
// `err.hookCount === 0 && !openHook` actually keep `hook_received` off the
// batch path, as the runtime comment claims ("hook_received ... deliberately
// never occurs under this gate")?
//
// A hook aborted in the SAME turn it is created is counted in
// `WorkflowSuspension.abortCount`, NOT `hookCount` (global.ts partitions
// disposed / abortRequested / active into three distinct counters). And
// `AbortController#abort()` only flips an in-memory queue flag — it needs no
// durable `hook_created`, so `openHookAndWaitState(cachedEvents)` (durable-log
// derived, computed BEFORE handleSuspension) reports `openHook === false`.
// Both gate terms therefore miss the abort. This test constructs exactly that
// shape and inspects the committed batch.

vi.mock('@vercel/functions', () => ({
  waitUntil: vi.fn((p: Promise<unknown>) => {
    p.catch(() => {});
  }),
}));

const bodyRuns: Record<string, number> = { fabt1: 0, fabt2: 0 };
for (const name of Object.keys(bodyRuns)) {
  registerStepFunction(name, async () => {
    bodyRuns[name] += 1;
    return `r-${name}`;
  });
}

const xform = (name: string) =>
  `;globalThis.__private_workflows = new Map();
   globalThis.__private_workflows.set(${JSON.stringify(name)}, ${name});`;

// Same-turn create+abort of a hook, PLUS a two-step fan-out — all before the
// first suspension. `new AbortController()` is the VM-global hook-backed
// controller (workflow.ts); `.abort()` marks the hook abortRequested in the
// invocations queue synchronously.
const abortPlusFanout = `const a = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fabt1");
  const b = globalThis[Symbol.for("WORKFLOW_USE_STEP")]("fabt2");
  async function workflow() {
    const controller = new AbortController();
    controller.abort('same-turn-abort');
    const [x, y] = await Promise.all([a(), b()]);
    return [x, y];
  }${xform('workflow')}`;

interface BatchCall {
  events: Array<{ eventType: string; correlationId?: string }>;
  params?: { expectedRunVersion?: number; batchId?: string };
}

async function drive(opts: { runId: string; source: string }) {
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
    // Single-path lazy-inline born-running write: a bare `step_started` that
    // carries `input` implies its `step_created`. Model the real World's
    // response (record the implied created, return the running step + the
    // `stepCreated` ownership stamp). Ported from the fan-out harness — the
    // abortCount gate routes this workflow to the single path, so the repro
    // needs a complete single-path mock to reach its assertion.
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
      const inputByStep = new Map<string, unknown>();
      for (const e of events) {
        if (e.eventType === 'step_created') {
          inputByStep.set(e.correlationId, e.eventData?.input);
        }
      }
      const results = events.map((data: any) => {
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
          for (let attempt = 1; attempt <= 4; attempt++) {
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
                requestId: 'req_abt',
                attempt,
                queueName: '__wkf_workflow_workflow',
                messageId: 'msg_abt',
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
    // Abort delivery writes a cancellation stream packet; provide a mock so the
    // abort path does not hit an undefined `world.streams`.
    streams: {
      write: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
    },
    getEncryptionKeyForRun: vi.fn(async () => undefined),
  };

  setWorld(world);
  await workflowEntrypoint(source)(new Request('https://example.test'));
  return { batchCalls, durable, queued };
}

describe('collect-mode gate vs same-turn hook abort (abortCount escape)', () => {
  const ORIG_TURBO = process.env.WORKFLOW_TURBO;
  const ORIG_BATCH = process.env.WORKFLOW_BATCH_TRANSITIONS;

  beforeEach(() => {
    process.env.WORKFLOW_TURBO = '0';
    delete process.env.WORKFLOW_BATCH_TRANSITIONS; // default-ON
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

  it('EXPECTED (post-fix): a same-turn-aborted hook keeps hook_received OFF the batch', async () => {
    const { batchCalls } = await drive({
      runId: 'wrun_abort_escape',
      source: abortPlusFanout,
    });

    // The invariant the runtime comment asserts: no hook_received ever rides
    // the collect batch. If the gate used `abortCount === 0`, this holds
    // (either the suspension does not collect, or the batch carries only
    // step frames).
    const anyHookReceivedInBatch = batchCalls.some((c) =>
      c.events.some((e) => e.eventType === 'hook_received')
    );
    expect(anyHookReceivedInBatch).toBe(false);
  });
});
