import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EntityConflictError, WorkflowWorldError } from '@workflow/errors';
import type { CreateEventRequest, Event, Step, Storage } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from './storage.js';
import { createRun } from './test-helpers.js';

// The three frames the runtime batches for a sequential step transition
// [step_completed(N), step_created(N+1), step_started(N+1)]. The step_started
// frame carries NO input (it lives on the step_created frame) — exactly the
// v4 batch shape workflow-server#646 accepts. createBatch folds create+start
// into world-local's lazy born-running write.
const completedFrame = (
  stepId: string,
  result: Uint8Array
): CreateEventRequest =>
  ({
    eventType: 'step_completed',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: { result },
  }) as CreateEventRequest;

const createdFrame = (
  stepId: string,
  stepName: string,
  input: Uint8Array
): CreateEventRequest =>
  ({
    eventType: 'step_created',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: { stepName, input },
  }) as CreateEventRequest;

const startedFrame = (stepId: string, stepName: string): CreateEventRequest =>
  ({
    eventType: 'step_started',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: { stepName },
  }) as CreateEventRequest;

// A bare step_created with no paired step_started is a queued (pending) fan-out
// step — the client will dispatch it to a background handler, not inline.
const pendingFrame = createdFrame;

const waitCreatedFrame = (waitId: string, resumeAt: Date): CreateEventRequest =>
  ({
    eventType: 'wait_created',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: waitId,
    eventData: { resumeAt },
  }) as CreateEventRequest;

const hookReceivedFrame = (
  hookId: string,
  payload: Uint8Array
): CreateEventRequest =>
  ({
    eventType: 'hook_received',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: hookId,
    eventData: { token: hookId, payload },
  }) as CreateEventRequest;

const runCompletedFrame = (output: Uint8Array): CreateEventRequest =>
  ({
    eventType: 'run_completed',
    specVersion: SPEC_VERSION_CURRENT,
    eventData: { output },
  }) as CreateEventRequest;

// Born-run a step the way the single-event path does for a fresh next step: a
// lazy step_started whose eventData carries the input (world-local folds the
// step_created + running transition from it).
const bornRun = (
  storage: Storage,
  runId: string,
  stepId: string,
  stepName: string,
  input: Uint8Array
) =>
  storage.events.create(runId, {
    eventType: 'step_started',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: stepId,
    eventData: { stepName, input },
  } as CreateEventRequest);

// Non-deterministic fields (ids / timestamps) differ between two structurally
// equivalent runs; project events and steps onto their meaningful content.
const projectEvent = (e: Event) => ({
  eventType: e.eventType,
  correlationId: e.correlationId,
  eventData: e.eventData,
});
const projectStep = (s: Step) => ({
  stepId: s.stepId,
  stepName: s.stepName,
  status: s.status,
  attempt: s.attempt,
  input: s.input,
  output: s.output,
});

async function listEvents(storage: Storage, runId: string): Promise<Event[]> {
  const listed = await storage.events.list({
    runId,
    pagination: { sortOrder: 'asc' },
  });
  return listed.data;
}

describe('world-local events.createBatch', () => {
  let testDir: string;
  let storage: Storage;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storage-batch-test-'));
    storage = createStorage(testDir);
  });
  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  async function freshRunWithStepN() {
    const run = await createRun(storage, {
      deploymentId: 'deployment-123',
      workflowName: 'test-workflow',
      input: new Uint8Array(),
    });
    // Step N is already born-running (its body produced the completion the
    // batch defers). The batch will complete N and born-run N+1.
    await bornRun(storage, run.runId, 'stepN', 'step-n', new Uint8Array([0]));
    return run.runId;
  }

  it('applies [completed(N), created(N+1), started(N+1)]: completes N, born-runs N+1, stamps stepCreated', async () => {
    const run = await createRun(storage, {
      deploymentId: 'deployment-123',
      workflowName: 'test-workflow',
      input: new Uint8Array(),
    });
    const runId = run.runId;
    await bornRun(storage, runId, 'stepN', 'step-n', new Uint8Array([0]));

    const batch = await storage.events.createBatch(runId, [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ]);

    // One result per input frame, in order; the runtime reads the last one.
    expect(batch.results).toHaveLength(3);
    const startedResult = batch.results[2];
    expect(startedResult.step?.stepId).toBe('stepM');
    expect(startedResult.step?.status).toBe('running');
    expect(startedResult.step?.attempt).toBe(1);
    expect(startedResult.step?.input).toEqual(new Uint8Array([1, 2, 3]));
    // Fresh born-running commit -> this caller won the create-claim.
    expect(startedResult.stepCreated).toBe(true);

    // Durable state: N completed, M born-running.
    const stepN = await storage.steps.get(runId, 'stepN');
    expect(stepN.status).toBe('completed');
    expect(stepN.output).toEqual(new Uint8Array([9]));
    const stepM = await storage.steps.get(runId, 'stepM');
    expect(stepM.status).toBe('running');
    expect(stepM.input).toEqual(new Uint8Array([1, 2, 3]));

    // A synthetic step_created event exists for M so replay observes the claim.
    const mEvents = await storage.events.listByCorrelationId({
      correlationId: 'stepM',
    });
    const mTypes = mEvents.data.map((e) => e.eventType);
    expect(mTypes).toContain('step_created');
    expect(mTypes).toContain('step_started');
  });

  it('produces durable state structurally identical to the kill-switch two-POST path', async () => {
    // Twin A: the batch.
    const runA = await freshRunWithStepN();
    await storage.events.createBatch(runA, [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ]);

    // Twin B: exactly what WORKFLOW_BATCH_TRANSITIONS=0 does — two separate
    // create() POSTs: step_completed(N), then the lazy born-running started(M).
    const runB = await freshRunWithStepN();
    await storage.events.create(
      runB,
      completedFrame('stepN', new Uint8Array([9]))
    );
    await bornRun(storage, runB, 'stepM', 'step-m', new Uint8Array([1, 2, 3]));

    const eventsA = (await listEvents(storage, runA)).map(projectEvent);
    const eventsB = (await listEvents(storage, runB)).map(projectEvent);
    expect(eventsA).toEqual(eventsB);

    for (const stepId of ['stepN', 'stepM']) {
      const a = projectStep(await storage.steps.get(runA, stepId));
      const b = projectStep(await storage.steps.get(runB, stepId));
      expect(a).toEqual(b);
    }
  });

  it('is idempotent on re-application: writes nothing and omits stepCreated', async () => {
    const runId = await freshRunWithStepN();
    const frames: CreateEventRequest[] = [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ];

    const first = await storage.events.createBatch(runId, frames);
    expect(first.results[2].stepCreated).toBe(true);
    const afterFirst = await listEvents(storage, runId);
    const stepMFirst = await storage.steps.get(runId, 'stepM');

    // Re-deliver the identical transition (concurrent/redelivered writer).
    const second = await storage.events.createBatch(runId, frames);

    // No new events, and the started result carries NO stepCreated — the
    // client must NOT re-run M's body; it re-derives from a fresh replay.
    const afterSecond = await listEvents(storage, runId);
    expect(afterSecond.map((e) => e.eventId)).toEqual(
      afterFirst.map((e) => e.eventId)
    );
    expect(second.results).toHaveLength(3);
    expect(second.results[2].stepCreated).toBeUndefined();
    // M unchanged (attempt not re-incremented).
    const stepMSecond = await storage.steps.get(runId, 'stepM');
    expect(stepMSecond.attempt).toBe(stepMFirst.attempt);
    expect(stepMSecond.status).toBe('running');
  });

  it('rejects a malformed batch (missing step_created/step_started or foreign types)', async () => {
    const runId = await freshRunWithStepN();

    // Only a completion — no create/start.
    await expect(
      storage.events.createBatch(runId, [
        completedFrame('stepN', new Uint8Array([9])),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);

    // create/start target different steps.
    await expect(
      storage.events.createBatch(runId, [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepOther', 'step-other'),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);

    // A foreign event type in the batch.
    await expect(
      storage.events.createBatch(runId, [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepM', 'step-m'),
        {
          eventType: 'run_completed',
          specVersion: SPEC_VERSION_CURRENT,
          eventData: {},
        } as CreateEventRequest,
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);

    // Empty batch and missing runId.
    await expect(storage.events.createBatch(runId, [])).rejects.toBeInstanceOf(
      WorkflowWorldError
    );
    await expect(
      storage.events.createBatch('', [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepM', 'step-m'),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });

  // ---- v2 suspension-batch fence ----------------------------------------

  const transition = (): CreateEventRequest[] => [
    completedFrame('stepN', new Uint8Array([9])),
    createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
    startedFrame('stepM', 'step-m'),
  ];

  it('v2 fence: applies against runVersion 0, advances to 1, records lastBatchId, stamps stepCreated', async () => {
    const runId = await freshRunWithStepN();
    const batch = await storage.events.createBatch(runId, transition(), {
      expectedRunVersion: 0,
      batchId: 'bat_one',
    });
    expect(batch.results[2].stepCreated).toBe(true);
    // The run's version advanced to expected + 1, and lastBatchId was recorded.
    expect(batch.runVersion).toBe(1);
    expect(batch.lastBatchId).toBe('bat_one');
    const run = await storage.runs.get(runId);
    expect(run.runVersion).toBe(1);
    expect(run.lastBatchId).toBe('bat_one');
  });

  it('v2 fence: an already-applied batchId is idempotent (no writes, no stepCreated) and echoes the current version', async () => {
    const runId = await freshRunWithStepN();
    const frames = transition();
    const first = await storage.events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_dup',
    });
    expect(first.runVersion).toBe(1);
    const eventsAfterFirst = await listEvents(storage, runId);

    // Re-deliver the SAME batchId. Even though the client (a transport retry)
    // still believes the run is at version 0, lastBatchId short-circuits it.
    const second = await storage.events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_dup',
    });
    expect(second.results[2].stepCreated).toBeUndefined();
    expect(second.runVersion).toBe(1);
    expect(second.lastBatchId).toBe('bat_dup');
    const eventsAfterSecond = await listEvents(storage, runId);
    expect(eventsAfterSecond.map((e) => e.eventId)).toEqual(
      eventsAfterFirst.map((e) => e.eventId)
    );
  });

  it('v2 fence: a run-version mismatch aborts all-or-nothing with EntityConflictError and writes nothing', async () => {
    const runId = await freshRunWithStepN();
    const before = await listEvents(storage, runId);
    // The run is at version 0; asserting 1 is stale.
    await expect(
      storage.events.createBatch(runId, transition(), {
        expectedRunVersion: 1,
        batchId: 'bat_stale',
      })
    ).rejects.toBeInstanceOf(EntityConflictError);
    // Nothing written; stepM never born.
    const after = await listEvents(storage, runId);
    expect(after.map((e) => e.eventId)).toEqual(before.map((e) => e.eventId));
    await expect(storage.steps.get(runId, 'stepM')).rejects.toBeTruthy();
    const run = await storage.runs.get(runId);
    expect(run.runVersion).toBe(0);
  });

  it('v2 fence: a pre-v2 run (no runVersion) is rejected run-not-versioned', async () => {
    const runId = await freshRunWithStepN();
    // Simulate a run created before the fence: strip runVersion from disk.
    const runFile = path.join(testDir, 'runs', `${runId}.json`);
    const raw = JSON.parse(await fs.readFile(runFile, 'utf8'));
    delete raw.runVersion;
    await fs.writeFile(runFile, JSON.stringify(raw));
    await expect(
      storage.events.createBatch(runId, transition(), {
        expectedRunVersion: 0,
        batchId: 'bat_prev2',
      })
    ).rejects.toMatchObject({ code: 'run-not-versioned', status: 409 });
  });

  // ---- v2 full suspension grammar (collect-mode) -------------------------
  // The fenced path accepts a whole suspension's event set, not just the
  // sequential triple: a leading outcome, fan-out (pending) creates, inline
  // born-running steps, waits, hook_received, and a trailing terminal.

  it('v2 grammar: fans out pending creates alongside one inline born-run in one batch', async () => {
    const runId = await freshRunWithStepN();
    // completed(N), two queued fan-out creates (p1, p2), one inline born-run (M).
    const batch = await storage.events.createBatch(
      runId,
      [
        completedFrame('stepN', new Uint8Array([9])),
        pendingFrame('p1', 'fan-1', new Uint8Array([1])),
        pendingFrame('p2', 'fan-2', new Uint8Array([2])),
        createdFrame('stepM', 'step-m', new Uint8Array([3])),
        startedFrame('stepM', 'step-m'),
      ],
      { expectedRunVersion: 0, batchId: 'bat_fanout' }
    );

    // One positional result per input frame.
    expect(batch.results).toHaveLength(5);
    // Leading outcome and pending creates carry NO stepCreated (the server
    // stamps it only on a born-running started frame).
    expect(batch.results[0].stepCreated).toBeUndefined();
    expect(batch.results[1].stepCreated).toBeUndefined();
    expect(batch.results[2].stepCreated).toBeUndefined();
    // Born-run M: the created-frame result has no stepCreated; the started
    // frame does — this caller won M's claim and runs its body.
    expect(batch.results[3].stepCreated).toBeUndefined();
    expect(batch.results[4].stepCreated).toBe(true);
    expect(batch.results[4].step?.status).toBe('running');
    expect(batch.runVersion).toBe(1);

    // Durable state: N completed, fan-out steps pending, M running.
    expect((await storage.steps.get(runId, 'stepN')).status).toBe('completed');
    const p1 = await storage.steps.get(runId, 'p1');
    expect(p1.status).toBe('pending');
    expect(p1.input).toEqual(new Uint8Array([1]));
    expect((await storage.steps.get(runId, 'p2')).status).toBe('pending');
    expect((await storage.steps.get(runId, 'stepM')).status).toBe('running');

    // Every frame is a durable event in request order.
    const types = (await listEvents(storage, runId)).map((e) => e.eventType);
    expect(types).toEqual(
      expect.arrayContaining(['step_completed', 'step_created', 'step_started'])
    );
    // stepN's create (from freshRunWithStepN) plus p1 + p2 + M from the batch.
    expect(types.filter((t) => t === 'step_created')).toHaveLength(4);
  });

  it('v2 grammar: batches a wait_created and a hook_received', async () => {
    const runId = await freshRunWithStepN();
    // hook_received requires the hook to exist — create it first (a plain
    // fire-and-forget hook lives on the single-event path, not the batch).
    await storage.events.create(runId, {
      eventType: 'hook_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'hook-1',
      eventData: { token: 'hook-1', isWebhook: false },
    } as CreateEventRequest);
    const resumeAt = new Date(Date.now() + 60_000);
    const batch = await storage.events.createBatch(
      runId,
      [
        completedFrame('stepN', new Uint8Array([9])),
        waitCreatedFrame('wait-1', resumeAt),
        hookReceivedFrame('hook-1', new Uint8Array([7])),
      ],
      { expectedRunVersion: 0, batchId: 'bat_waithook' }
    );

    expect(batch.results).toHaveLength(3);
    // No step claims in this batch.
    for (const r of batch.results) expect(r.stepCreated).toBeUndefined();
    // The wait op materializes a waiting wait entity.
    expect(batch.results[1].wait?.status).toBe('waiting');
    expect(batch.runVersion).toBe(1);

    const types = (await listEvents(storage, runId)).map((e) => e.eventType);
    expect(types).toContain('wait_created');
    expect(types).toContain('hook_received');
  });

  it('v2 grammar: folds a terminal run_completed into the same fenced commit', async () => {
    const runId = await freshRunWithStepN();
    const batch = await storage.events.createBatch(
      runId,
      [
        completedFrame('stepN', new Uint8Array([9])),
        runCompletedFrame(new Uint8Array([42])),
      ],
      { expectedRunVersion: 0, batchId: 'bat_terminal' }
    );

    expect(batch.results).toHaveLength(2);
    expect(batch.runVersion).toBe(1);
    expect(batch.lastBatchId).toBe('bat_terminal');

    // The run is completed AND still carries the advanced fence — the terminal
    // status write and the fence advance coexist on the one run row.
    const run = await storage.runs.get(runId);
    expect(run.status).toBe('completed');
    expect(run.runVersion).toBe(1);
    expect(run.lastBatchId).toBe('bat_terminal');
    expect((await storage.steps.get(runId, 'stepN')).status).toBe('completed');
  });

  it('v2 grammar: rejects a terminal that is not last, and a step referenced twice', async () => {
    const runId = await freshRunWithStepN();
    // Terminal not last.
    await expect(
      storage.events.createBatch(
        runId,
        [
          runCompletedFrame(new Uint8Array([1])),
          createdFrame('stepM', 'step-m', new Uint8Array([2])),
          startedFrame('stepM', 'step-m'),
        ],
        { expectedRunVersion: 0, batchId: 'bat_badterm' }
      )
    ).rejects.toBeInstanceOf(WorkflowWorldError);

    // Same step id in two operations.
    await expect(
      storage.events.createBatch(
        runId,
        [
          createdFrame('dup', 'step-dup', new Uint8Array([1])),
          startedFrame('dup', 'step-dup'),
          pendingFrame('dup', 'step-dup', new Uint8Array([1])),
        ],
        { expectedRunVersion: 0, batchId: 'bat_dup2' }
      )
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });

  it('rejects the full grammar on the UNFENCED (legacy) path', async () => {
    const runId = await freshRunWithStepN();
    // A fan-out (two bare creates) is valid v2 grammar but not the v1 triple;
    // without a batchId fence it must be rejected.
    await expect(
      storage.events.createBatch(runId, [
        pendingFrame('p1', 'fan-1', new Uint8Array([1])),
        pendingFrame('p2', 'fan-2', new Uint8Array([2])),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });
});
