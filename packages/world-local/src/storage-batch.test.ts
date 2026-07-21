import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { WorkflowWorldError } from '@workflow/errors';
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
});
