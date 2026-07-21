import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EntityConflictError } from '@workflow/errors';
import type {
  BatchEventResult,
  CreateEventRequest,
  Event,
  Storage,
} from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from './storage.js';
import { createRun } from './test-helpers.js';

// ADVERSARIAL (R3-2, highest priority): B1 exactly-once `stepCreated` under a
// CONCURRENT lost race, not merely sequential redelivery.
//
// The tracked storage-batch.test.ts proves the SEQUENTIAL already-applied case
// (deliver the same transition twice; the second omits `stepCreated`). It does
// NOT prove the concurrent case: two writers racing the SAME batch transition
// at once, neither having observed the other's commit at peek time. That race
// is exactly where a World could wrongly stamp `stepCreated` on a non-winner
// and reintroduce B1 (double body execution). world-local claims its on-disk
// `writeExclusive` create-claim arbitrates this even across processes; the
// module simulates cross-process with two storage instances sharing one data
// directory (independent in-process locks, shared filesystem — see
// events-storage.ts). We drive several such instances at the same batch and
// assert the create-claim is won by exactly one.
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

async function listEvents(storage: Storage, runId: string): Promise<Event[]> {
  const listed = await storage.events.list({
    runId,
    pagination: { sortOrder: 'asc' },
  });
  return listed.data;
}

describe('world-local createBatch — concurrent lost race (adversarial)', () => {
  let testDir: string;
  // One "seed" instance to set up the run; the racing instances are created
  // per-test so each has an independent in-process lock map but shares the
  // filesystem (the cross-process model).
  let seed: Storage;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'storage-batch-concurrent-')
    );
    seed = createStorage(testDir);
  });
  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  it('N racing writers born-run step N+1: exactly one stamps stepCreated, exactly one born-run committed', async () => {
    const run = await createRun(seed, {
      deploymentId: 'deployment-123',
      workflowName: 'test-workflow',
      input: new Uint8Array(),
    });
    const runId = run.runId;
    // Step N is born-running; the batch defers-completes it and born-runs M.
    await seed.events.create(runId, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'stepN',
      eventData: { stepName: 'step-n', input: new Uint8Array([0]) },
    } as CreateEventRequest);

    const frames: CreateEventRequest[] = [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ];

    // Five independent storage instances (independent in-process locks, shared
    // FS) fire the identical transition at the same time. Only the on-disk
    // exclusive claim files can arbitrate.
    const racers = Array.from({ length: 5 }, () => createStorage(testDir));
    const settled = await Promise.allSettled(
      racers.map((s) => s.events.createBatch(runId, frames))
    );

    let stampedCount = 0;
    let rejectedConflicts = 0;
    let resolvedWithoutStamp = 0;
    for (const outcome of settled) {
      if (outcome.status === 'fulfilled') {
        const batch = outcome.value as BatchEventResult;
        const started = batch.results[batch.results.length - 1];
        if (started?.stepCreated === true) {
          stampedCount++;
        } else {
          resolvedWithoutStamp++;
        }
      } else {
        // A loser must reject with EntityConflictError (lost the completion of
        // N, or lost M's create-claim) — the shape the client maps to
        // reinvoke. Never any other error type.
        expect(
          EntityConflictError.is(outcome.reason),
          `loser rejected with ${outcome.reason?.constructor?.name}: ${outcome.reason?.message}`
        ).toBe(true);
        rejectedConflicts++;
      }
    }

    // THE B1 INVARIANT: exactly one racer is told it created the step (and so
    // may run the body). Every other racer either rejected (conflict) or
    // resolved without the stamp — none may run the body.
    expect(stampedCount).toBe(1);
    expect(rejectedConflicts + resolvedWithoutStamp).toBe(4);

    // Durable state proves the same thing structurally: exactly one born-run.
    const evs = await listEvents(seed, runId);
    const mCreated = evs.filter(
      (e) => e.eventType === 'step_created' && e.correlationId === 'stepM'
    );
    const mStarted = evs.filter(
      (e) => e.eventType === 'step_started' && e.correlationId === 'stepM'
    );
    const nCompleted = evs.filter(
      (e) => e.eventType === 'step_completed' && e.correlationId === 'stepN'
    );
    expect(mCreated).toHaveLength(1);
    // At most one started event survives (a losing born-run throws before its
    // step_started row is written; the winner writes exactly one).
    expect(mStarted.length).toBeGreaterThanOrEqual(1);
    expect(mStarted).toHaveLength(1);
    // Step N completed exactly once — no duplicate completion event.
    expect(nCompleted).toHaveLength(1);

    const stepM = await seed.steps.get(runId, 'stepM');
    expect(stepM.status).toBe('running');
    // The create-claim winner incremented attempt exactly once; no racer
    // re-started it.
    expect(stepM.attempt).toBe(1);
  });

  it('a writer racing an already-committed transition never re-stamps stepCreated', async () => {
    // Sequential-then-concurrent: one instance fully commits the transition,
    // then several instances re-deliver it at once. None may claim creation.
    const run = await createRun(seed, {
      deploymentId: 'deployment-123',
      workflowName: 'test-workflow',
      input: new Uint8Array(),
    });
    const runId = run.runId;
    await seed.events.create(runId, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'stepN',
      eventData: { stepName: 'step-n', input: new Uint8Array([0]) },
    } as CreateEventRequest);
    const frames: CreateEventRequest[] = [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ];

    const first = await seed.events.createBatch(runId, frames);
    expect(first.results[first.results.length - 1].stepCreated).toBe(true);
    const eventsAfterFirst = (await listEvents(seed, runId)).map(
      (e) => e.eventId
    );

    const racers = Array.from({ length: 4 }, () => createStorage(testDir));
    const settled = await Promise.allSettled(
      racers.map((s) => s.events.createBatch(runId, frames))
    );
    for (const outcome of settled) {
      if (outcome.status === 'fulfilled') {
        const batch = outcome.value as BatchEventResult;
        const started = batch.results[batch.results.length - 1];
        expect(started?.stepCreated).toBeUndefined();
      } else {
        expect(EntityConflictError.is(outcome.reason)).toBe(true);
      }
    }

    // No new events were written by any redelivery.
    const eventsAfterRacers = (await listEvents(seed, runId)).map(
      (e) => e.eventId
    );
    expect(eventsAfterRacers).toEqual(eventsAfterFirst);
  });
});
