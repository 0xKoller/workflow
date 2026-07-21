import { execSync } from 'node:child_process';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { EntityConflictError, WorkflowWorldError } from '@workflow/errors';
import type { CreateEventRequest, Event, Step } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { Pool } from 'pg';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  test,
} from 'vitest';
import { createClient } from '../src/drizzle/index.js';
import { createEventsStorage, createStepsStorage } from '../src/storage.js';

type EventsStorage = ReturnType<typeof createEventsStorage>;
type StepsStorage = ReturnType<typeof createStepsStorage>;

// The v4 batch step-transition frames workflow-server#646 commits atomically:
// [step_completed(N), step_created(N+1), step_started(N+1)]. The step_started
// frame carries NO input (input lives on the step_created frame).
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

// Non-deterministic fields (ids / timestamps) differ across two structurally
// equivalent runs; project onto meaningful content.
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

describe('world-postgres events.createBatch (Postgres integration)', () => {
  if (process.platform === 'win32') {
    test.skip('skipped on Windows since it relies on a docker container', () => {});
    return;
  }

  let container: Awaited<ReturnType<PostgreSqlContainer['start']>>;
  let pool: Pool;
  let events: EventsStorage;
  let steps: StepsStorage;

  async function truncateTables() {
    await pool.query(
      'TRUNCATE TABLE workflow.workflow_events, workflow.workflow_steps, workflow.workflow_hooks, workflow.workflow_runs RESTART IDENTITY CASCADE'
    );
  }

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:15-alpine').start();
    const dbUrl = container.getConnectionUri();
    process.env.DATABASE_URL = dbUrl;
    process.env.WORKFLOW_POSTGRES_URL = dbUrl;
    execSync('pnpm db:push', {
      stdio: 'inherit',
      cwd: process.cwd(),
      env: process.env,
    });
    pool = new Pool({ connectionString: dbUrl, max: 1 });
    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle);
    steps = createStepsStorage(drizzle);
  }, 120_000);

  beforeEach(async () => {
    await truncateTables();
  });

  afterAll(async () => {
    await pool.end();
    await container.stop();
  });

  async function freshRun(): Promise<string> {
    const result = await events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'deployment-123',
        workflowName: 'test-workflow',
        input: new Uint8Array(),
      },
    } as CreateEventRequest);
    if (!result.run) throw new Error('expected run');
    return result.run.runId;
  }

  // Born-run step N via a lazy step_started carrying its input (the single-path
  // fold), so the batch can defer-complete N and born-run N+1. A stable
  // stepName ('step-n') keeps two twins comparable when their step IDS differ
  // (stepId is a GLOBAL primary key in world-postgres).
  async function bornRun(runId: string, stepId: string, input: Uint8Array) {
    await events.create(runId, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: stepId,
      eventData: { stepName: 'step-n', input },
    } as CreateEventRequest);
  }

  async function listEvents(runId: string): Promise<Event[]> {
    const listed = await events.list({
      runId,
      pagination: { sortOrder: 'asc' },
    });
    return listed.data;
  }

  it('applies the transition in one tx: completes N, born-runs N+1, stamps stepCreated', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));

    const batch = await events.createBatch(runId, [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ]);

    expect(batch.results).toHaveLength(3);
    const started = batch.results[2];
    expect(started.step?.stepId).toBe('stepM');
    expect(started.step?.status).toBe('running');
    expect(started.step?.attempt).toBe(1);
    expect(started.step?.input).toEqual(new Uint8Array([1, 2, 3]));
    expect(started.stepCreated).toBe(true);

    const stepN = await steps.get(runId, 'stepN');
    expect(stepN.status).toBe('completed');
    expect(stepN.output).toEqual(new Uint8Array([9]));
    const stepM = await steps.get(runId, 'stepM');
    expect(stepM.status).toBe('running');

    const mEvents = await events.listByCorrelationId({
      correlationId: 'stepM',
    });
    const mTypes = mEvents.data.map((e) => e.eventType);
    expect(mTypes).toContain('step_created');
    expect(mTypes).toContain('step_started');
  });

  it('produces durable state structurally identical to the two-POST kill-switch path', async () => {
    // stepId is a GLOBAL primary key in world-postgres (real ids are unique
    // ULIDs), so the two coexisting twins must use distinct step ids; we
    // normalize each twin's ids back to the roles N/M before comparing.
    const normEvents = (evs: Event[], nId: string, mId: string) =>
      evs.map((e) => {
        const role =
          e.correlationId === nId
            ? 'N'
            : e.correlationId === mId
              ? 'M'
              : e.correlationId;
        return { ...projectEvent(e), correlationId: role };
      });
    const normStep = (s: Step) => {
      const { stepId: _omit, ...rest } = projectStep(s);
      return rest;
    };

    // Twin A: batch.
    const runA = await freshRun();
    await bornRun(runA, 'nA', new Uint8Array([0]));
    await events.createBatch(runA, [
      completedFrame('nA', new Uint8Array([9])),
      createdFrame('mA', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('mA', 'step-m'),
    ]);

    // Twin B: what WORKFLOW_BATCH_TRANSITIONS=0 does — two separate create()
    // POSTs: step_completed(N), then the lazy born-running started(M).
    const runB = await freshRun();
    await bornRun(runB, 'nB', new Uint8Array([0]));
    await events.create(runB, completedFrame('nB', new Uint8Array([9])));
    await events.create(runB, {
      eventType: 'step_started',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'mB',
      eventData: { stepName: 'step-m', input: new Uint8Array([1, 2, 3]) },
    } as CreateEventRequest);

    const eventsA = normEvents(await listEvents(runA), 'nA', 'mA');
    const eventsB = normEvents(await listEvents(runB), 'nB', 'mB');
    expect(eventsA).toEqual(eventsB);

    expect(normStep(await steps.get(runA, 'nA'))).toEqual(
      normStep(await steps.get(runB, 'nB'))
    );
    expect(normStep(await steps.get(runA, 'mA'))).toEqual(
      normStep(await steps.get(runB, 'mB'))
    );
  });

  it('is idempotent on re-application: writes nothing new and omits stepCreated', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const frames: CreateEventRequest[] = [
      completedFrame('stepN', new Uint8Array([9])),
      createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
      startedFrame('stepM', 'step-m'),
    ];

    const first = await events.createBatch(runId, frames);
    expect(first.results[2].stepCreated).toBe(true);
    const afterFirst = await listEvents(runId);
    const stepMFirst = await steps.get(runId, 'stepM');

    // Redeliver the identical transition.
    const second = await events.createBatch(runId, frames);
    const afterSecond = await listEvents(runId);

    expect(afterSecond.map((e) => e.eventId)).toEqual(
      afterFirst.map((e) => e.eventId)
    );
    expect(second.results[2].stepCreated).toBeUndefined();
    const stepMSecond = await steps.get(runId, 'stepM');
    expect(stepMSecond.attempt).toBe(stepMFirst.attempt);
    expect(stepMSecond.status).toBe('running');
  });

  it('is all-or-nothing: a failure mid-batch rolls back every prior write in the tx', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const before = await listEvents(runId);

    // A valid transition followed by an unsupported event: the unsupported
    // frame throws, and the single Postgres transaction must roll back the
    // completed(N) update and the born-running M insert with it.
    await expect(
      events.createBatch(runId, [
        completedFrame('stepN', new Uint8Array([9])),
        createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
        startedFrame('stepM', 'step-m'),
        {
          eventType: 'run_completed',
          specVersion: SPEC_VERSION_CURRENT,
          eventData: {},
        } as CreateEventRequest,
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);

    // N was NOT completed, M was NOT created, and no new events were written.
    const stepN = await steps.get(runId, 'stepN');
    expect(stepN.status).toBe('running');
    await expect(steps.get(runId, 'stepM')).rejects.toThrow();
    const after = await listEvents(runId);
    expect(after.map((e) => e.eventId)).toEqual(before.map((e) => e.eventId));
  });

  it('rejects empty batch and missing runId', async () => {
    const runId = await freshRun();
    await expect(events.createBatch(runId, [])).rejects.toBeInstanceOf(
      WorkflowWorldError
    );
    await expect(
      events.createBatch('', [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });

  // ---- v2 suspension-batch fence ----------------------------------------

  const transition = (): CreateEventRequest[] => [
    completedFrame('stepN', new Uint8Array([9])),
    createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
    startedFrame('stepM', 'step-m'),
  ];

  const runFence = async (runId: string) => {
    const { rows } = await pool.query(
      'SELECT run_version, last_batch_id FROM workflow.workflow_runs WHERE id = $1',
      [runId]
    );
    return rows[0] as {
      run_version: number | null;
      last_batch_id: string | null;
    };
  };

  it('v2 fence: applies against runVersion 0, advances to 1 in the same tx, records lastBatchId', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const batch = await events.createBatch(runId, transition(), {
      expectedRunVersion: 0,
      batchId: 'bat_one',
    });
    expect(batch.results[2].stepCreated).toBe(true);
    expect(batch.runVersion).toBe(1);
    expect(batch.lastBatchId).toBe('bat_one');
    const fence = await runFence(runId);
    expect(fence.run_version).toBe(1);
    expect(fence.last_batch_id).toBe('bat_one');
  });

  it('v2 fence: an already-applied batchId is idempotent (no writes, no stepCreated) and echoes the current version', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const frames = transition();
    const first = await events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_dup',
    });
    expect(first.runVersion).toBe(1);
    const afterFirst = await listEvents(runId);

    // Redeliver the SAME batchId (a transport retry still believing v0).
    const second = await events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_dup',
    });
    expect(second.results[2].stepCreated).toBeUndefined();
    expect(second.runVersion).toBe(1);
    expect(second.lastBatchId).toBe('bat_dup');
    const afterSecond = await listEvents(runId);
    expect(afterSecond.map((e) => e.eventId)).toEqual(
      afterFirst.map((e) => e.eventId)
    );
    // The version did NOT advance a second time.
    expect((await runFence(runId)).run_version).toBe(1);
  });

  it('v2 fence: a run-version mismatch aborts all-or-nothing (EntityConflictError), rolling back every write', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const before = await listEvents(runId);
    // Run is at version 0; asserting 1 is stale.
    await expect(
      events.createBatch(runId, transition(), {
        expectedRunVersion: 1,
        batchId: 'bat_stale',
      })
    ).rejects.toBeInstanceOf(EntityConflictError);
    // Nothing written, N still running, M never created, version unchanged.
    const stepN = await steps.get(runId, 'stepN');
    expect(stepN.status).toBe('running');
    await expect(steps.get(runId, 'stepM')).rejects.toThrow();
    const after = await listEvents(runId);
    expect(after.map((e) => e.eventId)).toEqual(before.map((e) => e.eventId));
    expect((await runFence(runId)).run_version).toBe(0);
  });

  it('v2 fence: a pre-v2 run (run_version NULL) is rejected run-not-versioned', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    // Simulate a run created before the fence.
    await pool.query(
      'UPDATE workflow.workflow_runs SET run_version = NULL WHERE id = $1',
      [runId]
    );
    await expect(
      events.createBatch(runId, transition(), {
        expectedRunVersion: 0,
        batchId: 'bat_prev2',
      })
    ).rejects.toMatchObject({ code: 'run-not-versioned', status: 409 });
  });
});
