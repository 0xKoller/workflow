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

// A fan-out `wait_created` frame (collect-mode folds these alongside the
// suspension's steps).
const waitCreatedFrame = (waitId: string, resumeAt: Date): CreateEventRequest =>
  ({
    eventType: 'wait_created',
    specVersion: SPEC_VERSION_CURRENT,
    correlationId: waitId,
    eventData: { resumeAt },
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
      'TRUNCATE TABLE workflow.workflow_events, workflow.workflow_steps, workflow.workflow_hooks, workflow.workflow_waits, workflow.workflow_runs RESTART IDENTITY CASCADE'
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

  it('is all-or-nothing: a mid-tx conflict rolls back every prior write in the same tx', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    // Pre-existing wait: a later batch frame that re-creates it will conflict
    // mid-transaction (the whole-batch short-circuit only covers same-batchId
    // retries, so a genuine collision here rolls back all-or-nothing).
    await events.create(runId, {
      eventType: 'wait_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'wDup',
      eventData: { resumeAt: new Date(Date.now() + 60_000) },
    } as CreateEventRequest);
    const before = await listEvents(runId);

    // A valid completion + born-running create, THEN a wait_created for the
    // already-existing wait: the wait op throws EntityConflictError, and the
    // single Postgres transaction must roll back completed(N) and the M insert.
    await expect(
      events.createBatch(
        runId,
        [
          completedFrame('stepN', new Uint8Array([9])),
          createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
          startedFrame('stepM', 'step-m'),
          waitCreatedFrame('wDup', new Date(Date.now() + 60_000)),
        ],
        { expectedRunVersion: 0, batchId: 'bat_rollback' }
      )
    ).rejects.toBeInstanceOf(EntityConflictError);

    // N still running, M never created, no new events, version NOT advanced.
    expect((await steps.get(runId, 'stepN')).status).toBe('running');
    await expect(steps.get(runId, 'stepM')).rejects.toThrow();
    const after = await listEvents(runId);
    expect(after.map((e) => e.eventId)).toEqual(before.map((e) => e.eventId));
    expect((await runFence(runId)).run_version).toBe(0);
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

  // ---- collect-mode fan-out grammar (steps + wait in one batch) ----------

  it('collect-mode: folds a two-step fan-out plus a wait into one fenced tx', async () => {
    const runId = await freshRun();
    const resumeAt = new Date(Date.now() + 60_000);
    const batch = await events.createBatch(
      runId,
      [
        createdFrame('fA', 'step-a', new Uint8Array([1])),
        startedFrame('fA', 'step-a'),
        createdFrame('fB', 'step-b', new Uint8Array([2])),
        startedFrame('fB', 'step-b'),
        waitCreatedFrame('wW', resumeAt),
      ],
      { expectedRunVersion: 0, batchId: 'bat_fanout_wait' }
    );
    // Both inline steps born-running + stamped; the wait frame recorded.
    expect(batch.results).toHaveLength(5);
    expect(batch.results[1].stepCreated).toBe(true);
    expect(batch.results[3].stepCreated).toBe(true);
    expect((await steps.get(runId, 'fA')).status).toBe('running');
    expect((await steps.get(runId, 'fB')).status).toBe('running');
    // The wait entity exists and its wait_created event is durable.
    const { rows: waitRows } = await pool.query(
      'SELECT status FROM workflow.workflow_waits WHERE wait_id = $1',
      [`${runId}-wW`]
    );
    expect(waitRows[0]?.status).toBe('waiting');
    const types = (await listEvents(runId)).map((e) => e.eventType);
    expect(types.filter((t) => t === 'wait_created')).toHaveLength(1);
    // Fence advanced exactly once for the whole fan-out.
    expect(batch.runVersion).toBe(1);
    expect((await runFence(runId)).run_version).toBe(1);
  });

  it('collect-mode: an already-applied fan-out+wait batch writes no duplicate wait event and does not re-advance', async () => {
    const runId = await freshRun();
    const resumeAt = new Date(Date.now() + 60_000);
    const frames = [
      createdFrame('gA', 'step-a', new Uint8Array([1])),
      startedFrame('gA', 'step-a'),
      waitCreatedFrame('gW', resumeAt),
    ];
    const first = await events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_fanout_wait_dup',
    });
    expect(first.runVersion).toBe(1);
    const afterFirst = await listEvents(runId);

    // Redeliver the SAME batchId — the whole-batch short-circuit returns the
    // current entities and writes nothing.
    const second = await events.createBatch(runId, frames, {
      expectedRunVersion: 0,
      batchId: 'bat_fanout_wait_dup',
    });
    expect(second.runVersion).toBe(1);
    const afterSecond = await listEvents(runId);
    // No duplicate wait_created (or any) event, and no second version advance.
    expect(afterSecond.map((e) => e.eventId)).toEqual(
      afterFirst.map((e) => e.eventId)
    );
    expect(
      afterSecond.filter((e) => e.eventType === 'wait_created')
    ).toHaveLength(1);
    expect((await runFence(runId)).run_version).toBe(1);
  });

  // ---- full v2 suspension grammar (world-local parity) -------------------

  it('collect-mode: a step_failed leading outcome born-runs the next step in one fenced tx', async () => {
    const runId = await freshRun();
    await bornRun(runId, 'stepN', new Uint8Array([0]));
    const batch = await events.createBatch(
      runId,
      [
        {
          eventType: 'step_failed',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: 'stepN',
          eventData: { error: new Uint8Array([7, 7]) },
        } as CreateEventRequest,
        createdFrame('stepM', 'step-m', new Uint8Array([1, 2, 3])),
        startedFrame('stepM', 'step-m'),
      ],
      { expectedRunVersion: 0, batchId: 'bat_failed_lead' }
    );
    // N failed, M born-running + stamped, fence advanced once.
    expect((await steps.get(runId, 'stepN')).status).toBe('failed');
    expect((await steps.get(runId, 'stepM')).status).toBe('running');
    expect(batch.results[2].stepCreated).toBe(true);
    expect(batch.runVersion).toBe(1);
    const types = (await listEvents(runId)).map((e) => e.eventType);
    expect(types.filter((t) => t === 'step_failed')).toHaveLength(1);
  });

  it('collect-mode: a terminal run_completed folds in, completes the run and clears waits', async () => {
    const runId = await freshRun();
    // A pre-existing wait must be cleared when the run reaches terminal.
    await events.create(runId, {
      eventType: 'wait_created',
      specVersion: SPEC_VERSION_CURRENT,
      correlationId: 'wLeftover',
      eventData: { resumeAt: new Date(Date.now() + 60_000) },
    } as CreateEventRequest);
    const batch = await events.createBatch(
      runId,
      [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepM', 'step-m'),
        {
          eventType: 'run_completed',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: runId,
          eventData: { output: new Uint8Array([42]) },
        } as CreateEventRequest,
      ],
      { expectedRunVersion: 0, batchId: 'bat_terminal' }
    );
    expect(batch.results[1].stepCreated).toBe(true);
    // Run is terminal, waits/hooks cleared, fence advanced.
    const { rows: runRows } = await pool.query(
      'SELECT status FROM workflow.workflow_runs WHERE id = $1',
      [runId]
    );
    expect(runRows[0]?.status).toBe('completed');
    const { rows: waitRows } = await pool.query(
      'SELECT count(*)::int AS n FROM workflow.workflow_waits WHERE run_id = $1',
      [runId]
    );
    expect(waitRows[0]?.n).toBe(0);
    expect(batch.runVersion).toBe(1);
    const types = (await listEvents(runId)).map((e) => e.eventType);
    expect(types.filter((t) => t === 'run_completed')).toHaveLength(1);
  });

  it('collect-mode: a hook_received frame rides the batch (event-only) without an unsupported-type reject', async () => {
    const runId = await freshRun();
    const batch = await events.createBatch(
      runId,
      [
        {
          eventType: 'hook_received',
          specVersion: SPEC_VERSION_CURRENT,
          correlationId: 'hookH',
          eventData: { payload: new Uint8Array([5]) },
        } as CreateEventRequest,
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepM', 'step-m'),
      ],
      { expectedRunVersion: 0, batchId: 'bat_hook_recv' }
    );
    // The step born-runs and the hook_received event is durable.
    expect(batch.results[2].stepCreated).toBe(true);
    const types = (await listEvents(runId)).map((e) => e.eventType);
    expect(types.filter((t) => t === 'hook_received')).toHaveLength(1);
    expect(batch.runVersion).toBe(1);
  });

  it('unfenced createBatch rejects the full grammar (a wait) — the v2 fence is required', async () => {
    const runId = await freshRun();
    // A fan-out step + wait with NO batchId: the unfenced path is triple-only.
    await expect(
      events.createBatch(runId, [
        createdFrame('stepM', 'step-m', new Uint8Array([1])),
        startedFrame('stepM', 'step-m'),
        waitCreatedFrame('wW', new Date(Date.now() + 60_000)),
      ])
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });

  it('rejects a malformed batch: a step_started not immediately preceded by its step_created', async () => {
    const runId = await freshRun();
    await expect(
      events.createBatch(runId, [startedFrame('stepM', 'step-m')], {
        expectedRunVersion: 0,
        batchId: 'bat_malformed',
      })
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });
});
