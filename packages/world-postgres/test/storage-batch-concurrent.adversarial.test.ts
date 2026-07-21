import { execSync } from 'node:child_process';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { EntityConflictError } from '@workflow/errors';
import type {
  BatchEventResult,
  CreateEventRequest,
  Event,
} from '@workflow/world';
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

// ADVERSARIAL (R3-2, highest priority): B1 exactly-once `stepCreated` under a
// CONCURRENT lost race in world-postgres — the case the tracked
// storage-batch.test.ts does not exercise (it redelivers sequentially, and its
// pool has max:1 so transactions can never overlap). Here we use a max:8 pool
// so several `drizzle.transaction` calls genuinely interleave, and assert the
// batch's create-claim (`onConflictDoNothing` on step_created + the
// `notInArray(status, terminal)` guard on step_completed, under READ
// COMMITTED row locks) admits exactly one committer of the born-run.

type EventsStorage = ReturnType<typeof createEventsStorage>;
type StepsStorage = ReturnType<typeof createStepsStorage>;

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

describe('world-postgres createBatch — concurrent lost race (adversarial)', () => {
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
    // max > 1 is load-bearing: with a single pooled connection the racing
    // transactions would serialize and the test would degrade to the
    // already-covered sequential case.
    pool = new Pool({ connectionString: dbUrl, max: 8 });
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

  it('N transactions race the same transition: exactly one stamps stepCreated, one born-run row', async () => {
    const runId = await freshRun();
    // Step N born-running.
    await events.create(runId, {
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

    const settled = await Promise.allSettled(
      Array.from({ length: 6 }, () => events.createBatch(runId, frames))
    );

    let stampedCount = 0;
    let rejectedConflicts = 0;
    let resolvedWithoutStamp = 0;
    for (const outcome of settled) {
      if (outcome.status === 'fulfilled') {
        const batch = outcome.value as BatchEventResult;
        const started = batch.results[batch.results.length - 1];
        if (started?.stepCreated === true) stampedCount++;
        else resolvedWithoutStamp++;
      } else {
        expect(
          EntityConflictError.is(outcome.reason),
          `loser rejected with ${outcome.reason?.constructor?.name}: ${outcome.reason?.message}`
        ).toBe(true);
        rejectedConflicts++;
      }
    }

    // THE B1 INVARIANT under concurrency: exactly one committer is told it
    // created the step and may run the body.
    expect(stampedCount).toBe(1);
    expect(rejectedConflicts + resolvedWithoutStamp).toBe(5);

    // Durable state: exactly one born-run of M, N completed exactly once.
    const all = (await events.list({ runId, pagination: { sortOrder: 'asc' } }))
      .data as Event[];
    const mCreated = all.filter(
      (e) => e.eventType === 'step_created' && e.correlationId === 'stepM'
    );
    const mStarted = all.filter(
      (e) => e.eventType === 'step_started' && e.correlationId === 'stepM'
    );
    const nCompleted = all.filter(
      (e) => e.eventType === 'step_completed' && e.correlationId === 'stepN'
    );
    expect(mCreated).toHaveLength(1);
    expect(mStarted).toHaveLength(1);
    expect(nCompleted).toHaveLength(1);

    const stepM = await steps.get(runId, 'stepM');
    expect(stepM.status).toBe('running');
    expect(stepM.attempt).toBe(1);
  });

  it('redelivery after a committed transition never re-stamps under concurrency', async () => {
    const runId = await freshRun();
    await events.create(runId, {
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

    const first = await events.createBatch(runId, frames);
    expect(first.results[first.results.length - 1].stepCreated).toBe(true);
    const idsAfterFirst = (
      await events.list({ runId, pagination: { sortOrder: 'asc' } })
    ).data.map((e) => e.eventId);

    const settled = await Promise.allSettled(
      Array.from({ length: 5 }, () => events.createBatch(runId, frames))
    );
    for (const outcome of settled) {
      if (outcome.status === 'fulfilled') {
        const batch = outcome.value as BatchEventResult;
        expect(
          batch.results[batch.results.length - 1]?.stepCreated
        ).toBeUndefined();
      } else {
        expect(EntityConflictError.is(outcome.reason)).toBe(true);
      }
    }

    const idsAfter = (
      await events.list({ runId, pagination: { sortOrder: 'asc' } })
    ).data.map((e) => e.eventId);
    expect(idsAfter).toEqual(idsAfterFirst);
  });
});
