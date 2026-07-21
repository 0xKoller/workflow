import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  AttributeChange,
  BatchEventResult,
  CreateEventRequest,
  Event,
  EventResult,
  ExperimentalSetAttributesResult,
  GetEventParams,
  Hook,
  ListEventsParams,
  ListHooksParams,
  PaginatedResponse,
  ResolveData,
  SerializedData,
  Step,
  StepWithoutData,
  Storage,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  ATTRIBUTE_MAX_PER_RUN,
  AttributeValidationError,
  EventSchema,
  HookSchema,
  isChildEntityCreationEvent,
  isChildEntityCreationEventType,
  isHookEventRequiringExistence,
  isLegacySpecVersion,
  isTerminalRunEventType,
  isTerminalStepStatus,
  isTerminalWorkflowRunStatus,
  requiresNewerWorld,
  SPEC_VERSION_CURRENT,
  StepSchema,
  stripEventDataRefs,
  TERMINAL_STEP_STATUSES,
  TERMINAL_WORKFLOW_RUN_STATUSES,
  validateAttributeChanges,
  validateUlidTimestamp,
  WorkflowRunSchema,
} from '@workflow/world';
import {
  and,
  asc,
  desc,
  eq,
  gt,
  inArray,
  lt,
  notInArray,
  sql,
} from 'drizzle-orm';
import { monotonicFactory } from 'ulid';
import { type Drizzle, Schema } from './drizzle/index.js';
import type { SerializedContent } from './drizzle/schema.js';
import { compact } from './util.js';

/**
 * Read helper for the deprecated `error` text column (legacy: JSON-stringified
 * `StructuredError`). In the current event-sourced model, the `error` field on
 * entities is `SerializedData` (Uint8Array) produced by the new error
 * serialization pipeline; legacy text-column records pre-date that pipeline
 * and cannot be hydrated back into the original thrown value.
 *
 * Returns `null` unconditionally so downstream consumers treat legacy errors
 * as absent rather than receiving a shape that `hydrateStepError` /
 * `hydrateRunError` can't process. Callers that need to inspect the raw
 * legacy payload should read the `errorJson` column directly.
 */
function parseErrorJson(_errorJson: string | null): SerializedData | null {
  return null;
}

/**
 * Pass-through helper kept for backwards compatibility with the run read path.
 * In the current event-sourced model, `error` is already `SerializedData`
 * (Uint8Array) on the entity, and any legacy `errorStack` / `errorCode`
 * fields are no longer populated by the current write path.
 */
function deserializeRunError(run: any): WorkflowRun {
  // Drop any stale legacy-only fields we might still encounter on read.
  const { errorStack: _errorStack, ...rest } = run;
  return rest as WorkflowRun;
}

/**
 * Deserialize step data, mapping DB columns to interface fields.
 * The error field should already be deserialized from CBOR or fallback to errorJson.
 */
function deserializeStepError(step: any): Step {
  const { startedAt, ...rest } = step;

  return {
    ...rest,
    startedAt,
  } as Step;
}

export function createRunsStorage(drizzle: Drizzle): Storage['runs'] {
  const { runs } = Schema;
  const get = drizzle
    .select()
    .from(runs)
    .where(eq(runs.runId, sql.placeholder('id')))
    .limit(1)
    .prepare('workflow_runs_get');

  return {
    get: (async (id, params) => {
      const [value] = await get.execute({ id });
      if (!value) {
        throw new WorkflowRunNotFoundError(id);
      }
      value.output ||= value.outputJson;
      value.input ||= value.inputJson;
      value.executionContext ||= value.executionContextJson;
      value.error ||= parseErrorJson(value.errorJson);
      const deserialized = deserializeRunError(compact(value));
      const parsed = WorkflowRunSchema.parse(deserialized);
      const resolveData = params?.resolveData ?? 'all';
      return filterRunData(parsed, resolveData);
    }) as Storage['runs']['get'],
    getMany: (async (ids, params) => {
      const uniqueIds = [...new Set(ids)];
      if (uniqueIds.length === 0) {
        return [];
      }

      const values = await drizzle
        .select()
        .from(runs)
        .where(inArray(runs.runId, uniqueIds));
      const resolveData = params?.resolveData ?? 'all';
      const runsById = new Map(
        values.map((value) => {
          value.output ||= value.outputJson;
          value.input ||= value.inputJson;
          value.executionContext ||= value.executionContextJson;
          value.error ||= parseErrorJson(value.errorJson);
          const parsed = WorkflowRunSchema.parse(
            deserializeRunError(compact(value))
          );
          return [value.runId, filterRunData(parsed, resolveData)] as const;
        })
      );

      return ids.map((id) => runsById.get(id) ?? null);
    }) as NonNullable<Storage['runs']['getMany']>,
    list: (async (params) => {
      const limit = params?.pagination?.limit ?? 20;
      const fromCursor = params?.pagination?.cursor;

      const all = await drizzle
        .select()
        .from(runs)
        .where(
          and(
            map(fromCursor, (c) => lt(runs.runId, c)),
            map(params?.workflowName, (wf) => eq(runs.workflowName, wf)),
            map(params?.status, (wf) => eq(runs.status, wf))
          )
        )
        .orderBy(desc(runs.runId))
        .limit(limit + 1);
      const values = all.slice(0, limit);
      const hasMore = all.length > limit;

      const resolveData = params?.resolveData ?? 'all';
      return {
        data: values.map((v) => {
          v.output ||= v.outputJson;
          v.input ||= v.inputJson;
          v.executionContext ||= v.executionContextJson;
          v.error ||= parseErrorJson(v.errorJson);
          const deserialized = deserializeRunError(compact(v));
          const parsed = WorkflowRunSchema.parse(deserialized);
          return filterRunData(parsed, resolveData);
        }),
        hasMore,
        cursor: values.at(-1)?.runId ?? null,
      };
    }) as Storage['runs']['list'],

    experimentalSetAttributes: async (
      runId: string,
      changes: AttributeChange[],
      options?: { allowReservedAttributes?: boolean }
    ): Promise<ExperimentalSetAttributesResult> => {
      // Load existing attributes so the SDK-shape validator can produce
      // a precise error message (cap, duplicate keys, reserved prefix,
      // byte length). The authoritative cap enforcement happens inside
      // the UPDATE statement below — see the `WHERE` clause — so the
      // race between this read and the UPDATE cannot push the row past
      // the per-run cap.
      const [existing] = await drizzle
        .select({ attributes: runs.attributes })
        .from(runs)
        .where(eq(runs.runId, runId))
        .limit(1);
      if (!existing) {
        throw new WorkflowRunNotFoundError(runId);
      }

      try {
        validateAttributeChanges(changes, {
          existingKeys: Object.keys(existing.attributes ?? {}),
          allowReservedAttributes: options?.allowReservedAttributes,
        });
      } catch (err) {
        if (err instanceof AttributeValidationError) throw err;
        throw err;
      }

      // Build a single SQL expression that applies all changes
      // atomically. Sets fold into nested `jsonb_set` calls; removes
      // fold into chained `-` (delete) operators.
      let expr = sql`COALESCE(${runs.attributes}, '{}'::jsonb)`;
      for (const { key, value } of changes) {
        if (value === null) {
          expr = sql`${expr} - ${key}`;
        } else {
          expr = sql`jsonb_set(${expr}, ARRAY[${key}]::text[], to_jsonb(${value}::text), true)`;
        }
      }

      // Atomic cap enforcement: only commit the UPDATE if the
      // post-merge key count fits the per-run cap. Computed against
      // the *current* row state, so two concurrent writers adding
      // disjoint keys at the cap boundary cannot both succeed.
      // Drizzle re-renders `expr` twice in the SQL (`SET attributes =
      // ...` + the count check); `jsonb_set` is cheap so the
      // duplication is harmless.
      const [updated] = await drizzle
        .update(runs)
        .set({
          attributes: expr as any,
          updatedAt: new Date(),
        })
        .where(
          and(
            eq(runs.runId, runId),
            sql`(SELECT COUNT(*) FROM jsonb_object_keys(${expr})) <= ${ATTRIBUTE_MAX_PER_RUN}`
          )
        )
        .returning({ attributes: runs.attributes });

      if (!updated) {
        // Either the run vanished mid-call, or the cap-check WHERE
        // clause rejected the UPDATE. Re-read to disambiguate.
        const [stillThere] = await drizzle
          .select({ attributes: runs.attributes })
          .from(runs)
          .where(eq(runs.runId, runId))
          .limit(1);
        if (!stillThere) {
          throw new WorkflowRunNotFoundError(runId);
        }
        throw new AttributeValidationError(
          `Run attribute count would exceed limit ${ATTRIBUTE_MAX_PER_RUN} after concurrent write`
        );
      }

      return { attributes: updated.attributes ?? {} };
    },
  };
}

function map<T, R>(obj: T | null | undefined, fn: (v: T) => R): undefined | R {
  return obj ? fn(obj) : undefined;
}

/**
 * A classified suspension-batch operation, tagged by role, keyed to the input
 * event position(s) that produced it. Module-level so `createBatch` stays
 * bounded in complexity; mirrors world-local's `LocalBatchOp` and the server's
 * `BatchOp`.
 */
type PostgresBatchOp =
  | { kind: 'outcome'; index: number; req: CreateEventRequest }
  | {
      kind: 'step-run';
      createdIndex: number;
      startedIndex: number;
      created: CreateEventRequest;
      started: CreateEventRequest;
    }
  | { kind: 'step-pending'; index: number; req: CreateEventRequest }
  | { kind: 'wait'; index: number; req: CreateEventRequest }
  | { kind: 'hook-received'; index: number; req: CreateEventRequest }
  | { kind: 'terminal'; index: number; req: CreateEventRequest };

/**
 * Validate a suspension batch against the grammar and classify its ordered
 * events into positional {@link PostgresBatchOp}s, mirroring workflow-server's
 * `classifySuspensionBatch` (lib/data/events.ts) and world-local's
 * `classifyLocalSuspensionBatch`. Throws `WorkflowWorldError` on any grammar
 * violation. Pure — no I/O. The caller gates the full grammar on the fence (the
 * unfenced path additionally restricts to the v1 triple):
 *
 *   (step_completed | step_failed)?          # leading outcome (index 0)
 *   ( wait_created | wait_completed | hook_received
 *     | (step_created step_started)          # inline born-running step
 *     | step_created )*                      # queued (pending) fan-out step
 *   (run_completed | run_failed)?            # terminal, MUST be last
 */
function classifyPostgresSuspensionBatch(
  batchEvents: CreateEventRequest[]
): PostgresBatchOp[] {
  const ops: PostgresBatchOp[] = [];
  const seenSteps = new Set<string>();
  const claimStep = (req: CreateEventRequest, index: number): void => {
    const id = req.correlationId;
    if (typeof id !== 'string') {
      throw new WorkflowWorldError(
        `world-postgres: createBatch event ${index} (${req.eventType}) requires a correlationId`
      );
    }
    if (seenSteps.has(id)) {
      throw new WorkflowWorldError(
        `world-postgres: createBatch references step ${id} more than once`
      );
    }
    seenSteps.add(id);
  };

  let i = 0;
  const first = batchEvents[0].eventType;
  if (first === 'step_completed' || first === 'step_failed') {
    claimStep(batchEvents[0], 0);
    ops.push({ kind: 'outcome', index: 0, req: batchEvents[0] });
    i = 1;
  }
  while (i < batchEvents.length) {
    const e = batchEvents[i];
    if (e.eventType === 'step_completed' || e.eventType === 'step_failed') {
      throw new WorkflowWorldError(
        `world-postgres: a ${e.eventType} may only lead a suspension batch (index 0)`
      );
    }
    if (e.eventType === 'run_completed' || e.eventType === 'run_failed') {
      if (i !== batchEvents.length - 1) {
        throw new WorkflowWorldError(
          `world-postgres: a terminal ${e.eventType} must be the last batch event`
        );
      }
      ops.push({ kind: 'terminal', index: i, req: e });
      i += 1;
      continue;
    }
    if (e.eventType === 'step_created') {
      claimStep(e, i);
      const next = batchEvents[i + 1];
      if (
        next &&
        next.eventType === 'step_started' &&
        next.correlationId === e.correlationId
      ) {
        ops.push({
          kind: 'step-run',
          createdIndex: i,
          startedIndex: i + 1,
          created: e,
          started: next,
        });
        i += 2;
      } else {
        ops.push({ kind: 'step-pending', index: i, req: e });
        i += 1;
      }
      continue;
    }
    if (e.eventType === 'step_started') {
      throw new WorkflowWorldError(
        'world-postgres: a step_started must be immediately preceded by its step_created'
      );
    }
    if (e.eventType === 'wait_created' || e.eventType === 'wait_completed') {
      ops.push({ kind: 'wait', index: i, req: e });
      i += 1;
      continue;
    }
    if (e.eventType === 'hook_received') {
      ops.push({ kind: 'hook-received', index: i, req: e });
      i += 1;
      continue;
    }
    throw new WorkflowWorldError(
      `world-postgres: unsupported eventType '${e.eventType}' in a suspension batch`
    );
  }
  return ops;
}

/**
 * Handle events for legacy runs (pre-event-sourcing, specVersion < 2).
 * Legacy runs use different behavior:
 * - run_cancelled: Skip event storage, directly update run
 * - wait_completed: Store event only (no entity mutation)
 * - hook_received: Store event only (hooks exist via old system, no entity mutation)
 * - Other events: Throw error (not supported for legacy runs)
 */
async function handleLegacyEventPostgres(
  drizzle: Drizzle,
  runId: string,
  eventId: string,
  data: any,
  currentRun: { status: string; specVersion: number | null },
  params?: { resolveData?: ResolveData }
): Promise<EventResult> {
  const resolveData = params?.resolveData ?? 'all';

  switch (data.eventType) {
    case 'run_cancelled': {
      // Legacy: Skip event storage, directly update run to cancelled
      const now = new Date();

      // Update run status to cancelled
      await drizzle
        .update(Schema.runs)
        .set({
          status: 'cancelled',
          completedAt: now,
          updatedAt: now,
        })
        .where(eq(Schema.runs.runId, runId));

      // Delete all hooks and waits for this run
      await Promise.all([
        drizzle.delete(Schema.hooks).where(eq(Schema.hooks.runId, runId)),
        drizzle.delete(Schema.waits).where(eq(Schema.waits.runId, runId)),
      ]);

      // Fetch updated run for return value
      const [updatedRun] = await drizzle
        .select()
        .from(Schema.runs)
        .where(eq(Schema.runs.runId, runId))
        .limit(1);

      // Return without event (legacy behavior skips event storage)
      // Type assertion: EventResult expects WorkflowRun, filterRunData may return WorkflowRunWithoutData
      return {
        run: updatedRun
          ? (filterRunData(
              deserializeRunError(compact(updatedRun)),
              resolveData
            ) as WorkflowRun)
          : undefined,
      };
    }

    case 'wait_completed':
    case 'hook_received': {
      // Legacy: Store event only (no entity mutation)
      // - wait_completed: for replay purposes
      // - hook_received: hooks exist via old system, just record the event
      const [insertedEvent] = await drizzle
        .insert(Schema.events)
        .values({
          runId,
          eventId,
          correlationId: data.correlationId,
          eventType: data.eventType,
          eventData: 'eventData' in data ? data.eventData : undefined,
          specVersion: SPEC_VERSION_CURRENT,
        })
        .returning({ createdAt: Schema.events.createdAt });

      const event = EventSchema.parse({
        ...data,
        ...insertedEvent,
        runId,
        eventId,
      });
      return { event: stripEventDataRefs(event, resolveData) };
    }

    default:
      throw new Error(
        `Event type '${data.eventType}' not supported for legacy runs ` +
          `(specVersion: ${currentRun.specVersion || 'undefined'}). ` +
          `Please upgrade @workflow packages.`
      );
  }
}

export function createEventsStorage(drizzle: Drizzle): Storage['events'] {
  const ulid = monotonicFactory();
  const { events } = Schema;

  // Prepared statements for validation queries (performance optimization)
  const getRunForValidation = drizzle
    .select({
      status: Schema.runs.status,
      specVersion: Schema.runs.specVersion,
    })
    .from(Schema.runs)
    .where(eq(Schema.runs.runId, sql.placeholder('runId')))
    .limit(1)
    .prepare('events_get_run_for_validation');

  const getStepForValidation = drizzle
    .select({
      status: Schema.steps.status,
      startedAt: Schema.steps.startedAt,
      retryAfter: Schema.steps.retryAfter,
    })
    .from(Schema.steps)
    .where(
      and(
        eq(Schema.steps.runId, sql.placeholder('runId')),
        eq(Schema.steps.stepId, sql.placeholder('stepId'))
      )
    )
    .limit(1)
    .prepare('events_get_step_for_validation');

  const getHookByToken = drizzle
    .select({ hookId: Schema.hooks.hookId, runId: Schema.hooks.runId })
    .from(Schema.hooks)
    .where(eq(Schema.hooks.token, sql.placeholder('token')))
    .limit(1)
    .prepare('events_get_hook_by_token');

  // Used to distinguish a real same-hook duplicate from an orphaned
  // hook row left behind by a process / database interruption between
  // the hook INSERT and the events INSERT below (see the recovery
  // logic in the hook_created branch).
  const getHookCreatedEvent = drizzle
    .select({ eventId: events.eventId })
    .from(events)
    .where(
      and(
        eq(events.runId, sql.placeholder('runId')),
        eq(events.correlationId, sql.placeholder('correlationId')),
        eq(events.eventType, sql.placeholder('eventType'))
      )
    )
    .limit(1)
    .prepare('events_get_hook_created_for_run_correlation');

  const getWaitForValidation = drizzle
    .select({
      status: Schema.waits.status,
    })
    .from(Schema.waits)
    .where(eq(Schema.waits.waitId, sql.placeholder('waitId')))
    .limit(1)
    .prepare('events_get_wait_for_validation');

  return {
    async create(runId, data, params): Promise<EventResult> {
      let eventId: string | undefined;
      const getEventId = () => (eventId ??= `wevt_${ulid()}`);

      // For run_created events, use client-provided runId or generate one server-side
      let effectiveRunId: string;
      if (data.eventType === 'run_created' && (!runId || runId === '')) {
        effectiveRunId = `wrun_${ulid()}`;
      } else if (!runId) {
        throw new Error('runId is required for non-run_created events');
      } else {
        effectiveRunId = runId;
      }

      // Validate client-provided runId timestamp is within acceptable threshold
      if (data.eventType === 'run_created' && runId && runId !== '') {
        const validationError = validateUlidTimestamp(effectiveRunId, 'wrun_');
        if (validationError) {
          throw new WorkflowWorldError(validationError);
        }
      }

      // specVersion is always sent by the runtime, but we provide a fallback for safety
      const effectiveSpecVersion = data.specVersion ?? SPEC_VERSION_CURRENT;

      // Track entity created/updated for EventResult
      let run: WorkflowRun | undefined;
      let step: Step | undefined;
      let hook: Hook | undefined;
      let wait: Wait | undefined;
      // Lazy step start: set true when this step_started atomically created
      // the step (the caller won the create-claim). Surfaced on EventResult
      // as the runtime's exactly-once ownership signal.
      let stepCreatedLazily = false;
      const now = new Date();

      // Terminal step statuses for use in SQL WHERE clauses (atomic guard).
      // Must match the Vercel world's conditional expressions:
      //   ne(status, 'completed') AND ne(status, 'failed') AND ne(status, 'cancelled')
      const terminalStepStatuses: (typeof Schema.steps.status.enumValues)[number][] =
        [...TERMINAL_STEP_STATUSES];

      // ============================================================
      // VALIDATION: Terminal state and event ordering checks
      // ============================================================

      // Get current run state for validation (if not creating a new run)
      // Skip run validation for step_completed and step_retrying - they only operate
      // on running steps, and running steps are always allowed to modify regardless
      // of run state. This optimization saves database queries per step event.
      let currentRun: { status: string; specVersion: number | null } | null =
        null;
      const skipRunValidationEvents = ['step_completed', 'step_retrying'];
      if (
        data.eventType !== 'run_created' &&
        !skipRunValidationEvents.includes(data.eventType)
      ) {
        // Use prepared statement for better performance
        const [runValue] = await getRunForValidation.execute({
          runId: effectiveRunId,
        });
        currentRun = runValue ?? null;

        // Resilient start: run_started on non-existent run with eventData
        // creates the run first, so the queue can bootstrap a run that
        // failed to create during start().
        if (
          data.eventType === 'run_started' &&
          !currentRun &&
          'eventData' in data &&
          data.eventData
        ) {
          const runInputData = (data as any).eventData as {
            deploymentId?: string;
            workflowName?: string;
            input?: any;
            executionContext?: Record<string, any>;
            attributes?: Record<string, string>;
            allowReservedAttributes?: true;
          };
          if (
            runInputData.deploymentId &&
            runInputData.workflowName &&
            runInputData.input !== undefined
          ) {
            validateAttributeChanges(
              Object.entries(runInputData.attributes ?? {}).map(
                ([key, value]) => ({ key, value })
              ),
              {
                allowReservedAttributes:
                  runInputData.allowReservedAttributes === true,
              }
            );
            // Create run + run_created event atomically. The
            // transaction ensures we never have an orphaned run
            // without its run_created event.
            const [inserted] = await drizzle
              .insert(Schema.runs)
              .values({
                runId: effectiveRunId,
                deploymentId: runInputData.deploymentId,
                workflowName: runInputData.workflowName,
                specVersion: effectiveSpecVersion,
                input: runInputData.input as SerializedContent,
                executionContext: runInputData.executionContext as
                  | SerializedContent
                  | undefined,
                attributes: runInputData.attributes,
                status: 'pending',
                // v2 suspension-batch fence (see run_created above).
                runVersion: 0,
              })
              .onConflictDoNothing()
              .returning();

            if (inserted) {
              const runCreatedEventId = `wevt_${ulid()}`;
              await drizzle.insert(events).values({
                runId: effectiveRunId,
                eventId: runCreatedEventId,
                eventType: 'run_created',
                eventData: {
                  deploymentId: runInputData.deploymentId,
                  workflowName: runInputData.workflowName,
                  input: runInputData.input,
                  executionContext: runInputData.executionContext,
                  attributes: runInputData.attributes,
                  allowReservedAttributes: runInputData.allowReservedAttributes,
                },
                specVersion: effectiveSpecVersion,
              });
            }
            const createdRun = inserted;

            if (createdRun) {
              currentRun = {
                status: 'pending',
                specVersion: effectiveSpecVersion,
              };
            } else {
              // Run already exists (concurrent run_created won the
              // race). Re-read so downstream logic sees the real state.
              const [runValue] = await getRunForValidation.execute({
                runId: effectiveRunId,
              });
              currentRun = runValue ?? null;
            }
          }
        }
      }

      // ============================================================
      // VERSION COMPATIBILITY: Check run spec version
      // ============================================================
      // For events that have fetched the run, check version compatibility.
      // Skip for run_created (no existing run) and runtime events (step_completed, step_retrying).
      if (currentRun) {
        // Check if run requires a newer world version
        if (requiresNewerWorld(currentRun.specVersion)) {
          throw new RunNotSupportedError(
            currentRun.specVersion!,
            SPEC_VERSION_CURRENT
          );
        }

        // Route to legacy handler for pre-event-sourcing runs
        if (isLegacySpecVersion(currentRun.specVersion)) {
          return handleLegacyEventPostgres(
            drizzle,
            effectiveRunId,
            getEventId(),
            data,
            currentRun,
            params
          );
        }
      }
      if (data.eventType === 'attr_set' && !currentRun) {
        throw new WorkflowRunNotFoundError(effectiveRunId);
      }

      // Lazy step start: a step_started carrying step-creation data
      // (stepName + input) may arrive with no prior step_created — it creates
      // the step on the fly (see the materialization block below). This
      // mirrors the resilient run_started path. Detect it here so the
      // entity-creation terminal-run guard treats it like a creation and the
      // "step must exist" ordering guard below doesn't reject it.
      const createsChildEntity = isChildEntityCreationEvent(data);
      const lazyStepStart =
        createsChildEntity && data.eventType === 'step_started';

      // Run terminal state validation
      if (currentRun && isTerminalWorkflowRunStatus(currentRun.status)) {
        // Idempotent operation: run_cancelled on already cancelled run is allowed
        if (
          data.eventType === 'run_cancelled' &&
          currentRun.status === 'cancelled'
        ) {
          // Get full run for return value
          const [fullRun] = await drizzle
            .select()
            .from(Schema.runs)
            .where(eq(Schema.runs.runId, effectiveRunId))
            .limit(1);

          // Create the event (still record it)
          const [value] = await drizzle
            .insert(Schema.events)
            .values({
              runId: effectiveRunId,
              eventId: getEventId(),
              correlationId: data.correlationId,
              eventType: data.eventType,
              eventData: 'eventData' in data ? data.eventData : undefined,
              specVersion: effectiveSpecVersion,
            })
            .returning({ createdAt: Schema.events.createdAt });

          const result = {
            ...data,
            ...value,
            runId: effectiveRunId,
            eventId: getEventId(),
          };
          const parsed = EventSchema.parse(result);
          const resolveData = params?.resolveData ?? 'all';
          return {
            event: stripEventDataRefs(parsed, resolveData),
            run: fullRun ? deserializeRunError(compact(fullRun)) : undefined,
          };
        }

        // For run_started on terminal runs, use RunExpiredError so the
        // runtime knows to exit without retrying.
        if (data.eventType === 'run_started') {
          throw new RunExpiredError(
            `Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
          );
        }

        // Other run state transitions are not allowed on terminal runs
        if (isTerminalRunEventType(data.eventType)) {
          throw new EntityConflictError(
            `Cannot transition run from terminal state "${currentRun.status}"`
          );
        }

        // Creating new entities on terminal runs is not allowed. A lazy
        // step_started creates a step, so it is rejected here too — a bare
        // (non-lazy) step_started falls through to the step-validation block
        // below, which uses RunExpiredError for terminal runs.
        if (createsChildEntity) {
          throw new EntityConflictError(
            `Cannot create new entities on run in terminal state "${currentRun.status}"`
          );
        }

        if (data.eventType === 'attr_set') {
          throw new EntityConflictError(
            `Cannot set attributes on run in terminal state "${currentRun.status}"`
          );
        }
      }

      // Step-related event validation (ordering and terminal state)
      // Fetch status + startedAt so we can reuse for step_started (avoid double read)
      // Skip validation for step_completed/step_failed - use conditional UPDATE instead
      let validatedStep: {
        status: string;
        startedAt: Date | null;
        retryAfter: Date | null;
      } | null = null;
      const stepEventsNeedingValidation = ['step_started', 'step_retrying'];
      if (
        stepEventsNeedingValidation.includes(data.eventType) &&
        data.correlationId
      ) {
        // Use prepared statement for better performance
        const [existingStep] = await getStepForValidation.execute({
          runId: effectiveRunId,
          stepId: data.correlationId,
        });

        validatedStep = existingStep ?? null;

        // Event ordering: step must exist before these events — except on the
        // lazy-start path, where step_started creates the step itself.
        if (!validatedStep && !lazyStepStart) {
          throw new WorkflowWorldError(
            `Step "${data.correlationId}" not found`
          );
        }

        // Lazy start exactly-once gate: a lazy step_started always CREATES the
        // step (the owned-inline path only sends one for a step whose
        // step_created it deferred). If the step already exists, a concurrent
        // handler won the create — this caller is a loser and must not start or
        // run the step. Throw EntityConflictError so the runtime's executeStep
        // maps it to `skipped`. Critical: the start UPDATE below permits
        // re-starting a non-terminal step (retries rely on that), so without
        // this gate a loser would re-start a running step and run the body a
        // second time. (A concurrent create that lands after this read is also
        // caught by the onConflictDoNothing()+returning() claim below.)
        if (lazyStepStart && validatedStep) {
          throw new EntityConflictError(
            `Step "${data.correlationId}" already created`
          );
        }

        // Terminal-state checks only apply when the step already exists.
        // validatedStep is null only on the lazy-start path (no step yet),
        // where there is nothing terminal to guard against.
        if (validatedStep) {
          // Step terminal state validation
          if (isTerminalStepStatus(validatedStep.status)) {
            throw new EntityConflictError(
              `Cannot modify step in terminal state "${validatedStep.status}"`
            );
          }

          // On terminal runs: only allow completing/failing in-progress steps
          if (currentRun && isTerminalWorkflowRunStatus(currentRun.status)) {
            if (validatedStep.status !== 'running') {
              throw new RunExpiredError(
                `Cannot modify non-running step on run in terminal state "${currentRun.status}"`
              );
            }
          }
        }
      }

      // Hook-related event validation (ordering)
      if (isHookEventRequiringExistence(data.eventType) && data.correlationId) {
        const [existingHook] = await drizzle
          .select({ hookId: Schema.hooks.hookId })
          .from(Schema.hooks)
          .where(eq(Schema.hooks.hookId, data.correlationId))
          .limit(1);

        if (!existingHook) {
          throw new HookNotFoundError(data.correlationId);
        }
      }

      // ============================================================
      // Entity creation/updates based on event type
      // ============================================================

      // Handle run_created event: create the run entity atomically
      if (data.eventType === 'run_created') {
        const eventData = (data as any).eventData as {
          deploymentId: string;
          workflowName: string;
          input: any[];
          executionContext?: Record<string, any>;
          attributes?: Record<string, string>;
          allowReservedAttributes?: true;
        };
        validateAttributeChanges(
          Object.entries(eventData.attributes ?? {}).map(([key, value]) => ({
            key,
            value,
          })),
          {
            allowReservedAttributes: eventData.allowReservedAttributes === true,
          }
        );
        const [runValue] = await drizzle
          .insert(Schema.runs)
          .values({
            runId: effectiveRunId,
            deploymentId: eventData.deploymentId,
            workflowName: eventData.workflowName,
            // Propagate specVersion from the event to the run entity
            specVersion: effectiveSpecVersion,
            input: eventData.input as SerializedContent,
            executionContext: eventData.executionContext as
              | SerializedContent
              | undefined,
            attributes: eventData.attributes,
            status: 'pending',
            // v2 suspension-batch fence: a run created under v2 starts at
            // version 0 so createBatch can assert + advance it. NULL marks a
            // pre-v2 run (the runtime latches batching off).
            runVersion: 0,
          })
          .onConflictDoNothing()
          .returning();
        // No row back means the run already exists: the resilient start path
        // (run_started on a non-existent run) won a TOCTOU race and created
        // it. Surface the conflict rather than returning `{ run: undefined }`
        // — start() already treats EntityConflictError as benign, and falling
        // through would append a duplicate run_created event to the log.
        if (!runValue) {
          throw new EntityConflictError(
            `Workflow run "${effectiveRunId}" already exists`
          );
        }
        run = deserializeRunError(compact(runValue));
      }

      // Handle run_started event: update run status
      if (data.eventType === 'run_started') {
        // If the run is already running, return it without inserting a
        // duplicate run_started event.  This makes run_started idempotent
        // for concurrent invocations: replay is deterministic, so letting
        // multiple callers proceed with the same run is safe.  We skip
        // preloaded events here because this is a rare race-condition path
        // — the runtime falls back to loadWorkflowRunEvents().
        if (currentRun?.status === 'running') {
          const [fullRun] = await drizzle
            .select()
            .from(Schema.runs)
            .where(eq(Schema.runs.runId, effectiveRunId))
            .limit(1);
          if (fullRun) {
            return { run: deserializeRunError(compact(fullRun)) };
          }
        }

        const [runValue] = await drizzle
          .update(Schema.runs)
          .set({
            status: 'running',
            startedAt: now,
            updatedAt: now,
          })
          .where(eq(Schema.runs.runId, effectiveRunId))
          .returning();
        if (runValue) {
          run = deserializeRunError(compact(runValue));
        }
      }

      // Terminal run statuses for use in SQL WHERE clauses (atomic guard).
      // Must match the Vercel world's conditional expressions:
      //   ne(status, 'completed') AND ne(status, 'failed') AND ne(status, 'cancelled')
      const terminalRunStatuses: (typeof Schema.runs.status.enumValues)[number][] =
        [...TERMINAL_WORKFLOW_RUN_STATUSES];

      // Handle run_completed event: update run status and cleanup hooks
      // Uses conditional UPDATE to prevent completing an already-terminal run.
      if (data.eventType === 'run_completed') {
        const eventData = (data as any).eventData as { output?: any };
        const [runValue] = await drizzle
          .update(Schema.runs)
          .set({
            status: 'completed',
            output: eventData.output as SerializedContent | undefined,
            completedAt: now,
            updatedAt: now,
          })
          .where(
            and(
              eq(Schema.runs.runId, effectiveRunId),
              notInArray(Schema.runs.status, terminalRunStatuses)
            )
          )
          .returning();
        if (runValue) {
          run = deserializeRunError(compact(runValue));
        } else {
          const [existing] = await getRunForValidation.execute({
            runId: effectiveRunId,
          });
          if (!existing) {
            throw new WorkflowRunNotFoundError(effectiveRunId);
          }
          if (isTerminalWorkflowRunStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot transition run from terminal state "${existing.status}"`
            );
          }
        }
        // Delete all hooks and waits for this run to allow token reuse
        await Promise.all([
          drizzle
            .delete(Schema.hooks)
            .where(eq(Schema.hooks.runId, effectiveRunId)),
          drizzle
            .delete(Schema.waits)
            .where(eq(Schema.waits.runId, effectiveRunId)),
        ]);
      }

      // Handle run_failed event: update run status and cleanup hooks
      // Uses conditional UPDATE to prevent failing an already-terminal run.
      if (data.eventType === 'run_failed') {
        const eventData = (data as any).eventData as {
          error: unknown;
          errorCode?: string;
        };
        // The error field is SerializedData (Uint8Array) produced by
        // dehydrateRunError. We store it verbatim in the error_cbor column;
        // consumers hydrate via hydrateRunError.
        const [runValue] = await drizzle
          .update(Schema.runs)
          .set({
            status: 'failed',
            error: eventData.error as SerializedData,
            errorCode: eventData.errorCode,
            completedAt: now,
            updatedAt: now,
          })
          .where(
            and(
              eq(Schema.runs.runId, effectiveRunId),
              notInArray(Schema.runs.status, terminalRunStatuses)
            )
          )
          .returning();
        if (runValue) {
          run = deserializeRunError(compact(runValue));
        } else {
          const [existing] = await getRunForValidation.execute({
            runId: effectiveRunId,
          });
          if (!existing) {
            throw new WorkflowRunNotFoundError(effectiveRunId);
          }
          if (isTerminalWorkflowRunStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot transition run from terminal state "${existing.status}"`
            );
          }
        }
        // Delete all hooks and waits for this run to allow token reuse
        await Promise.all([
          drizzle
            .delete(Schema.hooks)
            .where(eq(Schema.hooks.runId, effectiveRunId)),
          drizzle
            .delete(Schema.waits)
            .where(eq(Schema.waits.runId, effectiveRunId)),
        ]);
      }

      // Handle run_cancelled event: update run status and cleanup hooks
      // Uses conditional UPDATE to prevent cancelling an already-terminal run.
      // Note: idempotent run_cancelled on already-cancelled runs is handled
      // earlier in the pre-validation block (creates event and returns early).
      if (data.eventType === 'run_cancelled') {
        const [runValue] = await drizzle
          .update(Schema.runs)
          .set({
            status: 'cancelled',
            completedAt: now,
            updatedAt: now,
          })
          .where(
            and(
              eq(Schema.runs.runId, effectiveRunId),
              notInArray(Schema.runs.status, terminalRunStatuses)
            )
          )
          .returning();
        if (runValue) {
          run = deserializeRunError(compact(runValue));
        } else {
          const [existing] = await getRunForValidation.execute({
            runId: effectiveRunId,
          });
          if (!existing) {
            throw new WorkflowRunNotFoundError(effectiveRunId);
          }
          if (isTerminalWorkflowRunStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot transition run from terminal state "${existing.status}"`
            );
          }
        }
        // Delete all hooks and waits for this run to allow token reuse
        await Promise.all([
          drizzle
            .delete(Schema.hooks)
            .where(eq(Schema.hooks.runId, effectiveRunId)),
          drizzle
            .delete(Schema.waits)
            .where(eq(Schema.waits.runId, effectiveRunId)),
        ]);
      }

      if (data.eventType === 'attr_set') {
        const { changes, allowReservedAttributes } = data.eventData;
        // Dedup pre-check for correlated workflow writes: if the event is
        // already in the log (a redelivered/replayed duplicate), reject
        // BEFORE materializing onto the run. Without this, a duplicate —
        // including a pathological one carrying different changes for the
        // same correlationId — would mutate `run.attributes` and then fail
        // the event insert, leaving the snapshot out of sync with the
        // event log. The unique index on the insert below still guards the
        // truly-concurrent race; both writers of that race carry identical
        // changes (deterministic replay), so the double-applied update is
        // idempotent there.
        if (data.correlationId && data.eventData.writer.type === 'workflow') {
          const [duplicate] = await drizzle
            .select({ eventId: events.eventId })
            .from(events)
            .where(
              and(
                eq(events.runId, effectiveRunId),
                eq(events.correlationId, data.correlationId),
                eq(events.eventType, 'attr_set')
              )
            )
            .limit(1);
          if (duplicate) {
            throw new EntityConflictError(
              `attr_set for correlationId "${data.correlationId}" already exists in run "${effectiveRunId}"`
            );
          }
        }
        const [existing] = await drizzle
          .select({ attributes: Schema.runs.attributes })
          .from(Schema.runs)
          .where(eq(Schema.runs.runId, effectiveRunId))
          .limit(1);
        if (!existing) {
          throw new WorkflowRunNotFoundError(effectiveRunId);
        }
        validateAttributeChanges(changes, {
          existingKeys: Object.keys(existing.attributes ?? {}),
          allowReservedAttributes: allowReservedAttributes === true,
        });

        let expr = sql`COALESCE(${Schema.runs.attributes}, '{}'::jsonb)`;
        for (const { key, value } of changes) {
          if (value === null) {
            expr = sql`${expr} - ${key}`;
          } else {
            expr = sql`jsonb_set(${expr}, ARRAY[${key}]::text[], to_jsonb(${value}::text), true)`;
          }
        }

        const [runValue] = await drizzle
          .update(Schema.runs)
          .set({
            attributes: expr as any,
            updatedAt: now,
          })
          .where(
            and(
              eq(Schema.runs.runId, effectiveRunId),
              sql`(SELECT COUNT(*) FROM jsonb_object_keys(${expr})) <= ${ATTRIBUTE_MAX_PER_RUN}`
            )
          )
          .returning();
        if (!runValue) {
          // The guarded update matches zero rows either because the cap
          // condition failed or because the run row disappeared between the
          // existence check above and this update — distinguish the two so
          // the error is not misattributed.
          const [stillExists] = await drizzle
            .select({ runId: Schema.runs.runId })
            .from(Schema.runs)
            .where(eq(Schema.runs.runId, effectiveRunId))
            .limit(1);
          if (!stillExists) {
            throw new WorkflowRunNotFoundError(effectiveRunId);
          }
          throw new AttributeValidationError(
            `Run attribute count would exceed limit ${ATTRIBUTE_MAX_PER_RUN}`
          );
        }
        run = deserializeRunError(compact(runValue));
      }

      // Strip eventData from run_started — it belongs on run_created only.
      // For step_started on the lazy-start path, strip only the step `input`
      // (it belongs on the synthetic step_created written below); `stepName`
      // is preserved for the client replay consumer's step-name divergence
      // check.
      let storedEventData: unknown;
      if (data.eventType === 'run_started') {
        storedEventData = undefined;
      } else if ('eventData' in data && data.eventData) {
        if (
          data.eventType === 'step_started' &&
          'input' in (data.eventData as Record<string, unknown>)
        ) {
          const { input: _strippedInput, ...rest } = data.eventData as {
            input?: unknown;
          } & Record<string, unknown>;
          storedEventData = rest;
        } else {
          storedEventData = data.eventData;
        }
      } else {
        storedEventData = undefined;
      }

      // Handle step_created event: create step entity
      if (data.eventType === 'step_created') {
        const eventData = (data as any).eventData as {
          stepName: string;
          input: any;
        };
        const [stepValue] = await drizzle
          .insert(Schema.steps)
          .values({
            runId: effectiveRunId,
            stepId: data.correlationId!,
            stepName: eventData.stepName,
            input: eventData.input as SerializedContent,
            status: 'pending',
            attempt: 0,
            // Propagate specVersion from the event to the step entity
            specVersion: effectiveSpecVersion,
          })
          .onConflictDoNothing()
          .returning();
        if (stepValue) {
          step = deserializeStepError(compact(stepValue));
        }
      }

      let value: { createdAt: Date } | undefined;

      // Handle step_started event: increment attempt and set the step to
      // running, then write the matching event log entry in the same
      // transaction. The guarded UPDATE takes the step row lock; keeping the
      // event INSERT behind that lock prevents a late step_started from being
      // ordered after a concurrent terminal event that already won the row.
      if (data.eventType === 'step_started') {
        value = await drizzle.transaction(async (tx) => {
          // Lazy step start: no prior step_created exists, but this
          // step_started carries the step-creation data. The step INSERT is
          // the ownership claim: only the caller that inserts the row gets to
          // run the step body inline.
          if (lazyStepStart && !validatedStep) {
            const lazyData = data.eventData;
            const [inserted] = await tx
              .insert(Schema.steps)
              .values({
                runId: effectiveRunId,
                stepId: data.correlationId,
                stepName: lazyData.stepName,
                input: lazyData.input as SerializedContent,
                status: 'pending',
                attempt: 0,
                specVersion: effectiveSpecVersion,
              })
              .onConflictDoNothing()
              .returning({ stepId: Schema.steps.stepId });

            if (!inserted) {
              throw new EntityConflictError(
                `Step "${data.correlationId}" already created`
              );
            }

            // Replay still needs to observe step_created before
            // step_started. Because this synthetic event is in the same
            // transaction as the lazy step row and step_started event, we
            // cannot leave behind only one side of that materialization.
            const stepCreatedEventId = `wevt_${ulid()}`;
            await tx
              .insert(events)
              .values({
                runId: effectiveRunId,
                eventId: stepCreatedEventId,
                correlationId: data.correlationId,
                eventType: 'step_created',
                eventData: {
                  stepName: lazyData.stepName,
                  input: lazyData.input,
                },
                specVersion: effectiveSpecVersion,
              })
              .onConflictDoNothing();
            stepCreatedLazily = true;
          }

          // Retried steps may be scheduled for later. Keep this check inside
          // the transaction so the step_started write cannot slip past it.
          if (
            validatedStep?.retryAfter &&
            validatedStep.retryAfter.getTime() > Date.now()
          ) {
            throw new TooEarlyError(
              `Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
              {
                retryAfter: Math.ceil(
                  (validatedStep.retryAfter.getTime() - Date.now()) / 1000
                ),
              }
            );
          }

          // The terminal-state guard is part of the UPDATE, not just the
          // earlier validation read. That closes the race where another
          // writer completes/fails the step between validation and start.
          const [stepValue] = await tx
            .update(Schema.steps)
            .set({
              status: 'running',
              attempt: sql`${Schema.steps.attempt} + 1`,
              // Preserve the original first-start timestamp across retries or
              // overlapping starts.
              startedAt: sql`COALESCE(${Schema.steps.startedAt}, ${now.toISOString()})`,
              retryAfter: null,
            })
            .where(
              and(
                eq(Schema.steps.runId, effectiveRunId),
                eq(Schema.steps.stepId, data.correlationId!),
                notInArray(Schema.steps.status, terminalStepStatuses)
              )
            )
            .returning();

          if (stepValue) {
            step = deserializeStepError(compact(stepValue));
          } else {
            const [existing] = await tx
              .select({ status: Schema.steps.status })
              .from(Schema.steps)
              .where(
                and(
                  eq(Schema.steps.runId, effectiveRunId),
                  eq(Schema.steps.stepId, data.correlationId!)
                )
              )
              .limit(1);
            if (!existing) {
              throw new WorkflowWorldError(
                `Step "${data.correlationId}" not found`
              );
            }
            if (isTerminalStepStatus(existing.status)) {
              throw new EntityConflictError(
                `Cannot modify step in terminal state "${existing.status}"`
              );
            }
          }

          // Allocate the step_started ULID only after the guarded step UPDATE
          // has acquired and passed the row lock. Without a sequence, this is
          // the local ordering guarantee we can provide: a writer blocked on
          // the step row will not carry an older event id into a later insert.
          const stepStartedEventId = `wevt_${ulid()}`;
          eventId = stepStartedEventId;
          const [eventValue] = await tx
            .insert(events)
            .values({
              runId: effectiveRunId,
              eventId: stepStartedEventId,
              correlationId: data.correlationId,
              eventType: data.eventType,
              eventData: storedEventData,
              specVersion: effectiveSpecVersion,
            })
            .returning({ createdAt: events.createdAt });

          if (!eventValue) {
            throw new EntityConflictError(
              `Event ${stepStartedEventId} could not be created`
            );
          }
          return eventValue;
        });
      }

      // Handle step_completed event: update step status
      // Uses conditional UPDATE to prevent completing an already-terminal step.
      if (data.eventType === 'step_completed') {
        const eventData = (data as any).eventData as { result?: any };
        const [stepValue] = await drizzle
          .update(Schema.steps)
          .set({
            status: 'completed',
            output: eventData.result as SerializedContent | undefined,
            completedAt: now,
          })
          .where(
            and(
              eq(Schema.steps.runId, effectiveRunId),
              eq(Schema.steps.stepId, data.correlationId!),
              notInArray(Schema.steps.status, terminalStepStatuses)
            )
          )
          .returning();
        if (stepValue) {
          step = deserializeStepError(compact(stepValue));
        } else {
          // Step not updated - check if it exists and why
          const [existing] = await getStepForValidation.execute({
            runId: effectiveRunId,
            stepId: data.correlationId!,
          });
          if (!existing) {
            throw new WorkflowWorldError(
              `Step "${data.correlationId}" not found`
            );
          }
          if (isTerminalStepStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot modify step in terminal state "${existing.status}"`
            );
          }
        }
      }

      // Handle step_failed event: terminal state with error
      // Uses conditional UPDATE to prevent failing an already-terminal step.
      if (data.eventType === 'step_failed') {
        const eventData = (data as any).eventData as {
          error?: unknown;
        };
        // The error field is SerializedData (Uint8Array) produced by
        // dehydrateStepError. We store it verbatim in the error_cbor column;
        // consumers hydrate via hydrateStepError.
        const [stepValue] = await drizzle
          .update(Schema.steps)
          .set({
            status: 'failed',
            error: eventData.error as SerializedData,
            completedAt: now,
          })
          .where(
            and(
              eq(Schema.steps.runId, effectiveRunId),
              eq(Schema.steps.stepId, data.correlationId!),
              notInArray(Schema.steps.status, terminalStepStatuses)
            )
          )
          .returning();
        if (stepValue) {
          step = deserializeStepError(compact(stepValue));
        } else {
          // Step not updated - check if it exists and why
          const [existing] = await getStepForValidation.execute({
            runId: effectiveRunId,
            stepId: data.correlationId!,
          });
          if (!existing) {
            throw new WorkflowWorldError(
              `Step "${data.correlationId}" not found`
            );
          }
          if (isTerminalStepStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot modify step in terminal state "${existing.status}"`
            );
          }
        }
      }

      // Handle step_retrying event: sets status back to 'pending', records error
      // Uses conditional UPDATE to prevent retrying an already-terminal step.
      if (data.eventType === 'step_retrying') {
        const eventData = (data as any).eventData as {
          error?: unknown;
          retryAfter?: Date;
        };
        const [stepValue] = await drizzle
          .update(Schema.steps)
          .set({
            status: 'pending',
            error: eventData.error as SerializedData,
            retryAfter: eventData.retryAfter,
          })
          .where(
            and(
              eq(Schema.steps.runId, effectiveRunId),
              eq(Schema.steps.stepId, data.correlationId!),
              notInArray(Schema.steps.status, terminalStepStatuses)
            )
          )
          .returning();
        if (stepValue) {
          step = deserializeStepError(compact(stepValue));
        } else {
          // Step not updated - check if it exists and why
          const [existing] = await getStepForValidation.execute({
            runId: effectiveRunId,
            stepId: data.correlationId!,
          });
          if (!existing) {
            throw new WorkflowWorldError(
              `Step "${data.correlationId}" not found`
            );
          }
          if (isTerminalStepStatus(existing.status)) {
            throw new EntityConflictError(
              `Cannot modify step in terminal state "${existing.status}"`
            );
          }
        }
      }

      // Handle hook_created event: create hook entity
      // Uses prepared statement for token uniqueness check (performance optimization)
      if (data.eventType === 'hook_created') {
        const eventData = (data as any).eventData as {
          token: string;
          metadata?: any;
          isWebhook?: boolean;
          isSystem?: boolean;
        };

        // Check for duplicate token using prepared statement
        const [existingHook] = await getHookByToken.execute({
          token: eventData.token,
        });
        if (existingHook) {
          // Idempotency: if the existing hook is the *same* (runId, hookId)
          // we are trying to create, this is either a duplicate / replayed
          // processing of the same hook_created (not a real conflict), or
          // an orphaned hook row from a prior crashed attempt (the hook
          // INSERT below landed but the events INSERT below didn't —
          // these writes are not in one transaction). Distinguish by
          // checking whether the `hook_created` event actually exists in
          // the event log:
          //   - exists → real duplicate: throw EntityConflictError so the
          //     runtime's concurrent-replay catch path (matching the
          //     step_created path) swallows it, instead of producing a
          //     self-conflict in the event log that would later replay
          //     as HookConflictError.
          //     See https://github.com/vercel/workflow/issues/2283.
          //   - missing → orphaned hook row (crash between hook INSERT
          //     and events INSERT): skip the hook insert (the existing
          //     row already has the desired state) and fall through to
          //     the events INSERT below, completing the partial write.
          if (
            existingHook.runId === effectiveRunId &&
            existingHook.hookId === data.correlationId
          ) {
            const [existingEvent] = await getHookCreatedEvent.execute({
              runId: effectiveRunId,
              correlationId: data.correlationId,
              eventType: 'hook_created',
            });
            if (existingEvent) {
              throw new EntityConflictError(
                `Hook "${data.correlationId}" already created`
              );
            }
            // Orphaned hook row: hook row exists but no hook_created
            // event in the log. Skip the hook insert below (the row
            // already exists with our (runId, hookId)) and let the
            // outer code path emit the hook_created event, completing
            // the partial write. We also re-fetch the existing hook
            // row so the EventResult carries the actual persisted
            // entity rather than `undefined`.
            const [recoveredHookValue] = await drizzle
              .select()
              .from(Schema.hooks)
              .where(eq(Schema.hooks.hookId, data.correlationId!))
              .limit(1);
            if (recoveredHookValue) {
              recoveredHookValue.metadata ||= recoveredHookValue.metadataJson;
              hook = HookSchema.parse(compact(recoveredHookValue));
            }
          } else {
            // Cross-hook / cross-run conflict: a different
            // (runId, hookId) holds this token. Create a hook_conflict
            // event instead of throwing 409 — this lets the workflow
            // continue and fail gracefully when the hook is awaited.
            const conflictEventData = {
              token: eventData.token,
              conflictingRunId: existingHook.runId,
            };
            const conflictEventId = getEventId();

            const [conflictValue] = await drizzle
              .insert(events)
              .values({
                runId: effectiveRunId,
                eventId: conflictEventId,
                correlationId: data.correlationId,
                eventType: 'hook_conflict',
                eventData: conflictEventData,
                specVersion: effectiveSpecVersion,
              })
              .returning({ createdAt: events.createdAt });

            if (!conflictValue) {
              throw new EntityConflictError(
                `Event ${conflictEventId} could not be created`
              );
            }

            const conflictResult = {
              eventType: 'hook_conflict' as const,
              correlationId: data.correlationId,
              eventData: conflictEventData,
              ...conflictValue,
              runId: effectiveRunId,
              eventId: conflictEventId,
            };
            const parsedConflict = EventSchema.parse(conflictResult);
            const resolveData = params?.resolveData ?? 'all';
            return {
              event: stripEventDataRefs(parsedConflict, resolveData),
              run,
              step,
              hook: undefined,
            };
          }
        } else {
          const [hookValue] = await drizzle
            .insert(Schema.hooks)
            .values({
              runId: effectiveRunId,
              hookId: data.correlationId!,
              token: eventData.token,
              metadata: eventData.metadata as SerializedContent,
              ownerId: '', // TODO: get from context
              projectId: '', // TODO: get from context
              environment: '', // TODO: get from context
              // Propagate specVersion from the event to the hook entity
              specVersion: effectiveSpecVersion,
              isWebhook: eventData.isWebhook,
              isSystem: eventData.isSystem ?? false,
            })
            .onConflictDoNothing()
            .returning();
          if (hookValue) {
            hookValue.metadata ||= hookValue.metadataJson;
            hook = HookSchema.parse(compact(hookValue));
          }
        }
      }

      // Handle hook_disposed event: delete hook entity atomically.
      // Uses DELETE ... RETURNING to ensure only one concurrent caller
      // succeeds — if no rows are returned, the hook was already disposed.
      if (data.eventType === 'hook_disposed' && data.correlationId) {
        const [deleted] = await drizzle
          .delete(Schema.hooks)
          .where(eq(Schema.hooks.hookId, data.correlationId))
          .returning({ hookId: Schema.hooks.hookId });
        if (!deleted) {
          throw new EntityConflictError(
            `Hook "${data.correlationId}" already disposed`
          );
        }
      }

      // Handle wait_created event: create wait entity
      if (data.eventType === 'wait_created') {
        const eventData = (data as any).eventData as {
          resumeAt?: Date;
        };
        const waitId = `${effectiveRunId}-${data.correlationId}`;
        const [waitValue] = await drizzle
          .insert(Schema.waits)
          .values({
            waitId,
            runId: effectiveRunId,
            status: 'waiting',
            resumeAt: eventData.resumeAt,
            specVersion: effectiveSpecVersion,
          })
          .onConflictDoNothing()
          .returning();
        if (waitValue) {
          wait = {
            waitId: waitValue.waitId,
            runId: waitValue.runId,
            status: waitValue.status,
            resumeAt: waitValue.resumeAt ?? undefined,
            completedAt: waitValue.completedAt ?? undefined,
            createdAt: waitValue.createdAt,
            updatedAt: waitValue.updatedAt,
            specVersion: waitValue.specVersion ?? undefined,
          };
        } else {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already exists`
          );
        }
      }

      // Handle wait_completed event: transition wait to 'completed'
      // Uses conditional UPDATE to reject duplicate completions (same pattern as step_completed)
      if (data.eventType === 'wait_completed') {
        const waitId = `${effectiveRunId}-${data.correlationId}`;
        const [waitValue] = await drizzle
          .update(Schema.waits)
          .set({
            status: 'completed',
            completedAt: now,
          })
          .where(
            and(
              eq(Schema.waits.waitId, waitId),
              eq(Schema.waits.status, 'waiting')
            )
          )
          .returning();
        if (waitValue) {
          wait = {
            waitId: waitValue.waitId,
            runId: waitValue.runId,
            status: waitValue.status,
            resumeAt: waitValue.resumeAt ?? undefined,
            completedAt: waitValue.completedAt ?? undefined,
            createdAt: waitValue.createdAt,
            updatedAt: waitValue.updatedAt,
            specVersion: waitValue.specVersion ?? undefined,
          };
        } else {
          // Wait not updated - check if it exists and why
          const [existing] = await getWaitForValidation.execute({
            waitId,
          });
          if (!existing) {
            throw new WorkflowWorldError(
              `Wait "${data.correlationId}" not found`
            );
          }
          if (existing.status === 'completed') {
            throw new EntityConflictError(
              `Wait "${data.correlationId}" already completed`
            );
          }
        }
      }

      try {
        if (!value) {
          [value] = await drizzle
            .insert(events)
            .values({
              runId: effectiveRunId,
              eventId: getEventId(),
              correlationId: data.correlationId,
              eventType: data.eventType,
              eventData: storedEventData,
              specVersion: effectiveSpecVersion,
            })
            .returning({ createdAt: events.createdAt });
        }
      } catch (err) {
        // Translate unique-violation on the correlated-event partial index
        // (workflow_events_entity_creation_unique) into EntityConflictError
        // so the runtime's existing dedup catch path can handle it. Without
        // this, two concurrent invocations producing identical
        // correlationIds (e.g. snapshot runtime deterministic ULIDs) would
        // surface as unhandled DB errors instead of dedup signals.
        // Drizzle wraps the underlying pg error in DrizzleQueryError; the
        // pg error (with .code === '23505') lives on .cause. We additionally
        // gate on the violated constraint name so other 23505 violations on
        // these event types (e.g. the events primary key, or any future
        // unique constraint we might add) don't get misclassified as a
        // correlationId conflict.
        const isDeduplicatedCorrelatedEvent =
          isChildEntityCreationEventType(data.eventType) ||
          (data.eventType === 'attr_set' &&
            data.eventData.writer.type === 'workflow');
        const pgErr = (err as { code?: string; constraint?: string }).code
          ? (err as { code?: string; constraint?: string })
          : ((err as { cause?: { code?: string; constraint?: string } })
              .cause ?? {});
        const pgCode = pgErr.code;
        const pgConstraint = pgErr.constraint;
        if (
          isDeduplicatedCorrelatedEvent &&
          pgCode === '23505' &&
          pgConstraint === 'workflow_events_entity_creation_unique'
        ) {
          throw new EntityConflictError(
            `${data.eventType} for correlationId "${data.correlationId}" already exists in run "${effectiveRunId}"`
          );
        }
        throw err;
      }
      if (!value) {
        throw new EntityConflictError(
          `Event ${getEventId()} could not be created`
        );
      }
      const result = {
        ...data,
        ...value,
        runId: effectiveRunId,
        eventId: getEventId(),
        ...(storedEventData !== undefined
          ? { eventData: storedEventData }
          : {}),
      };
      // Strip eventData leaked by ...data spread for run_started events.
      // The eventData (run input for resilient start) belongs on
      // run_created only; storedEventData is already undefined above.
      if (data.eventType === 'run_started') {
        delete (result as any).eventData;
      }
      const parsed = EventSchema.parse(result);
      const resolveData = params?.resolveData ?? 'all';

      // For run_started: include all events so the runtime can skip
      // the initial events.list call and reduce TTFB.
      let allEvents: Event[] | undefined;
      let cursor: string | null | undefined;
      let hasMore: boolean | undefined;
      if (data.eventType === 'run_started' && run) {
        const eventRows = await drizzle
          .select()
          .from(Schema.events)
          .where(eq(Schema.events.runId, effectiveRunId))
          .orderBy(Schema.events.eventId);
        allEvents = eventRows.map((e) => {
          e.eventData ||= e.eventDataJson;
          const parsed = EventSchema.parse(compact(e));
          return stripEventDataRefs(parsed, resolveData);
        });
        cursor = allEvents.at(-1)?.eventId ?? null;
        hasMore = false;
      }

      return {
        event: stripEventDataRefs(parsed, resolveData),
        run,
        step,
        hook,
        wait,
        events: allEvents,
        cursor,
        hasMore,
        ...(stepCreatedLazily ? { stepCreated: true } : {}),
      };
    },
    /**
     * Batch step transition (WORKFLOW_BATCH_TRANSITIONS). Applies the ordered
     * transition [step_completed(N), step_created(N+1), step_started(N+1)] for
     * one run in a SINGLE Postgres transaction — the same all-or-nothing
     * contract workflow-server#646 provides over HTTP. Either every write
     * commits or none does: any thrown error rolls the whole transaction back.
     *
     * Mirrors the single-`create` per-type logic (guarded UPDATEs, the
     * born-running create-claim, the unique-index dedup) but threads every read
     * and write through the transaction handle `tx`. The module-level prepared
     * statements run on a separate pooled connection and would NOT see the tx's
     * uncommitted rows, so they are deliberately not used here.
     *
     * Result contract the runtime relies on (packages/core/src/runtime.ts):
     * - `results` holds one EventResult per input event, in order; the runtime
     *   reads the LAST (the step_started) for the born-running `step` entity and
     *   the `stepCreated` create-claim signal.
     * - `stepCreated: true` is stamped on the step_started result ONLY when THIS
     *   batch won step N+1's create-claim (a fresh born-running commit). On an
     *   already-applied retry or a lost race it is omitted, so the non-committer
     *   re-derives from a fresh replay instead of double-running the body —
     *   matching the server (events.ts:4808-4824).
     * - A genuine all-or-nothing conflict throws EntityConflictError (the client
     *   maps it to reinvoke). world-postgres does not enforce the optimistic
     *   `stateUpdatedAt` guard (neither does its single-`create` path), so it
     *   never raises 412; the client tolerates its absence.
     */
    async createBatch(runId, batchEvents, params): Promise<BatchEventResult> {
      if (!runId) {
        throw new WorkflowWorldError(
          'world-postgres: createBatch requires a runId'
        );
      }
      if (!batchEvents || batchEvents.length === 0) {
        throw new WorkflowWorldError(
          'world-postgres: createBatch requires at least one event'
        );
      }
      const resolveData: ResolveData = 'all';
      const now = new Date();
      const terminalStepStatuses: (typeof Schema.steps.status.enumValues)[number][] =
        [...TERMINAL_STEP_STATUSES];
      const terminalRunStatuses: (typeof Schema.runs.status.enumValues)[number][] =
        [...TERMINAL_WORKFLOW_RUN_STATUSES];

      // Classify + validate the ordered events into positional ops (mirrors
      // world-local's `classifyLocalSuspensionBatch` and workflow-server's
      // `classifySuspensionBatch`). Pure — each input index maps to exactly one
      // result position. The UNFENCED (legacy / direct-test) path keeps the v1
      // triple-only contract; the full suspension grammar is admitted only when
      // a `batchId` fence is present (the runtime always supplies it for a v2
      // run), so existing unfenced callers are unaffected.
      const ops = classifyPostgresSuspensionBatch(batchEvents);
      const stepRunOps = ops.filter(
        (o): o is Extract<PostgresBatchOp, { kind: 'step-run' }> =>
          o.kind === 'step-run'
      );
      const isV1Triple =
        stepRunOps.length === 1 &&
        ops.every((o) => o.kind === 'outcome' || o.kind === 'step-run');
      const batchId = params?.batchId;
      const expectedRunVersion = params?.expectedRunVersion;
      const fenced = batchId !== undefined;
      if (!fenced && !isV1Triple) {
        throw new WorkflowWorldError(
          'world-postgres: unfenced createBatch supports only the [step_completed?, step_created, step_started] transition; the full suspension grammar requires the v2 fence (batchId)'
        );
      }

      return drizzle.transaction(async (tx) => {
        // Positional result array — one entry per input event index. A
        // `step-run` op fills two positions (created + started); every other op
        // fills one.
        const results: EventResult[] = new Array(batchEvents.length);

        // Positional materialization of the batch's current step entities — for
        // an idempotent / already-applied return (no writes, no `stepCreated`).
        // Only step entities are read (the client inspects nothing else here),
        // mirroring world-local's `currentResults`.
        const currentResults = async (): Promise<EventResult[]> => {
          const out: EventResult[] = [];
          for (const e of batchEvents) {
            if (
              typeof e.correlationId === 'string' &&
              (e.eventType === 'step_created' ||
                e.eventType === 'step_started' ||
                e.eventType === 'step_completed' ||
                e.eventType === 'step_failed')
            ) {
              const [row] = await tx
                .select()
                .from(Schema.steps)
                .where(
                  and(
                    eq(Schema.steps.runId, runId),
                    eq(Schema.steps.stepId, e.correlationId)
                  )
                )
                .limit(1);
              out.push(row ? { step: deserializeStepError(compact(row)) } : {});
            } else {
              out.push({});
            }
          }
          return out;
        };

        // v2 suspension-batch fence. Present iff the caller supplied a batchId.
        // Lock the run row FOR UPDATE so the version check and the advance below
        // are a genuine CAS within this transaction.
        let currentRunVersion: number | undefined;
        if (fenced) {
          const [runRow] = await tx
            .select({
              runVersion: Schema.runs.runVersion,
              lastBatchId: Schema.runs.lastBatchId,
            })
            .from(Schema.runs)
            .where(eq(Schema.runs.runId, runId))
            .limit(1)
            .for('update');
          if (!runRow) {
            throw new WorkflowWorldError(
              `world-postgres: run "${runId}" not found`,
              { status: 404 }
            );
          }
          currentRunVersion = runRow.runVersion ?? undefined;
          if (currentRunVersion === undefined) {
            // Pre-v2 run: reject with a distinct code so the client latches
            // batching off permanently for this run.
            throw new WorkflowWorldError(
              'world-postgres: run predates the suspension-batch fence (no runVersion)',
              { status: 409, code: 'run-not-versioned' }
            );
          }
          // Idempotent already-applied: the SAME batch committed earlier (a
          // transport retry / redelivery). Return the current entities without
          // `stepCreated` and WRITE NOTHING — a whole-batch short-circuit
          // (world-local's proven shape). The client re-derives from a fresh
          // replay. This removes every per-branch already-applied gate below and
          // with it any risk of minting a duplicate eventId on a retry.
          if (runRow.lastBatchId === batchId) {
            return {
              results: await currentResults(),
              runVersion: currentRunVersion,
              lastBatchId: batchId,
            };
          }
          // Optimistic-concurrency fence: a different write advanced the run
          // since the client's snapshot. Abort all-or-nothing (rolls back) — the
          // client abandons the deferred completion and re-derives.
          if (expectedRunVersion !== currentRunVersion) {
            throw new EntityConflictError(
              `world-postgres: suspension-batch run-version conflict (expected ${expectedRunVersion}, run at ${currentRunVersion})`
            );
          }
        }

        // v1 idempotency (triple only): if the lone born-running step already
        // exists running/terminal, the transition committed earlier. Write
        // nothing and return current entities WITHOUT `stepCreated` so the
        // client re-derives instead of double-running the body. The fenced
        // general path relies on `lastBatchId` (above) + the per-op create-claim
        // instead, so this peek is scoped to the triple.
        if (isV1Triple) {
          const [existing] = await tx
            .select()
            .from(Schema.steps)
            .where(
              and(
                eq(Schema.steps.runId, runId),
                // biome-ignore lint/style/noNonNullAssertion: classifier claimStep guarantees a string correlationId
                eq(Schema.steps.stepId, stepRunOps[0].created.correlationId!)
              )
            )
            .limit(1);
          const existingStep = existing
            ? deserializeStepError(compact(existing))
            : undefined;
          if (
            existingStep &&
            (existingStep.status === 'running' ||
              isTerminalStepStatus(existingStep.status))
          ) {
            const idempotent = await currentResults();
            return fenced
              ? {
                  results: idempotent,
                  runVersion: currentRunVersion,
                  lastBatchId: batchId,
                }
              : { results: idempotent };
          }
        }

        // Insert one event row on `tx` and return the hydrated Event, mirroring
        // the single-path result construction (see the create() tail above).
        const insertEvent = async (
          data: CreateEventRequest,
          storedEventData: unknown
        ): Promise<Event> => {
          const eventId = `wevt_${ulid()}`;
          const [value] = await tx
            .insert(events)
            .values({
              runId,
              eventId,
              correlationId: data.correlationId,
              eventType: data.eventType,
              eventData: storedEventData as SerializedContent | undefined,
              specVersion: data.specVersion ?? SPEC_VERSION_CURRENT,
            })
            .returning({ createdAt: events.createdAt });
          if (!value) {
            throw new EntityConflictError(
              `Event ${eventId} could not be created`
            );
          }
          const parsed = EventSchema.parse({
            ...data,
            createdAt: value.createdAt,
            runId,
            eventId,
            ...(storedEventData !== undefined
              ? { eventData: storedEventData }
              : {}),
          });
          return stripEventDataRefs(parsed, resolveData);
        };

        // Full step re-read on `tx` (for idempotent / already-applied results).
        const readStep = async (stepId: string): Promise<Step | undefined> => {
          const [row] = await tx
            .select()
            .from(Schema.steps)
            .where(
              and(
                eq(Schema.steps.runId, runId),
                eq(Schema.steps.stepId, stepId)
              )
            )
            .limit(1);
          return row ? deserializeStepError(compact(row)) : undefined;
        };

        // Apply every op in request order, filling one result per input event
        // position. Ordering is load-bearing: a leading outcome (index 0) is
        // applied first so completed(N) < created(N+1) holds, and eventIds are
        // minted in request order. Each op mirrors its single-`create` write; a
        // genuine conflict throws EntityConflictError and rolls the whole batch
        // back (all-or-nothing), exactly like the single path. The whole-batch
        // short-circuit above already handled the same-batch retry, so no op
        // needs a per-branch already-applied gate.
        for (const op of ops) {
          switch (op.kind) {
            case 'outcome': {
              // biome-ignore lint/style/noNonNullAssertion: classifier claimStep guarantees a string correlationId
              const correlationId = op.req.correlationId!;
              if (op.req.eventType === 'step_completed') {
                const eventData = (
                  op.req as { eventData?: { result?: unknown } }
                ).eventData;
                const [stepValue] = await tx
                  .update(Schema.steps)
                  .set({
                    status: 'completed',
                    output: eventData?.result as SerializedContent | undefined,
                    completedAt: now,
                  })
                  .where(
                    and(
                      eq(Schema.steps.runId, runId),
                      eq(Schema.steps.stepId, correlationId),
                      notInArray(Schema.steps.status, terminalStepStatuses)
                    )
                  )
                  .returning();
                if (stepValue) {
                  const step = deserializeStepError(compact(stepValue));
                  const event = await insertEvent(op.req, eventData);
                  results[op.index] = { event, step };
                } else {
                  // 0 rows: the step is missing or already terminal.
                  const existing = await readStep(correlationId);
                  if (!existing) {
                    throw new WorkflowWorldError(
                      `Step "${correlationId}" not found`
                    );
                  }
                  if (existing.status === 'completed') {
                    // Idempotent: a prior write already completed step N.
                    results[op.index] = { step: existing };
                  } else {
                    throw new EntityConflictError(
                      `Cannot modify step in terminal state "${existing.status}"`
                    );
                  }
                }
              } else {
                // step_failed: mirror the single path (error stored verbatim in
                // the error_cbor column; consumers hydrate via hydrateStepError).
                const eventData = (
                  op.req as { eventData?: { error?: unknown } }
                ).eventData;
                const [stepValue] = await tx
                  .update(Schema.steps)
                  .set({
                    status: 'failed',
                    error: eventData?.error as SerializedData | undefined,
                    completedAt: now,
                  })
                  .where(
                    and(
                      eq(Schema.steps.runId, runId),
                      eq(Schema.steps.stepId, correlationId),
                      notInArray(Schema.steps.status, terminalStepStatuses)
                    )
                  )
                  .returning();
                if (stepValue) {
                  const step = deserializeStepError(compact(stepValue));
                  const event = await insertEvent(op.req, eventData);
                  results[op.index] = { event, step };
                } else {
                  const existing = await readStep(correlationId);
                  if (!existing) {
                    throw new WorkflowWorldError(
                      `Step "${correlationId}" not found`
                    );
                  }
                  if (existing.status === 'failed') {
                    results[op.index] = { step: existing };
                  } else {
                    throw new EntityConflictError(
                      `Cannot modify step in terminal state "${existing.status}"`
                    );
                  }
                }
              }
              break;
            }
            case 'step-pending': {
              // A queued (non-inline) fan-out create: write the step entity +
              // its step_created event; NO `stepCreated` stamp (the server
              // stamps that only on a born-running started frame).
              // biome-ignore lint/style/noNonNullAssertion: classifier claimStep guarantees a string correlationId
              const correlationId = op.req.correlationId!;
              const eventData = (
                op.req as { eventData: { stepName: string; input: unknown } }
              ).eventData;
              const [inserted] = await tx
                .insert(Schema.steps)
                .values({
                  runId,
                  stepId: correlationId,
                  stepName: eventData.stepName,
                  input: eventData.input as SerializedContent,
                  status: 'pending',
                  attempt: 0,
                  specVersion: op.req.specVersion ?? SPEC_VERSION_CURRENT,
                })
                .onConflictDoNothing()
                .returning();
              if (inserted) {
                const step = deserializeStepError(compact(inserted));
                const event = await insertEvent(op.req, eventData);
                results[op.index] = { event, step };
              } else {
                // Lost / already-applied: don't write a duplicate step_created
                // event (would violate the entity-creation unique index).
                const existing = await readStep(correlationId);
                results[op.index] = existing ? { step: existing } : {};
              }
              break;
            }
            case 'step-run': {
              // Born-running: fold step_created + step_started. Win the
              // create-claim, record create-before-start, transition to running,
              // and stamp `stepCreated` on the started result (the exactly-one
              // -winner signal the client reads for ownership).
              //
              // World divergence (LOW, client-safe): unlike world-vercel's
              // atomic TransactWriteItems (all inline creates stamp or the whole
              // tx cancels — mixed is impossible), `onConflictDoNothing` here does
              // NOT abort the tx. So across a K-step fan-out, if one correlationId
              // was already created by a concurrent single-event writer, the batch
              // can commit with a MIXED stamp (some `stepCreated`, some not). The
              // client's all-or-nothing ownership loop treats any unstamped inline
              // frame as not-owned → reinvoke(0), runs NO body — so a mixed stamp
              // never double-runs a body; the fresh step this batch did create is
              // recovered via owned-recovery (same-messageId reinvoke) or backstop.
              // Reaching it needs double-delivery + a path-split on world-postgres
              // (rollout skew / transient batch-disable on exactly one delivery),
              // which the shared fence + identical seeding otherwise prevent.
              // biome-ignore lint/style/noNonNullAssertion: classifier claimStep guarantees a string correlationId
              const correlationId = op.created.correlationId!;
              const createdData = (
                op.created as {
                  eventData: { stepName: string; input: unknown };
                }
              ).eventData;
              const [inserted] = await tx
                .insert(Schema.steps)
                .values({
                  runId,
                  stepId: correlationId,
                  stepName: createdData.stepName,
                  input: createdData.input as SerializedContent,
                  status: 'pending',
                  attempt: 0,
                  specVersion: op.created.specVersion ?? SPEC_VERSION_CURRENT,
                })
                .onConflictDoNothing()
                .returning();
              if (inserted) {
                const createdEvent = await insertEvent(op.created, createdData);
                const [stepValue] = await tx
                  .update(Schema.steps)
                  .set({
                    status: 'running',
                    attempt: sql`${Schema.steps.attempt} + 1`,
                    startedAt: sql`COALESCE(${Schema.steps.startedAt}, ${now.toISOString()})`,
                    retryAfter: null,
                  })
                  .where(
                    and(
                      eq(Schema.steps.runId, runId),
                      eq(Schema.steps.stepId, correlationId),
                      notInArray(Schema.steps.status, terminalStepStatuses)
                    )
                  )
                  .returning();
                if (!stepValue) {
                  // The step we just created cannot already be terminal — a
                  // 0-row update here is a real invariant violation.
                  throw new WorkflowWorldError(
                    `Step "${correlationId}" could not be started in batch`
                  );
                }
                const step = deserializeStepError(compact(stepValue));
                // step_started never carries `input` (it lives on step_created);
                // strip defensively to match the single path.
                const { input: _omitInput, ...rest } = ((
                  op.started as { eventData?: Record<string, unknown> }
                ).eventData ?? {}) as Record<string, unknown>;
                const startedEvent = await insertEvent(op.started, rest);
                results[op.createdIndex] = { event: createdEvent, step };
                results[op.startedIndex] = {
                  event: startedEvent,
                  step,
                  stepCreated: true,
                };
              } else {
                // Lost / already-applied: return the current entity WITHOUT
                // `stepCreated` so the client re-derives from a fresh replay.
                const existing = await readStep(correlationId);
                const r = existing ? { step: existing } : {};
                results[op.createdIndex] = r;
                results[op.startedIndex] = existing ? { step: existing } : {};
              }
              break;
            }
            case 'wait': {
              // biome-ignore lint/style/noNonNullAssertion: classifier guarantees a string correlationId
              const correlationId = op.req.correlationId!;
              const waitId = `${runId}-${correlationId}`;
              if (op.req.eventType === 'wait_created') {
                const eventData = (
                  op.req as { eventData?: { resumeAt?: Date } }
                ).eventData;
                const [inserted] = await tx
                  .insert(Schema.waits)
                  .values({
                    waitId,
                    runId,
                    status: 'waiting',
                    resumeAt: eventData?.resumeAt,
                    specVersion: op.req.specVersion ?? SPEC_VERSION_CURRENT,
                  })
                  .onConflictDoNothing()
                  .returning();
                if (!inserted) {
                  // A different write created this wait — genuine conflict, roll
                  // back all-or-nothing (the same-batch retry was short-circuited
                  // above). Mirrors the single path.
                  throw new EntityConflictError(
                    `Wait "${correlationId}" already exists`
                  );
                }
                const event = await insertEvent(op.req, eventData);
                results[op.index] = { event };
              } else {
                // wait_completed: transition waiting → completed (idempotent
                // reject on an already-completed wait, mirroring the single path).
                const [waitValue] = await tx
                  .update(Schema.waits)
                  .set({ status: 'completed', completedAt: now })
                  .where(
                    and(
                      eq(Schema.waits.waitId, waitId),
                      eq(Schema.waits.status, 'waiting')
                    )
                  )
                  .returning();
                if (!waitValue) {
                  const [existing] = await tx
                    .select({ status: Schema.waits.status })
                    .from(Schema.waits)
                    .where(eq(Schema.waits.waitId, waitId))
                    .limit(1);
                  if (!existing) {
                    throw new WorkflowWorldError(
                      `Wait "${correlationId}" not found`
                    );
                  }
                  throw new EntityConflictError(
                    `Wait "${correlationId}" already completed`
                  );
                }
                const event = await insertEvent(
                  op.req,
                  (op.req as { eventData?: unknown }).eventData
                );
                results[op.index] = { event };
              }
              break;
            }
            case 'hook-received': {
              // Event-insert only: world-postgres has no hook_received entity
              // mutation (see handleLegacyEventPostgres note and the single
              // path). Not emitted by collect-mode today (the runtime gates on
              // abortCount === 0); implemented for full world-local parity so a
              // grammar-legal batch never hits an unsupported-type reject.
              const event = await insertEvent(
                op.req,
                (op.req as { eventData?: unknown }).eventData
              );
              results[op.index] = { event };
              break;
            }
            case 'terminal': {
              // run_completed | run_failed as the last batch event: update run
              // status, delete hooks + waits (token reuse), record the event.
              // Mirrors the single path. Not emitted by collect-mode today
              // (terminal folding is a documented follow-up gated on #3023);
              // implemented for full world-local parity.
              let run: WorkflowRun | undefined;
              if (op.req.eventType === 'run_completed') {
                const eventData = (
                  op.req as { eventData?: { output?: unknown } }
                ).eventData;
                const [runValue] = await tx
                  .update(Schema.runs)
                  .set({
                    status: 'completed',
                    output: eventData?.output as SerializedContent | undefined,
                    completedAt: now,
                    updatedAt: now,
                  })
                  .where(
                    and(
                      eq(Schema.runs.runId, runId),
                      notInArray(Schema.runs.status, terminalRunStatuses)
                    )
                  )
                  .returning();
                if (runValue) {
                  run = deserializeRunError(compact(runValue));
                } else {
                  const [existing] = await tx
                    .select({ status: Schema.runs.status })
                    .from(Schema.runs)
                    .where(eq(Schema.runs.runId, runId))
                    .limit(1);
                  if (!existing) {
                    throw new WorkflowRunNotFoundError(runId);
                  }
                  if (isTerminalWorkflowRunStatus(existing.status)) {
                    throw new EntityConflictError(
                      `Cannot transition run from terminal state "${existing.status}"`
                    );
                  }
                }
              } else {
                const eventData = (
                  op.req as {
                    eventData?: { error?: unknown; errorCode?: string };
                  }
                ).eventData;
                const [runValue] = await tx
                  .update(Schema.runs)
                  .set({
                    status: 'failed',
                    error: eventData?.error as SerializedData | undefined,
                    errorCode: eventData?.errorCode,
                    completedAt: now,
                    updatedAt: now,
                  })
                  .where(
                    and(
                      eq(Schema.runs.runId, runId),
                      notInArray(Schema.runs.status, terminalRunStatuses)
                    )
                  )
                  .returning();
                if (runValue) {
                  run = deserializeRunError(compact(runValue));
                } else {
                  const [existing] = await tx
                    .select({ status: Schema.runs.status })
                    .from(Schema.runs)
                    .where(eq(Schema.runs.runId, runId))
                    .limit(1);
                  if (!existing) {
                    throw new WorkflowRunNotFoundError(runId);
                  }
                  if (isTerminalWorkflowRunStatus(existing.status)) {
                    throw new EntityConflictError(
                      `Cannot transition run from terminal state "${existing.status}"`
                    );
                  }
                }
              }
              await Promise.all([
                tx.delete(Schema.hooks).where(eq(Schema.hooks.runId, runId)),
                tx.delete(Schema.waits).where(eq(Schema.waits.runId, runId)),
              ]);
              const event = await insertEvent(
                op.req,
                (op.req as { eventData?: unknown }).eventData
              );
              results[op.index] = run ? { event, run } : { event };
              break;
            }
          }
        }

        // Optional inline delta: the events written strictly after
        // `sinceCursor`, same semantics as events.list (ULID-ordered). Computed
        // in-tx so it reflects exactly what this batch committed.
        let deltaEvents: Event[] | undefined;
        let cursor: string | null | undefined;
        let hasMore: boolean | undefined;
        if (typeof params?.sinceCursor === 'string') {
          const limit = 100;
          const rows = await tx
            .select()
            .from(events)
            .where(
              and(
                eq(events.runId, runId),
                gt(events.eventId, params.sinceCursor)
              )
            )
            .orderBy(events.eventId)
            .limit(limit + 1);
          const page = rows.slice(0, limit);
          deltaEvents = page.map((e) => {
            e.eventData ||= e.eventDataJson;
            const parsed = EventSchema.parse(compact(e));
            return stripEventDataRefs(parsed, resolveData);
          });
          cursor = page.at(-1)?.eventId ?? null;
          hasMore = rows.length > limit;
        }

        // Advance the fence: bump runVersion to expectedRunVersion + 1 (the
        // server's contract) and record lastBatchId, conditional on the version
        // we locked — a 0-row result means a concurrent advance slipped in and
        // the whole batch rolls back. Unconditional when fenced: an
        // already-applied retry returned early via the whole-batch short-circuit
        // above (it never reaches here). v2 only.
        let newRunVersion: number | undefined = currentRunVersion;
        if (fenced) {
          const [updated] = await tx
            .update(Schema.runs)
            .set({
              runVersion: (expectedRunVersion as number) + 1,
              lastBatchId: batchId,
            })
            .where(
              and(
                eq(Schema.runs.runId, runId),
                eq(Schema.runs.runVersion, expectedRunVersion as number)
              )
            )
            .returning({ runVersion: Schema.runs.runVersion });
          if (!updated) {
            throw new EntityConflictError(
              'world-postgres: suspension-batch run-version conflict on advance'
            );
          }
          newRunVersion =
            updated.runVersion ?? (expectedRunVersion as number) + 1;
        }

        return {
          results,
          events: deltaEvents,
          cursor,
          hasMore,
          ...(fenced
            ? { runVersion: newRunVersion, lastBatchId: batchId }
            : {}),
        };
      });
    },
    async get(
      runId: string,
      eventId: string,
      params?: GetEventParams
    ): Promise<Event> {
      const [value] = await drizzle
        .select()
        .from(events)
        .where(and(eq(events.runId, runId), eq(events.eventId, eventId)))
        .limit(1);

      if (!value) {
        throw new WorkflowWorldError(`Event not found: ${eventId}`);
      }

      value.eventData ||= value.eventDataJson;
      const parsed = EventSchema.parse(compact(value));
      const resolveData = params?.resolveData ?? 'all';
      return stripEventDataRefs(parsed, resolveData);
    },
    async list(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
      const limit = params?.pagination?.limit ?? 100;
      const sortOrder = params.pagination?.sortOrder || 'asc';
      const order =
        sortOrder === 'desc'
          ? { by: desc(events.eventId), compare: lt }
          : { by: events.eventId, compare: gt };
      const all = await drizzle
        .select()
        .from(events)
        .where(
          and(
            eq(events.runId, params.runId),
            map(params.pagination?.cursor, (c) =>
              order.compare(events.eventId, c)
            )
          )
        )
        .orderBy(order.by)
        .limit(limit + 1);

      const values = all.slice(0, limit);

      const resolveData = params?.resolveData ?? 'all';
      return {
        data: values.map((v) => {
          v.eventData ||= v.eventDataJson;
          const parsed = EventSchema.parse(compact(v));
          return stripEventDataRefs(parsed, resolveData);
        }),
        cursor: values.at(-1)?.eventId ?? null,
        hasMore: all.length > limit,
      };
    },
    async listByCorrelationId(params) {
      const limit = params?.pagination?.limit ?? 100;
      const sortOrder = params.pagination?.sortOrder || 'asc';
      const order =
        sortOrder === 'desc'
          ? { by: desc(events.eventId), compare: lt }
          : { by: events.eventId, compare: gt };
      const all = await drizzle
        .select()
        .from(events)
        .where(
          and(
            eq(events.correlationId, params.correlationId),
            map(params.pagination?.cursor, (c) =>
              order.compare(events.eventId, c)
            )
          )
        )
        .orderBy(order.by)
        .limit(limit + 1);

      const values = all.slice(0, limit);

      const resolveData = params?.resolveData ?? 'all';
      return {
        data: values.map((v) => {
          v.eventData ||= v.eventDataJson;
          const parsed = EventSchema.parse(compact(v));
          return stripEventDataRefs(parsed, resolveData);
        }),
        cursor: values.at(-1)?.eventId ?? null,
        hasMore: all.length > limit,
      };
    },
  };
}

export function createHooksStorage(drizzle: Drizzle): Storage['hooks'] {
  const { hooks } = Schema;
  const getByToken = drizzle
    .select()
    .from(hooks)
    .where(eq(hooks.token, sql.placeholder('token')))
    .limit(1)
    .prepare('workflow_hooks_get_by_token');

  return {
    async get(hookId, params) {
      const [value] = await drizzle
        .select()
        .from(hooks)
        .where(eq(hooks.hookId, hookId))
        .limit(1);
      value.metadata ||= value.metadataJson;
      const parsed = HookSchema.parse(compact(value));
      parsed.isWebhook ??= true;
      const resolveData = params?.resolveData ?? 'all';
      return filterHookData(parsed, resolveData);
    },
    async getByToken(token, params) {
      const [value] = await getByToken.execute({ token });
      if (!value) {
        throw new HookNotFoundError(token);
      }
      value.metadata ||= value.metadataJson;
      const parsed = HookSchema.parse(compact(value));
      parsed.isWebhook ??= true;
      const resolveData = params?.resolveData ?? 'all';
      return filterHookData(parsed, resolveData);
    },
    async list(params: ListHooksParams) {
      const limit = params?.pagination?.limit ?? 100;
      const fromCursor = params?.pagination?.cursor;
      const sortOrder = params?.pagination?.sortOrder ?? 'asc';
      const orderFn = sortOrder === 'asc' ? asc : desc;
      const cursorFn = sortOrder === 'asc' ? gt : lt;
      const all = await drizzle
        .select()
        .from(hooks)
        .where(
          and(
            map(params.runId, (id) => eq(hooks.runId, id)),
            map(fromCursor, (c) => cursorFn(hooks.hookId, c))
          )
        )
        .orderBy(orderFn(hooks.hookId))
        .limit(limit + 1);
      const values = all.slice(0, limit);
      const hasMore = all.length > limit;

      const resolveData = params?.resolveData ?? 'all';
      return {
        data: values.map((v) => {
          v.metadata ||= v.metadataJson;
          const parsed = HookSchema.parse(compact(v));
          return filterHookData(parsed, resolveData);
        }),
        cursor: values.at(-1)?.hookId ?? null,
        hasMore,
      };
    },
  };
}

export function createStepsStorage(drizzle: Drizzle): Storage['steps'] {
  const { steps } = Schema;

  return {
    get: (async (runId, stepId, params) => {
      const [value] = await drizzle
        .select()
        .from(steps)
        .where(and(eq(steps.runId, runId), eq(steps.stepId, stepId)))
        .limit(1);

      if (!value) {
        throw new WorkflowWorldError(`Step not found: ${stepId}`);
      }
      value.output ||= value.outputJson;
      value.input ||= value.inputJson;
      value.error ||= parseErrorJson(value.errorJson);
      const deserialized = deserializeStepError(compact(value));
      const parsed = StepSchema.parse(deserialized);
      const resolveData = params?.resolveData ?? 'all';
      return filterStepData(parsed, resolveData);
    }) as Storage['steps']['get'],
    list: (async (params) => {
      const limit = params?.pagination?.limit ?? 20;
      const fromCursor = params?.pagination?.cursor;

      const all = await drizzle
        .select()
        .from(steps)
        .where(
          and(
            eq(steps.runId, params.runId),
            map(fromCursor, (c) => lt(steps.stepId, c))
          )
        )
        .orderBy(desc(steps.stepId))
        .limit(limit + 1);
      const values = all.slice(0, limit);
      const hasMore = all.length > limit;

      const resolveData = params?.resolveData ?? 'all';
      return {
        data: values.map((v) => {
          v.output ||= v.outputJson;
          v.input ||= v.inputJson;
          v.error ||= parseErrorJson(v.errorJson);
          const deserialized = deserializeStepError(compact(v));
          const parsed = StepSchema.parse(deserialized);
          return filterStepData(parsed, resolveData);
        }),
        hasMore,
        cursor: values.at(-1)?.stepId ?? null,
      };
    }) as Storage['steps']['list'],
  };
}

function filterStepData(step: Step, resolveData: 'none'): StepWithoutData;
function filterStepData(step: Step, resolveData: 'all'): Step;
function filterStepData(
  step: Step,
  resolveData: ResolveData
): Step | StepWithoutData;
function filterStepData(
  step: Step,
  resolveData: ResolveData
): Step | StepWithoutData {
  if (resolveData === 'none') {
    const { input: _, output: __, ...rest } = step;

    return { input: undefined, output: undefined, ...rest };
  }
  return step;
}

function filterRunData(
  run: WorkflowRun,
  resolveData: 'none'
): WorkflowRunWithoutData;
function filterRunData(run: WorkflowRun, resolveData: 'all'): WorkflowRun;
function filterRunData(
  run: WorkflowRun,
  resolveData: ResolveData
): WorkflowRun | WorkflowRunWithoutData;
function filterRunData(
  run: WorkflowRun,
  resolveData: ResolveData
): WorkflowRun | WorkflowRunWithoutData {
  if (resolveData === 'none') {
    const { input: _, output: __, ...rest } = run;

    return { input: undefined, output: undefined, ...rest };
  }
  return run;
}

function filterHookData(hook: Hook, resolveData: ResolveData): Hook {
  if (resolveData === 'none' && 'metadata' in hook) {
    const { metadata: _, ...rest } = hook;

    return { metadata: undefined, ...rest };
  }
  return hook;
}
