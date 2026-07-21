import {
  EntityConflictError,
  PreconditionFailedError,
  RunExpiredError,
  WorkflowWorldError,
} from '@workflow/errors';
import { decode, encode } from 'cbor-x';
import { MockAgent } from 'undici';
import { describe, expect, it } from 'vitest';
import { createWorkflowRunEventsBatch } from './events.js';
import { createWorkflowRunEventsBatchV4 } from './events-v4.js';
import { WORKFLOW_SERVER_URL_OVERRIDE } from './utils.js';

const ORIGIN = WORKFLOW_SERVER_URL_OVERRIDE || 'https://vercel-workflow.com';

/**
 * Parse every v4 frame out of a batch request body —
 *   frame := u32_be(meta_len) || cbor_meta || u32_be(body_len) || body
 * concatenated back-to-back with NO sentinel/terminator (the server reads to
 * EOF). Mirrors the server-side V4BatchFrameReader so the tests assert the
 * exact wire contract.
 */
function parseBatchFrames(
  rawBody: unknown
): Array<{ meta: Record<string, unknown>; body: Uint8Array }> {
  const bytes =
    typeof rawBody === 'string'
      ? new TextEncoder().encode(rawBody)
      : new Uint8Array(rawBody as ArrayBufferLike);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const frames: Array<{ meta: Record<string, unknown>; body: Uint8Array }> = [];
  let cursor = 0;
  while (cursor < bytes.byteLength) {
    const metaLen = view.getUint32(cursor, false);
    cursor += 4;
    const meta = decode(bytes.subarray(cursor, cursor + metaLen)) as Record<
      string,
      unknown
    >;
    cursor += metaLen;
    const bodyLen = view.getUint32(cursor, false);
    cursor += 4;
    const body = bytes.slice(cursor, cursor + bodyLen);
    cursor += bodyLen;
    frames.push({ meta, body });
  }
  return frames;
}

/** Intercept the batch POST, capture the request frames + headers, and reply
 *  with the given CBOR body/status. */
function interceptBatch(
  agent: MockAgent,
  runId: string,
  reply: { status: number; body?: unknown; contentType?: string },
  captured: {
    frames?: Array<{ meta: Record<string, unknown>; body: Uint8Array }>;
    contentType?: string;
  }
) {
  agent
    .get(ORIGIN)
    .intercept({
      path: `/api/v4/runs/${runId}/events/batch`,
      method: 'POST',
    })
    .reply(
      reply.status,
      (opts: { body?: unknown; headers?: Record<string, string> }) => {
        captured.frames = parseBatchFrames(opts.body);
        const h = opts.headers ?? {};
        captured.contentType = (h['content-type'] ?? h['Content-Type']) as
          | string
          | undefined;
        return reply.body === undefined
          ? ''
          : reply.body instanceof Uint8Array
            ? reply.body
            : encode(reply.body);
      },
      {
        headers: {
          'content-type': reply.contentType ?? 'application/cbor',
        },
      }
    );
}

const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

describe('createWorkflowRunEventsBatchV4 wire encoding', () => {
  it('sends concatenated frames (no sentinel) with octet-stream content type', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    const captured: {
      frames?: ReturnType<typeof parseBatchFrames>;
      contentType?: string;
    } = {};
    interceptBatch(
      agent,
      'wrun_1',
      { status: 200, body: { results: [{}, {}, {}] } },
      captured
    );

    await createWorkflowRunEventsBatchV4(
      {
        runId: 'wrun_1',
        events: [
          {
            runId: 'wrun_1',
            eventType: 'step_completed',
            specVersion: 5,
            correlationId: 'step_a',
            payload: enc({ result: 1 }),
          },
          {
            runId: 'wrun_1',
            eventType: 'step_created',
            specVersion: 5,
            correlationId: 'step_b',
            stepName: 'step-b',
            payload: enc({ next: 'input' }),
          },
          {
            runId: 'wrun_1',
            eventType: 'step_started',
            specVersion: 5,
            correlationId: 'step_b',
            stepName: 'step-b',
          },
        ],
      },
      { token: 'test-token', dispatcher: agent }
    );

    // Exactly three frames, in order, with NO trailing sentinel frame.
    expect(captured.frames).toHaveLength(3);
    expect(captured.frames?.map((f) => f.meta.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    // The batch never carries an `_end` sentinel — the server reads to EOF.
    expect(captured.frames?.some((f) => f.meta._end !== undefined)).toBe(false);
    // Byte-identical single-POST framing: the completed/created payloads ride
    // in their frame bodies; step_started has none.
    expect(new Uint8Array(captured.frames![0].body)).toEqual(
      enc({ result: 1 })
    );
    expect(new Uint8Array(captured.frames![1].body)).toEqual(
      enc({ next: 'input' })
    );
    expect(captured.frames![2].body.byteLength).toBe(0);
    expect(captured.contentType).toBe('application/octet-stream');
    agent.assertNoPendingInterceptors();
  });

  it('decodes the response results and the NESTED eventsDelta', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    const captured = {};
    interceptBatch(
      agent,
      'wrun_1',
      {
        status: 200,
        body: {
          results: [
            { step: { stepId: 'step_a', status: 'completed' } },
            { step: { stepId: 'step_b', status: 'running' } },
            {
              step: { stepId: 'step_b', status: 'running' },
              stepCreated: true,
            },
          ],
          eventsDelta: {
            events: [{ eventId: 'evnt_1', eventType: 'step_completed' }],
            cursor: 'eid:evnt_3',
            hasMore: false,
          },
        },
      },
      captured
    );

    const result = await createWorkflowRunEventsBatchV4(
      {
        runId: 'wrun_1',
        events: [
          {
            runId: 'wrun_1',
            eventType: 'step_completed',
            specVersion: 5,
            correlationId: 'step_a',
          },
        ],
      },
      { token: 'test-token', dispatcher: agent }
    );

    expect(result.results).toHaveLength(3);
    expect(result.results[2].stepCreated).toBe(true);
    expect(result.eventsDelta?.cursor).toBe('eid:evnt_3');
    expect(result.eventsDelta?.hasMore).toBe(false);
    expect(result.eventsDelta?.events).toHaveLength(1);
    agent.assertNoPendingInterceptors();
  });
});

describe('createWorkflowRunEventsBatch adapter', () => {
  it('rides sinceCursor/stateUpdatedAt on the step_completed frame and vercelId on every frame', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    const captured: {
      frames?: ReturnType<typeof parseBatchFrames>;
      contentType?: string;
    } = {};
    interceptBatch(
      agent,
      'wrun_1',
      {
        status: 200,
        body: {
          results: [
            { step: {} },
            { step: {} },
            { step: {}, stepCreated: true },
          ],
          eventsDelta: { events: [], cursor: 'eid:evnt_9', hasMore: false },
        },
      },
      captured
    );

    const out = await createWorkflowRunEventsBatch(
      'wrun_1',
      [
        {
          eventType: 'step_completed',
          specVersion: 5,
          correlationId: 'step_a',
          eventData: {
            stepName: 'step-a',
            workflowName: 'wf',
            result: enc({ ok: true }),
          },
        },
        {
          eventType: 'step_created',
          specVersion: 5,
          correlationId: 'step_b',
          eventData: {
            stepName: 'step-b',
            workflowName: 'wf',
            input: enc({ n: 2 }),
          },
        },
        {
          eventType: 'step_started',
          specVersion: 5,
          correlationId: 'step_b',
          eventData: { stepName: 'step-b', workflowName: 'wf' },
        },
      ],
      { requestId: 'req_abc', sinceCursor: 'eid:evnt_0', stateUpdatedAt: 1234 },
      { token: 'test-token', dispatcher: agent }
    );

    const [completed, created, started] = captured.frames!;
    // sinceCursor + stateUpdatedAt ride on the step_completed frame ONLY (the
    // server's "primary"), never on create/start.
    expect(completed.meta.sinceCursor).toBe('eid:evnt_0');
    expect(completed.meta.stateUpdatedAt).toBe(1234);
    expect(created.meta.sinceCursor).toBeUndefined();
    expect(created.meta.stateUpdatedAt).toBeUndefined();
    expect(started.meta.sinceCursor).toBeUndefined();
    // vercelId (requestId) rides on every frame, as it would per single POST.
    expect(completed.meta.vercelId).toBe('req_abc');
    expect(created.meta.vercelId).toBe('req_abc');
    expect(started.meta.vercelId).toBe('req_abc');

    // The nested eventsDelta is surfaced as events/cursor/hasMore.
    expect(out.results).toHaveLength(3);
    expect(out.cursor).toBe('eid:evnt_9');
    expect(out.hasMore).toBe(false);
    agent.assertNoPendingInterceptors();
  });

  it('throws on an empty batch without issuing a request', async () => {
    await expect(
      createWorkflowRunEventsBatch('wrun_1', [], undefined, {
        token: 'test-token',
      })
    ).rejects.toBeInstanceOf(WorkflowWorldError);
  });

  /** Reply to the batch POST with a non-2xx status + JSON error body. */
  function interceptBatchError(agent: MockAgent, status: number) {
    agent
      .get(ORIGIN)
      .intercept({ path: '/api/v4/runs/wrun_1/events/batch', method: 'POST' })
      .reply(status, JSON.stringify({ message: 'batch failed' }), {
        headers: { 'content-type': 'application/json' },
      });
  }

  const oneCompleted = [
    {
      eventType: 'step_completed' as const,
      specVersion: 5,
      correlationId: 'step_a',
      eventData: { stepName: 'a', workflowName: 'wf', result: enc(1) },
    },
  ];

  it.each([
    [409, EntityConflictError],
    [410, RunExpiredError],
    [412, PreconditionFailedError],
    [404, WorkflowWorldError],
  ])('surfaces a %s definitively (no retry) as the mapped error', async (status, ErrorType) => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    interceptBatchError(agent, status);
    await expect(
      createWorkflowRunEventsBatch('wrun_1', oneCompleted, undefined, {
        token: 'test-token',
        dispatcher: agent,
      })
    ).rejects.toBeInstanceOf(ErrorType);
    // Single interceptor consumed → the definitive error was not retried.
    agent.assertNoPendingInterceptors();
  });

  it('carries the 404/405 status through for the runtime disable-batching gate', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    interceptBatchError(agent, 405);
    await createWorkflowRunEventsBatch('wrun_1', oneCompleted, undefined, {
      token: 'test-token',
      dispatcher: agent,
    }).then(
      () => expect.unreachable(),
      (err) => {
        expect(WorkflowWorldError.is(err)).toBe(true);
        expect((err as WorkflowWorldError).status).toBe(405);
      }
    );
    agent.assertNoPendingInterceptors();
  });

  // ---- v2 suspension-batch fence ----------------------------------------

  const fenceTriple = [
    {
      eventType: 'step_completed' as const,
      specVersion: 5,
      correlationId: 'step_a',
      eventData: { stepName: 'a', workflowName: 'wf', result: enc(1) },
    },
    {
      eventType: 'step_created' as const,
      specVersion: 5,
      correlationId: 'step_b',
      eventData: { stepName: 'b', workflowName: 'wf', input: enc(2) },
    },
    {
      eventType: 'step_started' as const,
      specVersion: 5,
      correlationId: 'step_b',
      eventData: { stepName: 'b', workflowName: 'wf' },
    },
  ];

  it('v2 fence: carries expectedRunVersion + batchId on the primary frame ONLY, and decodes the top-level runVersion', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    const captured: { frames?: ReturnType<typeof parseBatchFrames> } = {};
    interceptBatch(
      agent,
      'wrun_1',
      {
        status: 200,
        body: {
          results: [
            { step: {} },
            { step: {} },
            { step: {}, stepCreated: true },
          ],
          runVersion: 4,
        },
      },
      captured
    );

    const out = await createWorkflowRunEventsBatch(
      'wrun_1',
      fenceTriple,
      { expectedRunVersion: 3, batchId: 'bat_x' },
      { token: 'test-token', dispatcher: agent }
    );

    const [completed, created, started] = captured.frames!;
    // Fence rides on frame 0 (the primary) only — the server reads frameMetas[0].
    expect(completed.meta.expectedRunVersion).toBe(3);
    expect(completed.meta.batchId).toBe('bat_x');
    expect(created.meta.expectedRunVersion).toBeUndefined();
    expect(created.meta.batchId).toBeUndefined();
    expect(started.meta.expectedRunVersion).toBeUndefined();
    expect(started.meta.batchId).toBeUndefined();
    // The server's post-batch runVersion is surfaced as the next fence value.
    expect(out.runVersion).toBe(4);
    agent.assertNoPendingInterceptors();
  });

  it('v2 fence: a 409 run-not-versioned surfaces as a plain WorkflowWorldError with the code (NOT EntityConflictError) for the permanent per-run latch', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    agent
      .get(ORIGIN)
      .intercept({ path: '/api/v4/runs/wrun_1/events/batch', method: 'POST' })
      .reply(
        409,
        JSON.stringify({
          message: 'run not versioned',
          code: 'run-not-versioned',
        }),
        { headers: { 'content-type': 'application/json' } }
      );
    await createWorkflowRunEventsBatch(
      'wrun_1',
      oneCompleted,
      { expectedRunVersion: 0, batchId: 'bat_y' },
      { token: 'test-token', dispatcher: agent }
    ).then(
      () => expect.unreachable(),
      (err) => {
        // Distinguishable from the transient suspension-batch-conflict (also
        // 409 → EntityConflictError): plain WorkflowWorldError carrying the code.
        expect(err).toBeInstanceOf(WorkflowWorldError);
        expect(err).not.toBeInstanceOf(EntityConflictError);
        expect((err as WorkflowWorldError).code).toBe('run-not-versioned');
        expect((err as WorkflowWorldError).status).toBe(409);
      }
    );
    agent.assertNoPendingInterceptors();
  });

  it('v2 fence: a 409 suspension-batch-conflict stays an EntityConflictError (transient abandon+reinvoke)', async () => {
    const agent = new MockAgent();
    agent.disableNetConnect();
    agent
      .get(ORIGIN)
      .intercept({ path: '/api/v4/runs/wrun_1/events/batch', method: 'POST' })
      .reply(
        409,
        JSON.stringify({
          message: 'run-version conflict',
          code: 'suspension-batch-conflict',
        }),
        { headers: { 'content-type': 'application/json' } }
      );
    await expect(
      createWorkflowRunEventsBatch(
        'wrun_1',
        oneCompleted,
        { expectedRunVersion: 1, batchId: 'bat_z' },
        { token: 'test-token', dispatcher: agent }
      )
    ).rejects.toBeInstanceOf(EntityConflictError);
    agent.assertNoPendingInterceptors();
  });
});
