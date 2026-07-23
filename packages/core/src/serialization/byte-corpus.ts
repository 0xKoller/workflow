/**
 * Shared corpus for the serialization byte-stability test.
 *
 * Each entry builds a value whose serialized devalue string is pinned in
 * `byte-corpus.snapshot.json`. The snapshot was recorded from the
 * serialization code as it existed BEFORE the hardened stringify operations
 * were introduced (see operations.ts), so the accompanying test proves the
 * hardening did not change a single byte of the wire format.
 *
 * Regenerating the snapshot is an explicit act: it means "I intend to change
 * the wire format". See byte-stability.test.ts for how.
 */

import { WORKFLOW_SERIALIZE } from '@workflow/serde';
import { FatalError, RetryableError } from '@workflow/errors';

/** An error with a deterministic stack, safe to pin in a snapshot. */
function fixedError<T extends Error>(error: T, stack: string): T {
  error.stack = stack;
  return error;
}

export class CorpusSerializableThing {
  static classId = 'corpus/Thing';
  static [WORKFLOW_SERIALIZE](instance: CorpusSerializableThing) {
    return { label: instance.label };
  }
  constructor(public label: string) {}
}

export function buildByteCorpus(): Array<{ name: string; value: unknown }> {
  const shared = { shared: true };
  const cyclic: Record<string, unknown> = { name: 'cycle' };
  cyclic.self = cyclic;

  const buffer = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]).buffer;

  const causeChain = fixedError(
    new Error('outer', {
      cause: fixedError(
        new TypeError('inner'),
        'TypeError: inner\n    at <corpus>'
      ),
    }),
    'Error: outer\n    at <corpus>'
  );

  const retryable = fixedError(
    new RetryableError('try again', {
      retryAfter: new Date(1_700_000_060_000),
    }),
    'RetryableError: try again\n    at <corpus>'
  );

  const aggregate = fixedError(
    new AggregateError(
      [fixedError(new Error('a'), 'Error: a\n    at <corpus>')],
      'many'
    ),
    'AggregateError: many\n    at <corpus>'
  );

  return [
    // ---- primitives & special numbers ----
    { name: 'primitives', value: [null, true, false, 'text', 42, -1.5] },
    { name: 'special numbers', value: [Number.NaN, Infinity, -Infinity, -0] },
    { name: 'undefined', value: undefined },
    { name: 'bigint', value: { big: 42n, negative: -7n } },
    {
      name: 'string escapes',
      value: ['<script>', 'line\nbreak', 'tab\there', 'u2028\u2028u2029\u2029'],
    },
    // ---- plain structures ----
    {
      name: 'nested plain object',
      value: { a: 1, nested: { b: [2, 3], c: { deep: 'yes' } } },
    },
    {
      name: 'null prototype object',
      value: Object.assign(Object.create(null), { x: 1 }),
    },
    // biome-ignore lint/suspicious/noSparseArray: sparse encoding is part of the wire format
    { name: 'sparse array', value: [1, , 3] },
    {
      name: 'very sparse array',
      value: (() => {
        const arr: number[] = [];
        arr[0] = 1;
        arr[99] = 2;
        return arr;
      })(),
    },
    { name: 'repeated references', value: { first: shared, second: shared } },
    { name: 'cyclic object', value: cyclic },
    // ---- built-in value types ----
    { name: 'date', value: new Date(1_700_000_000_000) },
    { name: 'invalid date', value: new Date(Number.NaN) },
    {
      name: 'map',
      value: new Map<unknown, unknown>([
        ['k', { v: 1 }],
        [2, 'two'],
      ]),
    },
    { name: 'set', value: new Set([1, 'two', { three: 3 }]) },
    { name: 'nested map in set', value: new Set([new Map([['inner', 1]])]) },
    { name: 'regexp', value: /ab+c/gi },
    { name: 'regexp no flags', value: /plain/ },
    { name: 'url', value: new URL('https://example.com/path?q=1#frag') },
    { name: 'url search params', value: new URLSearchParams('a=1&b=2') },
    { name: 'empty url search params', value: new URLSearchParams() },
    { name: 'headers', value: new Headers([['content-type', 'text/plain']]) },
    // ---- binary ----
    { name: 'array buffer', value: buffer },
    { name: 'empty array buffer', value: new ArrayBuffer(0) },
    { name: 'uint8 array', value: new Uint8Array([1, 2, 3]) },
    { name: 'uint8 subarray', value: new Uint8Array(buffer, 2, 4) },
    { name: 'int16 array', value: new Int16Array([-1, 2, -3]) },
    { name: 'float32 array', value: new Float32Array([1.5, -2.5]) },
    { name: 'float64 array', value: new Float64Array([Math.PI]) },
    { name: 'bigint64 array', value: new BigInt64Array([1n, -2n]) },
    { name: 'uint8 clamped array', value: new Uint8ClampedArray([0, 255]) },
    {
      name: 'data view',
      value: new DataView(new Uint8Array([9, 8, 7]).buffer),
    },
    { name: 'data view subview', value: new DataView(buffer, 1, 4) },
    { name: 'node buffer', value: Buffer.from([104, 105]) },
    // ---- boxed primitives (devalue-internal handling) ----
    { name: 'boxed number', value: new Number(42) },
    { name: 'boxed string', value: new String('boxed') },
    { name: 'boxed boolean', value: new Boolean(false) },
    { name: 'boxed bigint', value: Object(123n) },
    // ---- errors ----
    {
      name: 'plain error',
      value: fixedError(new Error('boom'), 'Error: boom\n    at <corpus>'),
    },
    {
      name: 'error subclasses',
      value: [
        fixedError(new TypeError('t'), 'TypeError: t\n    at <corpus>'),
        fixedError(new RangeError('r'), 'RangeError: r\n    at <corpus>'),
        fixedError(new SyntaxError('s'), 'SyntaxError: s\n    at <corpus>'),
      ],
    },
    { name: 'error with cause chain', value: causeChain },
    {
      name: 'error with undefined cause',
      value: fixedError(
        new Error('has undefined cause', { cause: undefined }),
        'Error: has undefined cause\n    at <corpus>'
      ),
    },
    {
      name: 'custom named error',
      value: fixedError(
        Object.assign(new Error('custom'), { name: 'MyCustomError' }),
        'MyCustomError: custom\n    at <corpus>'
      ),
    },
    {
      name: 'fatal error',
      value: fixedError(
        new FatalError('fatal'),
        'FatalError: fatal\n    at <corpus>'
      ),
    },
    { name: 'retryable error', value: retryable },
    { name: 'aggregate error', value: aggregate },
    // ---- workflow-specific shapes ----
    {
      name: 'workflow function reference',
      value: Object.assign(() => {}, { workflowId: 'workflow//corpus' }),
    },
    {
      name: 'class reference',
      value: Object.assign(function CorpusClass() {}, {
        classId: 'corpus/Class',
      }),
    },
    {
      name: 'custom serialized instance',
      value: new CorpusSerializableThing('hi'),
    },
    // ---- values that execute user code during serialization ----
    // These serialize identically before and after the hardening; the
    // hardening additionally *reports* them (see the taint tests).
    {
      name: 'object with getter',
      value: {
        plain: 1,
        get computed() {
          return 'from getter';
        },
      },
    },
    {
      name: 'proxy over plain object',
      value: new Proxy({ wrapped: true, n: 2 }, {}),
    },
    // ---- step-argument envelope shape (what suspension serializes) ----
    {
      name: 'step argument envelope',
      value: {
        args: [{ userId: 'u_1', when: new Date(1_700_000_000_000) }, 3],
        closureVars: undefined,
        thisVal: undefined,
      },
    },
  ];
}

/**
 * Inputs `stringify` must reject — the thrown message is part of the
 * user-visible contract and is pinned alongside the byte corpus.
 */
export function buildThrowingCorpus(): Array<{ name: string; value: unknown }> {
  return [
    { name: 'bare function', value: () => {} },
    { name: 'symbol', value: Symbol('nope') },
    { name: 'pojo with symbol keys', value: { [Symbol('k')]: 1 } },
    {
      name: 'class instance without serializer',
      value: new (class Widget {})(),
    },
    { name: 'promise (sync stringify)', value: Promise.resolve(1) },
    {
      name: 'proto key',
      value: JSON.parse('{"__proto__": {"polluted": true}}'),
    },
  ];
}
