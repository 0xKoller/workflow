import { stringify } from 'devalue';
import { describe, expect, it } from 'vitest';
import {
  hardenedStringify,
  type SerializationPassivityReport,
} from './operations.js';
import { getCommonReducers } from './reducers/common.js';

function freshReport(): SerializationPassivityReport {
  return { tainted: false, reasons: [] };
}

/** Stringify with the workflow-relevant reducers and a fresh report. */
function run(value: unknown): {
  output: string;
  report: SerializationPassivityReport;
} {
  const report = freshReport();
  const reducers = getCommonReducers() as Record<string, (value: any) => any>;
  const output = hardenedStringify(value, reducers, report);
  return { output, report };
}

describe('hardenedStringify passivity', () => {
  describe('admitted (untainted) values', () => {
    it.each([
      ['primitives', [1, 'two', true, null, undefined, 3n]],
      ['plain objects and arrays', { a: [1, { b: 2 }], c: 'x' }],
      ['sparse array', (() => Object.assign([], { 5: 'x' }))()],
      ['null-proto object', Object.assign(Object.create(null), { a: 1 })],
      ['Date', new Date(1700000000000)],
      ['invalid Date', new Date(Number.NaN)],
      [
        'Map',
        new Map<unknown, unknown>([
          ['k', 1],
          [{ o: 1 }, [2]],
        ]),
      ],
      ['Set', new Set([1, 'a', { b: 2 }])],
      ['RegExp', /ab+c/gi],
      ['typed arrays', new Uint8Array([1, 2, 3])],
      ['subarray view', new Uint8Array([1, 2, 3, 4]).subarray(1, 3)],
      ['DataView', new DataView(new ArrayBuffer(8))],
      ['ArrayBuffer', new Uint8Array([9, 8]).buffer],
      ['boxed primitives', [new Number(3), new String('s'), new Boolean(true)]],
      ['Error', new Error('boom')],
      ['TypeError with cause', new TypeError('t', { cause: 'c' })],
      ['AggregateError', new AggregateError([new Error('a')], 'agg')],
      ['URL', new URL('https://example.com/x?y=1')],
      ['URLSearchParams', new URLSearchParams('a=1&b=2')],
      ['empty URLSearchParams', new URLSearchParams()],
      ['Headers', new Headers({ 'x-a': '1' })],
      ['DOMException', new DOMException('msg', 'DataError')],
      [
        'circular reference',
        (() => {
          const o: any = { a: 1 };
          o.self = o;
          return o;
        })(),
      ],
    ])('%s serialize without taint', (_name, value) => {
      const { report } = run(value);
      expect(report.tainted).toBe(false);
      expect(report.reasons).toEqual([]);
    });

    it('produces the same bytes as plain devalue for well-behaved values', () => {
      const value = {
        n: 1,
        s: 'str',
        d: new Date(1700000000000),
        m: new Map([['k', [1, 2]]]),
        set: new Set(['a']),
        re: /x/g,
        u8: new Uint8Array([1, 2, 3]),
        url: new URL('https://example.com/'),
        err: Object.assign(new Error('e'), { stack: 'fixed' }),
        nested: { deep: [{ deeper: true }] },
      };
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      expect(hardenedStringify(value, reducers, report)).toBe(
        stringify(value, reducers)
      );
      expect(report.tainted).toBe(false);
    });
  });

  describe('tainting values', () => {
    it('taints on an own getter (and still reads it)', () => {
      const value: Record<string, unknown> = {};
      let invoked = 0;
      Object.defineProperty(value, 'x', {
        enumerable: true,
        get() {
          invoked++;
          return 42;
        },
      });
      const { output, report } = run(value);
      expect(report.tainted).toBe(true);
      expect(report.reasons[0]).toContain('getter for "x"');
      expect(invoked).toBe(1);
      expect(output).toBe(stringify({ x: 42 }));
    });

    it('taints on a getter nested deep inside a plain structure', () => {
      const inner: Record<string, unknown> = {};
      Object.defineProperty(inner, 'lazy', {
        enumerable: true,
        get: () => 'v',
      });
      const { report } = run({ a: [1, { b: inner }] });
      expect(report.tainted).toBe(true);
    });

    it('taints on a proxy', () => {
      const { report } = run({ p: new Proxy({ a: 1 }, {}) });
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('proxy');
    });

    it('taints on an array getter element', () => {
      const arr: unknown[] = [1];
      Object.defineProperty(arr, 1, { enumerable: true, get: () => 2 });
      const { report, output } = run(arr);
      expect(report.tainted).toBe(true);
      expect(output).toBe(stringify([1, 2]));
    });

    it('taints on a thenable probe hitting a getter', () => {
      const value: Record<string, unknown> = { a: 1 };
      Object.defineProperty(value, 'then', {
        enumerable: false,
        get: () => undefined,
      });
      const { report } = run(value);
      expect(report.tainted).toBe(true);
    });

    it('taints on an error with a message getter', () => {
      const error = new Error('base');
      Object.defineProperty(error, 'message', { get: () => 'dynamic' });
      const { report } = run(error);
      expect(report.tainted).toBe(true);
    });

    it('caps and dedupes taint reasons', () => {
      const mk = (key: string) => {
        const o: Record<string, unknown> = {};
        Object.defineProperty(o, key, { enumerable: true, get: () => 1 });
        return o;
      };
      const { report } = run([
        mk('a'),
        mk('a'),
        mk('b'),
        mk('c'),
        mk('d'),
        mk('e'),
        mk('f'),
        mk('g'),
      ]);
      expect(report.tainted).toBe(true);
      expect(report.reasons.length).toBeLessThanOrEqual(5);
      expect(new Set(report.reasons).size).toBe(report.reasons.length);
    });
  });

  describe('report scoping', () => {
    it('does not record anything without a report', () => {
      const value: Record<string, unknown> = {};
      Object.defineProperty(value, 'x', { enumerable: true, get: () => 1 });
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      // Must not throw and must serialize identically.
      expect(hardenedStringify(value, reducers)).toBe(stringify({ x: 1 }));
    });

    it('restores the outer report after a nested stringify', () => {
      const outer = freshReport();
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const value: Record<string, unknown> = {};
      Object.defineProperty(value, 'x', {
        enumerable: true,
        get() {
          // Nested serialization with its own report must not leak.
          const inner = freshReport();
          hardenedStringify({ plain: true }, reducers, inner);
          expect(inner.tainted).toBe(false);
          return 1;
        },
      });
      hardenedStringify(value, reducers, outer);
      expect(outer.tainted).toBe(true);
    });
  });

  describe('patched prototypes (host hardening)', () => {
    it('serializes a Map without dispatching patched Map iteration', () => {
      const entries = Map.prototype.entries;
      let called = false;
      // biome-ignore lint/suspicious/noGlobalAssign: intentional for the test
      Map.prototype.entries = function (this: Map<unknown, unknown>) {
        called = true;
        return entries.call(this);
      } as typeof Map.prototype.entries;
      try {
        const { report, output } = run(new Map([['k', 1]]));
        expect(called).toBe(false);
        expect(report.tainted).toBe(false);
        expect(output).toBe(
          stringify(new Map([['k', 1]]), getCommonReducers() as any)
        );
      } finally {
        Map.prototype.entries = entries;
      }
    });

    it('serializes a Date without dispatching a patched toISOString', () => {
      const toISOString = Date.prototype.toISOString;
      let called = false;
      Date.prototype.toISOString = function (this: Date) {
        called = true;
        return toISOString.call(this);
      };
      try {
        const { report } = run(new Date(1700000000000));
        expect(called).toBe(false);
        expect(report.tainted).toBe(false);
      } finally {
        Date.prototype.toISOString = toISOString;
      }
    });
  });
});
