import { createContext, runInContext } from 'node:vm';
import { describe, expect, it, vi } from 'vitest';
import { dehydrateStepArguments } from '../serialization.js';
import { stringify } from '../vendor/devalue/index.js';
import {
  hardenedStringify,
  registerRealmSerializationIntrinsics,
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

    it('reads proxy properties through the trap, matching stock bytes', () => {
      const proxy = new Proxy(
        { a: 1 },
        { get: (target, key) => (key === 'a' ? 2 : Reflect.get(target, key)) }
      );
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      const output = hardenedStringify({ p: proxy }, reducers, report);
      expect(output).toBe(stringify({ p: proxy }, reducers));
      expect(output).toBe(stringify({ p: { a: 2 } }, reducers));
      expect(report.tainted).toBe(true);
    });

    it('taints a tag-spoofed Array-like whose length coerces via valueOf', () => {
      let coerced = 0;
      const arrayLike = {
        [Symbol.toStringTag]: 'Array',
        length: {
          valueOf() {
            coerced++;
            return 0;
          },
        },
      };
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      const output = hardenedStringify(arrayLike, reducers, report);
      expect(output).toBe(stringify(arrayLike, reducers));
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('Array tag without Array brand');
      expect(coerced).toBeGreaterThan(0);
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

    it('taints on a plain-looking object with a Proxy prototype', () => {
      // A Proxy around Object.prototype passes devalue's is_plain_object
      // check (through its traps), so stock devalue serializes the object
      // as plain while the traps run. The hardened objectShape must taint.
      let trapped = 0;
      const proto = new Proxy(Object.prototype, {
        getPrototypeOf(target) {
          trapped++;
          return Object.getPrototypeOf(target);
        },
      });
      const value = Object.assign(Object.create(proto), { a: 1 });
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      const output = hardenedStringify({ wrapper: value }, reducers, report);
      expect(output).toBe(stringify({ wrapper: value }, reducers));
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('proxy in prototype chain');
      expect(trapped).toBeGreaterThan(0);
    });

    it('taints on an Error with a Proxy in its prototype chain', () => {
      const error = new TypeError('t');
      Object.setPrototypeOf(
        error,
        new Proxy(TypeError.prototype, {
          has(target, key) {
            return Reflect.has(target, key);
          },
        })
      );
      // Give the error own data props so reducers read it without accessors.
      Object.defineProperties(error, {
        name: { value: 'TypeError', enumerable: false },
        message: { value: 't', enumerable: false },
        stack: { value: 'fixed', enumerable: false, writable: true },
      });
      const { report } = run(error);
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('proxy in prototype chain');
    });

    it('taints a tag-spoofed Map-like instead of throwing, preserving stock bytes', () => {
      const mapLike = {
        [Symbol.toStringTag]: 'Map',
        *[Symbol.iterator](): Generator<[unknown, unknown]> {
          yield ['k', 1];
        },
      };
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      const output = hardenedStringify(mapLike, reducers, report);
      expect(output).toBe(stringify(mapLike, reducers));
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('Map tag without Map brand');
    });

    it('taints a tag-spoofed Date-like instead of throwing, preserving stock bytes', () => {
      const dateLike = {
        [Symbol.toStringTag]: 'Date',
        getDate: () => 1,
        toISOString: () => '2024-01-01T00:00:00.000Z',
      };
      const reducers = getCommonReducers() as Record<
        string,
        (value: any) => any
      >;
      const report = freshReport();
      const output = hardenedStringify(dateLike, reducers, report);
      expect(output).toBe(stringify(dateLike, reducers));
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('Date tag without Date brand');
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

    it('derives URLSearchParams emptiness natively, ignoring a toString override', () => {
      const params = new URLSearchParams();
      let called = 0;
      Object.defineProperty(params, 'toString', {
        value: () => {
          called++;
          return 'injected=1';
        },
      });
      const { output, report } = run(params);
      // Serializes as empty (the sentinel), like the pre-hardening
      // `size === 0` probe — without ever invoking the override.
      expect(output).toBe(run(new URLSearchParams()).output);
      expect(called).toBe(0);
      expect(report.tainted).toBe(false);
    });

    it('taints and keeps dynamic bytes for nonempty URLSearchParams with overridden toString', () => {
      const params = new URLSearchParams('a=1');
      Object.defineProperty(params, 'toString', { value: () => 'x=y' });
      const { output, report } = run(params);
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('URLSearchParams toString dispatch');
      expect(output).toBe(run(new URLSearchParams('x=y')).output);
    });

    it('invokes a URLSearchParams toString conversion getter exactly once', () => {
      // The probe that selects the native fast path must not perform reads:
      // a stateful conversion getter has to fire exactly once, from the
      // single `String(value)` dispatch, like it did pre-hardening.
      const params = new URLSearchParams('a=1');
      let reads = 0;
      Object.defineProperty(params, 'toString', {
        configurable: true,
        get() {
          reads++;
          return () => 'x=y';
        },
      });
      const { output, report } = run(params);
      expect(reads).toBe(1);
      expect(report.tainted).toBe(true);
      expect(output).toBe(run(new URLSearchParams('x=y')).output);
    });

    it('serializes RegExp flags without dispatching an own flag getter', () => {
      const value = /ab+c/gi;
      let called = false;
      Object.defineProperty(value, 'global', {
        get() {
          called = true;
          return false; // Lie — internal-slot reads must not see this.
        },
      });
      const { report, output } = run(value);
      expect(called).toBe(false);
      expect(report.tainted).toBe(false);
      expect(output).toBe(
        stringify(/ab+c/gi, getCommonReducers() as Record<string, any>)
      );
    });
  });

  describe('prototypes patched before module load (pristine-realm captures)', () => {
    // Module-load captures from the host realm cannot be trusted either:
    // application code (a polyfill, an instrumentation shim) may patch a
    // prototype before @workflow/core is imported, and the patch would then
    // be invoked as "passive" forever. The ECMAScript intrinsics therefore
    // come from a freshly created VM realm — simulated here by patching
    // first and only then importing a fresh copy of the module.
    it('does not dispatch a Map.prototype.entries patched before import', async () => {
      const original = Map.prototype.entries;
      const value = () => new Map<unknown, unknown>([['k', 1]]);
      const expected = stringify(value());
      let called = 0;
      Map.prototype.entries = function (this: Map<unknown, unknown>) {
        called++;
        return original.call(this);
      } as typeof Map.prototype.entries;
      try {
        vi.resetModules();
        const fresh = await import('./operations.js');
        const report = freshReport();
        expect(fresh.hardenedStringify(value(), {}, report)).toBe(expected);
        expect(called).toBe(0);
        expect(report.tainted).toBe(false);
      } finally {
        Map.prototype.entries = original;
        vi.resetModules();
      }
    });

    it('does not dispatch a Date.prototype.toISOString patched before import', async () => {
      const original = Date.prototype.toISOString;
      const expected = stringify(new Date(1700000000000));
      let called = 0;
      Date.prototype.toISOString = function (this: Date) {
        called++;
        return original.call(this);
      };
      try {
        vi.resetModules();
        const fresh = await import('./operations.js');
        const report = freshReport();
        expect(
          fresh.hardenedStringify(new Date(1700000000000), {}, report)
        ).toBe(expected);
        expect(called).toBe(0);
        expect(report.tainted).toBe(false);
      } finally {
        Date.prototype.toISOString = original;
        vi.resetModules();
      }
    });
  });

  describe('error stack reads', () => {
    it('taints when Error.prepareStackTrace was replaced', () => {
      const original = Object.getOwnPropertyDescriptor(
        Error,
        'prepareStackTrace'
      );
      let invoked = false;
      Error.prepareStackTrace = (_error, _trace) => {
        invoked = true;
        return 'formatted';
      };
      try {
        // A fresh error's `stack` is still the engine's lazy accessor;
        // reading it would execute the replaced formatter.
        const { report } = run(new Error('lazy'));
        expect(report.tainted).toBe(true);
        expect(report.reasons).toContain('Error.prepareStackTrace');
        expect(invoked).toBe(true);
      } finally {
        if (original) {
          Object.defineProperty(Error, 'prepareStackTrace', original);
        } else {
          (Error as { prepareStackTrace?: unknown }).prepareStackTrace =
            undefined;
        }
      }
    });

    it('allows a registered realm stack getter and taints an unregistered one', () => {
      const makeRealmError = () => {
        const context = createContext();
        const realmGlobal = runInContext('globalThis', context) as object;
        const error = runInContext('new Error("realm")', context) as Error;
        return { realmGlobal, error };
      };

      const unregistered = makeRealmError();
      {
        const report = freshReport();
        hardenedStringify(
          unregistered.error,
          getCommonReducers(
            unregistered.realmGlobal as typeof globalThis
          ) as Record<string, any>,
          report
        );
        expect(report.tainted).toBe(true);
        expect(report.reasons).toContain('stack accessor');
      }

      const registered = makeRealmError();
      registerRealmSerializationIntrinsics(registered.realmGlobal);
      {
        const report = freshReport();
        hardenedStringify(
          registered.error,
          getCommonReducers(
            registered.realmGlobal as typeof globalThis
          ) as Record<string, any>,
          report
        );
        expect(report.tainted).toBe(false);
      }
    });

    it('taints when a registered realm planted an inherited prepareStackTrace', () => {
      // The engine resolves Error.prepareStackTrace with an ordinary Get on
      // the realm's Error constructor, so a formatter inherited from the
      // realm's Function.prototype is honored too and must read as
      // "replaced".
      const context = createContext();
      const realmGlobal = runInContext('globalThis', context) as object;
      registerRealmSerializationIntrinsics(realmGlobal);
      const error = runInContext(
        'Object.getPrototypeOf(Error).prepareStackTrace = () => "inherited";' +
          'new Error("realm")',
        context
      ) as Error;
      const report = freshReport();
      hardenedStringify(
        error,
        getCommonReducers(realmGlobal as typeof globalThis) as Record<
          string,
          any
        >,
        report
      );
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('Error.prepareStackTrace');
    });

    it('taints when a registered realm replaced its prepareStackTrace', () => {
      const context = createContext();
      const realmGlobal = runInContext('globalThis', context) as object;
      registerRealmSerializationIntrinsics(realmGlobal);
      const error = runInContext(
        'Error.prepareStackTrace = () => "patched"; new Error("realm")',
        context
      ) as Error;
      const report = freshReport();
      hardenedStringify(
        error,
        getCommonReducers(realmGlobal as typeof globalThis) as Record<
          string,
          any
        >,
        report
      );
      expect(report.tainted).toBe(true);
      expect(report.reasons).toContain('Error.prepareStackTrace');
    });
  });

  describe('reducer construction scope', () => {
    it('taints when reducer construction hits a getter on the workflow global', async () => {
      const global: Record<string, any> = Object.create(globalThis);
      let invoked = false;
      Object.defineProperty(global, 'Request', {
        get() {
          invoked = true;
          return Request;
        },
      });
      const report = freshReport();
      await dehydrateStepArguments(
        { plain: 1 },
        'run_test',
        undefined,
        global,
        false,
        false,
        report
      );
      expect(invoked).toBe(true);
      expect(report.tainted).toBe(true);
    });
  });
});
