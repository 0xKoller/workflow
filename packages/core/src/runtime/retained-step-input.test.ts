import vm from 'node:vm';
import { describe, expect, it } from 'vitest';
import { dehydrateStepArguments } from '../serialization.js';
import { createContext, freezeSerializationIntrinsics } from '../vm/index.js';
import {
  isRetainedSerializationPassive,
  registerSerializationPins,
} from './retained-step-input.js';

const seed = 'retained-step-input';
const fixedTimestamp = 1_700_000_000_000;

function makeContext({ freeze = true, register = freeze } = {}) {
  const { context, globalThis: workflowGlobal } = createContext({
    seed,
    fixedTimestamp,
  });
  // Mimic the globals workflow.ts installs before any serialization happens
  // (the stream/request reducers dispatch on them unguarded).
  for (const name of [
    'ReadableStream',
    'WritableStream',
    'TransformStream',
    'Request',
    'Response',
    'AbortController',
    'AbortSignal',
  ]) {
    if ((workflowGlobal as any)[name] === undefined) {
      (workflowGlobal as any)[name] = (globalThis as any)[name];
    }
  }
  if (freeze) freezeSerializationIntrinsics(workflowGlobal);
  if (register) registerSerializationPins(workflowGlobal);
  return { context, workflowGlobal };
}

describe('isRetainedSerializationPassive', () => {
  it('accepts plain cross-realm data', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext(
      `({
        nested: [{ ok: true }, "text", 42n],
        sparse: [1, , 3],
        flag: false,
      })`,
      context
    );

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(true);
  });

  it('accepts the supported built-ins on a registered realm', () => {
    const { context, workflowGlobal } = makeContext();
    for (const expression of [
      'new Map([["k", { ok: true }]])',
      'new Set([1, "two"])',
      'new Date(1234)',
      'new Uint8Array([1, 2, 3])',
      'new Float32Array([1.5])',
      'new ArrayBuffer(8)',
      '({ when: new Date(0), bytes: new Uint8Array(2), index: new Map() })',
    ]) {
      const value = vm.runInContext(expression, context);
      expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(true);
    }
  });

  it('accepts host-realm plain data but not host-realm built-ins', () => {
    const { workflowGlobal } = makeContext();
    // Hydrated step results are host-realm plain objects/arrays.
    expect(
      isRetainedSerializationPassive({ nested: [{ ok: true }] }, workflowGlobal)
    ).toBe(true);
    // Host built-in prototypes cannot be frozen (process-shared) and are
    // reachable from workflow code, so host-realm instances decline.
    expect(
      isRetainedSerializationPassive(new Map([['k', 1]]), workflowGlobal)
    ).toBe(false);
    expect(isRetainedSerializationPassive(new Date(0), workflowGlobal)).toBe(
      false
    );
  });

  it('declines types whose serialization surface is not pinned', () => {
    const { context, workflowGlobal } = makeContext();
    for (const expression of [
      '/workflow/gi',
      'new DataView(new ArrayBuffer(8))',
      'new SharedArrayBuffer(8)',
      'new Uint8Array(new SharedArrayBuffer(4))',
      'new Error("boom")',
      'new (class Sub extends Map {})()',
      'Object.assign(new Map(), { expando: 1 })',
      'Object.create(null)',
    ]) {
      const value = vm.runInContext(expression, context);
      expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    }
  });

  it('declines accessors without invoking them', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext(
      `(() => {
        globalThis.__retainedTestCalls = 0;
        return { get value() { globalThis.__retainedTestCalls++; return 1; } };
      })()`,
      context
    );

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    expect(vm.runInContext('globalThis.__retainedTestCalls', context)).toBe(0);
  });

  it('declines proxies without invoking their traps', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext(
      `(() => {
        globalThis.__retainedTestCalls = 0;
        return new Proxy({ value: 1 }, {
          ownKeys(target) {
            globalThis.__retainedTestCalls++;
            return Reflect.ownKeys(target);
          }
        });
      })()`,
      context
    );

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    expect(vm.runInContext('globalThis.__retainedTestCalls', context)).toBe(0);
  });

  it('declines custom class serializers without invoking them', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext(
      `(() => {
        globalThis.__retainedTestCalls = 0;
        class Value {
          static classId = "test/Value";
          static [Symbol.for("workflow-serialize")](instance) {
            globalThis.__retainedTestCalls++;
            return { value: instance.value };
          }
          constructor(value) { this.value = value; }
        }
        return new Value(1);
      })()`,
      context
    );

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    expect(vm.runInContext('globalThis.__retainedTestCalls', context)).toBe(0);
  });

  it('declines hidden own keys (symbols, non-enumerables, constructor)', () => {
    const { context, workflowGlobal } = makeContext();
    for (const expression of [
      `(() => {
        const tagged = { plain: true };
        Object.defineProperty(tagged, Symbol.for("WORKFLOW_ABORT_STREAM_NAME"), {
          value: "abort-stream", enumerable: false,
        });
        return tagged;
      })()`,
      `(() => {
        const hidden = { plain: true };
        Object.defineProperty(hidden, "signal", {
          get() { return { aborted: false }; }, enumerable: false,
        });
        return hidden;
      })()`,
      `(() => {
        const arr = [1];
        Object.defineProperty(arr, "constructor", {
          value: class Fake { static classId = "fake"; }, enumerable: false,
        });
        return arr;
      })()`,
      `(() => {
        const arr = [1, 2];
        Object.defineProperty(arr, "0", { value: 7, enumerable: false });
        return arr;
      })()`,
    ]) {
      const value = vm.runInContext(expression, context);
      expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    }
  });

  it('allows polyfilled data members on built-in prototypes and statics', () => {
    // The Temporal / core-js pattern: new methods on value-type prototypes
    // and new constructor statics. Serialization never reads them, so
    // retention is unaffected.
    const { context, workflowGlobal } = makeContext();
    vm.runInContext(
      `
        Date.prototype.toTemporalInstant = function () { return "instant"; };
        Set.prototype.union = function (other) { return new Set([...this, ...other]); };
        Map.groupBy = function () { return new Map(); };
        Object.groupBy = function () { return {}; };
      `,
      context
    );
    for (const expression of [
      'new Date(1234)',
      'new Set([1, 2])',
      'new Map([["k", 1]])',
      '({ ok: true })',
    ]) {
      const value = vm.runInContext(expression, context);
      expect(
        isRetainedSerializationPassive(value, workflowGlobal),
        expression
      ).toBe(true);
    }
  });

  it('declines only the types whose executed members were replaced', () => {
    const { context, workflowGlobal } = makeContext();
    vm.runInContext(
      'Date.prototype.toISOString = function () { return "x"; };',
      context
    );
    // A Date argument would execute the replaced method while serializing.
    const date = vm.runInContext('new Date(1234)', context);
    expect(isRetainedSerializationPassive(date, workflowGlobal)).toBe(false);
    // Every other type keeps its verified surface — the user's workaround of
    // passing `date.toISOString()` (a string) instead still retains.
    for (const expression of ['new Map([["k", 1]])', '"2024-01-01"']) {
      const value = vm.runInContext(expression, context);
      expect(
        isRetainedSerializationPassive(value, workflowGlobal),
        expression
      ).toBe(true);
    }
  });

  it('declines per replaced surface: iterators, getters, constructor back-refs', () => {
    for (const [patch, expression] of [
      ['Map.prototype[Symbol.iterator] = function () {};', 'new Map()'],
      [
        `const proto = Object.getPrototypeOf(new Set()[Symbol.iterator]());
         proto.next = function () { return { done: true }; };`,
        'new Set([1])',
      ],
      [
        `Object.defineProperty(Object.getPrototypeOf(Uint8Array.prototype),
           "buffer", { get() { return new ArrayBuffer(0); } });`,
        'new Uint8Array([1])',
      ],
      [
        'Object.defineProperty(Map.prototype, "constructor", { value: class Fake {} });',
        'new Map()',
      ],
      ['Object.setPrototypeOf(Date.prototype, { evil: true });', 'new Date(0)'],
    ] as const) {
      const { context, workflowGlobal } = makeContext();
      vm.runInContext(patch, context);
      const value = vm.runInContext(expression, context);
      expect(isRetainedSerializationPassive(value, workflowGlobal), patch).toBe(
        false
      );
    }
  });

  it('declines new accessors on pinned prototypes without invoking them', () => {
    const { context, workflowGlobal } = makeContext();
    const map = vm.runInContext(
      `(() => {
        globalThis.__retainedTestCalls = 0;
        Object.defineProperty(Map.prototype, "then", {
          get() { globalThis.__retainedTestCalls++; return undefined; },
          configurable: true,
        });
        return new Map([["k", 1]]);
      })()`,
      context
    );
    expect(isRetainedSerializationPassive(map, workflowGlobal)).toBe(false);
    expect(vm.runInContext('globalThis.__retainedTestCalls', context)).toBe(0);
  });

  it('declines shadowed typed-array members on subclass prototypes', () => {
    const { context, workflowGlobal } = makeContext();
    vm.runInContext(
      'Object.defineProperty(Uint8Array.prototype, "buffer", { value: new ArrayBuffer(0) });',
      context
    );
    const u8 = vm.runInContext('new Uint8Array([1, 2])', context);
    expect(isRetainedSerializationPassive(u8, workflowGlobal)).toBe(false);
    // Sibling typed arrays read through the unshadowed %TypedArray% getters.
    const f32 = vm.runInContext('new Float32Array([1.5])', context);
    expect(isRetainedSerializationPassive(f32, workflowGlobal)).toBe(true);
  });

  it('declines everything while a realm dispatch constructor is hooked', () => {
    for (const patch of [
      'Object.defineProperty(Set, Symbol.hasInstance, { value: () => false });',
      'Object.setPrototypeOf(Date, { [Symbol.hasInstance]: () => false });',
      'Object[Symbol.for("workflow-serialize")] = function () { return {}; };',
      'Map.classId = "hijack";',
    ]) {
      const { context, workflowGlobal } = makeContext();
      vm.runInContext(patch, context);
      // Reducer dispatch runs `instanceof` / constructor probes for every
      // non-primitive value, so even a plain object declines.
      const plain = vm.runInContext('({ ok: true })', context);
      expect(isRetainedSerializationPassive(plain, workflowGlobal), patch).toBe(
        false
      );
    }
  });

  it('declines everything when the realm has no registered pins', () => {
    const { context, workflowGlobal } = makeContext({
      freeze: true,
      register: false,
    });
    for (const expression of ['new Map()', '({ ok: true })', '[1, 2]']) {
      const value = vm.runInContext(expression, context);
      expect(
        isRetainedSerializationPassive(value, workflowGlobal),
        expression
      ).toBe(false);
    }
  });

  it('refuses to register pins for an unfrozen realm', () => {
    const { workflowGlobal } = makeContext({ freeze: false, register: false });
    expect(() => registerSerializationPins(workflowGlobal)).toThrow(
      /freezeSerializationIntrinsics/
    );
  });

  it('declines retention while a host dispatch constructor is hooked', () => {
    const { context, workflowGlobal } = makeContext();
    const plain = vm.runInContext('({ ok: true })', context);
    expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(true);

    Object.defineProperty(Headers, Symbol.hasInstance, {
      value: () => false,
      configurable: true,
    });
    try {
      expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(false);
    } finally {
      delete (Headers as any)[Symbol.hasInstance];
    }

    expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(true);
  });
});

describe('serialization touches only the pinned surface', () => {
  // THE coupling test: retention is only sound if every prototype member
  // `dehydrateStepArguments` executes for the supported built-ins is one
  // `registerSerializationPins` pins and re-verifies per boundary. Wrap
  // every configurable member on the relevant prototypes (in an unfrozen,
  // unregistered realm) with a recorder and assert the serializer hits
  // nothing beyond this measured set. If serde starts touching something
  // new, this fails loudly: extend the pins (and this list) together.
  const PINNED_SURFACE = new Set([
    'Map.prototype.Symbol(Symbol.iterator)',
    '%MapIteratorPrototype%.next',
    'Set.prototype.Symbol(Symbol.iterator)',
    '%SetIteratorPrototype%.next',
    'Date.prototype.getDate',
    'Date.prototype.toISOString',
    '%TypedArray%.prototype.buffer',
    '%TypedArray%.prototype.byteOffset',
    '%TypedArray%.prototype.byteLength',
    'ArrayBuffer.prototype.byteLength',
  ]);

  it('for Map, Set, Date, typed arrays, and ArrayBuffer', async () => {
    // Unfrozen realm: the recorders themselves need to redefine members.
    const { context, workflowGlobal } = makeContext({ freeze: false });
    const g = workflowGlobal as any;
    const touched = new Set<string>();

    const wrapPrototype = (prototype: object, label: string) => {
      for (const key of Reflect.ownKeys(prototype)) {
        if (key === 'constructor') continue;
        const descriptor = Object.getOwnPropertyDescriptor(prototype, key);
        if (!descriptor || !descriptor.configurable) continue;
        const name = `${label}.${String(key)}`;
        if (typeof descriptor.value === 'function') {
          const original = descriptor.value;
          Object.defineProperty(prototype, key, {
            ...descriptor,
            value: function (this: unknown, ...args: unknown[]) {
              touched.add(name);
              return original.apply(this, args);
            },
          });
        } else if (descriptor.get) {
          const originalGet = descriptor.get;
          Object.defineProperty(prototype, key, {
            ...descriptor,
            get() {
              touched.add(name);
              return originalGet.call(this);
            },
            set: descriptor.set,
          });
        }
      }
    };

    const values = vm.runInContext(
      `({
        map: new Map([["k", 1]]),
        set: new Set([1, 2]),
        date: new Date(1234),
        f32: new Float32Array([1.5, 2.5]),
        u8: new Uint8Array([1, 2, 3]),
        ab: new ArrayBuffer(8),
      })`,
      context
    );

    wrapPrototype(g.Map.prototype, 'Map.prototype');
    wrapPrototype(
      Object.getPrototypeOf(new g.Map()[Symbol.iterator]()),
      '%MapIteratorPrototype%'
    );
    wrapPrototype(g.Set.prototype, 'Set.prototype');
    wrapPrototype(
      Object.getPrototypeOf(new g.Set()[Symbol.iterator]()),
      '%SetIteratorPrototype%'
    );
    const datePrototype = Object.getOwnPropertyDescriptor(g.Date, 'prototype')!
      .value as object;
    wrapPrototype(datePrototype, 'Date.prototype');
    wrapPrototype(
      Object.getPrototypeOf(g.Uint8Array.prototype),
      '%TypedArray%.prototype'
    );
    wrapPrototype(g.Uint8Array.prototype, 'Uint8Array.prototype');
    wrapPrototype(g.Float32Array.prototype, 'Float32Array.prototype');
    wrapPrototype(g.ArrayBuffer.prototype, 'ArrayBuffer.prototype');

    for (const value of Object.values(values as Record<string, unknown>)) {
      touched.clear();
      await dehydrateStepArguments(
        { args: [value], closureVars: undefined, thisVal: undefined },
        'wrun_pin_coverage',
        undefined,
        g,
        false,
        false
      );
      for (const name of touched) {
        expect(
          PINNED_SURFACE,
          `member outside the pinned surface executed: ${name}`
        ).toContain(name);
      }
    }
  });
});

describe('checker execution surface', () => {
  it('never invokes live host collection methods', () => {
    const { context, workflowGlobal } = makeContext();
    const map = vm.runInContext('new Map([["k", 1]])', context);

    let invoked = 0;
    const original = Map.prototype.forEach;
    // Simulate workflow code having replaced the reachable host method.
    Map.prototype.forEach = function (
      this: Map<unknown, unknown>,
      ...args: [any]
    ) {
      invoked++;
      return original.apply(this, args);
    };
    try {
      // The checker uses the module-captured primordial: same verdict,
      // replaced method never runs.
      expect(isRetainedSerializationPassive(map, workflowGlobal)).toBe(true);
      expect(invoked).toBe(0);
    } finally {
      Map.prototype.forEach = original;
    }
  });

  it('declines typed arrays re-prototyped onto a frozen hostile prototype', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext(
      `(() => {
        globalThis.__retainedTestCalls = 0;
        const realGetter = Object.getOwnPropertyDescriptor(
          Object.getPrototypeOf(Uint8Array.prototype), "buffer").get;
        const hostile = Object.create(
          Object.getPrototypeOf(Uint8Array.prototype),
          { buffer: { get() { globalThis.__retainedTestCalls++; return realGetter.call(this); } } }
        );
        Object.freeze(hostile);
        const ta = new Uint8Array([1, 2]);
        Object.setPrototypeOf(ta, hostile);
        return ta;
      })()`,
      context
    );

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    expect(vm.runInContext('globalThis.__retainedTestCalls', context)).toBe(0);
  });
});

describe('bigint serialization', () => {
  it('declines retention while host BigInt.prototype.toString is replaced', () => {
    const { context, workflowGlobal } = makeContext();
    const value = vm.runInContext('({ big: 42n })', context);
    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(true);

    const original = BigInt.prototype.toString;
    // biome-ignore lint/suspicious/noGlobalAssign: simulating workflow-realm tampering
    BigInt.prototype.toString = function (this: bigint, ...args: [number?]) {
      return original.apply(this, args);
    };
    try {
      expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(false);
    } finally {
      BigInt.prototype.toString = original;
    }

    expect(isRetainedSerializationPassive(value, workflowGlobal)).toBe(true);
  });
});

describe('walker primordials', () => {
  it('uses captured primordials, not live host statics', () => {
    const { context, workflowGlobal } = makeContext();
    const map = vm.runInContext('new Map([["k", { ok: true }]])', context);

    // If the walker consulted the live host statics, these would throw.
    const originalDescriptor = Object.getOwnPropertyDescriptor;
    const originalOwnKeys = Reflect.ownKeys;
    (Object as any).getOwnPropertyDescriptor = () => {
      throw new Error('live getOwnPropertyDescriptor used');
    };
    (Reflect as any).ownKeys = () => {
      throw new Error('live ownKeys used');
    };
    try {
      expect(isRetainedSerializationPassive(map, workflowGlobal)).toBe(true);
    } finally {
      (Object as any).getOwnPropertyDescriptor = originalDescriptor;
      (Reflect as any).ownKeys = originalOwnKeys;
    }
  });

  it('declines when host Function.prototype carries serializer statics', () => {
    const { context, workflowGlobal } = makeContext();
    const plain = vm.runInContext('({ ok: true })', context);
    expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(true);

    Object.defineProperty(
      Function.prototype,
      Symbol.for('workflow-serialize'),
      {
        get() {
          return undefined;
        },
        configurable: true,
      }
    );
    try {
      expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(false);
    } finally {
      delete (Function.prototype as any)[Symbol.for('workflow-serialize')];
    }

    expect(isRetainedSerializationPassive(plain, workflowGlobal)).toBe(true);
  });
});
