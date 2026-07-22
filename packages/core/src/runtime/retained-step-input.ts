import { types } from 'node:util';
import { WORKFLOW_SERIALIZE } from '@workflow/serde';

// What this module proves, per suspension boundary: serializing the queued
// step inputs through the ordinary pipeline executes no workflow code.
//
// Retained sessions keep running after suspension, so a serialization side
// effect (a getter, a patched prototype member, a custom serializer) would
// mutate the live VM in a way a cold replay never repeats — replay reads the
// recorded `step_created` event instead of re-serializing. The bytes cannot
// differ by mode (serialization happens exactly once, on the one shared
// path); passivity is the only property retention needs.
//
// The sandbox freezes only the universal lookup backstops
// (Object/Array/Function.prototype — see vm/index.ts). Value-type prototypes
// stay patchable so polyfills (Temporal's `Date.prototype.toTemporalInstant`,
// core-js `Set.prototype.union`) keep working. In exchange, every surface
// serialization can *execute* is pinned at context creation
// (registerSerializationPins) and re-verified per boundary: any drift
// declines retention for that boundary and the session falls back to
// ordinary replay, which serializes the exact same bytes.

// Host constructors that serialization dispatches on (`value instanceof
// global.X`), plus Object/Array, whose statics and prototypes the class
// reducer and devalue's tag lookup read for host-prototype values (hydrated
// step results are host-realm plain objects). Host intrinsics are shared
// with the whole process and cannot be frozen — and workflow code can reach
// them (exposed host classes, `structuredClone` results) and plant
// workflow-realm hooks. Verified instead: any dirt declines retention, so a
// planted hook can never execute while a retained VM's inputs serialize.
const HOST_DISPATCH_CONSTRUCTORS = [
  'Object',
  'Array',
  'Function',
  'Map',
  'Set',
  'Date',
  'RegExp',
  'ArrayBuffer',
  'SharedArrayBuffer',
  'DataView',
  'Int8Array',
  'Uint8Array',
  'Uint8ClampedArray',
  'Int16Array',
  'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float16Array',
  'Float32Array',
  'Float64Array',
  'BigInt64Array',
  'BigUint64Array',
  'Headers',
  'URL',
  'URLSearchParams',
  'DOMException',
  'AbortController',
  'AbortSignal',
  'Request',
  'Response',
  'ReadableStream',
  'WritableStream',
  'TransformStream',
] as const;

// Host primordials captured at module load — before any workflow code can
// exist in the process — so the checker itself never invokes a live host
// member workflow code could have replaced (host constructors are reachable
// via e.g. `structuredClone(new Map()).constructor` or exposed classes).
const hostGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const hostGetPrototypeOf = Object.getPrototypeOf;
const hostIsFrozen = Object.isFrozen;
const hostOwnKeys = Reflect.ownKeys;
const hostIsArray = Array.isArray;
const hostIsInteger = Number.isInteger;
const hostNumber = Number;
const hostString = String;
const hostBigIntToString = BigInt.prototype.toString;
const hostMapForEach = Map.prototype.forEach;
const hostSetForEach = Set.prototype.forEach;
const hostObjectPrototype = Object.prototype;
const hostArrayPrototype = Array.prototype;
const hostFunctionPrototype = Function.prototype;
// biome-ignore lint/style/noNonNullAssertion: the %TypedArray% buffer getter always exists
const hostTypedArrayBuffer = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype),
  'buffer'
)!.get!;

const TYPED_ARRAY_CONSTRUCTORS = [
  'Int8Array',
  'Uint8Array',
  'Uint8ClampedArray',
  'Int16Array',
  'Uint16Array',
  'Int32Array',
  'Uint32Array',
  'Float16Array',
  'Float32Array',
  'Float64Array',
  'BigInt64Array',
  'BigUint64Array',
] as const;

function hasOwn(target: object, key: string | symbol): boolean {
  return hostGetOwnPropertyDescriptor(target, key) !== undefined;
}

function isHostDispatchPristine(): boolean {
  const hostGlobal = globalThis as unknown as Record<string, unknown>;
  for (const name of HOST_DISPATCH_CONSTRUCTORS) {
    const constructor = hostGlobal[name];
    if (constructor === undefined) continue;
    if (hasOwn(constructor as object, Symbol.hasInstance)) return false;
  }
  // The class reducer reads `cls[WORKFLOW_SERIALIZE]` / `cls.classId` as
  // inherited Gets (serialization/reducers/class.ts:28-41), so the
  // constructors' whole prototype chains — host Function.prototype and
  // Object.prototype — must be clean too.
  for (const target of [Object, Array, Function.prototype, Object.prototype]) {
    if (hasOwn(target, WORKFLOW_SERIALIZE) || hasOwn(target, 'classId')) {
      return false;
    }
  }
  for (const [prototype, constructor] of [
    [Object.prototype, Object],
    [Array.prototype, Array],
  ] as const) {
    if (
      hasOwn(prototype, Symbol.toStringTag) ||
      ownDataProperty(prototype, 'constructor') !== constructor
    ) {
      return false;
    }
  }
  // Constructor prototype chains end at host Object.prototype, where an
  // added @@hasInstance would be found by dispatch lookup. (Host
  // Function.prototype's @@hasInstance is spec non-configurable.)
  if (hasOwn(Object.prototype, Symbol.hasInstance)) return false;
  // The BigInt reducer calls `value.toString()` on bigint primitives
  // (serialization/reducers/common.ts:179) from host code, which resolves on
  // the HOST BigInt.prototype (primitives are realm-less; method lookup uses
  // the running code's realm).
  if (ownDataProperty(BigInt.prototype, 'toString') !== hostBigIntToString) {
    return false;
  }
  return true;
}

// Own data-property read that never performs a property Get — workflow code
// can redefine members with accessors, and validation must not execute
// workflow-owned code.
function ownDataProperty(target: object, key: string | symbol): unknown {
  const descriptor = hostGetOwnPropertyDescriptor(target, key);
  return descriptor && 'value' in descriptor ? descriptor.value : undefined;
}

// ---------------------------------------------------------------------------
// Realm serialization pins
//
// Captured once per workflow realm at context creation — after
// freezeSerializationIntrinsics, before any workflow code evaluates — and
// re-verified per suspension boundary with descriptor-only reads. The pin
// list is the measured set of members serialization executes for the
// supported built-ins (see the instrumentation test in
// retained-step-input.test.ts, which wraps every member and asserts nothing
// outside this set runs):
//
// - `Map.prototype[Symbol.iterator]` + `%MapIteratorPrototype%.next` —
//   the Map reducer does `Array.from(value)`
//   (serialization/reducers/common.ts:319; iteration protocol per
//   https://tc39.es/ecma262/#sec-getiterator)
// - `Set.prototype[Symbol.iterator]` + `%SetIteratorPrototype%.next` —
//   Set reducer, common.ts:329
// - `Date.prototype.getDate` / `.toISOString` — Date reducer,
//   common.ts:184-188
// - `%TypedArray%.prototype` `buffer`/`byteOffset`/`byteLength` getters —
//   typed-array reducer calls
//   `arrayBufferToBase64(value.buffer, value.byteOffset, value.byteLength)`,
//   common.ts:23-36
// - `ArrayBuffer.prototype.byteLength` getter — ArrayBuffer reducer,
//   common.ts:177-178
//
// Two more lookup families execute during dispatch and are verified on the
// constructors below: `value instanceof global.X` (devalue runs every custom
// reducer predicate on every non-primitive value —
// https://github.com/sveltejs/devalue/blob/v5.8.1/src/stringify.js#L111-L116)
// and the Instance reducer's `value.constructor` /
// `cls[WORKFLOW_SERIALIZE]` / `cls.classId` reads
// (serialization/reducers/class.ts:26-43). Plain objects and arrays need no
// pins of their own: devalue walks them purely via own-property reads
// (arrays: stringify.js#L186-L192; objects: `Object.keys` at
// stringify.js#L329-L357), and their lookup backstops are frozen.
// ---------------------------------------------------------------------------

interface AccessorPin {
  get: unknown;
  set: unknown;
}

interface PrototypePin {
  proto: object;
  // Required [[Prototype]] identity, so a missed lookup on a passive value
  // terminates on the frozen backstops. null: lookups never traverse past
  // this object (iterator prototypes — only `next` is read, and it is own).
  parent: object | null;
  // Required own `constructor` data value: the Instance reducer Gets
  // `value.constructor` (class.ts:28) on every object, and a swapped value
  // would route the follow-up WORKFLOW_SERIALIZE/classId Gets to an
  // arbitrary workflow object. undefined: never consulted for this proto.
  ctor: unknown;
  // Own keys that must remain absent — they would shadow an executed member
  // defined further up the chain (e.g. `buffer` on Uint8Array.prototype
  // shadowing the pinned %TypedArray%.prototype getter).
  banned: readonly (string | symbol)[];
  // Own data-valued members serialization calls: identity must hold.
  executedData: ReadonlyMap<string | symbol, unknown>;
  // Own accessors serialization reads: getter identity must hold.
  executedGetters: ReadonlyMap<string | symbol, unknown>;
  // All own accessors at registration. Any other own accessor declines: a
  // Get reaching this prototype would execute it. New own DATA members stay
  // allowed — reading a data property runs no code, and serialization only
  // ever *executes* the pinned members — which is what keeps polyfills
  // (Temporal's `Date.prototype.toTemporalInstant`, core-js
  // `Set.prototype.union`) compatible with retention.
  accessors: ReadonlyMap<string | symbol, AccessorPin>;
}

// Pins are grouped per value type: a violated pin declines only values whose
// lookup chains include that prototype (a replaced `Date.prototype.toISOString`
// declines Date arguments; Maps, strings, and everything else keep retaining).
// The constructor checks stay realm-global — reducer dispatch runs
// `instanceof` probes for every non-primitive value regardless of its type.
interface RealmPins {
  dispatchConstructors: readonly object[];
  functionPrototype: object;
  objectPrototype: object;
  arrayPrototype: object;
  mapPrototype: object;
  mapPins: readonly PrototypePin[];
  setPrototype: object;
  setPins: readonly PrototypePin[];
  datePrototype: object;
  datePins: readonly PrototypePin[];
  arrayBufferPrototype: object;
  arrayBufferPins: readonly PrototypePin[];
  // Concrete typed-array prototype → its pins (the concrete pin plus the
  // shared %TypedArray%.prototype pin).
  typedArrayPins: ReadonlyMap<object, readonly PrototypePin[]>;
}

const pinsByRealm = new WeakMap<object, RealmPins>();

function snapshotAccessors(proto: object): Map<string | symbol, AccessorPin> {
  const accessors = new Map<string | symbol, AccessorPin>();
  for (const key of hostOwnKeys(proto)) {
    // biome-ignore lint/style/noNonNullAssertion: key is an own key
    const descriptor = hostGetOwnPropertyDescriptor(proto, key)!;
    if (!('value' in descriptor)) {
      accessors.set(key, { get: descriptor.get, set: descriptor.set });
    }
  }
  return accessors;
}

/**
 * Capture the serialization pins for a freshly created workflow realm. Must
 * run after `freezeSerializationIntrinsics` and before any workflow code
 * evaluates — registration takes the realm's current members as the trusted
 * originals.
 */
export function registerSerializationPins(
  workflowGlobal: Record<string, any>
): void {
  const objectPrototype = prototypeOf(workflowGlobal, 'Object');
  const arrayPrototype = prototypeOf(workflowGlobal, 'Array');
  const functionPrototype = prototypeOf(workflowGlobal, 'Function');
  if (
    !hostIsFrozen(objectPrototype) ||
    !hostIsFrozen(arrayPrototype) ||
    !hostIsFrozen(functionPrototype)
  ) {
    throw new Error(
      'registerSerializationPins requires freezeSerializationIntrinsics to have run'
    );
  }

  const pinPrototype = (
    proto: object,
    pin: Omit<Partial<PrototypePin>, 'proto' | 'accessors'> & {
      parent: object | null;
    }
  ): PrototypePin => ({
    proto,
    ctor: undefined,
    banned: [],
    executedData: new Map(),
    executedGetters: new Map(),
    ...pin,
    accessors: snapshotAccessors(proto),
  });

  const pinIterableCollection = (
    name: 'Map' | 'Set'
  ): { proto: object; pins: PrototypePin[] } => {
    const constructor = ownDataProperty(workflowGlobal, name) as new () => {};
    const proto = prototypeOf(workflowGlobal, name);
    const iterator = ownDataProperty(proto, Symbol.iterator) as (
      this: unknown
    ) => object;
    // A scratch instance yields the realm's %MapIteratorPrototype% /
    // %SetIteratorPrototype% — no workflow code exists yet, so calling the
    // realm constructor and iterator here is safe.
    const iteratorProto = hostGetPrototypeOf(iterator.call(new constructor()));
    return {
      proto,
      pins: [
        pinPrototype(proto, {
          parent: objectPrototype,
          ctor: ownDataProperty(proto, 'constructor'),
          executedData: new Map([[Symbol.iterator, iterator]]),
        }),
        pinPrototype(iteratorProto, {
          parent: null,
          executedData: new Map([
            ['next', ownDataProperty(iteratorProto, 'next')],
          ]),
        }),
      ],
    };
  };

  const map = pinIterableCollection('Map');
  const set = pinIterableCollection('Set');

  // The realm's `Date` binding is the sandbox's deterministic wrapper
  // (vm/index.ts); its `.prototype` is the realm's original Date.prototype,
  // whose `constructor` back-ref is the original constructor — capture the
  // back-ref itself, since that is what the Instance reducer's
  // `value.constructor` Get resolves to.
  const datePrototype = prototypeOf(workflowGlobal, 'Date');
  const datePins = [
    pinPrototype(datePrototype, {
      parent: objectPrototype,
      ctor: ownDataProperty(datePrototype, 'constructor'),
      executedData: new Map([
        ['getDate', ownDataProperty(datePrototype, 'getDate')],
        ['toISOString', ownDataProperty(datePrototype, 'toISOString')],
      ]),
    }),
  ];

  const typedArrayPrototype = hostGetPrototypeOf(
    prototypeOf(workflowGlobal, 'Uint8Array')
  ) as object;
  const typedArrayGetter = (key: string) =>
    // biome-ignore lint/style/noNonNullAssertion: spec accessors on %TypedArray%.prototype
    hostGetOwnPropertyDescriptor(typedArrayPrototype, key)!.get;
  const typedArraySharedPin = pinPrototype(typedArrayPrototype, {
    parent: objectPrototype,
    ctor: ownDataProperty(typedArrayPrototype, 'constructor'),
    executedGetters: new Map([
      ['buffer', typedArrayGetter('buffer')],
      ['byteOffset', typedArrayGetter('byteOffset')],
      ['byteLength', typedArrayGetter('byteLength')],
    ]),
  });
  const typedArrayPins = new Map<object, readonly PrototypePin[]>();
  for (const name of TYPED_ARRAY_CONSTRUCTORS) {
    const constructor = ownDataProperty(workflowGlobal, name);
    if (constructor === undefined) continue; // Float16Array on older Node
    const proto = prototypeOf(workflowGlobal, name);
    typedArrayPins.set(proto, [
      pinPrototype(proto, {
        parent: typedArrayPrototype,
        ctor: ownDataProperty(proto, 'constructor'),
        banned: ['buffer', 'byteOffset', 'byteLength'],
      }),
      typedArraySharedPin,
    ]);
  }

  const arrayBufferPrototype = prototypeOf(workflowGlobal, 'ArrayBuffer');
  const arrayBufferPins = [
    pinPrototype(arrayBufferPrototype, {
      parent: objectPrototype,
      ctor: ownDataProperty(arrayBufferPrototype, 'constructor'),
      executedGetters: new Map([
        [
          'byteLength',
          // biome-ignore lint/style/noNonNullAssertion: spec accessor on ArrayBuffer.prototype
          hostGetOwnPropertyDescriptor(arrayBufferPrototype, 'byteLength')!.get,
        ],
      ]),
    }),
  ];

  const dispatchConstructors: object[] = [];
  for (const name of HOST_DISPATCH_CONSTRUCTORS) {
    const constructor = ownDataProperty(workflowGlobal, name);
    if (typeof constructor === 'function') {
      dispatchConstructors.push(constructor);
    }
  }

  pinsByRealm.set(workflowGlobal, {
    dispatchConstructors,
    functionPrototype,
    objectPrototype,
    arrayPrototype,
    mapPrototype: map.proto,
    mapPins: map.pins,
    setPrototype: set.proto,
    setPins: set.pins,
    datePrototype,
    datePins,
    arrayBufferPrototype,
    arrayBufferPins,
    typedArrayPins,
  });
}

function prototypeOf(
  realmGlobal: Record<string, any>,
  constructorName: string
): object {
  const constructor = ownDataProperty(realmGlobal, constructorName);
  const proto = ownDataProperty(constructor as object, 'prototype');
  // Function.prototype is itself a function; every other pinned prototype is
  // an ordinary object.
  if (
    (typeof proto !== 'object' && typeof proto !== 'function') ||
    proto === null
  ) {
    throw new Error(`realm has no ${constructorName}.prototype`);
  }
  return proto;
}

function verifyPrototypePin(pin: PrototypePin): boolean {
  if (pin.parent !== null && hostGetPrototypeOf(pin.proto) !== pin.parent) {
    return false;
  }
  if (
    pin.ctor !== undefined &&
    ownDataProperty(pin.proto, 'constructor') !== pin.ctor
  ) {
    return false;
  }
  for (const key of pin.banned) {
    if (hasOwn(pin.proto, key)) return false;
  }
  for (const [key, value] of pin.executedData) {
    const descriptor = hostGetOwnPropertyDescriptor(pin.proto, key);
    if (!descriptor || !('value' in descriptor) || descriptor.value !== value) {
      return false;
    }
  }
  for (const [key, get] of pin.executedGetters) {
    const descriptor = hostGetOwnPropertyDescriptor(pin.proto, key);
    if (!descriptor || 'value' in descriptor || descriptor.get !== get) {
      return false;
    }
  }
  // New own data members (polyfill methods) are inert; any own accessor that
  // was not present at registration — or whose get/set changed — declines.
  for (const key of hostOwnKeys(pin.proto)) {
    // biome-ignore lint/style/noNonNullAssertion: key is an own key
    const descriptor = hostGetOwnPropertyDescriptor(pin.proto, key)!;
    if ('value' in descriptor) continue;
    const pinned = pin.accessors.get(key);
    if (
      !pinned ||
      pinned.get !== descriptor.get ||
      pinned.set !== descriptor.set
    ) {
      return false;
    }
  }
  return true;
}

// The lookups serialization performs on a constructor traverse its whole
// [[Prototype]] chain:
// - `value instanceof global.X` first Gets C[@@hasInstance]
//   (https://tc39.es/ecma262/#sec-instanceofoperator), and devalue runs every
//   custom reducer predicate on every non-primitive value
//   (devalue v5.8.1 src/stringify.js#L111-L116)
// - the Instance reducer Gets `cls[WORKFLOW_SERIALIZE]` / `cls.classId` on
//   `value.constructor` (serialization/reducers/class.ts:26-43): a
//   data-valued serializer would be *called*, an accessor executes on the
//   read itself. Either declines. (Statics under any other name — e.g. a
//   polyfilled `Object.groupBy` — are never read and stay allowed.)
// Walk the chain: every link must be a real function carrying none of those
// keys, ending at a real Function.prototype — the realm's (frozen), or the
// host's for host-implemented classes the SDK installs (Request, streams;
// their spec @@hasInstance is non-configurable there, and
// isHostDispatchPristine covers the serializer-static keys). Chains are
// legitimately more than one link deep: the sandbox's deterministic `Date`
// wrapper chains to the realm's original Date (vm/index.ts), AbortSignal
// chains to EventTarget.
function verifyConstructorChain(
  constructor: unknown,
  pins: RealmPins
): boolean {
  let link: unknown = constructor;
  for (let depth = 0; depth < 8; depth++) {
    if (link === pins.functionPrototype || link === hostFunctionPrototype) {
      return true;
    }
    if (typeof link !== 'function' || types.isProxy(link)) return false;
    if (
      hasOwn(link, Symbol.hasInstance) ||
      hasOwn(link, WORKFLOW_SERIALIZE) ||
      hasOwn(link, 'classId')
    ) {
      return false;
    }
    link = hostGetPrototypeOf(link);
  }
  return false;
}

// Per-boundary walk state: pin verdicts are computed lazily (only for the
// types actually present in the inputs) and memoized for the boundary.
interface BoundaryContext {
  pins: RealmPins;
  seen: WeakSet<object>;
  verified: Map<PrototypePin, boolean>;
}

function verifiedPins(
  pins: readonly PrototypePin[],
  ctx: BoundaryContext
): boolean {
  for (const pin of pins) {
    let ok = ctx.verified.get(pin);
    if (ok === undefined) {
      ok =
        verifyPrototypePin(pin) &&
        // The `constructor` back-ref is itself the target of inherited Gets
        // (the Instance reducer), so its chain must be clean too.
        (pin.ctor === undefined || verifyConstructorChain(pin.ctor, ctx.pins));
      ctx.verified.set(pin, ok);
    }
    if (!ok) return false;
  }
  return true;
}

function isArrayIndex(key: string): boolean {
  const index = hostNumber(key);
  return (
    hostIsInteger(index) &&
    index >= 0 &&
    index < 2 ** 32 - 1 &&
    hostString(index) === key
  );
}

function isPassiveArrayProperty(
  array: unknown[],
  key: string | symbol,
  ctx: BoundaryContext
): boolean {
  if (key === 'length') return true;
  const descriptor = hostGetOwnPropertyDescriptor(array, key);
  if (!descriptor) return false;
  // Only own enumerable data indices are passive. Anything hidden — symbol
  // tags, non-enumerable properties, accessors — can be observed by
  // serialization dispatch (reducer probes, thenable checks, the class
  // reducer) and can execute workflow code when read. devalue reads array
  // elements as own indexed properties (v5.8.1 src/stringify.js#L186-L192).
  return (
    typeof key === 'string' &&
    isArrayIndex(key) &&
    descriptor.enumerable === true &&
    'value' in descriptor &&
    isPassive(descriptor.value, ctx)
  );
}

function isPassiveArray(value: unknown[], ctx: BoundaryContext): boolean {
  const prototype = hostGetPrototypeOf(value);
  return (
    (prototype === ctx.pins.arrayPrototype ||
      prototype === hostArrayPrototype) &&
    hostOwnKeys(value).every((key) => isPassiveArrayProperty(value, key, ctx))
  );
}

// Instances of the supported built-ins must carry no own properties at all:
// serialization never reads own properties on them, but an own accessor or
// symbol could still be observed through other dispatch lookups, and clean
// instances are the overwhelmingly common case anyway.
function hasNoOwnProperties(value: object): boolean {
  return hostOwnKeys(value).length === 0;
}

function isPassiveMap(
  value: Map<unknown, unknown>,
  ctx: BoundaryContext
): boolean {
  if (hostGetPrototypeOf(value) !== ctx.pins.mapPrototype) return false;
  if (!verifiedPins(ctx.pins.mapPins, ctx)) return false;
  if (!hasNoOwnProperties(value)) return false;
  let passive = true;
  // The captured host forEach iterates via internal slots — no realm
  // members (and no live, replaceable host members) execute.
  hostMapForEach.call(value, (entryValue: unknown, key: unknown) => {
    passive &&= isPassive(key, ctx) && isPassive(entryValue, ctx);
  });
  return passive;
}

function isPassiveSet(value: Set<unknown>, ctx: BoundaryContext): boolean {
  if (hostGetPrototypeOf(value) !== ctx.pins.setPrototype) return false;
  if (!verifiedPins(ctx.pins.setPins, ctx)) return false;
  if (!hasNoOwnProperties(value)) return false;
  let passive = true;
  hostSetForEach.call(value, (entryValue: unknown) => {
    passive &&= isPassive(entryValue, ctx);
  });
  return passive;
}

function isPassiveDate(value: object, ctx: BoundaryContext): boolean {
  return (
    hostGetPrototypeOf(value) === ctx.pins.datePrototype &&
    verifiedPins(ctx.pins.datePins, ctx) &&
    hasNoOwnProperties(value)
  );
}

function isPassiveTypedArray(value: object, ctx: BoundaryContext): boolean {
  // Identity against the finite set of the realm's pinned typed-array
  // prototypes — NOT "anything chaining to %TypedArray%": a workflow can
  // manufacture a hostile prototype with a delegating `buffer` getter and
  // setPrototypeOf a real typed array onto it.
  const pins = ctx.pins.typedArrayPins.get(hostGetPrototypeOf(value));
  if (pins === undefined || !verifiedPins(pins, ctx)) return false;
  // Own keys on a typed array are exactly its canonical indices.
  if (
    !hostOwnKeys(value).every(
      (key) => typeof key === 'string' && isArrayIndex(key)
    )
  ) {
    return false;
  }
  // Read the backing buffer via the captured host getter (internal slots
  // work cross-realm); reject SharedArrayBuffer backing.
  return !types.isSharedArrayBuffer(hostTypedArrayBuffer.call(value));
}

function isPassiveArrayBuffer(value: object, ctx: BoundaryContext): boolean {
  return (
    hostGetPrototypeOf(value) === ctx.pins.arrayBufferPrototype &&
    verifiedPins(ctx.pins.arrayBufferPins, ctx) &&
    hasNoOwnProperties(value)
  );
}

function isPassiveObjectProperty(
  object: object,
  key: string | symbol,
  ctx: BoundaryContext
): boolean {
  const descriptor = hostGetOwnPropertyDescriptor(object, key);
  if (!descriptor) return false;
  // Only own enumerable string-keyed data properties are passive. Anything
  // hidden — symbol tags, non-enumerable properties, accessors — can be
  // observed by serialization dispatch (reducer probes like `.signal`,
  // thenable checks, the class reducer) and can execute workflow code.
  // devalue walks objects purely via `Object.keys` + own reads
  // (v5.8.1 src/stringify.js#L329-L357).
  return (
    typeof key === 'string' &&
    key !== '__proto__' &&
    descriptor.enumerable === true &&
    'value' in descriptor &&
    isPassive(descriptor.value, ctx)
  );
}

function isPassivePlainObject(value: object, ctx: BoundaryContext): boolean {
  const prototype = hostGetPrototypeOf(value);
  if (
    prototype !== ctx.pins.objectPrototype &&
    prototype !== hostObjectPrototype
  ) {
    return false;
  }
  return hostOwnKeys(value).every((key) =>
    isPassiveObjectProperty(value, key, ctx)
  );
}

/**
 * Whether serializing `value` through the ordinary pipeline provably executes
 * no workflow code and draws no workflow-realm randomness.
 *
 * Passive values: primitives; plain objects and arrays (own enumerable
 * string-keyed data properties only — devalue traverses these purely via own
 * reads); and workflow-realm Map/Set/Date/typed arrays/ArrayBuffer whose
 * serialization surfaces still match the pins captured at context creation.
 * Everything else — proxies, accessors, functions, custom classes, RegExp,
 * hidden keys — declines, serializes exactly the same way, and the session
 * falls back to ordinary replay for that boundary.
 */
function isPassive(value: unknown, ctx: BoundaryContext): boolean {
  if (
    value === null ||
    value === undefined ||
    typeof value === 'boolean' ||
    typeof value === 'bigint' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return true;
  }
  if (typeof value !== 'object' || types.isProxy(value)) return false;
  if (ctx.seen.has(value)) return true;
  ctx.seen.add(value);

  if (hostIsArray(value)) return isPassiveArray(value, ctx);
  if (types.isMap(value)) return isPassiveMap(value, ctx);
  if (types.isSet(value)) return isPassiveSet(value, ctx);
  if (types.isDate(value)) return isPassiveDate(value, ctx);
  if (types.isTypedArray(value)) return isPassiveTypedArray(value, ctx);
  if (types.isArrayBuffer(value)) return isPassiveArrayBuffer(value, ctx);
  if (
    types.isSharedArrayBuffer(value) ||
    types.isRegExp(value) ||
    types.isDataView(value) ||
    types.isBoxedPrimitive(value) ||
    types.isNativeError(value) ||
    types.isPromise(value) ||
    types.isArgumentsObject(value)
  ) {
    return false;
  }
  return isPassivePlainObject(value, ctx);
}

export function isRetainedSerializationPassive(
  value: unknown,
  workflowGlobal: Record<string, any>
): boolean {
  const pins = pinsByRealm.get(workflowGlobal);
  // No pins: the realm never went through context creation's registration,
  // so nothing about its lookup surfaces is known.
  if (pins === undefined) return false;
  if (!isHostDispatchPristine()) return false;
  // Reducer dispatch runs `instanceof` probes against every dispatch
  // constructor for every non-primitive value, so the constructor chains
  // gate the whole boundary regardless of which types the inputs use.
  for (const constructor of pins.dispatchConstructors) {
    if (!verifyConstructorChain(constructor, pins)) return false;
  }
  return isPassive(value, { pins, seen: new WeakSet(), verified: new Map() });
}
