/**
 * Hardened, side-effect-free stringify operations + passivity tainting.
 *
 * Serializing a value with plain devalue can execute code the value's owner
 * controls: getters and proxy traps fire on property reads, `Object.prototype
 * .toString` consults (possibly getter-defined) `Symbol.toStringTag`, and
 * prototype methods like `Date.prototype.toISOString` or
 * `Map.prototype[Symbol.iterator]` are dispatched dynamically, so a workflow
 * that patched them would have its code run mid-serialization.
 *
 * That matters for retained-VM replay (see runtime/suspension-handler.ts):
 * a retained workflow VM is only equivalent to a cold replay if serializing
 * a suspension's step inputs did not execute workflow code — a cold replay
 * serializes nothing, so any side effect would exist in one timeline and not
 * the other.
 *
 * Two mechanisms, both wired through `hardenedStringify`:
 *
 * 1. **Hardening** — every introspection devalue performs goes through the
 *    `operations` override below, built on captured intrinsics that read
 *    internal slots (cross-realm safe, immune to prototype patching; the
 *    ECMAScript ones come from a pristine VM realm so patches applied
 *    before this module loaded can't hide in them). The reducers in
 *    ./reducers/ are built the same way (`util.types` brand checks +
 *    captured originals). For well-behaved values the serialized bytes are
 *    identical to plain devalue — see byte-stability.test.ts.
 *
 * 2. **Tainting** — where behavior preservation *requires* running value-
 *    owned code (own getters, proxy traps, custom `WORKFLOW_SERIALIZE`
 *    methods, resource-type reducers like Request/Headers), we run it
 *    exactly as before but record the fact in the active
 *    `SerializationPassivityReport`. The suspension handler reads the report
 *    and demotes the retained session to an ordinary replay, so the side
 *    effects land in a VM that is about to be discarded — exactly like the
 *    pre-retention runtime.
 */

import { types } from 'node:util';
import { runInNewContext } from 'node:vm';
import { defaultOperations, stringify } from '../vendor/devalue/index.js';

// ---------------------------------------------------------------------------
// Passivity taint context
// ---------------------------------------------------------------------------

/**
 * Filled in while serializing with `hardenedStringify(value, reducers,
 * report)`. `tainted` means the serialization executed code the serialized
 * value's owner controls, so it must not be treated as side-effect free.
 */
export interface SerializationPassivityReport {
  tainted: boolean;
  /** Human-readable reasons, capped, for logs/telemetry. */
  reasons: string[];
}

const MAX_TAINT_REASONS = 5;

/**
 * The active report. Module-scoped rather than threaded through devalue:
 * `stringify` is synchronous, and `hardenedStringify` restores the previous
 * value in `finally`, so concurrent async serializations cannot observe each
 * other's context.
 */
let activeReport: SerializationPassivityReport | null = null;

/** Record that value-owned code ran (or is about to run). */
export function taintSerialization(reason: string): void {
  if (!activeReport) return;
  activeReport.tainted = true;
  if (
    activeReport.reasons.length < MAX_TAINT_REASONS &&
    !activeReport.reasons.includes(reason)
  ) {
    activeReport.reasons.push(reason);
  }
}

// ---------------------------------------------------------------------------
// Captured intrinsics
// ---------------------------------------------------------------------------
// Invoking a captured member must never run code an application can replace.
// A capture taken from the host realm can't guarantee that: code that ran
// *before this module loaded* (a polyfill, an instrumentation shim) may
// already have patched the prototype, and the patch would then be invoked
// as "passive" forever. So every ECMAScript intrinsic below is captured
// from a freshly created VM realm that no user code has ever touched. They
// all read internal slots, so they work on values from any realm (host or
// workflow VM) and are unaffected by prototype patching in any realm.
//
// Host web APIs (URL, URLSearchParams, AbortController/AbortSignal,
// Headers, DOMException) do not exist in a bare VM realm and are
// implemented in JavaScript inside Node (so they don't even stringify as
// native code); there is no patch-proof source for them, and they are
// captured from the host realm at module load. The residual trust — shared
// with `util.types` brand checks, the statics the vendored devalue
// dispatches internally, and an `Error.prepareStackTrace` installed before
// this module loaded — is that code loaded before @workflow/core has not
// patched them.

/**
 * Source realm for ECMAScript intrinsics: a fresh VM realm when available,
 * the host realm otherwise (environments without a working `node:vm`, where
 * behavior falls back to plain module-load captures).
 */
const pristine: typeof globalThis = (() => {
  try {
    return runInNewContext('globalThis');
  } catch {
    return globalThis;
  }
})();

// Statics this module itself dispatches during passive reads, captured for
// the same reason as the prototype members: a patched
// `Object.getPrototypeOf` must not run inside a "passive" helper.
const getOwnPropertyDescriptor = pristine.Object.getOwnPropertyDescriptor;
const getPrototypeOf = pristine.Object.getPrototypeOf;
const reflectGet = pristine.Reflect.get;
const reflectHas = pristine.Reflect.has;
const arrayIsArray = pristine.Array.isArray;
const numberIsNaN = pristine.Number.isNaN;

function protoGetter(prototype: object, name: string | symbol) {
  // biome-ignore lint/style/noNonNullAssertion: intrinsic accessors always exist
  return getOwnPropertyDescriptor(prototype, name)!.get!;
}

const DatePrototype = pristine.Date.prototype;
const dateGetDate = DatePrototype.getDate;
const dateGetTime = DatePrototype.getTime;
const dateToISOString = DatePrototype.toISOString;

const mapEntriesIntrinsic = pristine.Map.prototype.entries;
const mapIteratorNext = getPrototypeOf(
  mapEntriesIntrinsic.call(new pristine.Map())
).next as (this: unknown) => IteratorResult<[unknown, unknown]>;
const setValuesIntrinsic = pristine.Set.prototype.values;
const setIteratorNext = getPrototypeOf(
  setValuesIntrinsic.call(new pristine.Set())
).next as (this: unknown) => IteratorResult<unknown>;

const regExpSource = protoGetter(pristine.RegExp.prototype, 'source');
// `RegExp.prototype.flags` is NOT internal-slot-only: the spec has it do
// ordinary Gets of `global`, `ignoreCase`, … on the receiver, which would
// dispatch own getters or patched per-flag accessors. Compose the flags
// string from the individual per-flag getters instead — each of those reads
// only the [[OriginalFlags]] internal slot. Order matches the spec'd
// `flags` getter (d g i m s u v y), so output is byte-identical.
const regExpFlagGetters: ReadonlyArray<
  [flag: string, get: (this: unknown) => unknown]
> = (
  [
    ['d', 'hasIndices'],
    ['g', 'global'],
    ['i', 'ignoreCase'],
    ['m', 'multiline'],
    ['s', 'dotAll'],
    ['u', 'unicode'],
    ['v', 'unicodeSets'],
    ['y', 'sticky'],
  ] as const
).flatMap(([flag, name]) => {
  const get = getOwnPropertyDescriptor(pristine.RegExp.prototype, name)?.get;
  return get ? [[flag, get] as [string, (this: unknown) => unknown]] : [];
});

/** A RegExp's flags string read purely from internal slots. */
export function intrinsicRegExpFlags(value: RegExp): string {
  let flags = '';
  for (const [flag, get] of regExpFlagGetters) {
    if (get.call(value)) flags += flag;
  }
  return flags;
}

const typedArrayPrototype = getPrototypeOf(
  pristine.Uint8Array.prototype
) as object;
const typedArrayBuffer = protoGetter(typedArrayPrototype, 'buffer');
const typedArrayByteOffset = protoGetter(typedArrayPrototype, 'byteOffset');
const typedArrayByteLength = protoGetter(typedArrayPrototype, 'byteLength');
const typedArrayLength = protoGetter(typedArrayPrototype, 'length');
// The %TypedArray% @@toStringTag getter returns the concrete subclass name
// ('Uint8Array', 'Float32Array', …) from internal slots.
const typedArrayTag = protoGetter(typedArrayPrototype, Symbol.toStringTag) as (
  this: unknown
) => string | undefined;

const dataViewBuffer = protoGetter(pristine.DataView.prototype, 'buffer');
const dataViewByteOffset = protoGetter(
  pristine.DataView.prototype,
  'byteOffset'
);
const dataViewByteLength = protoGetter(
  pristine.DataView.prototype,
  'byteLength'
);

const arrayBufferByteLength = protoGetter(
  pristine.ArrayBuffer.prototype,
  'byteLength'
);
const sharedArrayBufferByteLength =
  typeof pristine.SharedArrayBuffer === 'function'
    ? protoGetter(pristine.SharedArrayBuffer.prototype, 'byteLength')
    : undefined;

const numberValueOf = pristine.Number.prototype.valueOf;
const stringValueOf = pristine.String.prototype.valueOf;
const booleanValueOf = pristine.Boolean.prototype.valueOf;
const bigIntValueOf = pristine.BigInt.prototype.valueOf;

// URL / URLSearchParams / Headers / DOMException are host classes that the
// workflow VM receives by reference (see vm/index.ts), so instances from
// either realm carry the host brand and these captured members work on all
// of them. Captured from the host realm at module load — see the
// residual-trust note above.
const urlHref = protoGetter(URL.prototype, 'href');
const urlSearchParamsToString = URLSearchParams.prototype.toString;
// Native AbortController/AbortSignal expose `signal` / `aborted` / `reason`
// as prototype accessors. (The workflow VM's WorkflowAbortController /
// WorkflowAbortSignal use plain data properties instead, which passiveGet
// reads without needing an allowed getter.)
const abortControllerSignal = protoGetter(AbortController.prototype, 'signal');
const abortSignalAborted = protoGetter(AbortSignal.prototype, 'aborted');
const abortSignalReason = protoGetter(AbortSignal.prototype, 'reason');
const headersIteratorIntrinsic = Headers.prototype[Symbol.iterator];
const headersIteratorNext = getPrototypeOf(new Headers()[Symbol.iterator]())
  .next as (this: unknown) => IteratorResult<[string, string]>;
const domExceptionMessage = protoGetter(DOMException.prototype, 'message');
const domExceptionName = protoGetter(DOMException.prototype, 'name');

export const capturedIntrinsics = {
  dateGetDate,
  dateGetTime,
  dateToISOString,
  arrayBufferByteLength,
  typedArrayBuffer,
  typedArrayByteOffset,
  typedArrayByteLength,
  regExpSource,
  urlHref,
  urlSearchParamsToString,
  abortControllerSignal,
  abortSignalAborted,
  abortSignalReason,
  domExceptionMessage,
  domExceptionName,
} as const;

// The helpers below collect into local arrays via index assignment rather
// than `.push`, which would dispatch a patchable Array.prototype method
// mid-serialization.

/** Materialize a Headers' entries without dispatching on the value. */
export function intrinsicHeadersEntries(
  headers: Headers
): Array<[string, string]> {
  const iterator = headersIteratorIntrinsic.call(headers);
  const entries: Array<[string, string]> = [];
  for (;;) {
    const result = headersIteratorNext.call(iterator);
    if (result.done) return entries;
    entries[entries.length] = result.value;
  }
}

/** Materialize a Map's entries without dispatching on the value's realm. */
export function intrinsicMapEntries(
  map: Map<unknown, unknown>
): Array<[unknown, unknown]> {
  const iterator = mapEntriesIntrinsic.call(map);
  const entries: Array<[unknown, unknown]> = [];
  for (;;) {
    const result = mapIteratorNext.call(iterator);
    if (result.done) return entries;
    entries[entries.length] = result.value;
  }
}

/** Materialize a Set's values without dispatching on the value's realm. */
export function intrinsicSetValues(set: Set<unknown>): unknown[] {
  const iterator = setValuesIntrinsic.call(set);
  const values: unknown[] = [];
  for (;;) {
    const result = setIteratorNext.call(iterator);
    if (result.done) return values;
    values[values.length] = result.value;
  }
}

// ---------------------------------------------------------------------------
// Passive property access
// ---------------------------------------------------------------------------

/**
 * Read `key` from `value` with `value[key]` semantics, but without silently
 * executing value-owned code: data properties (own or inherited) are read
 * from their descriptors, and accessor properties taint the report before
 * being invoked exactly as a plain read would. Proxies taint up front —
 * every introspection on them is a trap.
 *
 * `allowedGetter` marks one accessor as known-passive (a captured host
 * intrinsic, e.g. `URL.prototype.href`'s getter): if the property resolves
 * to exactly that getter it is invoked without tainting.
 */
export function passiveGet(
  value: object,
  key: string | symbol,
  allowedGetter?: (this: unknown) => unknown
): unknown {
  let target: object | null = value;
  if (types.isProxy(target)) {
    taintSerialization('proxy');
    return reflectGet(value, key);
  }
  while (target !== null) {
    const descriptor = getOwnPropertyDescriptor(target, key);
    if (descriptor !== undefined) {
      if ('value' in descriptor) return descriptor.value;
      if (descriptor.get === undefined) return undefined;
      if (allowedGetter !== undefined && descriptor.get === allowedGetter) {
        return allowedGetter.call(value);
      }
      taintSerialization(`accessor property "${String(key)}"`);
      return reflectGet(value, key);
    }
    target = getPrototypeOf(target);
    if (target !== null && types.isProxy(target)) {
      taintSerialization('proxy in prototype chain');
      return reflectGet(value, key);
    }
  }
  return undefined;
}

// V8 materializes `error.stack` as an own *accessor* property whose getter
// is a single engine-provided function shared by every error in a realm.
// It formats the stack captured at construction time, which is passive —
// UNLESS that realm's `Error.prepareStackTrace` is set, in which case the
// getter calls it (arbitrary realm-owned code). So a stack read is allowed
// untainted only when (a) the own getter is a known engine stack getter for
// its realm and (b) that realm's `Error.prepareStackTrace` is unset at read
// time.
const hostErrorStackGetter = getOwnPropertyDescriptor(
  new Error(),
  'stack'
)?.get;

interface RealmErrorIntrinsics {
  errorCtor: ErrorConstructor;
  stackGetter: ((this: unknown) => unknown) | undefined;
  /**
   * The realm's `Error.prepareStackTrace` while pristine. Node installs its
   * own default formatter on every realm, so "unset" is wrong to test for —
   * what matters is whether realm code *replaced* the pristine value.
   */
  initialPrepareStackTrace: unknown;
}

// A pristine-realm WeakMap so lookups during serialization dispatch pristine
// prototype methods, not patchable host ones.
const realmErrorIntrinsics = new pristine.WeakMap() as WeakMap<
  object,
  RealmErrorIntrinsics
>;

/**
 * The engine resolves `Error.prepareStackTrace` with an ordinary Get on the
 * realm's Error constructor, so an *inherited* formatter (e.g. planted on
 * that realm's `Function.prototype`) is honored too. Replicate the lookup
 * passively: walk own descriptors up the constructor's prototype chain and
 * return the first data value found. Anything that cannot be read without
 * running realm code (an accessor, a Proxy on the chain) resolves to a
 * fresh object, which never matches any captured baseline — the stack read
 * then taints, which is right, since invoking the engine stack getter would
 * run that code.
 */
function readPrepareStackTrace(ctor: ErrorConstructor): unknown {
  let node: object | null = ctor;
  while (node !== null) {
    if (types.isProxy(node)) return { unreadable: 'proxy' };
    const descriptor = getOwnPropertyDescriptor(node, 'prepareStackTrace');
    if (descriptor !== undefined) {
      if ('value' in descriptor) return descriptor.value;
      return descriptor;
    }
    node = getPrototypeOf(node);
  }
  return undefined;
}

/**
 * Capture a realm's engine `Error` intrinsics from its global object. MUST
 * be called while the realm is pristine (before any realm code evaluates):
 * it constructs `realmGlobal.Error`, so calling it later would run whatever
 * that binding was replaced with. workflow.ts calls this right after
 * creating the VM context. Errors from unregistered realms simply taint on
 * stack reads — a safe fallback, never an unsafe one.
 */
export function registerRealmSerializationIntrinsics(
  realmGlobal: object
): void {
  if (realmErrorIntrinsics.has(realmGlobal)) return;
  const errorCtor = (realmGlobal as { Error?: ErrorConstructor }).Error;
  if (typeof errorCtor !== 'function') return;
  const stackGetter = getOwnPropertyDescriptor(
    new errorCtor('probe'),
    'stack'
  )?.get;
  realmErrorIntrinsics.set(realmGlobal, {
    errorCtor,
    stackGetter,
    initialPrepareStackTrace: readPrepareStackTrace(errorCtor),
  });
}

// Register the host realm at module load. Best-effort: an
// `Error.prepareStackTrace` installed *before* this module loaded becomes
// the pristine baseline (see the residual-trust note above) — a mismatch in
// the other direction only ever taints, which is safe.
registerRealmSerializationIntrinsics(globalThis);

/** Whether realm code replaced the realm's pristine `prepareStackTrace`. */
function prepareStackTraceReplaced(realm: RealmErrorIntrinsics): boolean {
  return (
    readPrepareStackTrace(realm.errorCtor) !== realm.initialPrepareStackTrace
  );
}

/**
 * Read `error.stack` passively, allowing the engine's realm-wide lazy stack
 * getter (for the host realm and for `global`'s registered realm) without
 * tainting — unless that realm set `Error.prepareStackTrace`, which the
 * getter would execute.
 */
export function passiveErrorStackRead(
  error: object,
  global: object = globalThis
): unknown {
  const descriptor = getOwnPropertyDescriptor(error, 'stack');
  if (descriptor !== undefined) {
    if ('value' in descriptor) return descriptor.value;
    if (descriptor.get === undefined) return undefined;
    let owner: RealmErrorIntrinsics | undefined;
    if (
      hostErrorStackGetter !== undefined &&
      descriptor.get === hostErrorStackGetter
    ) {
      owner = realmErrorIntrinsics.get(globalThis);
    } else {
      const realm = realmErrorIntrinsics.get(global);
      if (
        realm?.stackGetter !== undefined &&
        descriptor.get === realm.stackGetter
      ) {
        owner = realm;
      }
    }
    if (owner !== undefined && !prepareStackTraceReplaced(owner)) {
      return descriptor.get.call(error);
    }
    taintSerialization(
      owner !== undefined ? 'Error.prepareStackTrace' : 'stack accessor'
    );
    return reflectGet(error, 'stack');
  }
  return passiveGet(error, 'stack');
}

/**
 * `value instanceof C` without dispatching `C[Symbol.hasInstance]`: walks
 * the value's prototype chain looking for `prototype`. Matches instanceof
 * for ordinary classes; ignores hasInstance spoofing by design.
 *
 * Calling `Object.getPrototypeOf` on a Proxy runs its `getPrototypeOf` trap
 * (user code), so any Proxy encountered on the chain taints before the walk
 * touches it. The walk still proceeds — the taint demotes retention, after
 * which running the trap is as safe as it was pre-retention.
 */
export function isInstanceOfPrototype(
  value: unknown,
  prototype: object | undefined | null
): boolean {
  if (!prototype || value === null || typeof value !== 'object') return false;
  let node: object = value;
  for (;;) {
    if (types.isProxy(node)) {
      taintSerialization('proxy in prototype chain');
    }
    const proto: object | null = getPrototypeOf(node);
    if (proto === null) return false;
    if (proto === prototype) return true;
    node = proto;
  }
}

/**
 * `key in value` without running a Proxy `has` trap untainted: walks own
 * descriptors up the prototype chain (same inherited-property semantics as
 * `in`), tainting and falling back to `Reflect.has` when a Proxy appears.
 */
/**
 * Resolved by `passivePeek` when reading the property would require running
 * value-owned code (an accessor, or a Proxy anywhere on the chain).
 */
export const PEEK_BLOCKED: unique symbol = Symbol('workflow.peekBlocked');

/**
 * Resolve `key` along `value`'s prototype chain with Get semantics but
 * without invoking *anything*: a data property resolves to its value, a
 * missing property to undefined, and anything that would need to run code
 * to read (accessor property, Proxy on the chain) resolves to
 * `PEEK_BLOCKED`. Unlike `passiveGet`, this neither taints nor performs the
 * read — callers use it to choose between a known-passive fast path and a
 * tainted dynamic dispatch that must run exactly once.
 */
export function passivePeek(value: object, key: string | symbol): unknown {
  let target: object | null = value;
  while (target !== null) {
    if (types.isProxy(target)) return PEEK_BLOCKED;
    const descriptor = getOwnPropertyDescriptor(target, key);
    if (descriptor !== undefined) {
      return 'value' in descriptor ? descriptor.value : PEEK_BLOCKED;
    }
    target = getPrototypeOf(target);
  }
  return undefined;
}

export function passiveHas(value: object, key: string | symbol): boolean {
  let target: object | null = value;
  if (types.isProxy(target)) {
    taintSerialization('proxy');
    return reflectHas(value, key);
  }
  while (target !== null) {
    if (getOwnPropertyDescriptor(target, key) !== undefined) {
      return true;
    }
    target = getPrototypeOf(target);
    if (target !== null && types.isProxy(target)) {
      taintSerialization('proxy in prototype chain');
      return reflectHas(target, key);
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Hardened operations
// ---------------------------------------------------------------------------

/**
 * Brand-based replacement for devalue's `Object.prototype.toString`-derived
 * tag. Engine-level checks classify every built-in devalue's tag switch
 * distinguishes; anything else falls back to a passive `Symbol.toStringTag`
 * probe that mirrors `Object.prototype.toString` semantics (a string data
 * property is honored; a non-string or absent tag classifies as
 * 'Object'/'Function'; an accessor taints and is read like the original).
 */
function hardenedTag(value: object): string {
  if (arrayIsArray(value)) return 'Array';
  if (types.isDate(value)) return 'Date';
  if (types.isMap(value)) return 'Map';
  if (types.isSet(value)) return 'Set';
  if (types.isRegExp(value)) return 'RegExp';
  if (types.isTypedArray(value)) {
    return typedArrayTag.call(value) ?? 'Object';
  }
  if (types.isDataView(value)) return 'DataView';
  if (types.isArrayBuffer(value)) return 'ArrayBuffer';
  if (types.isSharedArrayBuffer(value)) return 'SharedArrayBuffer';
  if (types.isNumberObject(value)) return 'Number';
  if (types.isStringObject(value)) return 'String';
  if (types.isBooleanObject(value)) return 'Boolean';
  if (types.isBigIntObject(value)) return 'BigInt';
  if (types.isPromise(value)) return 'Promise';
  const tag = passiveGet(value, Symbol.toStringTag);
  if (typeof tag === 'string') return tag;
  return typeof value === 'function' ? 'Function' : 'Object';
}

/**
 * The `operations` override passed to every `stringify` call. Only the
 * operations that could execute value-owned code (or dispatch through a
 * patchable prototype) are replaced; structural ones (`arrayIndices`,
 * `hasOwnIndex`, …) already use passive engine-level primitives in devalue
 * itself. (`objectShape` is wrapped only to taint Proxy prototypes.)
 */
const hardenedOperations = {
  typeOf(value: unknown): string {
    if (value === null) return 'null';
    const type = typeof value;
    if (
      (type === 'object' || type === 'function') &&
      types.isProxy(value as object)
    ) {
      taintSerialization('proxy');
    }
    return type;
  },

  tag: hardenedTag,

  isThenable(value: object): boolean {
    return typeof passiveGet(value, 'then') === 'function';
  },

  unbox(value: object): unknown {
    if (types.isNumberObject(value)) return numberValueOf.call(value);
    if (types.isStringObject(value)) return stringValueOf.call(value);
    if (types.isBooleanObject(value)) return booleanValueOf.call(value);
    if (types.isBigIntObject(value)) return bigIntValueOf.call(value);
    // Only reachable for a non-boxed object whose Symbol.toStringTag spoofs
    // 'Number'/'String'/'Boolean'/'BigInt'. Preserve the default behavior
    // (dispatch valueOf, which the value's owner controls) and taint.
    taintSerialization('valueOf dispatch');
    return (value as { valueOf(): unknown }).valueOf();
  },

  // The tag-dispatched operations below can be reached without the matching
  // brand: `hardenedTag` falls back to a (data-property) Symbol.toStringTag
  // probe, so an object *tagged* 'Map'/'Date'/… lands in the corresponding
  // case just like it does under stock devalue's Object.prototype.toString.
  // The captured intrinsics would throw a brand-check TypeError on such a
  // value where stock devalue serialized it dynamically, so each operation
  // brand-checks first and otherwise taints + preserves the stock behavior.

  dateISO(value: Date): string {
    if (types.isDate(value)) {
      return numberIsNaN(dateGetDate.call(value))
        ? ''
        : dateToISOString.call(value);
    }
    taintSerialization('Date tag without Date brand');
    return defaultOperations.dateISO(value);
  },

  toStringValue(value: object): string {
    // Reachable only for URL/URLSearchParams/Temporal.* tags. Genuine URLs
    // and URLSearchParams are claimed by reducers before devalue's tag
    // switch, so whatever lands here stringifies through code its owner can
    // control (a Temporal polyfill's toString, a spoofed toStringTag).
    taintSerialization('toString dispatch');
    return (value as { toString(): string }).toString();
  },

  regExp(value: RegExp): { source: string; flags: string } {
    if (types.isRegExp(value)) {
      return {
        source: regExpSource.call(value) as string,
        flags: intrinsicRegExpFlags(value),
      };
    }
    taintSerialization('RegExp tag without RegExp brand');
    return defaultOperations.regExp(value);
  },

  setValues(value: Set<unknown>): Iterable<unknown> {
    if (types.isSet(value)) return intrinsicSetValues(value);
    taintSerialization('Set tag without Set brand');
    // Stock behavior: stringify iterates the value itself.
    return defaultOperations.setValues(value);
  },

  mapEntries(value: Map<unknown, unknown>): Iterable<[unknown, unknown]> {
    if (types.isMap(value)) return intrinsicMapEntries(value);
    taintSerialization('Map tag without Map brand');
    return defaultOperations.mapEntries(value);
  },

  arrayLength(value: unknown[]): number {
    if (!arrayIsArray(value)) {
      // Tag-spoofed 'Array': the serializer loop coerces this length (which
      // can run an object-valued length's valueOf/Symbol.toPrimitive), so
      // taint and preserve the stock read.
      taintSerialization('Array tag without Array brand');
      return defaultOperations.arrayLength(value);
    }
    // On a genuine array `length` is an own (non-configurable) data property.
    return passiveGet(value, 'length') as number;
  },

  arrayBuffer(value: ArrayBuffer): ArrayBuffer {
    if (!types.isArrayBuffer(value) && !types.isSharedArrayBuffer(value)) {
      // Tag-spoofed 'ArrayBuffer': base64 encoding coerces it through
      // Uint8Array/Buffer, which can execute value-owned code.
      taintSerialization('ArrayBuffer tag without ArrayBuffer brand');
    }
    return defaultOperations.arrayBuffer(value);
  },

  viewInfo(value: ArrayBufferView): {
    buffer: ArrayBufferLike;
    byteOffset: number;
    byteLength: number;
    length?: number;
    bufferByteLength: number;
  } {
    const isDataView = types.isDataView(value);
    if (!isDataView && !types.isTypedArray(value)) {
      taintSerialization('view tag without view brand');
      return defaultOperations.viewInfo(value);
    }
    const buffer = (
      isDataView ? dataViewBuffer.call(value) : typedArrayBuffer.call(value)
    ) as ArrayBufferLike;
    const bufferByteLength = (
      types.isSharedArrayBuffer(buffer)
        ? // biome-ignore lint/style/noNonNullAssertion: a SharedArrayBuffer-branded value implies the intrinsic exists
          sharedArrayBufferByteLength!.call(buffer)
        : arrayBufferByteLength.call(buffer)
    ) as number;
    return {
      buffer,
      byteOffset: (isDataView
        ? dataViewByteOffset.call(value)
        : typedArrayByteOffset.call(value)) as number,
      byteLength: (isDataView
        ? dataViewByteLength.call(value)
        : typedArrayByteLength.call(value)) as number,
      // Undefined for DataViews, matching `value.length` under the default
      // operations (the byte format encodes it as-is for subviews).
      length: isDataView ? undefined : (typedArrayLength.call(value) as number),
      bufferByteLength,
    };
  },

  objectShape(value: object) {
    // The default implementation reads the value's prototype (and the
    // prototype's own property names) to detect plain objects. Both run
    // traps when the *prototype* is a Proxy — the value itself already
    // taints at typeOf — so taint that case before the default touches it.
    // Deeper chain members are only read as values, never trapped. Skip the
    // probe for Proxy values (already tainted): it would run their
    // getPrototypeOf trap one extra time relative to stock devalue.
    if (!types.isProxy(value)) {
      const proto = getPrototypeOf(value);
      if (proto !== null && types.isProxy(proto)) {
        taintSerialization('proxy in prototype chain');
      }
    }
    return defaultOperations.objectShape(value);
  },

  get(value: object, key: string | number): unknown {
    if (types.isProxy(value)) {
      // A Proxy's `get` trap may return something other than the target's
      // descriptor value (stock devalue reads `value[key]` through the
      // trap). Taint — typeOf already did, but `get` can also be reached
      // through reducer-produced wrappers — and preserve the stock read.
      taintSerialization('proxy');
      return reflectGet(value, key);
    }
    // devalue only reads keys it discovered via Object.keys/Object.hasOwn.
    // Reading own data properties from the descriptor never executes code;
    // own accessors taint and then run exactly as a plain `value[key]` read
    // would.
    const descriptor = getOwnPropertyDescriptor(value, key);
    if (descriptor !== undefined && 'value' in descriptor) {
      return descriptor.value;
    }
    if (descriptor !== undefined && descriptor.get === undefined) {
      return undefined;
    }
    taintSerialization(`getter for "${String(key)}"`);
    return reflectGet(value, key);
  },
};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

const stringifyOptions = { operations: hardenedOperations };

/**
 * `devalue.stringify` with the hardened operations, optionally recording
 * passivity taint into `report`. Every stringify call in @workflow/core goes
 * through here so the wire format cannot depend on the caller.
 */
export function hardenedStringify(
  value: unknown,
  reducers: Record<string, (value: any) => any>,
  report?: SerializationPassivityReport
): string {
  const previous = activeReport;
  activeReport = report ?? null;
  try {
    return stringify(value, reducers, stringifyOptions);
  } finally {
    activeReport = previous;
  }
}

/**
 * Run `fn` with `report` active so `passiveGet`/`taintSerialization` calls
 * outside of a `hardenedStringify` invocation still record into it. Needed
 * around synchronous reducer *construction* (which resolves prototypes off
 * the workflow global — see `resolvePrototype` in serialization.ts) so a
 * getter or proxy planted on a global constructor taints the boundary.
 */
export function withPassivityReport<T>(
  report: SerializationPassivityReport | undefined,
  fn: () => T
): T {
  if (report === undefined) return fn();
  const previous = activeReport;
  activeReport = report;
  try {
    return fn();
  } finally {
    activeReport = previous;
  }
}
