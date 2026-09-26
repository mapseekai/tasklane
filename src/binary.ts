import { integer, RuntimeError } from './errors.js';

const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const typedBuffer = Object.getOwnPropertyDescriptor(typedArrayPrototype, 'buffer')!.get!;
const dataBuffer = Object.getOwnPropertyDescriptor(DataView.prototype, 'buffer')!.get!;
const mapSize = Object.getOwnPropertyDescriptor(Map.prototype, 'size')!.get!;
const setSize = Object.getOwnPropertyDescriptor(Set.prototype, 'size')!.get!;
const arrayBufferBytes = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength')!.get!;
const sharedBufferBytes =
  typeof SharedArrayBuffer === 'undefined'
    ? undefined
    : Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, 'byteLength')!.get!;
const regexpSource = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')!.get!;

/** Fresh native view over the same storage; never retains caller-added view properties. */
export function snapshotBinaryView(value: ArrayBufferView): ArrayBufferView {
  if (!ArrayBuffer.isView(value))
    throw new RuntimeError(
      'INVALID_ARGUMENT',
      'Binary cache values must be typed arrays or DataView',
    );
  const dataView = value instanceof DataView;
  const proto = dataView ? DataView.prototype : typedArrayPrototype;
  const buffer = (dataView ? dataBuffer : typedBuffer).call(value);
  const offset = Object.getOwnPropertyDescriptor(proto, 'byteOffset')!.get!.call(value) as number;
  if (dataView)
    return new DataView(
      buffer,
      offset,
      Object.getOwnPropertyDescriptor(proto, 'byteLength')!.get!.call(value) as number,
    );
  // Use the intrinsic tag, not a caller-controlled constructor or Symbol.toStringTag.
  const name = Object.getOwnPropertyDescriptor(typedArrayPrototype, Symbol.toStringTag)!.get!.call(
    value,
  );
  const constructors = {
    Int8Array,
    Uint8Array,
    Uint8ClampedArray,
    Int16Array,
    Uint16Array,
    Int32Array,
    Uint32Array,
    Float32Array,
    Float64Array,
    BigInt64Array,
    BigUint64Array,
  };
  const ctor = constructors[name as keyof typeof constructors];
  if (!ctor) throw new RuntimeError('INVALID_ARGUMENT', 'Unsupported binary view type');
  return new ctor(
    buffer,
    offset,
    Object.getOwnPropertyDescriptor(typedArrayPrototype, 'length')!.get!.call(value) as number,
  );
}

export interface TraversalLimits {
  maxObjects?: number;
  /** Inspect custom view fields for resident data; may enumerate typed-array indices. */
  resident?: boolean;
  maxEntries?: number;
  maxPending?: number;
  /** Separate from binary budgets; UTF-16 strings and scalar metadata are charged conservatively. */
  maxMetadataBytes?: number;
}

/** Binary backing stores only. Metadata has independent work/size limits, not a JS heap guarantee.
 * Inert values only: accessors and custom prototypes are rejected. Proxies are not supported.
 */
export function binaryByteLength(value: unknown, limits: TraversalLimits = {}): number {
  return measure(value, limits).binary;
}

/** Deterministic data accounting, including resident metadata. Not a JS heap estimator. */
export function dataByteLength(value: unknown, limits: TraversalLimits = {}): number {
  const bytes = measure(value, limits);
  return bytes.binary + bytes.metadata;
}
function measure(value: unknown, limits: TraversalLimits): { binary: number; metadata: number } {
  if (!limits || typeof limits !== 'object' || Array.isArray(limits)) {
    throw new RuntimeError('INVALID_ARGUMENT', 'Traversal limits must be an options object');
  }
  const maxObjects = integer(limits.maxObjects ?? 100_000, 'maxObjects', 1);
  const maxEntries = integer(limits.maxEntries ?? 1_000_000, 'maxEntries', 1);
  const maxPending = integer(limits.maxPending ?? maxObjects, 'maxPending', 1);
  const maxMetadataBytes = integer(limits.maxMetadataBytes ?? 64 * 1024 ** 2, 'maxMetadataBytes');
  const seen = new Set<object>();
  const pending = new Set<object>();
  const buffers = new Set<object>();
  const stack: object[] = [];
  let bytes = 0,
    entries = 0,
    metadata = 0;
  const fail = () => {
    throw new RuntimeError('BUDGET_EXCEEDED', 'Packet metadata traversal limit exceeded');
  };
  const charge = (count: number) => {
    metadata += count;
    if (metadata > maxMetadataBytes) fail();
  };
  const push = (item: unknown) => {
    if (typeof item === 'function' || typeof item === 'symbol')
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Packets require cloneable data; use setResource for opaque state',
      );
    if (item === null || typeof item !== 'object') {
      if (typeof item === 'string') charge(item.length * 2);
      else if (typeof item === 'boolean') charge(1);
      else if (typeof item === 'number') charge(8);
      else if (typeof item === 'bigint') {
        if (item < -(1n << 63n) || item >= 1n << 64n)
          throw new RuntimeError('INVALID_ARGUMENT', 'Packet BigInt metadata must fit in 64 bits');
        charge(8);
      }
      return;
    }
    if (seen.has(item) || pending.has(item)) return;
    // A fixed structural charge prevents collections of empty objects from being free.
    if (
      !ArrayBuffer.isView(item) &&
      !(item instanceof ArrayBuffer) &&
      !(typeof SharedArrayBuffer !== 'undefined' && item instanceof SharedArrayBuffer)
    )
      charge(16);
    if (seen.size + pending.size >= maxObjects || pending.size >= maxPending) fail();
    pending.add(item);
    stack.push(item);
  };
  const edge = (item: unknown) => {
    if (++entries > maxEntries) fail();
    push(item);
  };
  push(value);
  while (stack.length) {
    const item = stack.pop()!;
    pending.delete(item);
    seen.add(item);
    const buffer: unknown = ArrayBuffer.isView(item)
      ? item instanceof DataView
        ? dataBuffer.call(item)
        : typedBuffer.call(item)
      : item;
    if (
      buffer instanceof ArrayBuffer ||
      (typeof SharedArrayBuffer !== 'undefined' && buffer instanceof SharedArrayBuffer)
    ) {
      if (!buffers.has(buffer)) {
        buffers.add(buffer);
        bytes +=
          buffer instanceof ArrayBuffer
            ? arrayBufferBytes.call(buffer)
            : sharedBufferBytes!.call(buffer);
        integer(bytes, 'binaryByteLength');
      }
      // Typed-array indices are backing-store data, not metadata. Additional own enumerable
      // fields on other built-ins are still visited (important for resident cache values).
      if (ArrayBuffer.isView(item)) {
        if (!limits.resident) continue;
        for (const key in item) {
          if (!Object.hasOwn(item, key)) continue;
          if (!(item instanceof DataView) && /^(0|[1-9][0-9]*)$/.test(key)) continue;
          const descriptor = Object.getOwnPropertyDescriptor(item, key);
          if (!descriptor || !('value' in descriptor))
            throw new RuntimeError(
              'INVALID_ARGUMENT',
              'Accessor properties are not allowed in packets',
            );
          charge(key.length * 2);
          edge(descriptor.value);
        }
        continue;
      }
    } else if (item instanceof Map) {
      if (mapSize.call(item) > Math.floor((maxEntries - entries) / 2)) fail();
      for (const [key, val] of Map.prototype.entries.call(item)) {
        edge(key);
        edge(val);
      }
    } else if (item instanceof Set) {
      if (setSize.call(item) > maxEntries - entries) fail();
      for (const val of Set.prototype.values.call(item)) edge(val);
    } else {
      const proto: unknown = Object.getPrototypeOf(item);
      if (
        !(item instanceof Date) &&
        !(item instanceof RegExp) &&
        proto !== Object.prototype &&
        proto !== null &&
        !Array.isArray(item)
      ) {
        throw new RuntimeError(
          'INVALID_ARGUMENT',
          'Packets require plain records, collections and typed buffers',
        );
      }
      if (item instanceof RegExp) charge(regexpSource.call(item).length * 2);
      if (Array.isArray(item) && item.length > maxEntries - entries) fail();
    }
    // Incremental consumption avoids our own full key array and full layer work stack.
    // JS engines may still allocate internally during enumeration; this is not an allocation sandbox.
    for (const key in item) {
      if (!Object.hasOwn(item, key)) continue;
      if (entries >= maxEntries) fail();
      const descriptor = Object.getOwnPropertyDescriptor(item, key);
      if (!descriptor || !('value' in descriptor))
        throw new RuntimeError(
          'INVALID_ARGUMENT',
          'Accessor properties are not allowed in packets',
        );
      charge(key.length * 2);
      edge(descriptor.value);
    }
  }
  return { binary: bytes, metadata };
}

/** Build an explicit, deduplicated list of owned buffers. Views must cover their
 * entire backing store. Passing a buffer explicitly asserts ownership of ALL views.
 * Never use a pooled Node Buffer or a buffer belonging to a live editable dataset.
 */
export function transferBuffers(
  ...values: readonly (ArrayBuffer | ArrayBufferView)[]
): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const value of values) {
    const buffer = ArrayBuffer.isView(value) ? value.buffer : value;
    if (!(buffer instanceof ArrayBuffer)) {
      throw new RuntimeError('INVALID_ARGUMENT', 'Only owned ArrayBuffers can be transferred');
    }
    if (
      ArrayBuffer.isView(value) &&
      (value.byteOffset !== 0 || value.byteLength !== buffer.byteLength)
    ) {
      throw new RuntimeError('INVALID_ARGUMENT', 'A partial view does not own its backing buffer');
    }
    buffers.add(buffer);
  }
  return [...buffers];
}

/** Explicit ownership-handoff spelling; ownership of external aliases remains a caller contract. */
export const transferOwnedBuffers = transferBuffers;
