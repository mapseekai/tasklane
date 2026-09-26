import { integer, RuntimeError } from './errors.js';

type BufferStore = ArrayBuffer | SharedArrayBuffer;
export type Packet =
  | { kind: 'scalar'; value: null | undefined | boolean | number | bigint | string }
  | { kind: 'binary'; value: BufferStore | ArrayBufferView }
  | { kind: 'graph'; metadata: string; buffers: BufferStore[]; blobs: Blob[] };
const MAX_NODES = 100_000;
const MAX_EDGES = 1_000_000;
const MAX_METADATA = 64 * 1024 ** 2;
const MAX_BUFFERS = 4096;
const MAX_BLOBS = 256;
const isBlob = (value: unknown): value is Blob =>
  typeof Blob !== 'undefined' && value instanceof Blob;
const blobSize = (value: Blob): number => intrinsic(Blob.prototype, 'size', value);
const blobType = (value: Blob): string =>
  Object.getOwnPropertyDescriptor(Blob.prototype, 'type')!.get!.call(value) as string;
const typedProto = Object.getPrototypeOf(Uint8Array.prototype);
const intrinsic = (proto: object, name: string, value: object): number =>
  Object.getOwnPropertyDescriptor(proto, name)!.get!.call(value) as number;
const viewBuffer = (view: ArrayBufferView): BufferStore =>
  Object.getOwnPropertyDescriptor(
    view instanceof DataView ? DataView.prototype : typedProto,
    'buffer',
  )!.get!.call(view) as BufferStore;
const bufferBytes = (buffer: BufferStore) =>
  intrinsic(
    buffer instanceof ArrayBuffer ? ArrayBuffer.prototype : SharedArrayBuffer.prototype,
    'byteLength',
    buffer,
  );
const views = {
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
  DataView,
};
type Token =
  | null
  | boolean
  | number
  | string
  | ['r', number]
  | ['u']
  | ['n', string]
  | ['i', string];
type Node = {
  type: string;
  props?: [string, Token][];
  items?: Token[];
  length?: number;
  buffer?: number;
  blob?: number;
  name?: string;
  lastModified?: number;
  offset?: number;
  size?: number;
  value?: string;
};
const failure = (message: string): never => {
  throw new RuntimeError('PROTOCOL_ERROR', message);
};
const isBuffer = (v: unknown): v is BufferStore =>
  v instanceof ArrayBuffer ||
  (typeof SharedArrayBuffer !== 'undefined' && v instanceof SharedArrayBuffer);
function scalarBytes(value: unknown): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === 'string') {
    if (value.length * 2 > MAX_METADATA)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Scalar metadata exceeds limit');
    return value.length * 2;
  }
  if (typeof value === 'boolean') return 1;
  if (typeof value === 'number') return 8;
  if (typeof value === 'bigint' && value >= -(1n << 63n) && value < 1n << 64n) return 8;
  return failure('Unsupported packet scalar');
}

/** Shallow wire validation: never walks a received application object graph. */
export function packetBytes(raw: unknown): number {
  if (!raw || typeof raw !== 'object') return failure('Missing packet');
  const packet = raw as Packet;
  if (packet.kind === 'scalar') return scalarBytes(packet.value);
  if (packet.kind === 'binary') {
    const value = packet.value;
    const buffer = ArrayBuffer.isView(value) ? viewBuffer(value) : value;
    if (!isBuffer(buffer)) return failure('Invalid binary packet');
    return bufferBytes(buffer);
  }
  if (
    packet.kind !== 'graph' ||
    typeof packet.metadata !== 'string' ||
    !Array.isArray(packet.buffers) ||
    packet.buffers.length > MAX_BUFFERS ||
    !Array.isArray(packet.blobs) ||
    packet.blobs.length > MAX_BLOBS
  )
    return failure('Invalid graph packet');
  let bytes = packet.metadata.length * 2;
  if (bytes > MAX_METADATA)
    throw new RuntimeError('BUDGET_EXCEEDED', 'Packet metadata exceeds limit');
  packetBlobBytes(packet);
  for (const blob of packet.blobs) bytes += 64 + blobType(blob).length * 2;
  if (bytes > MAX_METADATA)
    throw new RuntimeError('BUDGET_EXCEEDED', 'Packet attachment metadata exceeds limit');
  const seen = new Set<BufferStore>();
  for (const buffer of packet.buffers) {
    if (!isBuffer(buffer) || seen.has(buffer)) return failure('Invalid or duplicate backing store');
    seen.add(buffer);
    bytes += bufferBytes(buffer);
    integer(bytes, 'packet bytes');
  }
  return bytes;
}

/** Blob/File are cloneable attachments, never transferable ownership. */
export function validateBlobTransfers(transfer: readonly Transferable[] | undefined): void {
  if (transfer?.some(isBlob))
    throw new RuntimeError('INVALID_ARGUMENT', 'Blob/File cannot appear in a transfer list');
}

/** Logical attachment bytes, separate from packet/heap accounting. Repeated references count once. */
export function packetBlobBytes(packet: Packet): number {
  if (packet.kind !== 'graph') return 0;
  if (!Array.isArray(packet.blobs) || packet.blobs.length > MAX_BLOBS)
    return failure('Invalid blob table');
  const seen = new Set<Blob>();
  let bytes = 0;
  for (const blob of packet.blobs) {
    if (!isBlob(blob) || seen.has(blob)) return failure('Invalid or duplicate blob attachment');
    seen.add(blob);
    bytes += blobSize(blob);
    integer(bytes, 'blob bytes');
  }
  return bytes;
}

export function checkBlobLimit(bytes: number, limit: number, option = 'blobLimits'): void {
  if (bytes > limit)
    throw new RuntimeError(
      'BUDGET_EXCEEDED',
      `Blob/File attachments require ${bytes} bytes; ${option} allows ${limit} bytes. Set ${option} to cover the attachment sizes (default: 0).`,
    );
}

/** Encode once at the sender. Metadata is a bounded flat graph; buffers retain transfer semantics. */
export function encodePacket(
  value: unknown,
  limit = Number.MAX_SAFE_INTEGER,
  maxBlobBytes = Number.MAX_SAFE_INTEGER,
  blobLimitName = 'blobLimits',
): Packet {
  integer(limit, 'packet limit');
  integer(maxBlobBytes, 'blob limit');
  const check = (packet: Packet): Packet => {
    if (packetBytes(packet) > limit)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Packet exceeds reserved bytes');
    checkBlobLimit(packetBlobBytes(packet), maxBlobBytes, blobLimitName);
    return packet;
  };
  if (value === null || typeof value !== 'object')
    return check({ kind: 'scalar', value: value as Extract<Packet, { kind: 'scalar' }>['value'] });
  if (isBuffer(value) || ArrayBuffer.isView(value)) return check({ kind: 'binary', value });
  const ids = new Map<object, number>();
  const pending: object[] = [];
  const nodes: Node[] = [];
  const buffers: BufferStore[] = [];
  const blobs: Blob[] = [];
  let logicalBlobBytes = 0;
  const bufferIds = new Map<BufferStore, number>();
  let edges = 0,
    lowerBound = 0;
  const charge = (n: number) => {
    lowerBound += n;
    if (lowerBound > limit || lowerBound > MAX_METADATA)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Metadata exceeds reserved bytes');
  };
  const token = (v: unknown): Token => {
    charge(2);
    if (++edges > MAX_EDGES) throw new RuntimeError('BUDGET_EXCEEDED', 'Too many packet entries');
    if (v === undefined) return ['u'];
    if (v === null || typeof v === 'boolean') return v;
    if (typeof v === 'string') {
      charge(v.length * 2);
      return v;
    }
    if (typeof v === 'number')
      return Number.isFinite(v) && !Object.is(v, -0)
        ? v
        : ['n', String(v) === '0' ? '-0' : String(v)];
    if (typeof v === 'bigint') {
      scalarBytes(v);
      return ['i', String(v)];
    }
    if (typeof v !== 'object') return failure('Packets require inert cloneable data');
    let id = ids.get(v);
    if (id === undefined) {
      if (ids.size >= MAX_NODES)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Too many packet objects');
      charge(20);
      id = ids.size;
      ids.set(v, id);
      pending.push(v);
      nodes.push({ type: '' });
    }
    return ['r', id];
  };
  const backing = (buffer: BufferStore) => {
    let id = bufferIds.get(buffer);
    if (id !== undefined) return id;
    if (buffers.length >= MAX_BUFFERS)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Too many packet buffers');
    id = buffers.length;
    bufferIds.set(buffer, id);
    buffers.push(buffer);
    return id;
  };
  const root = token(value);
  for (let cursor = 0; cursor < pending.length; cursor++) {
    const item = pending[cursor]!;
    let node: Node;
    if (isBuffer(item)) {
      node = { type: 'buffer', buffer: backing(item) };
    } else if (ArrayBuffer.isView(item)) {
      const type =
        Object.keys(views).find((name) => item instanceof views[name as keyof typeof views]) ?? '';
      if (!Object.hasOwn(views, type)) return failure('Unsupported binary view');
      node = {
        type,
        buffer: backing(viewBuffer(item)),
        offset: intrinsic(
          item instanceof DataView ? DataView.prototype : typedProto,
          'byteOffset',
          item,
        ),
        size: intrinsic(
          item instanceof DataView ? DataView.prototype : typedProto,
          'byteLength',
          item,
        ),
      };
    } else if (isBlob(item)) {
      if (blobs.length >= MAX_BLOBS)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Too many blob attachments');
      logicalBlobBytes += blobSize(item);
      integer(logicalBlobBytes, 'blob bytes');
      checkBlobLimit(logicalBlobBytes, maxBlobBytes, blobLimitName);
      charge(64 + blobType(item).length * 2);
      node = { type: 'blob', blob: blobs.length };
      if (typeof File !== 'undefined' && item instanceof File) {
        const name = Object.getOwnPropertyDescriptor(File.prototype, 'name')!.get!.call(
          item,
        ) as string;
        const lastModified = intrinsic(File.prototype, 'lastModified', item);
        charge(name.length * 2 + 16);
        node = { type: 'file', blob: blobs.length, name, lastModified };
      }
      // Normalize File to Blob: Node's structured clone does not preserve File metadata.
      blobs.push(Blob.prototype.slice.call(item, 0, blobSize(item), blobType(item)) as Blob);
    } else if (item instanceof Map) {
      if (intrinsic(Map.prototype, 'size', item) * 2 > MAX_EDGES - edges)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Too many map entries');
      node = { type: 'map', items: [] };
      for (const [k, v] of Map.prototype.entries.call(item)) node.items!.push(token(k), token(v));
    } else if (item instanceof Set) {
      if (intrinsic(Set.prototype, 'size', item) > MAX_EDGES - edges)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Too many set entries');
      node = { type: 'set', items: [] };
      for (const v of Set.prototype.values.call(item)) node.items!.push(token(v));
    } else if (item instanceof Date) {
      node = { type: 'date', value: String(Date.prototype.getTime.call(item)) };
    } else if (item instanceof RegExp) {
      const source = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')!.get!.call(
        item,
      ) as string;
      const flags = [
        'hasIndices',
        'global',
        'ignoreCase',
        'multiline',
        'dotAll',
        'unicode',
        'unicodeSets',
        'sticky',
      ]
        .map((name, i) =>
          Object.getOwnPropertyDescriptor(RegExp.prototype, name)?.get?.call(item)
            ? 'dgimsuvy'[i]
            : '',
        )
        .join('');
      charge(source.length * 2);
      node = { type: 'regexp', value: JSON.stringify([source, flags]) };
    } else if (Array.isArray(item)) {
      if (item.length > MAX_EDGES - edges)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Array exceeds entry limit');
      node = { type: 'array', length: item.length, items: [] };
    } else {
      const proto = Object.getPrototypeOf(item);
      if (proto !== null && proto !== Object.prototype)
        return failure('Packets require plain objects');
      node = { type: proto === null ? 'null-object' : 'object' };
    }
    // Typed arrays retain native structured-clone semantics; numeric indices live in the buffer.
    if (!ArrayBuffer.isView(item) && !isBlob(item)) {
      for (const key in item) {
        if (!Object.hasOwn(item, key)) continue;
        const d = Object.getOwnPropertyDescriptor(item, key);
        if (!d || !('value' in d)) return failure('Packet accessors are forbidden');
        // Store the dense prefix positionally; holes and named properties keep explicit keys.
        if (node.type === 'array' && key === String(node.items!.length)) {
          node.items!.push(token(d.value));
        } else {
          charge(key.length * 2);
          (node.props ??= []).push([key, token(d.value)]);
        }
      }
    }
    nodes[cursor] = node;
  }
  return check({ kind: 'graph', metadata: JSON.stringify({ root, nodes }), buffers, blobs });
}

/** Decoding is demand-driven by ResultLease.value, outside the message listener. */
export function decodePacket(packet: Packet): unknown {
  packetBytes(packet);
  if (packet.kind !== 'graph') return packet.value;
  const graph = JSON.parse(packet.metadata) as { root: Token; nodes: Node[] };
  if (!Array.isArray(graph.nodes) || graph.nodes.length > MAX_NODES)
    return failure('Invalid node table');
  const values: unknown[] = graph.nodes.map((node) => {
    switch (node.type) {
      case 'object':
        return {};
      case 'null-object':
        return Object.create(null);
      case 'array': {
        integer(node.length!, 'array length');
        if (node.length! > MAX_EDGES) return failure('Array too wide');
        if (node.items !== undefined) {
          if (!Array.isArray(node.items) || node.items.length > node.length!)
            return failure('Invalid array entries');
          // JSON.parse already allocated an array with own writable elements. Reuse it,
          // resolving references in place below, rather than defining every index again.
          return node.items;
        }
        return new Array(node.length);
      }
      case 'map':
        return new Map();
      case 'set':
        return new Set();
      case 'blob':
      case 'file': {
        integer(node.blob!, 'blob index');
        const blob = packet.blobs[node.blob!];
        if (!blob) return failure('Missing blob attachment');
        if (node.type === 'blob') return blob;
        if (
          typeof File === 'undefined' ||
          typeof node.name !== 'string' ||
          !Number.isSafeInteger(node.lastModified)
        )
          return failure('Invalid or unsupported File');
        return new File([blob], node.name, {
          type: blobType(blob),
          lastModified: node.lastModified,
        });
      }
      case 'date':
        return new Date(Number(node.value));
      case 'regexp': {
        const pair = JSON.parse(node.value!);
        return new RegExp(pair[0], pair[1]);
      }
      default: {
        integer(node.buffer!, 'buffer index');
        const buffer = packet.buffers[node.buffer!];
        if (!buffer) return failure('Missing backing store');
        if (node.type === 'buffer') return buffer;
        if (!Object.hasOwn(views, node.type)) return failure('Unknown view type');
        integer(node.offset!, 'view offset');
        integer(node.size!, 'view size');
        if (node.offset! + node.size! > buffer.byteLength)
          return failure('View outside backing store');
        const ctor = views[node.type as keyof typeof views];
        if (ctor === DataView) return new DataView(buffer, node.offset, node.size);
        const typed = ctor as Uint8ArrayConstructor;
        if (node.size! % typed.BYTES_PER_ELEMENT) return failure('Misaligned view');
        return new typed(buffer, node.offset, node.size! / typed.BYTES_PER_ELEMENT);
      }
    }
  });
  const read = (token: Token): unknown => {
    if (!Array.isArray(token)) return token;
    if (token[0] === 'r') {
      integer(token[1], 'reference');
      if (token[1] >= values.length) return failure('Missing reference');
      return values[token[1]];
    }
    if (token[0] === 'u') return undefined;
    if (token[0] === 'n') return Number(token[1]);
    if (token[0] === 'i') return BigInt(token[1]);
    return failure('Invalid token');
  };
  let edges = 0;
  for (let i = 0; i < graph.nodes.length; i++) {
    const node = graph.nodes[i]!,
      value = values[i];
    if (node.items) {
      if (!Array.isArray(node.items) || (edges += node.items.length) > MAX_EDGES)
        return failure('Invalid collection');
      if (node.type === 'array') {
        for (let k = 0; k < node.items.length; k++) (value as unknown[])[k] = read(node.items[k]!);
        (value as unknown[]).length = node.length!;
      } else if (node.type === 'map') {
        if (node.items.length % 2) return failure('Invalid map');
        for (let k = 0; k < node.items.length; k += 2)
          (value as Map<unknown, unknown>).set(read(node.items[k]!), read(node.items[k + 1]!));
      } else if (node.type === 'set') {
        for (const item of node.items) (value as Set<unknown>).add(read(item));
      } else return failure('Unexpected collection entries');
    }
    if ((node.type === 'blob' || node.type === 'file') && (node.props || node.items))
      return failure('Blob attachments cannot have custom properties');
    if (node.props) {
      if (!Array.isArray(node.props) || (edges += node.props.length) > MAX_EDGES)
        return failure('Invalid properties');
      for (const pair of node.props) {
        if (!Array.isArray(pair) || typeof pair[0] !== 'string') return failure('Invalid property');
        Object.defineProperty(value, pair[0], {
          value: read(pair[1]),
          enumerable: true,
          writable: true,
          configurable: true,
        });
      }
    }
  }
  return read(graph.root);
}
export function packetByteLength(value: unknown): number {
  return packetBytes(encodePacket(value));
}
