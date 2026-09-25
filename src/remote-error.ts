import { ERROR_CODES, RuntimeError, type ErrorDetail, type RemoteErrorInfo } from './errors.js';
import type { WireError } from './protocol.js';

// UTF-16 accounting: bounded fields plus 4 KiB details keep each envelope below 16 KiB.
const fields = { name: 128, remoteCode: 128, message: 1024, stack: 4096 } as const;

function field(value: unknown, key: string): unknown {
  if (!value || typeof value !== 'object') return undefined;
  try {
    for (let item: object | null = value, depth = 0; item && depth < 8; depth++) {
      const descriptor = Object.getOwnPropertyDescriptor(item, key);
      if (descriptor) return 'value' in descriptor ? descriptor.value : undefined;
      item = Object.getPrototypeOf(item) as object | null;
    }
  } catch {
    /* Invalid metadata falls back to a bounded basic error. */
  }
  return undefined;
}

function details(value: unknown): ErrorDetail {
  let entries = 0,
    bytes = 0;
  const seen = new Set<object>();
  const charge = (size: number) => {
    bytes += size;
    if (bytes > 4096 || ++entries > 128) throw new Error('Error details exceed limits');
  };
  const copy = (item: unknown, depth: number): ErrorDetail => {
    if (depth > 8) throw new Error('Error details are too deep');
    charge(16);
    if (item === null || typeof item === 'boolean') return item;
    if (typeof item === 'number' && Number.isFinite(item)) return item;
    if (typeof item === 'string') {
      charge(item.length * 2);
      return item;
    }
    if (!item || typeof item !== 'object' || seen.has(item))
      throw new Error('Invalid error details');
    const array = Array.isArray(item);
    if (
      !array &&
      Object.getPrototypeOf(item) !== Object.prototype &&
      Object.getPrototypeOf(item) !== null
    )
      throw new Error('Error details require plain data');
    if (array && item.length > 128) throw new Error('Error detail array is too wide');
    seen.add(item);
    const result: ErrorDetail[] | { [key: string]: ErrorDetail } = array ? [] : {};
    for (const key in item) {
      if (!Object.hasOwn(item, key)) continue;
      charge(key.length * 2);
      const descriptor = Object.getOwnPropertyDescriptor(item, key);
      if (!descriptor || !('value' in descriptor)) throw new Error('Error detail accessor');
      Object.defineProperty(result, key, {
        value: copy(descriptor.value, depth + 1),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    seen.delete(item);
    return result;
  };
  return copy(value, 0);
}

export function encodeError(value: unknown): WireError {
  const result: WireError = {
    code:
      value instanceof RuntimeError && ERROR_CODES.includes(value.code)
        ? value.code
        : 'REMOTE_ERROR',
    name: 'Error',
    message: 'Remote task failed',
  };
  for (const [key, limit] of Object.entries(fields)) {
    const source = key === 'remoteCode' ? 'code' : key;
    const raw = source === 'message' && typeof value === 'string' ? value : field(value, source);
    if (typeof raw !== 'string') continue;
    if (raw.length > limit) result.truncated = true;
    Object.assign(result, { [key]: raw.slice(0, limit) });
  }
  const data = field(value, 'details');
  if (data !== undefined) {
    try {
      result.details = details(data);
    } catch {
      result.detailsOmitted = true;
    }
  } else if (value && typeof value === 'object' && Object.hasOwn(value, 'details')) {
    result.detailsOmitted = true;
  }
  return result;
}

export function decodeError(value: WireError): RuntimeError {
  const invalid = () => {
    throw new RuntimeError('PROTOCOL_ERROR', 'Malformed remote error');
  };
  if (!value || !ERROR_CODES.includes(value.code)) return invalid();
  if (typeof value.name !== 'string' || typeof value.message !== 'string') return invalid();
  for (const [key, limit] of Object.entries(fields)) {
    const item = (value as unknown as Record<string, unknown>)[key];
    if (item !== undefined && (typeof item !== 'string' || item.length > limit)) return invalid();
  }
  for (const key of ['detailsOmitted', 'truncated'] as const)
    if (value[key] !== undefined && typeof value[key] !== 'boolean') return invalid();
  const remote: RemoteErrorInfo = { name: value.name, message: value.message };
  if (value.remoteCode !== undefined) remote.code = value.remoteCode;
  if (value.stack !== undefined) remote.stack = value.stack;
  if (value.truncated) remote.truncated = true;
  if (value.detailsOmitted) remote.detailsOmitted = true;
  if (value.details !== undefined) {
    try {
      remote.details = details(value.details);
    } catch {
      return invalid();
    }
  }
  return new RuntimeError(value.code, value.message, { remoteError: remote });
}
