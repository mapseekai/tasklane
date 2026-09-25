import { binaryByteLength } from './binary.js';
import { RuntimeError } from './errors.js';

/** Progress is a small control packet, never a second binary result channel. */
export function validateProgress(value: unknown): void {
  if (
    binaryByteLength(value, {
      maxObjects: 64,
      maxEntries: 256,
      maxPending: 64,
      maxMetadataBytes: 4096,
    }) !== 0
  ) {
    throw new RuntimeError(
      'BUDGET_EXCEEDED',
      'Progress cannot contain binary data; use a bounded result task',
    );
  }
  const stack = [value];
  const seen = new Set<object>();
  while (stack.length) {
    const item = stack.pop();
    if (item === null || item === undefined) continue;
    if (typeof item === 'string' || typeof item === 'boolean') continue;
    if (typeof item === 'number' && Number.isFinite(item)) continue;
    if (typeof item !== 'object')
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Progress requires scalar values and plain records/arrays',
      );
    if (seen.has(item)) continue;
    seen.add(item);
    const proto: unknown = Object.getPrototypeOf(item);
    if (!Array.isArray(item) && proto !== Object.prototype && proto !== null) {
      throw new RuntimeError('INVALID_ARGUMENT', 'Progress requires plain records/arrays');
    }
    for (const key in item) {
      if (Object.hasOwn(item, key)) stack.push(Object.getOwnPropertyDescriptor(item, key)!.value);
    }
  }
}
