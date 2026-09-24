import { integer, RuntimeError } from './errors.js';

/** Count unique backing stores, not view lengths. This is NOT a JS-heap estimator.
 * Only inert, cloneable records/arrays/maps/sets are traversed; accessors are rejected.
 * The traversal is bounded so a control operation cannot walk millions of objects.
 */
export function binaryByteLength(value: unknown, maxObjects = 100_000): number {
  integer(maxObjects, 'maxObjects', 1);
  const seen = new Set<object>();
  const buffers = new Set<object>();
  const stack: unknown[] = [value];
  let bytes = 0;
  while (stack.length) {
    const item = stack.pop();
    if (item === null || typeof item !== 'object') continue;
    if (seen.has(item)) continue;
    seen.add(item);
    if (seen.size > maxObjects) {
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Binary metadata traversal exceeded its object limit',
      );
    }
    const buffer = ArrayBuffer.isView(item) ? item.buffer : item;
    if (
      buffer instanceof ArrayBuffer ||
      (typeof SharedArrayBuffer !== 'undefined' && buffer instanceof SharedArrayBuffer)
    ) {
      if (!buffers.has(buffer)) {
        buffers.add(buffer);
        bytes += buffer.byteLength;
        integer(bytes, 'binaryByteLength');
      }
      continue;
    }
    if (item instanceof Map) {
      for (const [key, val] of item) stack.push(key, val);
    } else if (item instanceof Set) {
      for (const val of item) stack.push(val);
    } else {
      // Dates/regexps are cloneable scalar metadata. Reject custom class instances
      // rather than silently ignoring binary fields hidden behind their prototype.
      const proto: unknown = Object.getPrototypeOf(item);
      if (item instanceof Date || item instanceof RegExp) continue;
      if (proto !== Object.prototype && proto !== null && !Array.isArray(item)) {
        throw new RuntimeError(
          'INVALID_ARGUMENT',
          'Packets require plain records, collections and typed buffers',
        );
      }
      for (const key of Object.keys(item)) {
        const descriptor = Object.getOwnPropertyDescriptor(item, key);
        if (!descriptor || !('value' in descriptor)) {
          throw new RuntimeError(
            'INVALID_ARGUMENT',
            'Accessor properties are not allowed in packets',
          );
        }
        stack.push(descriptor.value);
      }
    }
    if (stack.length > maxObjects) {
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Binary metadata traversal exceeded its pending limit',
      );
    }
  }
  return bytes;
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
