import assert from 'node:assert/strict';
import test from 'node:test';
import { binaryByteLength, transferBuffers } from '../dist/index.js';
import { BudgetLedger } from '../dist/resources/budget.js';
import { CacheStore } from '../dist/resources/cache.js';
import { OwnedResult } from '../dist/resources/lease.js';

for (const value of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
  test(`invalid reservation: ${value}`, () =>
    assert.throws(
      () => new BudgetLedger({ inputBytes: value, scratchBytes: 0, outputBytes: 0, cacheBytes: 0 }),
    ));
}
test('unique full backing stores, cycles, Map and Set', () => {
  const a = new Uint8Array(100);
  const value = { a, slice: a.subarray(1), map: new Map([[a, a]]), set: new Set([a]) };
  value.self = value;
  assert.equal(binaryByteLength(value), 100);
});
test('accessors are rejected without invoking them', () => {
  let called = false;
  assert.throws(() =>
    binaryByteLength({
      get bad() {
        called = true;
        return 1;
      },
    }),
  );
  assert.equal(called, false);
});
test('bounded traversal rejects oversized object graphs', () =>
  assert.throws(() =>
    binaryByteLength(
      Array.from({ length: 11 }, () => ({})),
      10,
    ),
  ));
test('transfer helper deduplicates explicitly owned buffers', () => {
  const a = new Uint8Array(8);
  assert.deepEqual(transferBuffers(a, a.buffer), [a.buffer]);
});
test('partial views cannot imply transfer of an entire source', () =>
  assert.throws(() => transferBuffers(new Uint8Array(32).subarray(2))));
test('SharedArrayBuffer is counted but cannot be transferred', () => {
  assert.equal(binaryByteLength(new SharedArrayBuffer(32)), 32);
  assert.throws(() => transferBuffers(new Uint8Array(new SharedArrayBuffer(32))));
});
test('budget reserve is atomic, idempotent and nonnegative', () => {
  const ledger = new BudgetLedger({
    inputBytes: 10,
    scratchBytes: 10,
    outputBytes: 10,
    cacheBytes: 10,
  });
  const release = ledger.reserve({ inputBytes: 8 });
  assert.throws(() => ledger.reserve({ inputBytes: 3 }));
  assert.equal(ledger.used.inputBytes, 8);
  release();
  release();
  assert.equal(ledger.used.inputBytes, 0);
  assert.equal(ledger.peak.inputBytes, 8);
});
test('lease invalidates value and releases once', () => {
  let released = 0;
  const result = new OwnedResult(new Uint8Array(10), 10, () => released++);
  assert.equal(result.value.length, 10);
  result.release();
  result.release();
  assert.equal(released, 1);
  assert.throws(() => result.value, { code: 'RESULT_RELEASED' });
});
test('cache LRU eviction preserves recently accessed entries', () => {
  const store = new CacheStore(16);
  const cache = store.scope('a');
  cache.set('x', 1, 8);
  cache.set('y', 2, 8);
  cache.get('x');
  cache.set('z', 3, 8);
  assert.equal(cache.get('y'), undefined);
  assert.equal(cache.get('x'), 1);
  assert.equal(store.bytes, 16);
});
test('cache entry count bounds zero-byte metadata', () => {
  const store = new CacheStore(0, 2);
  const c = store.scope('a');
  c.set('a', 1, 0);
  c.set('b', 2, 0);
  c.set('c', 3, 0);
  assert.equal(c.get('a'), undefined);
  assert.equal(c.get('c'), 3);
});
test('cache scope isolation and owner cleanup', () => {
  const store = new CacheStore(32);
  const a = store.scope('a'),
    b = store.scope('b');
  a.set('x', 1, 8);
  b.set('x', 2, 8);
  store.release('a');
  assert.equal(a.get('x'), undefined);
  assert.equal(b.get('x'), 2);
  assert.equal(store.bytes, 8);
});
test('required state cannot be silently evicted', () => {
  const store = new CacheStore(16);
  const c = store.scope('a', 'session');
  c.setPinned('x', 1, 16);
  assert.throws(() => c.set('y', 2, 8), { code: 'BUDGET_EXCEEDED' });
  assert.equal(c.get('x'), 1);
});
test('pinning without a session is rejected', () =>
  assert.throws(() => new CacheStore(16).scope('a').setPinned('x', 1, 8)));
test('cache validation rejects under-declared binary allocations', () =>
  assert.throws(() => new CacheStore(16).scope('a').set('x', new Uint8Array(10), 1)));

test('class instances cannot hide unaccounted binary allocations', () => {
  class Packet {
    constructor() {
      this.bytes = new Uint8Array(32);
    }
  }
  assert.throws(() => binaryByteLength(new Packet()), { code: 'INVALID_ARGUMENT' });
});
test('Date and RegExp remain supported scalar metadata', () => {
  assert.equal(binaryByteLength({ date: new Date(), pattern: /test/ }), 0);
});
