import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, dataByteLength, transferOwnedBuffers } from '../dist/index.js';
import { CacheStore } from '../dist/resources/cache.js';
import { deferred } from '../dist/runtime/deferred.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { until } from './helpers.mjs';

function runtime() {
  return createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory() {
          const link = createLoopback();
          serve(link.host, { echo: (v) => output(v) });
          return link.endpoint;
        },
      },
    },
    budgets: { residentBytes: 32 },
    maxResourceLeases: 3,
  });
}
test('resident credits survive result release and resize atomically across owners', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const scope = rt.createScope();
  const result = await scope.enqueue('echo', {
    pool: 'cpu',
    budget: { inputBytes: 16, outputBytes: 16, scratchBytes: 0 },
    prepare: () => ({ payload: new Uint8Array(16) }),
  }).result;
  const value = result.value;
  const resource = scope.resources.acquire({ kind: 'resident', bytes: value.byteLength });
  result.release();
  assert.equal(rt.stats.reserved.outputBytes, 0);
  assert.equal(rt.stats.reserved.residentBytes, 16);
  const other = rt.resources.acquire({ kind: 'resident', bytes: 16 });
  assert.throws(() => resource.resize(17), { code: 'BUDGET_EXCEEDED' });
  assert.equal(resource.bytes, 16);
  assert.equal(rt.stats.reserved.residentBytes, 32);
  other.release();
  resource.resize(32);
  resource.resize(8);
  assert.equal(rt.stats.peakReserved.residentBytes, 32);
  await scope.dispose();
  assert.equal(resource.released, true);
  assert.equal(resource.bytes, 0);
  assert.equal(rt.stats.resourceLeases, 0);
  assert.equal(value.byteLength, 16);
  assert.throws(() => resource.resize(1), { code: 'CLOSED' });
  assert.throws(() => scope.resources.acquire({ kind: 'resident', bytes: 0 }), { code: 'CLOSED' });
});
test('resident handles are bounded and runtime disposal releases unscoped credits', async () => {
  const rt = runtime();
  for (const bytes of [undefined, -1, NaN, Infinity, 0.5])
    assert.throws(() => rt.resources.acquire({ kind: 'resident', bytes }), {
      code: 'INVALID_ARGUMENT',
    });
  assert.throws(() => rt.resources.acquire({ kind: 'gpu', bytes: 0 }), {
    code: 'INVALID_ARGUMENT',
  });
  const leases = Array.from({ length: 3 }, () =>
    rt.resources.acquire({ kind: 'resident', bytes: 0 }),
  );
  assert.throws(() => rt.resources.acquire({ kind: 'resident', bytes: 0 }), {
    code: 'BUDGET_EXCEEDED',
  });
  leases[0].release();
  leases[0].release();
  const held = rt.resources.acquire({ kind: 'resident', bytes: 32 });
  await rt.dispose();
  assert.equal(held.released, true);
  assert.equal(rt.stats.reserved.residentBytes, 0);
  assert.throws(() => rt.resources.acquire({ kind: 'resident', bytes: 0 }), { code: 'CLOSED' });
});
test('scope resident credits stay held until cancelled asynchronous production physically returns', async () => {
  const rt = runtime(),
    scope = rt.createScope(),
    gate = deferred();
  const lease = scope.resources.acquire({ kind: 'resident', bytes: 32 });
  const task = scope.enqueuePrepared('echo', {
    pool: 'cpu',
    budget: { inputBytes: 8, outputBytes: 8, scratchBytes: 0 },
    preparationScratchBytes: 0,
    prepareAsync: async () => {
      await gate.promise;
      return { payload: 1 };
    },
  });
  await until(() => task.state === 'preparing');
  const disposal = scope.dispose();
  assert.equal(lease.released, false);
  assert.equal(rt.stats.reserved.residentBytes, 32);
  gate.resolve();
  await disposal;
  await rt.dispose();
  assert.equal(lease.released, true);
});
test('large resident TypedArrays charge backing bytes and only custom fields consume edges', () => {
  const values = new Float32Array(4_000_000);
  assert.equal(dataByteLength(values, { resident: true, maxEntries: 1 }), values.byteLength);
  values.extra = new Uint8Array(8);
  assert.equal(dataByteLength(values, { resident: true, maxEntries: 1 }), values.byteLength + 18);
  values.second = 0;
  assert.throws(() => dataByteLength(values, { resident: true, maxEntries: 1 }), {
    code: 'BUDGET_EXCEEDED',
  });
  const bytes = new Uint8Array(8);
  assert.deepEqual(transferOwnedBuffers(bytes, bytes.buffer), [bytes.buffer]);
  assert.throws(() => transferOwnedBuffers(bytes.subarray(1)), { code: 'INVALID_ARGUMENT' });
});
test('opaque resize evicts only on success and retains credits through failed disposal', async () => {
  const store = new CacheStore(32),
    cache = store.scope('s', 'session');
  let tries = 0;
  const gate = deferred();
  const resource = cache.setResource('reader', {}, 8, async () => {
    if (++tries === 1) throw Error('retry');
    await gate.promise;
  });
  cache.set('tile', new Uint8Array(24), 24);
  assert.throws(() => resource.resize(33), { code: 'BUDGET_EXCEEDED' });
  assert.equal(cache.get('tile').byteLength, 24);
  resource.resize(16);
  assert.equal(cache.get('tile'), undefined);
  assert.equal(store.bytes, 16);
  await assert.rejects(resource.release(), /retry/);
  assert.equal(resource.released, false);
  const closing = resource.release();
  assert.throws(() => resource.resize(0), { code: 'CLOSED' });
  assert.equal(store.bytes, 16);
  gate.resolve();
  await closing;
  assert.equal(store.bytes, 0);
  cache.setResource('reader', {}, 8, () => {});
  await resource.release();
  assert.equal(store.bytes, 8);
  await store.release();
});

test('binary cache snapshots native view metadata without retaining custom fields or copying data', async () => {
  const store = new CacheStore(32),
    cache = store.scope('scope');
  for (const View of [
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
  ]) {
    const buffer = new ArrayBuffer(32),
      value = new View(buffer, View.BYTES_PER_ELEMENT, 2);
    Object.defineProperty(value, 'extra', {
      enumerable: true,
      get() {
        assert.fail('must not read custom fields');
      },
    });
    Object.defineProperty(value, 'constructor', {
      get() {
        assert.fail('must not use user constructor');
      },
    });
    cache.setBinary('data', value);
    const saved = cache.get('data');
    assert.notEqual(saved, value);
    assert.equal(saved.constructor, View);
    assert.equal(saved.buffer, buffer);
    assert.equal(saved.byteOffset, value.byteOffset);
    assert.equal(saved.length, 2);
    assert.equal(saved.extra, undefined);
    assert.equal(store.bytes, 32);
  }
  const data = new DataView(new ArrayBuffer(32), 4, 8);
  cache.setBinary('data', data);
  assert.equal(cache.get('data').byteLength, 8);
  assert.equal(cache.get('data').byteOffset, 4);
  assert.equal(store.bytes, 32);
  assert.throws(() => cache.setBinary('large', new Uint8Array(new ArrayBuffer(64), 0, 1)), {
    code: 'BUDGET_EXCEEDED',
  });
  assert.equal(cache.get('data').buffer, data.buffer);
  assert.throws(() => cache.setBinary('bad', {}), { code: 'INVALID_ARGUMENT' });
  await store.release();
});

test('cache counters distinguish eviction from deletion and failed admission', async () => {
  const store = new CacheStore(16),
    cache = store.scope('scope', 'session');
  cache.get('absent');
  cache.set('a', 1, 8);
  cache.set('b', 2, 8);
  cache.get('a');
  assert.throws(() => cache.set('bad', 3, 17), { code: 'BUDGET_EXCEEDED' });
  cache.set('c', 3, 8);
  cache.get('b');
  cache.delete('a');
  assert.deepEqual(store.stats, { hits: 1, misses: 2, evictions: 1 });
  const snapshot = store.stats;
  snapshot.hits = 99;
  assert.equal(store.stats.hits, 1);
  await store.release();
  assert.equal(store.stats.evictions, 1);
});
