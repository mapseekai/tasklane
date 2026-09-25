import assert from 'node:assert/strict';
import test from 'node:test';
import { EventEmitter } from 'node:events';
import { Worker } from 'node:worker_threads';
import { encodePacket, decodePacket, packetBytes, packetByteLength } from '../dist/packet.js';
import { nodeEndpoint } from '../dist/adapters/node.js';
import { ScratchArena } from '../dist/resources/scratch.js';
import { OwnedResult } from '../dist/resources/lease.js';
import { CacheStore } from '../dist/resources/cache.js';
import { Scheduler } from '../dist/runtime/scheduler.js';
import { createLoopback } from '../dist/testing.js';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { serve, output } from '../dist/host.js';

test('packets account strings, scalars, numeric arrays and preserve graph identity', () => {
  assert.equal(packetByteLength('x'.repeat(2_000_000)), 4_000_000);
  assert.throws(() => encodePacket('x'.repeat(2_000_000), 8), { code: 'BUDGET_EXCEEDED' });
  const numbers = Array(200_000).fill(1);
  const encoded = encodePacket(numbers, 16 * 1024 ** 2);
  assert.ok(packetBytes(encoded) > numbers.length);
  assert.deepEqual(decodePacket(encoded), numbers);
  const buffer = new ArrayBuffer(32);
  const value = {
    buffer,
    a: new Uint16Array(buffer, 2, 3),
    map: new Map(),
    set: new Set(),
    date: new Date(0),
    re: /foo/gi,
    special: [-0, NaN, Infinity, undefined, 9n],
  };
  value.map.set(value, value.a);
  value.set.add(value);
  value.self = value;
  const copy = decodePacket(structuredClone(encodePacket(value)));
  assert.equal(copy.self, copy);
  assert.equal(copy.a.buffer, copy.buffer);
  assert.equal(copy.map.get(copy), copy.a);
  assert.ok(copy.set.has(copy));
  assert.deepEqual(copy.special, value.special);
  assert.equal(copy.re.toString(), '/foo/gi');
  assert.equal(copy.date.getTime(), 0);
});

test('codec rejects wide arrays before touching entries and never invokes accessors', () => {
  let calls = 0;
  const wide = new Array(2_000_000);
  Object.defineProperty(wide, '0', {
    enumerable: true,
    get() {
      calls++;
      return 1;
    },
  });
  assert.throws(() => encodePacket(wide), { code: 'BUDGET_EXCEEDED' });
  const map = new Map();
  Object.defineProperty(map, 'size', {
    enumerable: true,
    get() {
      calls++;
      return 0;
    },
  });
  assert.throws(() => encodePacket(map));
  assert.equal(calls, 0);
});

test('result receipt only validates wire sizes; decode is lazy and failure releases credits', () => {
  const packet = { kind: 'graph', metadata: 'invalid json', buffers: [], blobs: [] };
  assert.equal(packetBytes(packet), 24);
  let released = 0;
  const lease = new OwnedResult(packet, 24, () => released++);
  assert.equal(released, 0);
  assert.throws(() => lease.value);
  assert.equal(released, 1);
  assert.equal(lease.released, true);
});

test('ordinary cache data cannot under-declare strings or arrays', () => {
  const cache = new CacheStore(8 * 1024 ** 2).scope('s');
  for (const value of ['x'.repeat(2_000_000), Array(200_000).fill(1), {}, new Set([{}, {}])])
    assert.throws(() => cache.set('x', value, 1), { code: 'BUDGET_EXCEEDED' });
  cache.set('x', 'abcd', 8);
  assert.equal(cache.get('x'), 'abcd');
});

test('scratch arena rejects excess allocation and detaches all aliases on release/close', () => {
  const arena = new ScratchArena(8);
  const a = arena.allocate(8),
    view = new Uint8Array(a);
  assert.throws(() => arena.allocate(1), { code: 'BUDGET_EXCEEDED' });
  arena.release(a);
  assert.equal(view.byteLength, 0);
  const b = arena.allocate(8);
  arena.close();
  assert.equal(b.byteLength, 0);
  assert.equal(arena.bytes, 0);
  assert.throws(() => arena.allocate(0), { code: 'CLOSED' });
});

test('nested Node buffers and shadowed view fields use intrinsic backing stores', () => {
  const view = new Uint8Array(8);
  Object.defineProperty(view, 'buffer', {
    get() {
      assert.fail('shadow getter invoked');
    },
  });
  assert.equal(packetBytes(encodePacket(view)), 8);
  const value = { view: Buffer.from([1, 2, 3]) };
  assert.deepEqual(Array.from(decodePacket(encodePacket(value)).view), [1, 2, 3]);
});

test('failed Session disposer preserves its worker and credits for retry', async () => {
  const link = createLoopback();
  let fail = true;
  const stop = serve(link.host, {
    open(_v, ctx) {
      ctx.cache.setResource('db', new Date(), 8, () => {
        if (fail) throw Error('close failed');
      });
      return output(null);
    },
  });
  const rt = createWorkerRuntime({
    pools: { cpu: { size: 1, cacheBytes: 8, factory: () => link.endpoint } },
  });
  const session = rt.createScope().session('cpu');
  try {
    await consumeResult(
      session.enqueue('open', {
        budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 0 },
        prepare: () => ({ payload: null }),
      }),
      () => {},
    );
    await assert.rejects(session.dispose(), /close failed/);
    assert.equal(rt.stats.workers, 1);
    assert.equal(rt.stats.reserved.cacheBytes, 8);
    fail = false;
    await session.dispose();
    assert.equal(rt.stats.workers, 0);
  } finally {
    fail = false;
    await rt.dispose();
    stop();
  }
});

test('runtime rejects under-budget metadata at both senders and reuses worker', async () => {
  const link = createLoopback();
  let calls = 0;
  const stop = serve(link.host, {
    ping(v) {
      calls++;
      return output(v);
    },
    large() {
      return output('x'.repeat(2_000_000));
    },
    scratch(_v, ctx) {
      ctx.scratch.allocate(9);
      return output(null);
    },
  });
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => link.endpoint } } });
  const scope = rt.createScope();
  const options = (value) => ({
    pool: 'cpu',
    budget: { inputBytes: 8, scratchBytes: 8, outputBytes: 8 },
    prepare: () => ({ payload: value }),
  });
  try {
    await assert.rejects(scope.enqueue('ping', options('x'.repeat(2_000_000))).result, {
      code: 'BUDGET_EXCEEDED',
    });
    assert.equal(calls, 0);
    await assert.rejects(scope.enqueue('large', options(null)).result, { code: 'BUDGET_EXCEEDED' });
    await assert.rejects(scope.enqueue('scratch', options(null)).result, {
      code: 'BUDGET_EXCEEDED',
    });
    assert.equal(await consumeResult(scope.enqueue('ping', options('abcd')), (v) => v), 'abcd');
    assert.equal(rt.stats.active, 0);
    assert.equal(rt.stats.leases, 0);
  } finally {
    await rt.dispose();
    stop();
  }
});

test('strict priority never promotes old background work above fresh interactive work', () => {
  const scheduler = new Scheduler(1);
  const background = {
    groupKey: 'b',
    order: 0,
    enqueuedAt: 0,
    options: { priority: 'background' },
  };
  const interactive = {
    groupKey: 'i',
    order: 1,
    enqueuedAt: 10000,
    options: { priority: 'interactive' },
  };
  scheduler.add(background);
  scheduler.add(interactive);
  assert.equal(
    scheduler.select(10000, () => true),
    interactive,
  );
});

test('Node endpoint keeps error protection across unsubscribe and terminate until exit', async () => {
  class FakeWorker extends EventEmitter {
    async terminate() {
      this.emit('error', Error('termination race'));
      this.emit('exit', 1);
    }
  }
  const worker = new FakeWorker();
  const endpoint = nodeEndpoint(worker);
  const off = endpoint.onFailure(() => assert.fail('removed listener'));
  off();
  assert.doesNotThrow(() => worker.emit('error', Error('unsubscribe race')));
  await endpoint.terminate();
  assert.equal(worker.listenerCount('error'), 0);
});

test('real Node worker error/termination races never escape detached failure listeners', async () => {
  for (let i = 0; i < 20; i++) {
    const worker = new Worker('setTimeout(() => { throw Error("race"); }, 0)', { eval: true });
    const endpoint = nodeEndpoint(worker);
    const off = endpoint.onFailure(() => {});
    await new Promise((resolve) => worker.once('online', resolve));
    off();
    if (i % 2) await new Promise((resolve) => setTimeout(resolve, 2));
    await endpoint.terminate();
  }
});
