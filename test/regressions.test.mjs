import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, binaryByteLength, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { header } from '../dist/protocol.js';
import { CacheStore } from '../dist/resources/cache.js';
import { Scheduler } from '../dist/runtime/scheduler.js';
import { nodeWorker } from '../dist/adapters/node.js';
import { sleep, options, take } from './helpers.mjs';
const opts = (payload = null, extra = {}) => options(payload, extra);
async function until(predicate) {
  for (let i = 0; i < 1000; i++) {
    if (predicate()) return;
    await sleep(1);
  }
  throw Error('condition timed out');
}
function runtime(t, handlers, options = {}) {
  const links = [];
  const stops = [];
  const factory = () => {
    const link = createLoopback();
    links.push(link);
    stops.push(serve(link.host, handlers));
    return link.endpoint;
  };
  const rt = createWorkerRuntime({
    pools: { cpu: { factory, size: 1, idleTimeoutMs: 0 } },
    ...options,
  });
  t.after(async () => {
    await rt.dispose();
    for (const stop of stops) stop();
  });
  return { rt, links };
}

test('wide primitive arrays use element limits, not pending-object limits', () => {
  assert.equal(binaryByteLength(Array(200_000).fill(1)), 0);
  let visited = 0;
  const value = Array(300_000).fill(1);
  const original = Object.getOwnPropertyDescriptor;
  Object.getOwnPropertyDescriptor = (object, key) => {
    if (object === value) visited++;
    return original(object, key);
  };
  try {
    assert.throws(() => binaryByteLength(value, { maxObjects: 10, maxEntries: 100 }), {
      code: 'BUDGET_EXCEEDED',
    });
    assert.equal(visited, 0);
  } finally {
    Object.getOwnPropertyDescriptor = original;
  }
  assert.throws(
    () =>
      binaryByteLength(new Map(Array.from({ length: 100 }, (_, i) => [i, i])), {
        maxObjects: 10,
        maxEntries: 100,
      }),
    { code: 'BUDGET_EXCEEDED' },
  );
  assert.throws(() => binaryByteLength('x'.repeat(2049), { maxMetadataBytes: 4096 }), {
    code: 'BUDGET_EXCEEDED',
  });
});
test('resident built-in custom fields cannot conceal binary allocations', () => {
  const cache = new CacheStore(2 ** 20).scope('a');
  for (const value of [
    new Map(),
    new Set(),
    new Date(),
    /x/,
    new ArrayBuffer(0),
    new Uint8Array(0),
  ]) {
    value.binary = new Uint8Array(1024);
    assert.throws(() => cache.set('x', value, 32), { code: 'BUDGET_EXCEEDED' });
  }
});

test('resident DataView numeric custom fields are inspected', () => {
  const value = new DataView(new ArrayBuffer(0));
  value[0] = new Uint8Array(1024);
  assert.throws(() => new CacheStore(2048).scope('s').set('v', value, 0), {
    code: 'BUDGET_EXCEEDED',
  });
});

test('built-in shadow accessors are rejected without invocation', () => {
  let called = 0;
  for (const [value, key] of [
    [new Map(), 'size'],
    [new Set(), 'size'],
    [/x/, 'source'],
    [new ArrayBuffer(8), 'byteLength'],
  ]) {
    Object.defineProperty(value, key, {
      enumerable: true,
      get() {
        called++;
        return 0;
      },
    });
    assert.throws(() => binaryByteLength(value), { code: 'INVALID_ARGUMENT' });
  }
  assert.equal(called, 0);
  assert.throws(() => new CacheStore(8).scope('s').set('closure', () => {}, 0), {
    code: 'INVALID_ARGUMENT',
  });
});
test('opaque resources retain credits until explicit async disposal and support retry', async () => {
  class Database {}
  const store = new CacheStore(8);
  const cache = store.scope('a', 'session');
  let resolve;
  let calls = 0;
  const value = new Database();
  cache.setResource('db', value, 8, () => {
    calls++;
    return new Promise((r) => (resolve = r));
  });
  assert.equal(cache.get('db'), value);
  const done = cache.delete('db');
  await until(() => resolve);
  assert.equal(store.bytes, 8);
  assert.throws(() => cache.set('new', new Uint8Array(8), 8), { code: 'BUDGET_EXCEEDED' });
  resolve();
  await done;
  assert.equal(store.bytes, 0);
  assert.equal(calls, 1);
  let fail = true;
  cache.setResource('retry', value, 8, () => {
    if (fail) throw Error('close failed');
  });
  await assert.rejects(cache.delete('retry'), /close failed/);
  assert.equal(store.bytes, 8);
  fail = false;
  await cache.delete('retry');
  assert.equal(store.bytes, 0);
});
test('retained cache handles cannot recreate a released scope', async () => {
  const store = new CacheStore(8);
  const c = store.scope('a');
  await store.release('a');
  assert.throws(() => c.set('x', 1, 0), { code: 'CLOSED' });
});

test('scope count is bounded and withScope cleans up on consumer failure', async (t) => {
  const { rt } = runtime(t, { ping: () => output(null) }, { maxScopes: 1 });
  const scope = rt.createScope('first');
  assert.throws(() => scope.createScope(), { code: 'BUDGET_EXCEEDED' });
  assert.equal(rt.resourceDiagnostics().owners[0].label, 'first');
  await scope.dispose();
  await assert.rejects(
    rt.withScope('temporary', async (s) => {
      await take(s.enqueue('ping', opts()));
      throw Error('consumer');
    }),
    /consumer/,
  );
  assert.equal(rt.stats.scopes, 0);
});
test('streaming groups do not reset fairness when their last result completes', async (t) => {
  const order = [];
  const { rt } = runtime(t, {
    ping: (v) => {
      order.push(v);
      return output(v);
    },
  });
  const s = rt.createScope();
  const add = (g, name) => s.enqueue('ping', opts(name, { group: g, priority: 'interactive' }));
  const a0 = add('A', 'A0'),
    a1 = add('A', 'A1');
  const stream = (n) => take(add('B', `B${n}`)).then(() => (n < 19 ? stream(n + 1) : undefined));
  await Promise.all([take(a0), take(a1), stream(0)]);
  assert.ok(order.indexOf('A1') <= 2, order.join(','));
});
test('scheduler selects once per runnable task and bounds idle history', () => {
  const scheduler = new Scheduler(1000, 32);
  let calls = 0;
  for (let i = 0; i < 8000; i++)
    scheduler.add({
      groupKey: `g${i}`,
      order: i,
      enqueuedAt: 0,
      options: { priority: 'interactive' },
    });
  for (let i = 0; i < 8000; i++) {
    const job = scheduler.select(0, () => {
      calls++;
      return true;
    });
    assert.ok(job);
    scheduler.remove(job);
  }
  assert.equal(calls, 8000);
  assert.ok(scheduler.historySize <= 32);
});
test('scheduler ageing promotes background while retaining FIFO within each group', () => {
  const scheduler = new Scheduler(10, 4096, 'ageing');
  const a = { groupKey: 'a', order: 0, enqueuedAt: 0, options: { priority: 'background' } };
  const b = { groupKey: 'b', order: 1, enqueuedAt: 0, options: { priority: 'interactive' } };
  scheduler.add(a);
  scheduler.add(b);
  assert.equal(
    scheduler.select(0, () => true),
    b,
  );
  scheduler.remove(b);
  assert.equal(
    scheduler.select(20, () => true),
    a,
  );
});

test('blocked session lane does not hide a runnable pool in the same group', () => {
  const scheduler = new Scheduler(1000);
  const a = {
    groupKey: 'g',
    laneKey: 'a',
    order: 0,
    enqueuedAt: 0,
    options: { priority: 'interactive' },
  };
  const b = {
    groupKey: 'g',
    laneKey: 'b',
    order: 1,
    enqueuedAt: 0,
    options: { priority: 'interactive' },
  };
  scheduler.add(a);
  scheduler.add(b);
  assert.equal(
    scheduler.select(0, (job) => job === b),
    b,
  );
});

test('default concurrency follows pool capacity and zero-byte result leases are bounded', async (t) => {
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  const factory = () => {
    const link = createLoopback();
    serve(link.host, {
      async work() {
        await gate;
        return output(null);
      },
    });
    return link.endpoint;
  };
  const rt = createWorkerRuntime({ pools: { cpu: { factory, size: 4 } }, maxResultLeases: 4 });
  t.after(() => rt.dispose());
  const scope = rt.createScope();
  const handles = Array.from({ length: 4 }, () => scope.enqueue('work', opts()));
  await until(() => rt.stats.active === 4);
  release();
  const leases = await Promise.all(handles.map((handle) => handle.result));
  const waiting = scope.enqueue('work', opts());
  await sleep(5);
  assert.equal(waiting.state, 'queued');
  leases[0].release();
  await take(waiting);
  for (const lease of leases) lease.release();
});
test('starting cancellation still obeys the execution deadline', async () => {
  let terminated = 0;
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory: () => ({
          postMessage() {},
          onMessage() {
            return () => {};
          },
          onFailure() {
            return () => {};
          },
          terminate() {
            terminated++;
          },
        }),
      },
    },
    startupTimeoutMs: 1000,
    executionTimeoutMs: 25,
  });
  const h = rt.createScope().enqueue('ping', opts());
  await until(() => h.state === 'starting');
  h.cancel();
  await h.settled;
  assert.equal(terminated, 1);
  assert.equal(rt.stats.active, 0);
  await rt.dispose();
});
test('synchronous hello failure never binds a dead slot to a session', async () => {
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory: () => ({
          postMessage() {
            throw Error('hello failed');
          },
          onMessage() {
            return () => {};
          },
          onFailure() {
            return () => {};
          },
          terminate() {},
        }),
      },
    },
  });
  const session = rt.createScope().session('cpu');
  for (let i = 0; i < 2; i++) {
    await assert.rejects(session.enqueue('ping', opts()).result, { code: 'WORKER_FAILED' });
    assert.equal(session.state, 'unbound');
  }
  await rt.dispose();
  assert.equal(rt.stats.workers, 0);
});
test('premature response during synchronous prepare retires without escaping listener', async (t) => {
  const { rt, links } = runtime(t, { ping: (v) => output(v) });
  const scope = rt.createScope();
  let h;
  h = scope.enqueue(
    'ping',
    opts(null, {
      prepare() {
        const slot = [...rt.pools.get('cpu').slots][0];
        assert.doesNotThrow(() =>
          links[0].inject({
            ...header(slot.epoch),
            type: 'result',
            id: h.id,
            scope: scope.id,
            value: null,
            byteLength: 0,
            cacheBytes: 0,
            workerMs: 0,
          }),
        );
        return { payload: null };
      },
    }),
  );
  await assert.rejects(h.result, { code: 'PROTOCOL_ERROR' });
  await h.settled;
  assert.equal(rt.stats.reserved.inputBytes, 0);
});
test('progress binary payloads fail before transport; completed contexts cannot send', async (t) => {
  let context;
  const { rt, links } = runtime(t, {
    big(_v, ctx) {
      ctx.progress(new Uint8Array(8 * 1024 ** 2));
      return output(null);
    },
    capture(_v, ctx) {
      context = ctx;
      return output(null);
    },
  });
  const s = rt.createScope();
  await assert.rejects(s.enqueue('big', opts()).result, { code: 'BUDGET_EXCEEDED' });
  const h = s.enqueue('capture', opts());
  await take(h);
  await h.settled;
  let messages = 0;
  links[0].endpoint.onMessage((m) => {
    if (m.type === 'progress') messages++;
  });
  context.progress(new Uint8Array(1024 ** 2));
  await sleep(20);
  assert.equal(messages, 0);
  await assert.rejects(context.checkpoint(), { code: 'CLOSED' });
});
test('progress keeps one in flight and only the latest pending snapshot', async () => {
  const link = createLoopback();
  let context, finish;
  const sent = [];
  const stop = serve(link.host, {
    work(_v, ctx) {
      context = ctx;
      return new Promise((r) => (finish = () => r(output(null))));
    },
  });
  link.endpoint.onMessage((m) => sent.push(m));
  link.endpoint.postMessage({ ...header(1), type: 'hello', cacheBytes: 0, cacheEntries: 10 });
  await sleep(0);
  link.endpoint.postMessage({
    ...header(1),
    type: 'request',
    id: 'x',
    scope: 's',
    task: 'work',
    payload: { kind: 'scalar', value: null },
    maxScratchBytes: 0,
    maxOutputBytes: 0,
    maxOutputBlobBytes: 0,
  });
  await until(() => context);
  context.progress(1);
  context.progress(2);
  context.progress(3);
  await sleep(20);
  assert.equal(sent.filter((m) => m.type === 'progress').length, 1);
  link.endpoint.postMessage({ ...header(1), type: 'progress-ack', id: 'x', scope: 's' });
  await sleep(20);
  assert.deepEqual(
    sent.filter((m) => m.type === 'progress').map((m) => m.value),
    [1, 3],
  );
  finish();
  await sleep(0);
  stop();
});
test('discardResult and consumeResult return credits even if nobody consumes or consumer throws', async (t) => {
  const { rt } = runtime(
    t,
    { ping: () => output(new Uint8Array(8)) },
    { budgets: { outputBytes: 8 } },
  );
  const s = rt.createScope();
  for (let i = 0; i < 20; i++)
    await s.enqueue(
      'ping',
      opts(null, {
        budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 8 },
        discardResult: true,
      }),
    ).settled;
  assert.equal(rt.stats.leases, 0);
  assert.equal(rt.stats.reserved.outputBytes, 0);
  await assert.rejects(
    consumeResult(
      s.enqueue('ping', opts(null, { budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 8 } })),
      () => {
        throw Error('consumer');
      },
    ),
    /consumer/,
  );
  assert.equal(rt.stats.leases, 0);
});
test('scope release waits for ACK and has a bounded missing-ACK failure', async () => {
  const link = createLoopback();
  let ack;
  const stop = serve(
    {
      ...link.host,
      postMessage(m, t) {
        if (m.type === 'released') ack = () => link.host.postMessage(m, t);
        else link.host.postMessage(m, t);
      },
    },
    { ping: () => output(null) },
  );
  const rt = createWorkerRuntime({
    pools: { cpu: { size: 1, factory: () => link.endpoint } },
    releaseTimeoutMs: 30,
  });
  const s = rt.createScope();
  await take(s.enqueue('ping', opts()));
  let done = false;
  const pending = s.dispose().then(() => (done = true));
  await until(() => ack);
  assert.equal(done, false);
  ack();
  await pending;
  const s2 = rt.createScope();
  await take(s2.enqueue('ping', opts()));
  await assert.rejects(s2.dispose(), { code: 'EXECUTION_TIMEOUT' });
  const end = rt.dispose();
  await until(() => ack);
  ack();
  await end;
  stop();
});
test('opaque session resource is disposed before successful session shutdown', async () => {
  const link = createLoopback();
  let resolve,
    closed = false;
  const stop = serve(link.host, {
    open(_v, ctx) {
      ctx.cache.setResource(
        'db',
        new Date(),
        8,
        () =>
          new Promise(
            (r) =>
              (resolve = () => {
                closed = true;
                r();
              }),
          ),
      );
      return output(null);
    },
  });
  const rt = createWorkerRuntime({
    pools: { cpu: { factory: () => link.endpoint, size: 1, cacheBytes: 8 } },
  });
  const session = rt.createScope().session('cpu');
  await take(session.enqueue('open', opts()));
  let done = false;
  const ending = session.dispose().then(() => (done = true));
  await until(() => resolve);
  assert.equal(done, false);
  assert.equal(rt.stats.workers, 1);
  resolve();
  await ending;
  assert.equal(closed, true);
  await rt.dispose();
  stop();
});
test('never-settling prepare is rejected and cannot hold disposal open', async () => {
  const link = createLoopback();
  const stop = serve(link.host, { ping: () => output(null) });
  const rt = createWorkerRuntime({ pools: { cpu: { factory: () => link.endpoint, size: 1 } } });
  const h = rt.createScope().enqueue('ping', opts(null, { prepare: () => new Promise(() => {}) }));
  await assert.rejects(h.result, { code: 'INVALID_ARGUMENT' });
  await rt.disposeWithin(100);
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
  stop();
});
test('failed physical termination can be explicitly retried without double release', async () => {
  const link = createLoopback();
  const stop = serve(link.host, { wait: () => new Promise(() => {}) });
  let fail = true;
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        factory: () => ({
          ...link.endpoint,
          terminate() {
            if (fail) throw Error('fail');
            return link.endpoint.terminate();
          },
        }),
        size: 1,
        allowHardCancel: true,
      },
    },
    executionTimeoutMs: 1000,
  });
  const h = rt.createScope().enqueue('wait', opts(null, { cancellation: 'terminate' }));
  await until(() => h.state === 'running');
  h.cancel();
  await until(() => rt.stats.quarantinedWorkers === 1);
  await assert.rejects(rt.dispose(), /quarantined/);
  fail = false;
  await rt.retryTermination();
  await h.settled;
  await rt.dispose();
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.workers, 0);
  stop();
});
test('pool of one reclaims only one of four idle workers', async () => {
  const factory = nodeWorker(new URL('./fixtures/node-worker.mjs', import.meta.url));
  const rt = createWorkerRuntime({
    pools: { a: { factory, size: 4, idleTimeoutMs: 0 }, b: { factory, size: 1, idleTimeoutMs: 0 } },
    maxWorkers: 4,
    maxActiveTasks: 4,
  });
  try {
    const s = rt.createScope();
    await Promise.all(
      Array.from({ length: 4 }, () => take(s.enqueue('wait', opts({ ms: 20 }, { pool: 'a' })))),
    );
    await Promise.all(
      Array.from({ length: 4 }, () => take(s.enqueue('ping', opts(null, { pool: 'b' })))),
    );
    assert.equal(rt.stats.workerTerminations, 1);
  } finally {
    await rt.dispose();
  }
});
test('waiting large reservation stops a continuous stream of small overlapping tasks', async (t) => {
  const { rt } = runtime(
    t,
    {
      async wait(ms) {
        await sleep(ms);
        return output(null);
      },
    },
    {
      pools: {
        cpu: {
          size: 2,
          factory: () => {
            const l = createLoopback();
            serve(l.host, {
              async wait(ms) {
                await sleep(ms);
                return output(null);
              },
            });
            return l.endpoint;
          },
        },
      },
      budgets: { inputBytes: 20 },
      budgetWaitMs: 20,
      queueTimeoutMs: 1000,
    },
  );
  const s = rt.createScope();
  let running = true;
  const small = async (delay) => {
    await sleep(delay);
    while (running)
      await take(
        s.enqueue(
          'wait',
          opts(10, { group: 'small', budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 0 } }),
        ),
      );
  };
  const streams = [small(0), small(5)];
  await sleep(20);
  try {
    await take(
      s.enqueue(
        'wait',
        opts(1, { group: 'large', budget: { inputBytes: 20, scratchBytes: 0, outputBytes: 0 } }),
      ),
    );
  } finally {
    running = false;
    await Promise.all(streams);
  }
});
