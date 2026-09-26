import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate } from 'node:timers';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { deferred } from '../dist/runtime/deferred.js';

const flush = () => new Promise(setImmediate);
const options = (pool, payload = null) => ({
  pool,
  budget: { inputBytes: 512, scratchBytes: 0, outputBytes: 128 },
  prepare: () => ({ payload }),
});
const take = (handle) => consumeResult(handle, (value) => value);

// Drive the runtime's public timers without relying on wall-clock scheduling margins.
function clock(t) {
  let now = 0,
    serial = 0;
  const timers = new Map();
  t.mock.method(performance, 'now', () => now);
  t.mock.method(globalThis, 'setTimeout', (callback, delay = 0, ...args) => {
    const id = ++serial;
    timers.set(id, { at: now + Math.max(1, delay), run: () => callback(...args) });
    return id;
  });
  t.mock.method(globalThis, 'clearTimeout', (id) => timers.delete(id));
  return {
    get pending() {
      return timers.size;
    },
    async advance(ms) {
      const end = now + ms;
      await flush();
      for (;;) {
        const next = [...timers]
          .filter(([, timer]) => timer.at <= end)
          .sort((a, b) => a[1].at - b[1].at)[0];
        if (!next) break;
        now = next[1].at;
        timers.delete(next[0]);
        next[1].run();
        await flush();
      }
      now = end;
      await flush();
    },
  };
}

function rig(t, configs, handlers = {}, overrides = {}) {
  const gates = [],
    stops = [],
    controls = [];
  const pools = Object.fromEntries(
    Object.entries(configs).map(([name, config]) => [
      name,
      {
        size: 3,
        cacheBytes: 128,
        idleTimeoutMs: 0,
        ...config,
        factory() {
          const link = createLoopback();
          stops.push(
            serve(link.host, {
              ping: () => output(null),
              hold: async (index) => {
                await gates[index].promise;
                return output(index);
              },
              ...handlers,
            }),
          );
          return {
            ...link.endpoint,
            postMessage(message, transfer) {
              if (message.type === 'cache-control')
                controls.push({ pool: name, at: performance.now(), limit: message.limit });
              link.endpoint.postMessage(message, transfer);
            },
          };
        },
      },
    ]),
  );
  const rt = createWorkerRuntime({ pools, releaseTimeoutMs: 100, ...overrides });
  t.after(async () => {
    gates.forEach((g) => g.resolve());
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  return {
    rt,
    scope: rt.createScope(),
    controls,
    gate() {
      const g = deferred();
      gates.push(g);
      return { ...g, index: gates.length - 1 };
    },
  };
}

test('adaptive pools respect independent sample periods and recover to their idle floors', async (t) => {
  const time = clock(t);
  const { rt, scope, gate } = rig(t, {
    fast: { cacheBytes: 0, adaptive: { sampleMs: 100, idleMs: 1000 } },
    slow: { cacheBytes: 0, adaptive: { sampleMs: 500, idleMs: 1000 } },
  });
  const g = gate();
  const tasks = ['fast', 'slow'].flatMap((pool) =>
    Array.from({ length: 3 }, () => scope.enqueue('hold', options(pool, g.index))),
  );
  const capacity = () => rt.diagnostics().pools.map((p) => p.capacity);
  await time.advance(99);
  assert.deepEqual(capacity(), [1, 1]);
  await time.advance(1);
  assert.deepEqual(capacity(), [2, 1]);
  await time.advance(399);
  assert.deepEqual(capacity(), [3, 1]);
  await time.advance(1);
  assert.deepEqual(capacity(), [3, 2]);
  await time.advance(500);
  assert.deepEqual(capacity(), [3, 3]);
  assert.equal(rt.stats.workers, 6);
  g.resolve();
  await Promise.all(tasks.map(take));
  await time.advance(999);
  assert.deepEqual(capacity(), [3, 3]);
  await time.advance(1);
  assert.deepEqual(capacity(), [1, 1]);
  assert.equal(rt.stats.workers, 2);
  assert.equal(rt.stats.reclaim.byReason.adaptive, 4);
  await rt.dispose();
  assert.equal(time.pending, 0);
});

test('repeated pressure transitions preserve original targets, pause adaptation and return replica credits', async (t) => {
  const time = clock(t);
  const { rt, scope } = rig(
    t,
    { cpu: { adaptive: { minWorkers: 2, minCacheBytes: 64, sampleMs: 100, idleMs: 1000 } } },
    {
      open: (_, ctx) => {
        const state = { hits: 0, misses: 0, evictions: 0, usedBytes: 64, keys: ['tile'] };
        const lease = ctx.cache.setResource('reader', state, 64, () => {}, {
          trim: (bytes) => {
            state.keys = [];
            state.usedBytes = bytes;
            state.evictions++;
            lease.report(state);
            return bytes;
          },
        });
        return output(null);
      },
    },
  );
  const primary = await scope.acquireSession('cpu', { residentBytes: 16 });
  await take(primary.enqueue('open', options('cpu')));
  for (let cycle = 0; cycle < 40; cycle++) {
    const replica = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 32 });
    const moderate = await rt.setMemoryPressure('moderate');
    assert.equal(moderate.workersReclaimed, 1);
    assert.deepEqual(moderate.failures, []);
    assert.equal(replica.reclaimed, true);
    await rt.setMemoryPressure('moderate');
    assert.equal(rt.diagnostics().pools[0].cacheBytesPerWorker, 32);
    assert.deepEqual((await rt.setMemoryPressure('critical')).failures, []);
    await time.advance(300);
    assert.equal(rt.diagnostics().pools[0].cacheBytesPerWorker, 0);
    assert.equal(rt.stats.reserved.cacheBytes, 0);
    assert.equal(rt.stats.reserved.residentBytes, 16);
    assert.equal(rt.stats.resourceLeases, 1);
    assert.deepEqual((await rt.setMemoryPressure('normal')).failures, []);
    assert.equal(rt.diagnostics().pools[0].capacity, 2);
    assert.equal(rt.diagnostics().pools[0].cacheBytesPerWorker, 64);
    assert.equal(rt.stats.reserved.cacheBytes, 64);
    await take(primary.enqueue('ping', options('cpu')));
  }
  assert.equal(rt.stats.sessionsReclaimed, 40);
  assert.equal(rt.stats.scheduler.blockedBuckets, 0);
  await rt.dispose();
  assert.ok(Object.values(rt.stats.reserved).every((n) => n === 0));
  assert.equal(time.pending, 0);
});

test('a busy Worker awaiting adaptive cache growth does not stall other pools and catches up when idle', async (t) => {
  const time = clock(t);
  const adaptive = { minCacheBytes: 32, sampleMs: 100, idleMs: 1000 };
  const { rt, scope, gate } = rig(
    t,
    { first: { adaptive }, second: { adaptive } },
    {
      churn: (_, ctx) => {
        ctx.cache.get('missing');
        ctx.cache.setBinary('a', new Uint8Array(24));
        ctx.cache.setBinary('b', new Uint8Array(24));
        return output(null);
      },
    },
  );
  await take(scope.enqueue('churn', options('first')));
  const g = gate(),
    running = scope.enqueue('hold', options('first', g.index));
  const tasks = Array.from({ length: 3 }, () => scope.enqueue('hold', options('second', g.index)));
  await time.advance(100);
  assert.deepEqual(
    rt.diagnostics().pools.map((p) => p.capacity),
    [1, 2],
  );
  assert.equal(rt.diagnostics().pools[0].cacheBytesPerWorker, 48);
  await time.advance(100);
  assert.equal(rt.diagnostics().pools[1].capacity, 3);
  assert.equal(running.state, 'running');
  g.resolve();
  await Promise.all([running, ...tasks].map(take));
  await time.advance(100);
  const first = rt.diagnostics().pools[0];
  assert.equal(first.cacheReservedBytes, first.workers * first.cacheBytesPerWorker);
});

test('maintenance rechecks replica eligibility after awaiting another replica disposer', async (t) => {
  const { rt, scope, gate } = rig(
    t,
    { cpu: {} },
    {
      open: (index, ctx) => {
        ctx.cache.setResource('reader', {}, 8, () => disposal.promise);
        return output(index);
      },
    },
  );
  const disposal = gate(),
    work = gate();
  const first = await scope.acquireSession('cpu', { reclaimable: true }),
    second = await scope.acquireSession('cpu', { reclaimable: true, reclaimPriority: 1 });
  await take(first.enqueue('open', options('cpu', 1)));
  const trimming = rt.trim({ workersPerPool: 0, cacheBytesPerWorker: 128 });
  await flush();
  assert.equal(first.state, 'closed');
  const running = second.enqueue('hold', options('cpu', work.index));
  await flush();
  assert.equal(running.state, 'running');
  disposal.resolve();
  const report = await trimming;
  assert.equal(report.workersReclaimed, 1);
  assert.equal(second.state, 'bound');
  assert.equal(running.state, 'running');
  work.resolve();
  await take(running);
});

for (const owner of ['session', 'scope', 'runtime']) {
  test(`${owner} disposal during resource trim converges after physical cleanup`, async (t) => {
    const time = clock(t);
    let trimming = false,
      disposals = 0;
    const { rt, scope, gate } = rig(
      t,
      { cpu: {} },
      {
        open: (_, ctx) => {
          ctx.cache.setResource(
            'reader',
            {},
            96,
            () => {
              disposals++;
            },
            {
              trim: async () => {
                trimming = true;
                await trimGate.promise;
                return 16;
              },
            },
          );
          return output(null);
        },
      },
    );
    const trimGate = gate();
    const session = await scope.acquireSession('cpu', { residentBytes: 32 });
    await take(session.enqueue('open', options('cpu')));
    const resizing = rt.resizePool('cpu', { cacheBytes: 32 });
    await flush();
    assert.equal(trimming, true);
    const closing =
      owner === 'session' ? session.dispose() : owner === 'scope' ? scope.dispose() : rt.dispose();
    await flush();
    assert.equal(rt.stats.reserved.residentBytes, 32);
    assert.equal(rt.stats.reserved.cacheBytes, 128);
    trimGate.resolve();
    await Promise.all([resizing, closing]);
    assert.equal(disposals, 1);
    assert.equal(rt.stats.workers, 0);
    assert.equal(rt.stats.resourceLeases, 0);
    assert.ok(Object.values(rt.stats.reserved).every((n) => n === 0));
    await rt.dispose();
    assert.equal(time.pending, 0);
  });
}

test('cache growth and runtime shutdown never return credits before the active task physically settles', async (t) => {
  const time = clock(t);
  const { rt, scope, gate } = rig(t, { cpu: {} });
  const g = gate(),
    session = await scope.acquireSession('cpu');
  await rt.resizePool('cpu', { cacheBytes: 32 });
  const job = session.enqueue('hold', options('cpu', g.index));
  await flush();
  const growing = rt.resizePool('cpu', { cacheBytes: 128 });
  await flush();
  assert.equal(rt.stats.reserved.cacheBytes, 128);
  const closing = rt.dispose();
  g.resolve();
  await Promise.all([growing, closing]);
  await assert.rejects(job.result, { code: 'ABORTED' });
  assert.equal(rt.stats.active, 0);
  assert.ok(Object.values(rt.stats.reserved).every((n) => n === 0));
  assert.equal(time.pending, 0);
});
