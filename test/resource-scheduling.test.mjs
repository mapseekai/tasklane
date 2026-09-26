import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { CacheStore } from '../dist/resources/cache.js';
import { deferred } from '../dist/runtime/deferred.js';
import { until, sleep } from './helpers.mjs';

const task = (payload = null, extra = {}) => ({
  budget: { inputBytes: 512, scratchBytes: 0, outputBytes: 128 },
  prepare: () => ({ payload }),
  ...extra,
});
const take = (handle) => consumeResult(handle, (value) => value);
function rig(t, { options = {}, pool = {}, handlers = {}, wrap, cleanup } = {}) {
  const stops = [],
    gates = [],
    links = [];
  const factory = () => {
    const link = createLoopback(),
      id = links.push(link);
    stops.push(
      serve(link.host, {
        id: () => output(id),
        hold: async (index) => {
          await gates[index].promise;
          return output(id);
        },
        open: (keys, ctx) => {
          const state = { keys, hits: 0, misses: 0, evictions: 0, usedBytes: 64 };
          state.lease = ctx.cache.setResource('reader', state, 64, () => {}, {
            trim: (bytes) => {
              state.usedBytes = bytes;
              state.keys = [];
              state.evictions++;
              state.lease.report(state);
              return bytes;
            },
          });
          state.lease.report(state);
          return output(id);
        },
        report: (keys, ctx) => {
          const state = ctx.cache.get('reader');
          state.keys = keys;
          state.hits += 2;
          state.misses++;
          state.evictions++;
          state.lease.report(state);
          return output(id);
        },
        ...handlers,
      }),
    );
    return wrap ? wrap(link, id) : link.endpoint;
  };
  const rt = createWorkerRuntime({
    pools: { cpu: { factory, size: 3, cacheBytes: 128, idleTimeoutMs: 0, ...pool } },
    ...options,
  });
  t.after(async () => {
    cleanup?.();
    gates.forEach((g) => g.resolve());
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  return {
    rt,
    scope: rt.createScope(),
    factory,
    links,
    gate() {
      const value = deferred();
      gates.push(value);
      return { ...value, index: gates.length - 1 };
    },
  };
}

test('Session groups route using replacement resource footprints, fall back when busy and survive member disposal', async (t) => {
  const { rt, scope, gate } = rig(t);
  const a = await scope.acquireSession('cpu'),
    b = await scope.acquireSession('cpu');
  const idA = await take(a.enqueue('open', task(['x', 'y'])));
  const idB = await take(b.enqueue('open', task(['x', 'z'])));
  const group = scope.sessionGroup([a, b]);
  assert.equal(
    await take(group.enqueue('id', task(null, { affinity: { keys: ['x', 'y'] } }))),
    idA,
  );
  await take(a.enqueue('report', task([])));
  await take(b.enqueue('report', task(['x', 'y'])));
  assert.equal(await take(group.enqueue('id', task(null, { affinity: 'y' }))), idB);
  const hold = gate(),
    busy = b.enqueue('hold', task(hold.index));
  await until(() => busy.state === 'running');
  assert.equal(await take(group.enqueue('id', task(null, { affinity: 'y' }))), idA);
  hold.resolve();
  await take(busy);
  assert.equal(
    await take(
      group.enqueuePrepared('id', {
        budget: task().budget,
        preparationScratchBytes: 0,
        prepareAsync: async () => ({ payload: null }),
        affinity: 'y',
      }),
    ),
    idB,
  );
  const snapshot = rt.diagnostics().pools[0];
  assert.equal(snapshot.resources.length, 2);
  assert.deepEqual(snapshot.resourceCacheStats, { hits: 4, misses: 2, evictions: 2 });
  snapshot.resources[1].keys.length = 0;
  assert.ok(rt.diagnostics().pools[0].resources.some((r) => r.keys.includes('y')));
  await b.dispose();
  assert.equal(await take(group.enqueue('id', task(null, { affinity: 'y' }))), idA);
  await a.dispose();
  await assert.rejects(group.enqueue('id', task()).result, { code: 'SESSION_LOST' });
  assert.deepEqual(rt.stats.resourceCacheStats, { hits: 4, misses: 2, evictions: 2 });
  assert.deepEqual(rt.diagnostics().pools[0].resources, []);
});

test('Session groups reject foreign scopes, unbound members and mixed admission classes', async (t) => {
  const { rt, scope } = rig(t);
  const a = await scope.acquireSession('cpu'),
    other = rt.createScope();
  assert.throws(() => other.sessionGroup([a]), { code: 'INVALID_ARGUMENT' });
  assert.throws(() => scope.sessionGroup([scope.session('cpu')]), { code: 'INVALID_ARGUMENT' });
  const interactive = await scope.acquireSession('cpu', { priority: 'interactive' });
  assert.throws(() => scope.sessionGroup([a, interactive]), { code: 'INVALID_ARGUMENT' });
});

test('interactive Worker, active and byte reserves remain available under non-interactive saturation', async (t) => {
  const { rt, scope, gate } = rig(t, {
    options: {
      maxWorkers: 3,
      maxActiveTasks: 3,
      budgets: { inputBytes: 1536 },
      interactiveReserve: { workers: 1, activeTasks: 1, budgets: { inputBytes: 512 } },
    },
  });
  const g = gate(),
    a = scope.enqueue('hold', task(g.index, { pool: 'cpu', priority: 'background' })),
    b = scope.enqueue('hold', task(g.index, { pool: 'cpu', priority: 'background' }));
  await until(() => rt.stats.active === 2);
  const waiting = scope.enqueue('id', task(null, { pool: 'cpu', priority: 'foreground' }));
  await until(() => rt.stats.scheduler.blockedBuckets > 0);
  assert.ok(
    rt
      .diagnostics()
      .waiting.find((j) => j.id === waiting.id)
      .reasons.includes('interactive-reserve'),
  );
  assert.equal(
    await take(scope.enqueue('id', task(null, { pool: 'cpu', priority: 'interactive' }))),
    3,
  );
  assert.equal(waiting.state, 'queued');
  assert.equal(rt.stats.peakReserved.inputBytes, 1536);
  g.resolve();
  await Promise.all([take(a), take(b), take(waiting)]);
});

test('interactive preparation and result-lease reserves cannot be consumed by foreground tasks', async (t) => {
  const { rt, scope, gate } = rig(t, {
    options: {
      maxPreparingTasks: 2,
      maxResultLeases: 3,
      interactiveReserve: { preparingTasks: 1, resultLeases: 1 },
    },
  });
  const g = gate(),
    prepared = [];
  const submit = (priority) =>
    scope.enqueuePrepared('id', {
      pool: 'cpu',
      priority,
      budget: task().budget,
      preparationScratchBytes: 0,
      prepareAsync: async () => {
        prepared.push(priority);
        await g.promise;
        return { payload: null };
      },
    });
  const a = submit('foreground'),
    b = submit('foreground'),
    interactive = submit('interactive');
  await until(() => rt.stats.preparing === 2);
  assert.deepEqual(prepared.sort(), ['foreground', 'interactive']);
  assert.equal(b.state, 'queued');
  g.resolve();
  const leases = await Promise.all([a.result, b.result, interactive.result]);
  leases.forEach((l) => l.release());
  const first = await scope.enqueue('id', task(null, { pool: 'cpu' })).result,
    second = await scope.enqueue('id', task(null, { pool: 'cpu' })).result;
  const waiting = scope.enqueue('id', task(null, { pool: 'cpu' }));
  await until(() => rt.stats.scheduler.blockedBuckets > 0);
  const priorityLease = await scope.enqueue(
    'id',
    task(null, { pool: 'cpu', priority: 'interactive' }),
  ).result;
  assert.equal(waiting.state, 'queued');
  first.release();
  await take(waiting);
  second.release();
  priorityLease.release();
});

test('interactive Session Worker/cache/resident reserves retain their admission class', async (t) => {
  const { rt, scope } = rig(t, {
    pool: { size: 2, interactiveWorkers: 1 },
    options: {
      budgets: { cacheBytes: 256, residentBytes: 100 },
      interactiveReserve: { budgets: { cacheBytes: 128, residentBytes: 40 } },
    },
  });
  const background = await scope.acquireSession('cpu', { residentBytes: 60 });
  await assert.rejects(
    scope.acquireSession('cpu', { mode: 'immediate' }),
    (e) => e.code === 'CAPACITY_UNAVAILABLE' && e.reasons.includes('interactive-reserve'),
  );
  const interactive = await scope.acquireSession('cpu', {
    priority: 'interactive',
    residentBytes: 40,
  });
  assert.equal(rt.stats.reserved.residentBytes, 100);
  assert.throws(() => interactive.enqueue('id', task(null, { priority: 'foreground' })), {
    code: 'INVALID_ARGUMENT',
  });
  assert.throws(() => background.resident.resize(61), { code: 'BUDGET_EXCEEDED' });
  await take(interactive.enqueue('id', task()));
  await interactive.dispose();
  // Releasing protected capacity cannot admit another foreground Session.
  await assert.rejects(scope.acquireSession('cpu', { mode: 'immediate' }), {
    code: 'CAPACITY_UNAVAILABLE',
  });
});

test('blocked pool buckets are not rescanned on unrelated pool traffic and wake after physical release', async (t) => {
  const base = rig(t);
  const rt = createWorkerRuntime({
    pools: {
      blocked: { factory: base.factory, size: 1, idleTimeoutMs: 0 },
      free: { factory: base.factory, size: 1, idleTimeoutMs: 0 },
    },
    maxQueuedTasks: 1024,
  });
  t.after(() => rt.dispose());
  const scope = rt.createScope(),
    session = await scope.acquireSession('blocked');
  const waiting = Array.from({ length: 300 }, (_, n) =>
    scope.enqueue('id', task(null, { pool: 'blocked', group: String(n) })),
  );
  await until(() => rt.stats.scheduler.blockedBuckets === 300);
  const before = rt.stats.scheduler.eligibilityChecks;
  for (let n = 0; n < 30; n++) await take(scope.enqueue('id', task(null, { pool: 'free' })));
  assert.ok(rt.stats.scheduler.eligibilityChecks - before < 100);
  await session.dispose();
  await Promise.all(waiting.map(take));
  assert.equal(rt.stats.scheduler.blockedBuckets, 0);
  await rt.dispose();
});

test('resource telemetry is bounded and monotonic; trim clears stale footprint and waits for disposal', async () => {
  const store = new CacheStore(100),
    cache = store.scope('scope', 'session'),
    gate = deferred();
  const lease = cache.setResource('reader', {}, 80, () => {}, {
    trim: async () => {
      await gate.promise;
      return 20;
    },
  });
  lease.report({ hits: 5, misses: 2, evictions: 1, usedBytes: 70, keys: ['tile'] });
  assert.throws(() => lease.report({ hits: 4, misses: 2, evictions: 1, usedBytes: 70, keys: [] }), {
    code: 'INVALID_ARGUMENT',
  });
  assert.throws(() => lease.report({ hits: 5, misses: 2, evictions: 1, usedBytes: 81, keys: [] }), {
    code: 'BUDGET_EXCEEDED',
  });
  const trimming = store.trim(20);
  assert.equal(lease.bytes, 80);
  gate.resolve();
  await trimming;
  assert.equal(lease.bytes, 20);
  assert.deepEqual(store.reports[0].keys, []);
  store.resize(20);
  await lease.release();
  assert.deepEqual(store.resourceStats, { hits: 5, misses: 2, evictions: 1 });
  assert.equal(store.bytes, 0);
});

test('resize acknowledges reader trim before returning cache credits and keeps required Sessions', async (t) => {
  const trimGate = deferred();
  let trimming = false;
  const { rt, scope } = rig(t, {
    cleanup: () => trimGate.resolve(),
    handlers: {
      pin: (_, ctx) => {
        ctx.cache.setResource('pin', {}, 96, () => {}, {
          trim: async () => {
            trimming = true;
            await trimGate.promise;
            return 24;
          },
        });
        return output(null);
      },
    },
  });
  t.after(() => trimGate.resolve());
  const session = await scope.acquireSession('cpu');
  await take(session.enqueue('pin', task()));
  const resizing = rt.resizePool('cpu', { cacheBytes: 32, size: 1 });
  await until(() => trimming);
  assert.equal(rt.stats.reserved.cacheBytes, 128);
  const queued = session.enqueue('id', task());
  await until(() =>
    rt.diagnostics().waiting.some((j) => j.id === queued.id && j.reasons.includes('maintenance')),
  );
  trimGate.resolve();
  const report = await resizing;
  assert.equal(report.cacheBytesReleased, 96);
  assert.deepEqual(report.failures, []);
  assert.equal(rt.stats.reserved.cacheBytes, 32);
  await take(queued);
  const critical = await rt.setMemoryPressure('critical');
  assert.equal(critical.workersReclaimed, 0);
  assert.equal(critical.failures.length, 1); // Reader cannot trim below its live 24-byte state.
  assert.equal(rt.stats.reserved.cacheBytes, 32);
  assert.equal(session.state, 'bound');
  await rt.setMemoryPressure('normal');
});

test('pressure reclaims only idle replicas, counts by pool/reason and restores bounded targets', async (t) => {
  const { rt, scope } = rig(t);
  const required = await scope.acquireSession('cpu'),
    replica = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 10 });
  await take(required.enqueue('open', task(['keep'])));
  await take(replica.enqueue('open', task(['drop'])));
  const report = await rt.setMemoryPressure('critical');
  assert.deepEqual(report.failures, []);
  assert.equal(report.workersReclaimed, 1);
  assert.equal(required.state, 'bound');
  assert.equal(replica.state, 'closed');
  assert.equal(replica.reclaimed, true);
  assert.equal(rt.stats.reserved.residentBytes, 0);
  assert.equal(rt.stats.reserved.cacheBytes, 0);
  assert.equal(rt.stats.reclaim.byReason.pressure, 1);
  assert.equal(rt.diagnostics().pools[0].reclaim.succeeded, 1);
  assert.deepEqual(rt.diagnostics().pools[0].resources[0].keys, []);
  await rt.setMemoryPressure('normal');
  assert.equal(rt.diagnostics().pools[0].capacity, 3);
  assert.equal(rt.stats.reserved.cacheBytes, 128);
});

test('failed Worker reclamation stays charged, is reported, and does not stop healthy victims', async (t) => {
  let fail = true;
  const { rt, scope } = rig(t, {
    cleanup: () => {
      fail = false;
    },
    wrap: (link, id) => ({
      ...link.endpoint,
      terminate() {
        if (id === 1 && fail) throw Error('termination failed');
        return link.endpoint.terminate();
      },
    }),
  });
  const a = await scope.acquireSession('cpu', { reclaimable: true }),
    b = await scope.acquireSession('cpu', { reclaimable: true });
  await take(a.enqueue('id', task()));
  await take(b.enqueue('id', task()));
  const report = await rt.trim();
  assert.equal(report.failures.length, 1);
  assert.equal(report.workersReclaimed, 1);
  assert.equal(rt.stats.quarantinedWorkers, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 128);
  assert.equal(rt.stats.reclaim.failed, 1);
  assert.equal(rt.stats.reclaim.succeeded, 1);
  fail = false;
  await rt.retryTermination();
  await a.dispose();
  assert.equal(rt.stats.reserved.cacheBytes, 0);
});

test('cache control timeout holds old and growth credits until physical termination completes', async (t) => {
  const termination = deferred();
  let blocked = false;
  const { rt, scope } = rig(t, {
    cleanup: () => termination.resolve(),
    options: { releaseTimeoutMs: 20 },
    wrap: (link) => ({
      ...link.endpoint,
      postMessage(message, transfer) {
        if (message.type !== 'cache-control' || !blocked)
          link.endpoint.postMessage(message, transfer);
      },
      async terminate() {
        await termination.promise;
        link.endpoint.terminate();
      },
    }),
  });
  t.after(() => termination.resolve());
  const session = await scope.acquireSession('cpu');
  await rt.resizePool('cpu', { cacheBytes: 32 });
  blocked = true;
  const report = await rt.resizePool('cpu', { cacheBytes: 128 });
  assert.equal(report.failures.length, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 128);
  termination.resolve();
  await until(() => rt.stats.workers === 0);
  assert.equal(rt.stats.reserved.cacheBytes, 0);
  assert.equal(session.state, 'lost');
});

test('maintenance waits for active business tasks before starting its acknowledgement deadline', async (t) => {
  const { rt, scope, gate } = rig(t, { options: { releaseTimeoutMs: 15 } });
  const g = gate(),
    session = await scope.acquireSession('cpu');
  const running = session.enqueue('hold', task(g.index));
  await until(() => running.state === 'running');
  const maintenance = rt.resizePool('cpu', { cacheBytes: 16 });
  await sleep(35);
  assert.equal(running.state, 'running');
  assert.equal(session.state, 'bound');
  assert.equal(rt.stats.reserved.cacheBytes, 128);
  g.resolve();
  await take(running);
  assert.deepEqual((await maintenance).failures, []);
  assert.equal(rt.stats.reserved.cacheBytes, 16);
});

test('adaptive policy grows under queue/cache pressure and shrinks only after idle hysteresis', async (t) => {
  const { rt, scope, gate } = rig(t, {
    pool: {
      adaptive: { minWorkers: 1, minCacheBytes: 32, sampleMs: 10, idleMs: 100, missRatio: 0.5 },
    },
    handlers: {
      churn: (_, ctx) => {
        ctx.cache.get('missing');
        ctx.cache.setBinary('a', new Uint8Array(24));
        ctx.cache.setBinary('b', new Uint8Array(24));
        return output(null);
      },
    },
  });
  const g = gate(),
    a = scope.enqueue('hold', task(g.index, { pool: 'cpu' }));
  await until(() => a.state === 'running');
  const b = scope.enqueue('churn', task(null, { pool: 'cpu' }));
  await until(() => rt.diagnostics().pools[0].capacity >= 2);
  await take(b); // Growth admits work even while the original Worker stays busy.
  g.resolve();
  await take(a);
  await until(() => rt.diagnostics().pools[0].cacheBytesPerWorker > 32);
  assert.ok(rt.diagnostics().pools[0].cacheBytesPerWorker <= 128);
  await until(
    () =>
      rt.diagnostics().pools[0].capacity === 1 &&
      rt.diagnostics().pools[0].cacheBytesPerWorker === 32,
  );
  assert.equal(rt.stats.workers, 1);
  assert.ok(rt.stats.reclaim.byReason.adaptive >= 1);
  await rt.setMemoryPressure('critical');
  await sleep(30);
  assert.equal(rt.diagnostics().pools[0].cacheBytesPerWorker, 0);
});
