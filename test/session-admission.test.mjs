import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult, SessionAdmissionError } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { deferred } from '../dist/runtime/deferred.js';
import { until, sleep } from './helpers.mjs';
const options = (payload = null) => ({
  budget: { inputBytes: 128, scratchBytes: 0, outputBytes: 128 },
  prepare: () => ({ payload }),
});
function setup(t, extra = {}, handlers = {}) {
  const stops = [],
    gates = [];
  let starts = 0;
  const factory = () => {
    const link = createLoopback(),
      id = ++starts;
    stops.push(serve(link.host, { id: () => output(id), ...handlers }));
    return link.endpoint;
  };
  const rt = createWorkerRuntime({
    pools: { cpu: { factory, size: 2, cacheBytes: 32, idleTimeoutMs: 0 } },
    ...extra,
  });
  t.after(async () => {
    gates.forEach((g) => g.resolve());
    await rt.dispose();
    stops.forEach((s) => s());
  });
  return {
    rt,
    scope: rt.createScope(),
    factory,
    gate() {
      const g = deferred();
      gates.push(g);
      return g;
    },
  };
}
test('immediate admission binds before concurrent callers and exposes detached snapshots', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1 });
  const first = scope.acquireSession('cpu', { mode: 'immediate' });
  await assert.rejects(scope.acquireSession('cpu', { mode: 'immediate' }), (error) => {
    assert.ok(error instanceof SessionAdmissionError);
    assert.equal(error.code, 'CAPACITY_UNAVAILABLE');
    assert.ok(error.reasons.includes('worker-capacity'));
    return true;
  });
  const session = await first;
  assert.equal(session.state, 'bound');
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.reserved.cacheBytes, 32);
  const snapshot = rt.diagnostics();
  snapshot.pools[0].capacity = 99;
  assert.equal(rt.diagnostics().pools[0].capacity, 2);
  await session.dispose();
  assert.equal((await scope.acquireSession('cpu', { mode: 'immediate' })).state, 'bound');
});
test('waiting admission is bounded, abortable and times out without task credits', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1, maxQueuedTasks: 1 });
  const primary = await scope.acquireSession('cpu'),
    controller = new AbortController();
  const waiting = scope.acquireSession('cpu', { signal: controller.signal });
  await assert.rejects(scope.acquireSession('cpu'), { code: 'QUEUE_FULL' });
  assert.equal(rt.diagnostics().pools[0].waitingSessions, 1);
  assert.equal(rt.stats.reserved.inputBytes, 0);
  controller.abort();
  await assert.rejects(waiting, { code: 'ABORTED' });
  await assert.rejects(scope.acquireSession('cpu', { timeoutMs: 10 }), { code: 'QUEUE_TIMEOUT' });
  assert.equal(rt.diagnostics().pools[0].waitingSessions, 0);
  const next = scope.acquireSession('cpu');
  await primary.dispose();
  assert.equal((await next).state, 'bound');
});

test('closing a waiting primary owner immediately admits replicas it was blocking', async (t) => {
  const { rt, scope } = setup(t, { budgets: { residentBytes: 10 } });
  rt.resources.acquire({ kind: 'resident', bytes: 10 });
  const other = rt.createScope();
  const primary = scope.acquireSession('cpu', { residentBytes: 10 });
  const rejected = assert.rejects(primary, { code: 'CLOSED' });
  const replica = other.acquireSession('cpu', { reclaimable: true, timeoutMs: 200 });
  assert.ok(rt.diagnostics().waiting.some((r) => r.reasons.includes('session-priority')));
  await scope.dispose();
  await rejected;
  assert.equal((await replica).state, 'bound');
  assert.equal(rt.stats.reserved.residentBytes, 10); // No unrelated release wakes admission.
});
test('required admission reclaims an idle replica only after its disposer completes', async (t) => {
  let disposalStarted = false,
    gate;
  const {
    rt,
    scope,
    gate: makeGate,
  } = setup(
    t,
    { maxWorkers: 1 },
    {
      open(_value, ctx) {
        ctx.cache.setResource('reader', {}, 16, async () => {
          disposalStarted = true;
          await gate.promise;
        });
        return output(null);
      },
    },
  );
  gate = makeGate();
  const replica = await scope.acquireSession('cpu', { reclaimable: true, mode: 'immediate' });
  const resident = replica.resources.acquire({ kind: 'resident', bytes: 16 });
  await consumeResult(replica.enqueue('open', options()), () => {});
  const primary = scope.acquireSession('cpu');
  await until(() => disposalStarted);
  assert.equal(replica.reclaimed, true);
  assert.equal(resident.released, false);
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 32);
  await assert.rejects(scope.acquireSession('cpu', { reclaimable: true, mode: 'immediate' }), {
    code: 'CAPACITY_UNAVAILABLE',
  });
  gate.resolve();
  assert.equal((await primary).state, 'bound');
  assert.equal(rt.stats.sessionsReclaimed, 1);
  assert.equal(resident.released, true);
  assert.equal(rt.stats.reserved.residentBytes, 0);
  assert.equal(rt.stats.workers, 1);
});
test('held replica results prevent reclamation until release wakes admission', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1 });
  const replica = await scope.acquireSession('cpu', { reclaimable: true });
  const result = await replica.enqueue('id', options()).result,
    primary = scope.acquireSession('cpu');
  await sleep(5);
  assert.equal(replica.state, 'bound');
  assert.equal(rt.diagnostics().pools[0].reclaimableSessions, 0);
  result.release();
  await primary;
  assert.equal(replica.state, 'closed');
});
test('preparing replica work prevents reclamation while its Worker is idle', async (t) => {
  const { rt, scope, gate } = setup(t, { maxWorkers: 1 });
  const replica = await scope.acquireSession('cpu', { reclaimable: true }),
    g = gate();
  const task = replica.enqueuePrepared('id', {
    budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 8 },
    preparationScratchBytes: 0,
    prepareAsync: async () => {
      await g.promise;
      return { payload: null };
    },
  });
  await until(() => task.state === 'preparing');
  const primary = scope.acquireSession('cpu');
  await sleep(5);
  assert.equal(replica.reclaimed, false);
  g.resolve();
  await consumeResult(task, () => {});
  await primary;
  assert.equal(rt.stats.sessionsReclaimed, 1);
});
test('reclaim priority is deterministic and failed cleanup retains capacity until retry', async (t) => {
  let attempts = 0;
  const { rt, scope } = setup(
    t,
    {},
    {
      open(_v, ctx) {
        ctx.cache.setResource('reader', {}, 8, () => {
          if (++attempts === 1) throw Error('retry cleanup');
        });
        return output(null);
      },
    },
  );
  const high = await scope.acquireSession('cpu', { reclaimable: true, reclaimPriority: 10 });
  const low = await scope.acquireSession('cpu', { reclaimable: true, reclaimPriority: 1 });
  const retained = low.resources.acquire({ kind: 'resident', bytes: 8 });
  await consumeResult(low.enqueue('open', options()), () => {});
  const primary = await scope.acquireSession('cpu', { timeoutMs: 500 });
  assert.equal(primary.state, 'bound');
  assert.equal(low.reclaimed, true);
  assert.equal(high.reclaimed, true);
  assert.equal(rt.stats.workers, 2);
  assert.equal(rt.stats.reserved.cacheBytes, 64);
  assert.equal(retained.released, false);
  await low.dispose();
  assert.equal(retained.released, true);
  assert.equal(rt.stats.workers, 1);
});
test('closing an owner during startup rejects admission without delivering a late Session', async (t) => {
  const link = createLoopback();
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => link.endpoint } } });
  t.after(() => rt.dispose());
  const scope = rt.createScope(),
    session = scope.acquireSession('cpu');
  const rejected = assert.rejects(session, { code: 'CLOSED' });
  await scope.dispose();
  await rejected;
  assert.equal(rt.stats.workers, 0);
});
test('startup failure, invalid and pre-aborted admission leave no Session slot', async (t) => {
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        cacheBytes: 32,
        factory() {
          throw Error('no worker');
        },
      },
    },
  });
  t.after(() => rt.dispose());
  const scope = rt.createScope();
  await assert.rejects(scope.acquireSession('cpu'), { code: 'WORKER_FAILED' });
  await assert.rejects(scope.acquireSession('cpu', { reclaimPriority: -1 }), {
    code: 'INVALID_ARGUMENT',
  });
  await assert.rejects(scope.acquireSession('cpu', { signal: AbortSignal.abort() }), {
    code: 'ABORTED',
  });
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.reserved.cacheBytes, 0);
});

test('waiting primaries protect their pool while unrelated pools can admit replicas', async (t) => {
  const stops = [];
  const factory = () => {
    const link = createLoopback();
    stops.push(serve(link.host, { ping: () => output(null) }));
    return link.endpoint;
  };
  const rt = createWorkerRuntime({
    pools: { a: { factory, size: 1 }, b: { factory, size: 1 } },
    maxWorkers: 2,
  });
  t.after(async () => {
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  const scope = rt.createScope();
  await scope.acquireSession('a');
  const controller = new AbortController();
  const waiting = scope.acquireSession('a', { signal: controller.signal });
  await assert.rejects(scope.acquireSession('a', { mode: 'immediate', reclaimable: true }), (e) => {
    assert.deepEqual(e.reasons, ['session-priority']);
    return true;
  });
  assert.equal(
    (await scope.acquireSession('b', { mode: 'immediate', reclaimable: true })).state,
    'bound',
  );
  controller.abort();
  await assert.rejects(waiting, { code: 'ABORTED' });
});

test('a just-admitted replica can enqueue its first task before a pending primary reclaims it', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1 });
  const acquiring = scope.acquireSession('cpu', { reclaimable: true });
  const primary = scope.acquireSession('cpu');
  const replica = await acquiring;
  assert.equal(replica.state, 'bound');
  const task = replica.enqueue('id', options());
  await consumeResult(task, () => {});
  await primary;
  assert.equal(rt.stats.sessionsReclaimed, 1);
});

test('quarantined termination does not prevent reclaiming a healthy idle Worker', async (t) => {
  const stops = [];
  let fail = true;
  const factory = (broken) => () => {
    const link = createLoopback();
    stops.push(serve(link.host, { ping: () => output(null) }));
    return {
      ...link.endpoint,
      terminate() {
        if (broken && fail) throw Error('termination failed');
        return link.endpoint.terminate();
      },
    };
  };
  const rt = createWorkerRuntime({
    maxWorkers: 2,
    pools: {
      bad: { size: 1, factory: factory(true), idleTimeoutMs: 0 },
      warm: { size: 1, factory: factory(false), idleTimeoutMs: 0 },
      fresh: { size: 1, factory: factory(false), idleTimeoutMs: 0 },
    },
  });
  t.after(async () => {
    fail = false;
    await rt.retryTermination();
    await rt.dispose();
    stops.forEach((s) => s());
  });
  const scope = rt.createScope();
  const bad = await scope.acquireSession('bad');
  await consumeResult(scope.enqueue('ping', { ...options(), pool: 'warm' }), () => {});
  await assert.rejects(bad.dispose(), { code: 'WORKER_FAILED' });
  const fresh = await scope.acquireSession('fresh', { timeoutMs: 500 });
  assert.equal(fresh.state, 'bound');
  assert.equal(rt.stats.quarantinedWorkers, 1);
  assert.equal(rt.stats.workers, 2);
  assert.equal(rt.stats.workerTerminations, 1);
});

test('resident pressure reclaims a replica before binding a new Worker', async (t) => {
  const { rt, scope, gate } = setup(
    t,
    { budgets: { residentBytes: 32 } },
    {
      open(_v, ctx) {
        ctx.cache.setResource('reader', {}, 8, () => cleanup.promise);
        return output(null);
      },
    },
  );
  const cleanup = gate();
  const replica = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 32 });
  await consumeResult(replica.enqueue('open', options()), () => {});
  const next = scope.acquireSession('cpu', { residentBytes: 32 });
  await until(() => replica.reclaimed);
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.reserved.residentBytes, 32);
  assert.equal(replica.resident.released, false);
  assert.ok(rt.diagnostics().waiting[0].reasons.includes('resident-budget'));
  cleanup.resolve();
  const primary = await next;
  assert.equal(primary.resident.bytes, 32);
  assert.equal(replica.resident.released, true);
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.resourceLeases, 1);
  await primary.dispose();
  assert.equal(primary.resident.released, true);
  assert.equal(rt.stats.reserved.residentBytes, 0);
});

test('immediate joint admission rejects without partial Worker or resident reservations', async (t) => {
  const { rt, scope } = setup(t, { budgets: { residentBytes: 32 } });
  const retained = scope.resources.acquire({ kind: 'resident', bytes: 32 });
  await assert.rejects(
    scope.acquireSession('cpu', { mode: 'immediate', residentBytes: 1 }),
    (e) => {
      assert.equal(e.code, 'CAPACITY_UNAVAILABLE');
      assert.ok(e.reasons.includes('resident-budget'));
      return true;
    },
  );
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.resourceLeases, 1);
  await assert.rejects(scope.acquireSession('cpu', { residentBytes: 33 }), {
    code: 'BUDGET_EXCEEDED',
  });
  await assert.rejects(scope.acquireSession('cpu', { residentBytes: -1 }), {
    code: 'INVALID_ARGUMENT',
  });
  const waiting = scope.acquireSession('cpu', { residentBytes: 16 });
  retained.resize(16);
  const session = await waiting;
  assert.equal(session.resident.bytes, 16);
  assert.equal(rt.stats.reserved.residentBytes, 32);
});

test('waiting for Worker capacity holds no resident lease and cancellation returns all provisional credits', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1, budgets: { residentBytes: 32 } });
  const first = await scope.acquireSession('cpu');
  const controller = new AbortController();
  const waiting = scope.acquireSession('cpu', { residentBytes: 32, signal: controller.signal });
  await sleep(5);
  assert.equal(rt.stats.resourceLeases, 0);
  controller.abort();
  await assert.rejects(waiting, { code: 'ABORTED' });
  await first.dispose();
  assert.equal(rt.stats.reserved.residentBytes, 0);
});

test('resident lease count admission can reclaim a zero-byte replica reservation', async (t) => {
  const { rt, scope } = setup(t, { maxResourceLeases: 1 });
  const replica = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 0 });
  await assert.rejects(
    scope.acquireSession('cpu', { mode: 'immediate', residentBytes: 0 }),
    (e) => {
      assert.ok(e.reasons.includes('resource-leases'));
      return true;
    },
  );
  const primary = await scope.acquireSession('cpu', { residentBytes: 0 });
  assert.equal(replica.reclaimed, true);
  assert.equal(primary.resident.bytes, 0);
  assert.equal(rt.stats.resourceLeases, 1);
});

test('aborted startup holds resident credits until physical termination completes', async () => {
  const link = createLoopback(),
    gate = deferred();
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory: () => ({
          ...link.endpoint,
          terminate: () => gate.promise.then(() => link.endpoint.terminate()),
        }),
      },
    },
    budgets: { residentBytes: 32 },
  });
  const controller = new AbortController();
  const acquiring = rt
    .createScope()
    .acquireSession('cpu', { residentBytes: 32, signal: controller.signal });
  assert.equal(rt.stats.reserved.residentBytes, 32);
  controller.abort();
  await assert.rejects(acquiring, { code: 'ABORTED' });
  assert.equal(rt.stats.reserved.residentBytes, 32);
  gate.resolve();
  await until(() => rt.stats.reserved.residentBytes === 0);
  assert.equal(rt.stats.resourceLeases, 0);
  await rt.dispose();
});

test('factory failure rolls back joint reservation', async () => {
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory() {
          throw Error('startup');
        },
      },
    },
  });
  await assert.rejects(rt.createScope().acquireSession('cpu', { residentBytes: 32 }), {
    code: 'WORKER_FAILED',
  });
  assert.equal(rt.stats.resourceLeases, 0);
  assert.equal(rt.stats.reserved.residentBytes, 0);
  assert.equal(rt.stats.workers, 0);
  await rt.dispose();
});

test('pool and runtime cache counters accumulate deltas through failure and Worker replacement', async (t) => {
  const { rt, scope } = setup(
    t,
    { maxWorkers: 1 },
    {
      cache(_v, ctx) {
        ctx.cache.get('missing');
        ctx.cache.setBinary('a', new Uint8Array(32));
        ctx.cache.get('a');
        ctx.cache.setBinary('b', new Uint8Array(32));
        return output(null);
      },
      fail(_v, ctx) {
        ctx.cache.get('missing');
        throw Error('task failed');
      },
    },
  );
  for (let i = 1; i <= 2; i++) {
    const session = await scope.acquireSession('cpu');
    await consumeResult(session.enqueue('cache', options()), () => {});
    await assert.rejects(session.enqueue('fail', options()).result);
    assert.deepEqual(rt.stats.cacheStats, { hits: i, misses: i * 2, evictions: i });
    await session.dispose();
    assert.deepEqual(rt.diagnostics().pools[0].cacheStats, rt.stats.cacheStats);
  }
  const snapshot = rt.diagnostics();
  snapshot.pools[0].cacheStats.hits = 99;
  assert.equal(rt.stats.cacheStats.hits, 2);
});

test('invalid cache counters fail closed instead of corrupting telemetry', async () => {
  const link = createLoopback();
  const stop = serve(
    {
      ...link.host,
      postMessage(message, transfer) {
        if (message.type === 'result') message.cacheStats = { hits: -1, misses: 0, evictions: 0 };
        link.host.postMessage(message, transfer);
      },
    },
    { ping: () => output(null) },
  );
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => link.endpoint } } });
  try {
    await assert.rejects(rt.createScope().enqueue('ping', { ...options(), pool: 'cpu' }).result, {
      code: 'PROTOCOL_ERROR',
    });
    assert.deepEqual(rt.stats.cacheStats, { hits: 0, misses: 0, evictions: 0 });
  } finally {
    await rt.dispose();
    stop();
  }
});

for (const action of ['abort', 'scope-dispose', 'runtime-dispose']) {
  test(`factory reentrancy: ${action} owns the late Worker until physical termination`, async (t) => {
    const link = createLoopback(),
      terminated = deferred(),
      gate = deferred();
    const stop = serve(link.host, { ping: () => output(null) });
    const controller = new AbortController();
    let scope,
      cleanup,
      cleanupDone = false;
    const rt = createWorkerRuntime({
      budgets: { residentBytes: 32 },
      pools: {
        cpu: {
          size: 1,
          cacheBytes: 32,
          idleTimeoutMs: 0,
          factory() {
            if (action === 'abort') controller.abort();
            else cleanup = action === 'scope-dispose' ? scope.dispose() : rt.dispose();
            cleanup?.then(() => {
              cleanupDone = true;
            });
            return {
              ...link.endpoint,
              terminate() {
                terminated.resolve();
                return gate.promise.then(() => link.endpoint.terminate());
              },
            };
          },
        },
      },
    });
    t.after(async () => {
      gate.resolve();
      await rt.dispose();
      stop();
    });
    scope = rt.createScope();
    await assert.rejects(
      scope.acquireSession('cpu', { residentBytes: 32, signal: controller.signal }),
      { code: action === 'abort' ? 'ABORTED' : 'CLOSED' },
    );
    await terminated.promise;
    assert.equal(cleanupDone, false);
    assert.equal(rt.stats.workers, 1);
    assert.equal(rt.stats.closingWorkers, 1);
    assert.equal(rt.stats.reserved.residentBytes, 32);
    assert.equal(rt.stats.reserved.cacheBytes, 32);
    gate.resolve();
    await cleanup;
    await until(() => rt.stats.resourceLeases === 0);
    assert.equal(rt.stats.workers, 0);
    assert.equal(rt.stats.reserved.cacheBytes, 0);
    assert.equal(rt.stats.reserved.residentBytes, 0);
  });
}

for (const failureAt of ['subscription', 'hello']) {
  test(`synchronous ${failureAt} failure retains resident admission until termination`, async (t) => {
    const link = createLoopback(),
      gate = deferred();
    let onFailure;
    const rt = createWorkerRuntime({
      budgets: { residentBytes: 32 },
      pools: {
        cpu: {
          size: 1,
          cacheBytes: 32,
          factory: () => ({
            ...link.endpoint,
            onMessage(listener) {
              if (failureAt === 'subscription') throw Error('subscription failure');
              return link.endpoint.onMessage(listener);
            },
            onFailure(listener) {
              onFailure = listener;
              return () => {};
            },
            postMessage() {
              onFailure(Error('synchronous startup failure'));
            },
            terminate: () => gate.promise.then(() => link.endpoint.terminate()),
          }),
        },
      },
    });
    t.after(async () => {
      gate.resolve();
      await rt.dispose();
    });
    const scope = rt.createScope();
    await assert.rejects(scope.acquireSession('cpu', { residentBytes: 32 }), {
      code: 'WORKER_FAILED',
    });
    assert.equal(rt.stats.closingWorkers, 1);
    assert.equal(rt.stats.reserved.residentBytes, 32);
    assert.equal(rt.stats.resourceLeases, 1);
    assert.equal(rt.stats.reserved.cacheBytes, 32);
    gate.resolve();
    await until(() => rt.stats.resourceLeases === 0);
    assert.equal(rt.stats.workers, 0);
    assert.equal(rt.stats.reserved.residentBytes, 0);
    assert.equal(rt.stats.reserved.cacheBytes, 0);
  });
}
