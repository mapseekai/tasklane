import { test, expect } from '@playwright/test';

const soakMs = Number(process.env.TASKLANE_SOAK_MS ?? 12000);
if (!Number.isSafeInteger(soakMs) || soakMs < 12000 || soakMs > 3600000)
  throw Error('TASKLANE_SOAK_MS must be an integer between 12000 and 3600000');

test.beforeEach(async ({ page }) => {
  await page.goto('/test/browser/harness.html');
});

test('maintenance wakes replica reclamation and busy Workers converge to a smaller pool', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const until = async (condition) => {
      const deadline = performance.now() + 5000;
      while (!condition()) {
        if (performance.now() >= deadline) throw Error('Worker state did not converge');
        await wait(5);
      }
    };
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
          size: 3,
          idleTimeoutMs: 0,
        },
      },
      queueTimeoutMs: 5000,
    });
    const scope = rt.createScope();
    const options = (ms) => ({
      pool: 'cpu',
      budget: { inputBytes: 512, scratchBytes: 0, outputBytes: 8 },
      prepare: () => ({ payload: { ms } }),
    });
    const take = (handle) => consumeResult(handle, (value) => value);
    try {
      await rt.resizePool('cpu', { size: 1 });
      const replica = await scope.acquireSession('cpu', { reclaimable: true });
      await wait(0);
      const resizing = rt.resizePool('cpu', { size: 1 });
      const primary = scope.session('cpu');
      const task = primary.enqueue('wait', options(1));
      await resizing;
      await take(task);
      const reclaimed = replica.reclaimed;
      await primary.dispose();
      await rt.resizePool('cpu', { size: 3 });
      const first = Array.from({ length: 3 }, () => scope.enqueue('wait', options(500)));
      await until(() => first.every((h) => h.state === 'running'));
      await rt.resizePool('cpu', { size: 1 });
      const activeBeforeFinish = rt.stats.active;
      await Promise.all(first.map(take));
      await until(() => rt.stats.workers === 1);
      const activePerBurst = [];
      for (let round = 0; round < 3; round++) {
        const next = Array.from({ length: 3 }, () => scope.enqueue('wait', options(30)));
        await until(() => rt.stats.active > 0);
        activePerBurst.push(rt.stats.active);
        await Promise.all(next.map(take));
      }
      return { reclaimed, activeBeforeFinish, workers: rt.stats.workers, activePerBurst };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    reclaimed: true,
    activeBeforeFinish: 3,
    workers: 1,
    activePerBurst: [1, 1, 1],
  });
});

test('class budget protection preserves interactive admission and closing primary owners wakes replicas', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const rt = createWorkerRuntime({
      pools: Object.fromEntries(
        ['holder', 'large', 'small'].map((name) => [
          name,
          {
            factory: browserWorker('/test/fixtures/browser-worker.mjs'),
            size: 1,
            idleTimeoutMs: 0,
          },
        ]),
      ),
      budgets: { scratchBytes: 100, residentBytes: 10 },
      interactiveReserve: { budgets: { scratchBytes: 40 } },
      priorityPolicy: 'ageing',
      ageingMs: 10,
      budgetWaitMs: 5,
      queueTimeoutMs: 5000,
    });
    const scope = rt.createScope();
    const options = (pool, scratchBytes, ms = 1, priority = 'foreground') => ({
      pool,
      priority,
      budget: { inputBytes: 512, scratchBytes, outputBytes: 8 },
      prepare: () => ({ payload: { ms } }),
    });
    const take = (handle) => consumeResult(handle, (value) => value);
    try {
      await Promise.all(
        ['holder', 'large', 'small'].map((pool) => take(scope.enqueue('wait', options(pool, 0)))),
      );
      const holder = scope.enqueue('wait', options('holder', 20, 500));
      await wait(10);
      const large = scope.enqueue('wait', options('large', 60));
      await wait(30);
      const small = scope.enqueue('wait', options('small', 20));
      await wait(10);
      const smallState = small.state;
      await take(scope.enqueue('wait', options('small', 20, 1, 'interactive')));
      const largeState = large.state;
      await Promise.all([holder, large, small].map(take));
      rt.resources.acquire({ kind: 'resident', bytes: 10 });
      const primaryScope = rt.createScope(),
        replicaScope = rt.createScope();
      const primary = primaryScope
        .acquireSession('large', { residentBytes: 10 })
        .catch((e) => e.code);
      const replica = replicaScope.acquireSession('large', { reclaimable: true });
      await primaryScope.dispose();
      return { smallState, largeState, primary: await primary, replica: (await replica).state };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    smallState: 'queued',
    largeState: 'queued',
    primary: 'CLOSED',
    replica: 'bound',
  });
});

test('a lazy Session retries healthy victims after failed full-pool reclamation', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const create = browserWorker('/test/fixtures/maintenance-worker.mjs');
    let serial = 0,
      fail = true;
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          size: 2,
          cacheBytes: 32,
          idleTimeoutMs: 0,
          factory() {
            const endpoint = create(),
              id = ++serial;
            return {
              ...endpoint,
              terminate() {
                if (id === 1 && fail) throw Error('Injected termination failure');
                return endpoint.terminate();
              },
            };
          },
        },
      },
      queueTimeoutMs: 3000,
    });
    try {
      const scope = rt.createScope();
      await scope.acquireSession('cpu', { reclaimable: true });
      const replica = await scope.acquireSession('cpu', { reclaimable: true });
      const primary = scope.session('cpu');
      await consumeResult(
        primary.enqueue('ping', {
          budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 0 },
          prepare: () => ({ payload: null }),
        }),
        () => {},
      );
      return {
        state: primary.state,
        reclaimed: replica.reclaimed,
        quarantined: rt.stats.quarantinedWorkers,
        failed: rt.stats.reclaim.failed,
        succeeded: rt.stats.reclaim.succeeded,
        cache: rt.stats.reserved.cacheBytes,
      };
    } finally {
      fail = false;
      await rt.retryTermination();
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    state: 'bound',
    reclaimed: true,
    quarantined: 1,
    failed: 1,
    succeeded: 1,
    cache: 64,
  });
});

test('shrinking keeps interactive capacity with idle and busy foreground Workers', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const create = browserWorker('/test/fixtures/maintenance-worker.mjs');
    const take = (handle) => consumeResult(handle, () => {});
    const until = async (condition) => {
      const deadline = performance.now() + 5000;
      while (!condition()) {
        if (performance.now() > deadline) throw Error('Worker state did not converge');
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    };
    const results = [];
    for (const mode of ['idle', 'busy', 'unspawned-interactive']) {
      const endpoints = [];
      const rt = createWorkerRuntime({
        pools: {
          cpu: {
            size: 3,
            interactiveWorkers: 1,
            idleTimeoutMs: 0,
            factory() {
              const endpoint = create();
              endpoints.push(endpoint);
              return endpoint;
            },
          },
        },
        queueTimeoutMs: 5000,
      });
      const release = (id) =>
        endpoints.forEach((endpoint) => endpoint.postMessage({ maintenanceGate: id }));
      const options = (payload, priority = 'foreground') => ({
        pool: 'cpu',
        priority,
        budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 0 },
        prepare: () => ({ payload }),
      });
      try {
        const scope = rt.createScope();
        const urgent =
          mode === 'unspawned-interactive'
            ? undefined
            : scope.enqueue('hold', options(0, 'interactive'));
        const work = Array.from({ length: 2 }, () => scope.enqueue('hold', options(1)));
        await until(() =>
          [...work, ...(urgent ? [urgent] : [])].every((h) => h.state === 'running'),
        );
        if (urgent) {
          release(0);
          await take(urgent);
        }
        if (mode === 'idle') {
          release(1);
          await Promise.all(work.map(take));
        }
        await rt.resizePool('cpu', { size: 2 });
        if (mode !== 'idle') {
          release(1);
          await Promise.all(work.map(take));
        }
        await until(() => rt.stats.workers <= 2);
        const next = Array.from({ length: 2 }, () => scope.enqueue('hold', options(2)));
        await until(() => rt.stats.active > 0);
        await take(scope.enqueue('ping', options(null, 'interactive')));
        results.push({ mode, states: next.map((h) => h.state).sort(), workers: rt.stats.workers });
        release(2);
        await Promise.all(next.map(take));
        const primary = await scope.acquireSession('cpu');
        const trimmed = await rt.trim();
        if (trimmed.workersReclaimed !== 1 || rt.stats.workers !== 1 || primary.state !== 'bound')
          throw Error('Explicit idle trim did not preserve the required Session');
      } finally {
        await rt.dispose();
      }
    }
    return results;
  });
  expect(result).toEqual(
    ['idle', 'busy', 'unspawned-interactive'].map((mode) => ({
      mode,
      states: ['queued', 'running'],
      workers: 2,
    })),
  );
});

test('adaptive cache growth reaches its unchanged target after a competing Session releases budget', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const factory = browserWorker('/test/fixtures/maintenance-worker.mjs');
    const diagnostics = [];
    const rt = createWorkerRuntime({
      pools: {
        adaptive: {
          factory,
          size: 1,
          idleTimeoutMs: 0,
          cacheBytes: 48,
          adaptive: { minCacheBytes: 32, sampleMs: 50, idleMs: 10000 },
        },
        holder: { factory, size: 1, idleTimeoutMs: 0, cacheBytes: 48 },
      },
      budgets: { cacheBytes: 80 },
      onDiagnostic: (e) => diagnostics.push(e.message),
    });
    const until = async (condition) => {
      const deadline = performance.now() + 5000;
      while (!condition()) {
        if (performance.now() > deadline) throw Error('Cache target did not converge');
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    };
    try {
      const scope = rt.createScope(),
        holder = await scope.acquireSession('holder');
      await consumeResult(
        scope.enqueue('churn', {
          pool: 'adaptive',
          budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 0 },
          prepare: () => ({ payload: null }),
        }),
        () => {},
      );
      await until(() => diagnostics.length === 1);
      const before = rt.diagnostics().pools[0].cacheReservedBytes;
      await new Promise((resolve) => setTimeout(resolve, 160));
      const failures = diagnostics.length;
      await holder.dispose();
      await until(() => rt.diagnostics().pools[0].cacheReservedBytes === 48);
      return { before, after: rt.diagnostics().pools[0].cacheReservedBytes, failures };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({ before: 32, after: 48, failures: 1 });
});

test('two adaptive pools sustain bursts, cancellations and repeated pressure transitions without resource drift', async ({
  page,
}) => {
  test.setTimeout(soakMs + 90000);
  const result = await page.evaluate(async (duration) => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const ensure = (condition, message) => {
      if (!condition) throw Error(message);
    };
    const diagnostics = [];
    const factory = browserWorker('/test/fixtures/browser-worker.mjs');
    const rt = createWorkerRuntime({
      pools: {
        fast: {
          factory,
          size: 3,
          cacheBytes: 4096,
          idleTimeoutMs: 0,
          adaptive: { minCacheBytes: 512, sampleMs: 25, idleMs: 200 },
        },
        slow: {
          factory,
          size: 3,
          cacheBytes: 4096,
          idleTimeoutMs: 0,
          adaptive: { minCacheBytes: 512, sampleMs: 75, idleMs: 400 },
        },
      },
      maxWorkers: 6,
      maxActiveTasks: 6,
      onDiagnostic: (error) => diagnostics.push(error.message),
    });
    const scope = rt.createScope(),
      started = performance.now();
    let cycles = 0,
      completed = 0,
      cancelled = 0,
      pressureCycles = 0;
    const grew = new Set();
    try {
      do {
        const handles = ['fast', 'slow'].flatMap((pool) =>
          Array.from({ length: 6 }, (_, index) => {
            const id = cycles * 100 + (pool === 'fast' ? 0 : 10) + index;
            const handle = scope.enqueue('maintenanceWork', {
              pool,
              group: String(index),
              budget: { inputBytes: 512, scratchBytes: 0, outputBytes: 8 },
              prepare: () => ({ payload: { id, delay: 35 } }),
            });
            const cancel = cycles % 5 === 0 && index === 5;
            if (cancel) handle.cancel();
            return { handle, id, cancel };
          }),
        );
        await Promise.all(
          handles.map(async ({ handle, id, cancel }) => {
            try {
              const value = await consumeResult(handle, (value) => value);
              ensure(!cancel && value === id, 'Result correlation changed');
              completed++;
            } catch (error) {
              ensure(
                cancel && error.code === 'ABORTED',
                `Unexpected task failure: ${error.message}`,
              );
              cancelled++;
            }
            await handle.settled;
          }),
        );
        const stats = rt.stats;
        ensure(
          stats.active === 0 && stats.queued === 0 && stats.leases === 0 && stats.preparing === 0,
          'Task credits did not converge',
        );
        ensure(
          stats.reserved.inputBytes === 0 &&
            stats.reserved.outputBytes === 0 &&
            stats.reserved.scratchBytes === 0,
          'Execution bytes leaked',
        );
        ensure(
          stats.workers <= 6 && stats.reserved.cacheBytes <= 24576 && stats.resourceLeases === 0,
          'Resource limit exceeded',
        );
        for (const pool of rt.diagnostics().pools) {
          if (pool.capacity > 1) grew.add(pool.name);
          ensure(
            pool.capacity >= 1 &&
              pool.capacity <= 3 &&
              pool.cacheBytesPerWorker <= 4096 &&
              pool.cacheUsedBytes <= pool.cacheReservedBytes,
            'Pool target or accounting escaped bounds',
          );
        }
        if (cycles % 3 === 0) {
          // Serial public maintenance is bounded. A sampling pass can briefly own the lock.
          const pressure = async (level) => {
            const deadline = performance.now() + 5000;
            for (;;) {
              try {
                return await rt.setMemoryPressure(level);
              } catch (error) {
                if (error.code !== 'INVALID_ARGUMENT' || performance.now() >= deadline) throw error;
                await wait(5);
              }
            }
          };
          ensure((await pressure('moderate')).failures.length === 0, 'Moderate pressure failed');
          ensure((await pressure('critical')).failures.length === 0, 'Critical pressure failed');
          await wait(90);
          ensure(
            rt.stats.workers === 0 && rt.stats.reserved.cacheBytes === 0,
            'Idle Workers or cache survived critical pressure',
          );
          ensure(
            rt.diagnostics().pools.every((pool) => pool.cacheBytesPerWorker === 0),
            'Adaptive controller ignored pressure',
          );
          ensure((await pressure('normal')).failures.length === 0, 'Pressure recovery failed');
          pressureCycles++;
        }
        cycles++;
      } while (cycles < 24 || performance.now() - started < duration);
      await wait(1000);
      const idle = rt.diagnostics().pools;
      ensure(
        idle.every(
          (pool) => pool.capacity === 1 && pool.workers <= 1 && pool.cacheBytesPerWorker === 512,
        ),
        'Adaptive targets did not reach idle floors',
      );
      ensure(diagnostics.length === 0, diagnostics.join('; '));
      await rt.dispose();
      const after = rt.stats;
      ensure(
        after.workers === 0 &&
          after.scopes === 0 &&
          after.leases === 0 &&
          after.resourceLeases === 0 &&
          Object.values(after.reserved).every((n) => n === 0),
        'Disposal left charged resources',
      );
      ensure(
        after.workerStarts === after.workerTerminations,
        'Physical Worker count did not converge',
      );
      return {
        cycles,
        completed,
        cancelled,
        pressureCycles,
        grew: [...grew].sort(),
        elapsedMs: performance.now() - started,
      };
    } finally {
      await rt.dispose();
    }
  }, soakMs);
  expect(result.cycles).toBeGreaterThanOrEqual(24);
  expect(result.completed).toBeGreaterThan(200);
  expect(result.cancelled).toBeGreaterThan(0);
  expect(result.pressureCycles).toBeGreaterThanOrEqual(8);
  expect(result.grew).toEqual(['fast', 'slow']);
  expect(result.elapsedMs).toBeGreaterThanOrEqual(soakMs);
  console.log(`Adaptive soak: ${JSON.stringify(result)}`);
});

test('session, scope and runtime shutdown during real Worker trim preserve cleanup barriers', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const ensure = (value, message) => {
      if (!value) throw Error(message);
    };
    const base = (payload) => ({
      budget: { inputBytes: 512, scratchBytes: 0, outputBytes: 8 },
      prepare: () => ({ payload }),
    });
    let rounds = 0;
    for (let repeat = 0; repeat < 4; repeat++)
      for (const owner of ['session', 'scope', 'runtime']) {
        let trimStarted;
        const started = new Promise((resolve) => {
          trimStarted = resolve;
        });
        const create = browserWorker('/test/fixtures/browser-worker.mjs');
        const rt = createWorkerRuntime({
          pools: {
            cpu: {
              size: 1,
              cacheBytes: 128,
              factory: () => {
                const endpoint = create();
                return {
                  ...endpoint,
                  onMessage(listener) {
                    return endpoint.onMessage((message) => {
                      if (message?.maintenanceTest === 'trim-started') trimStarted();
                      else listener(message);
                    });
                  },
                };
              },
            },
          },
          releaseTimeoutMs: 3000,
        });
        try {
          const scope = rt.createScope(),
            session = await scope.acquireSession('cpu', { residentBytes: 32 });
          await consumeResult(
            session.enqueue('maintenanceResource', base({ trimMs: 50 })),
            () => {},
          );
          const resizing = rt.resizePool('cpu', { cacheBytes: 16 });
          await started;
          const closing =
            owner === 'session'
              ? session.dispose()
              : owner === 'scope'
                ? scope.dispose()
                : rt.dispose();
          ensure(
            rt.stats.reserved.residentBytes === 32 && rt.stats.reserved.cacheBytes === 128,
            'Credits returned before Worker cleanup',
          );
          const [report] = await Promise.all([resizing, closing]);
          ensure(report.failures.length === 0, 'Maintenance failed during graceful disposal');
          await rt.dispose();
          ensure(
            rt.stats.workers === 0 &&
              rt.stats.resourceLeases === 0 &&
              Object.values(rt.stats.reserved).every((n) => n === 0),
            'Cleanup left reservations',
          );
          rounds++;
        } finally {
          await rt.dispose();
        }
      }
    return rounds;
  });
  expect(result).toBe(12);
});
