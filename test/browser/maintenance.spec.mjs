import { test, expect } from '@playwright/test';

const soakMs = Number(process.env.TASKLANE_SOAK_MS ?? 12000);
if (!Number.isSafeInteger(soakMs) || soakMs < 12000 || soakMs > 3600000)
  throw Error('TASKLANE_SOAK_MS must be an integer between 12000 and 3600000');

test.beforeEach(async ({ page }) => {
  await page.goto('/test/browser/harness.html');
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
