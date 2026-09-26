import { test, expect } from '@playwright/test';

test.beforeEach(async ({ page }) => {
  await page.goto('/test/browser/harness.html');
});

test('Session footprints, interactive reserves and pressure trim cross real Worker control messages', async ({
  page,
}) => {
  const actual = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
          size: 3,
          cacheBytes: 128,
          interactiveWorkers: 1,
        },
      },
      maxActiveTasks: 3,
      maxResultLeases: 3,
      interactiveReserve: { workers: 1, activeTasks: 1, resultLeases: 1 },
    });
    const scope = rt.createScope();
    const options = (payload) => ({
      budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
      prepare: () => ({ payload }),
    });
    const take = (h) => consumeResult(h, (v) => v);
    try {
      const a = await scope.acquireSession('cpu'),
        b = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 64 });
      const epochA = await take(a.enqueue('footprint', options(['block-a']))),
        epochB = await take(b.enqueue('footprint', options(['block-b', 'block-c'])));
      const group = scope.sessionGroup([a, b]);
      const selected = await take(
        group.enqueue('ping', { ...options(null), affinity: { keys: ['block-b', 'block-c'] } }),
      );
      let blocked;
      try {
        await scope.acquireSession('cpu', { mode: 'immediate' });
      } catch (error) {
        blocked = error.reasons.includes('interactive-reserve');
      }
      const interactive = await scope.acquireSession('cpu', { priority: 'interactive' });
      await take(interactive.enqueue('ping', options(null)));
      await interactive.dispose();
      const report = await rt.setMemoryPressure('critical');
      const after = {
        cache: rt.stats.reserved.cacheBytes,
        resident: rt.stats.reserved.residentBytes,
        keys: rt.diagnostics().pools[0].resources[0].keys,
        reclaim: rt.diagnostics().pools[0].reclaim,
        resource: rt.stats.resourceCacheStats,
      };
      const fallback = await take(group.enqueue('ping', { ...options(null), affinity: 'block-b' }));
      await rt.setMemoryPressure('normal');
      return {
        routed: selected.epoch === epochB,
        fallback: fallback.epoch === epochA,
        blocked,
        reclaimed: report.workersReclaimed,
        failures: report.failures.length,
        after,
        restored: rt.diagnostics().pools[0].cacheBytesPerWorker,
      };
    } finally {
      await rt.dispose();
    }
  });
  expect(actual.routed).toBe(true);
  expect(actual.fallback).toBe(true);
  expect(actual.blocked).toBe(true);
  expect(actual.reclaimed).toBe(1);
  expect(actual.failures).toBe(0);
  expect(actual.restored).toBe(128);
  expect(actual.after.cache).toBe(0);
  expect(actual.after.resident).toBe(0);
  expect(actual.after.keys).toEqual([]);
  expect(actual.after.reclaim.succeeded).toBe(1);
  expect(actual.after.reclaim.byReason.pressure).toBe(1);
  expect(actual.after.resource).toEqual({ hits: 4, misses: 2, evictions: 1 });
});

test('budgeted preparation, remote errors and public pull helper use real Workers', async ({
  page,
}) => {
  const actual = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult, iterateResults } = await import(
      '/dist/index.js'
    );
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          size: 1,
          cacheBytes: 128,
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
        },
      },
    });
    const scope = rt.createScope();
    const base = { pool: 'cpu', budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 } };
    let release, begin;
    const gate = new Promise((r) => {
      release = r;
    });
    const started = new Promise((r) => {
      begin = r;
    });
    try {
      const task = scope.enqueuePrepared('ping', {
        ...base,
        preparationScratchBytes: 16,
        prepareAsync: async () => {
          begin();
          await gate;
          return { payload: 42 };
        },
      });
      await started;
      const during = {
        workers: rt.stats.workers,
        input: rt.stats.reserved.inputBytes,
        preparing: rt.stats.preparing,
      };
      release();
      const value = await consumeResult(task, (v) => v.value);
      let error;
      try {
        await scope.enqueue('businessError', { ...base, prepare: () => ({ payload: null }) })
          .result;
      } catch (e) {
        error = {
          code: e.code,
          name: e.remoteError.name,
          remoteCode: e.remoteError.code,
          details: e.remoteError.details,
        };
      }
      const session = scope.session('cpu');
      const chunks = [];
      const iterator = iterateResults({
        next: (signal) =>
          session.enqueue('cursorNext', { ...base, prepare: () => ({ payload: null }), signal }),
        isDone: (v) => v === null,
        close: () =>
          consumeResult(
            session.enqueue('cursorClose', { ...base, prepare: () => ({ payload: null }) }),
            () => {},
          ),
      });
      for await (const item of iterator) chunks.push(item);
      const borrowed = session.state;
      await scope.dispose();
      return {
        during,
        value,
        error,
        chunks,
        borrowed,
        scopes: rt.stats.scopes,
        leases: rt.stats.leases,
        bytes: rt.stats.reserved,
      };
    } finally {
      release();
      await rt.dispose();
    }
  });
  expect(actual).toEqual({
    during: { workers: 0, input: 4096, preparing: 1 },
    value: 42,
    error: {
      code: 'REMOTE_ERROR',
      name: 'DataError',
      remoteCode: 'READ_BUDGET',
      details: { limit: 32 },
    },
    chunks: [0, 1],
    borrowed: 'bound',
    scopes: 0,
    leases: 0,
    bytes: { inputBytes: 0, scratchBytes: 0, outputBytes: 0, cacheBytes: 0, residentBytes: 0 },
  });
});

test('File metadata, range reads, logical limits and pull-session cleanup in a real Worker', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const { readFileChunks } = await import('/examples/file-chunks/read.mjs');
    const rt = createWorkerRuntime({
      pools: {
        cpu: { size: 1, factory: browserWorker('/test/fixtures/browser-worker.mjs') },
        files: {
          size: 1,
          cacheBytes: 128,
          factory: browserWorker('/examples/file-chunks/worker.mjs'),
        },
      },
    });
    const file = new File([new Uint8Array(1024 * 1024).fill(42)], '地图.tif', {
      type: 'image/tiff',
      lastModified: 123,
    });
    const scope = rt.createScope();
    const options = {
      pool: 'cpu',
      budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
      blobLimits: { inputBytes: file.size, outputBytes: file.size },
      prepare: () => ({ payload: { file, again: file } }),
    };
    try {
      const metadata = await consumeResult(scope.enqueue('ping', options), async ({ value }) => ({
        name: value.file.name,
        type: value.file.type,
        lastModified: value.file.lastModified,
        size: value.file.size,
        alias: value.file === value.again,
        range: Array.from(new Uint8Array(await value.file.slice(17, 20).arrayBuffer())),
      }));
      const errors = [];
      for (const blobLimits of [undefined, { inputBytes: file.size, outputBytes: 0 }]) {
        try {
          await consumeResult(scope.enqueue('ping', { ...options, blobLimits }), () => {});
        } catch (error) {
          errors.push(error.code);
        }
      }
      let total = 0;
      for await (const chunk of readFileChunks(rt, file)) total += chunk.byteLength;
      for await (const _chunk of readFileChunks(rt, file)) break;
      const controller = new AbortController();
      try {
        for await (const _chunk of readFileChunks(rt, file, { signal: controller.signal }))
          controller.abort();
      } catch (error) {
        errors.push(error.code);
      }
      await scope.dispose();
      return {
        metadata,
        errors,
        total,
        leases: rt.stats.leases,
        scopes: rt.stats.scopes,
        cache: rt.stats.reserved.cacheBytes,
      };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    metadata: {
      name: '地图.tif',
      type: 'image/tiff',
      lastModified: 123,
      size: 1024 * 1024,
      alias: true,
      range: [42, 42, 42],
    },
    errors: ['BUDGET_EXCEEDED', 'BUDGET_EXCEEDED', 'ABORTED'],
    total: 1024 * 1024,
    leases: 0,
    scopes: 0,
    cache: 0,
  });
});

test('metadata budgets, numeric graph roundtrip, scratch and synchronous preparation', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: { cpu: { size: 1, factory: browserWorker('/test/fixtures/browser-worker.mjs') } },
    });
    const scope = rt.createScope();
    const options = (value) => ({
      pool: 'cpu',
      budget: { inputBytes: 8, scratchBytes: 8, outputBytes: 8 },
      prepare: () => ({ payload: value }),
    });
    const errors = [];
    try {
      for (const [name, opts] of [
        ['ping', options('x'.repeat(2_000_000))],
        ['oversizedOutput', options(null)],
        ['scratch', options(null)],
        ['ping', { ...options(null), prepare: () => new Promise(() => {}) }],
      ])
        errors.push(
          await scope.enqueue(name, opts).result.then(
            () => 'unexpected success',
            (e) => e.code,
          ),
        );
      const numbers = Array(200_000).fill(42);
      const valid = await consumeResult(
        scope.enqueue('ping', {
          ...options(numbers),
          budget: { inputBytes: 16 * 1024 ** 2, scratchBytes: 0, outputBytes: 16 * 1024 ** 2 },
        }),
        (value) => value.value.length === numbers.length && value.value.every((n) => n === 42),
      );
      return { errors, valid, active: rt.stats.active, leases: rt.stats.leases };
    } finally {
      await rt.disposeWithin(1000);
    }
  });
  expect(result).toEqual({
    errors: ['BUDGET_EXCEEDED', 'BUDGET_EXCEEDED', 'BUDGET_EXCEEDED', 'INVALID_ARGUMENT'],
    valid: true,
    active: 0,
    leases: 0,
  });
});

test('bounded progress, completed contexts, discarded results and async resource cleanup', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    let progressMessages = 0;
    const factory = browserWorker('/test/fixtures/browser-worker.mjs');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          size: 1,
          cacheBytes: 8,
          factory: () => {
            const endpoint = factory();
            return {
              ...endpoint,
              onMessage(listener) {
                return endpoint.onMessage((message) => {
                  if (message.type === 'progress') progressMessages++;
                  listener(message);
                });
              },
            };
          },
        },
      },
    });
    const options = {
      pool: 'cpu',
      budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
      prepare: () => ({ payload: null }),
    };
    try {
      const scope = rt.createScope();
      const error = await scope.enqueue('oversizedProgress', options).result.then(
        () => null,
        (e) => e.code,
      );
      await consumeResult(scope.enqueue('lateProgress', options), () => {});
      await new Promise((resolve) => setTimeout(resolve, 80));
      for (let i = 0; i < 10; i++)
        await scope.enqueue('ping', { ...options, discardResult: true }).settled;
      const leases = rt.stats.leases;
      const session = scope.session('cpu');
      await consumeResult(session.enqueue('resource', options), () => {});
      let closed = false;
      const end = session.dispose().then(() => (closed = true));
      await new Promise((resolve) => setTimeout(resolve, 5));
      const early = closed;
      await end;
      return { error, progressMessages, leases, early, workers: rt.stats.workers };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    error: 'BUDGET_EXCEEDED',
    progressMessages: 0,
    leases: 0,
    early: false,
    workers: 0,
  });
});

for (const transfer of [true, false]) {
  test(`real worker binary ownership: transfer=${transfer}`, async ({ page }) => {
    const result = await page.evaluate(async (transfer) => {
      const { createWorkerRuntime, browserWorker, transferBuffers } = await import(
        '/dist/index.js'
      );
      const rt = createWorkerRuntime({
        pools: { cpu: { factory: browserWorker('/test/fixtures/browser-worker.mjs'), size: 2 } },
      });
      const bytes = new Uint8Array(8 * 1024 ** 2);
      bytes[123] = 91;
      const scope = rt.createScope();
      const lease = await scope.enqueue('echo', {
        pool: 'cpu',
        budget: {
          inputBytes: bytes.byteLength + 1024,
          scratchBytes: bytes.byteLength,
          outputBytes: bytes.byteLength,
        },
        prepare: () => ({
          payload: { bytes, transfer },
          transfer: transfer ? transferBuffers(bytes) : [],
        }),
      }).result;
      const result = {
        size: lease.value.byteLength,
        byte: lease.value[123],
        detached: bytes.byteLength === 0,
      };
      lease.release();
      await rt.dispose();
      return {
        ...result,
        workers: rt.stats.workers,
        active: rt.stats.active,
        outputBytes: rt.stats.reserved.outputBytes,
      };
    }, transfer);
    expect(result).toEqual({
      size: 8 * 1024 ** 2,
      byte: 91,
      detached: transfer,
      workers: 0,
      active: 0,
      outputBytes: 0,
    });
  });
}

test('slow result consumer applies backpressure before prepare', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: { cpu: { factory: browserWorker('/test/fixtures/browser-worker.mjs'), size: 2 } },
      budgets: { outputBytes: 64 },
      maxActiveTasks: 2,
    });
    const scope = rt.createScope();
    let prepared = 0;
    const opts = () => ({
      pool: 'cpu',
      budget: { inputBytes: 1024, scratchBytes: 0, outputBytes: 64 },
      prepare: () => {
        prepared++;
        return { payload: { size: 64 } };
      },
    });
    const a = scope.enqueue('allocate', opts()),
      b = scope.enqueue('allocate', opts());
    const first = await a.result;
    await new Promise((r) => setTimeout(r, 30));
    const before = { prepared, state: b.state, bytes: rt.stats.reserved.outputBytes };
    first.release();
    const second = await b.result;
    second.release();
    await rt.dispose();
    return { before, prepared, peak: rt.stats.peakReserved.outputBytes };
  });
  expect(result).toEqual({
    before: { prepared: 1, state: 'queued', bytes: 64 },
    prepared: 2,
    peak: 64,
  });
});

test('scope isolation, ordered result correlation, errors and session state', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
          size: 2,
          cacheBytes: 1024,
        },
      },
    });
    const a = rt.createScope('map'),
      b = rt.createScope('map');
    const opts = (payload) => ({
      pool: 'cpu',
      budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
      prepare: () => ({ payload }),
    });
    const take = async (h) => {
      const lease = await h.result;
      const v = lease.value;
      lease.release();
      return v;
    };
    const values = await Promise.all(
      Array.from({ length: 40 }, (_, i) => take((i % 2 ? a : b).enqueue('ping', opts(i)))),
    );
    let error;
    try {
      await take(a.enqueue('missing', opts(null)));
    } catch (e) {
      error = e.code;
    }
    const session = a.session('cpu');
    const { pool, ...first } = opts({ key: 'state', value: 23, pinned: true });
    await take(session.enqueue('cache', first));
    const { pool: ignored, ...second } = opts({ key: 'state' });
    const state = await take(session.enqueue('cache', second));
    await session.dispose();
    await a.dispose();
    const remaining = await take(b.enqueue('ping', opts(99)));
    await rt.dispose();
    return {
      values: values.map((v) => v.value),
      isolated: values[0].scope !== values[1].scope,
      error,
      state,
      remaining: remaining.value,
      workers: rt.stats.workers,
    };
  });
  expect(result).toEqual({
    values: Array.from({ length: 40 }, (_, i) => i),
    isolated: true,
    error: 'UNKNOWN_TASK',
    state: 23,
    remaining: 99,
    workers: 0,
  });
});

for (const mode of ['cooperative', 'discard', 'terminate']) {
  test(`physical cancellation semantics: ${mode}`, async ({ page }) => {
    const result = await page.evaluate(async (mode) => {
      const { createWorkerRuntime, browserWorker } = await import('/dist/index.js');
      const rt = createWorkerRuntime({
        pools: {
          cpu: {
            factory: browserWorker('/test/fixtures/browser-worker.mjs'),
            size: 1,
            allowHardCancel: true,
          },
        },
        maxActiveTasks: 1,
      });
      const scope = rt.createScope();
      const task = scope.enqueue(mode === 'terminate' ? 'spin' : 'wait', {
        pool: 'cpu',
        budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
        cancellation: mode,
        prepare: () => ({
          payload: { ms: mode === 'terminate' ? 2000 : 150, cooperate: mode === 'cooperative' },
        }),
      });
      while (task.state !== 'running') await new Promise((r) => setTimeout(r, 2));
      task.cancel();
      let code;
      try {
        await task.result;
      } catch (e) {
        code = e.code;
      }
      const occupied = rt.stats.active;
      await task.settled;
      const stopped = rt.stats.workerTerminations;
      await rt.dispose();
      return { code, occupied, stopped, state: task.state, workers: rt.stats.workers };
    }, mode);
    expect(result.code).toBe('ABORTED');
    expect(result.state).toBe('cancelled');
    expect(result.workers).toBe(0);
    if (mode === 'discard') expect(result.occupied).toBe(1);
    if (mode === 'terminate') expect(result.stopped).toBe(1);
  });
}

test('one million points: exact binary output agrees with main-thread reference', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, transferBuffers } = await import('/dist/index.js');
    const { coordinates, convert, checksum } = await import('/benchmarks/workloads.mjs');
    const xy = coordinates(1_000_000);
    const expected = convert({ xy });
    const rt = createWorkerRuntime({
      pools: { cpu: { factory: browserWorker('/test/fixtures/browser-worker.mjs'), size: 1 } },
    });
    const scope = rt.createScope();
    const lease = await scope.enqueue('convert', {
      pool: 'cpu',
      budget: {
        inputBytes: xy.byteLength + 1024,
        scratchBytes: 0,
        outputBytes: xy.byteLength + 1024,
      },
      prepare: () => ({ payload: { xy }, transfer: transferBuffers(xy) }),
    }).result;
    const result = {
      match: checksum(expected.vertices) === checksum(lease.value.vertices),
      count: lease.value.count,
      bounds: Array.from(lease.value.bounds),
      referenceBounds: Array.from(expected.bounds),
    };
    lease.release();
    await rt.dispose();
    return result;
  });
  expect(result.match).toBe(true);
  expect(result.count).toBe(1_000_000);
  expect(result.bounds).toEqual(result.referenceBounds);
});

test('named Worker keeps the default module type', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          factory: browserWorker('/test/fixtures/browser-worker.mjs', { name: 'named-test' }),
          size: 1,
        },
      },
    });
    try {
      const scope = rt.createScope();
      const lease = await scope.enqueue('ping', {
        pool: 'cpu',
        budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
        prepare: () => ({ payload: 12 }),
      }).result;
      const value = lease.value.value;
      lease.release();
      return value;
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toBe(12);
});
test('standalone example completes and releases its workers', async ({ page }) => {
  await page.goto('/examples/index.html');
  await page.getByRole('button', { name: '开始转换' }).click();
  await expect(page.locator('#status')).toContainText('完成：64 MiB');
  await expect(page.locator('#status')).toContainText('已释放 Worker：0；活跃任务：0');
  await expect(page.getByRole('button', { name: '开始转换' })).toBeEnabled();
});
test('standalone example cancellation converges', async ({ page }) => {
  await page.goto('/examples/index.html');
  await page.locator('#size').selectOption('1024');
  await page.getByRole('button', { name: '开始转换' }).click();
  await page.getByRole('button', { name: '取消', exact: true }).click();
  await expect(page.locator('#status')).toContainText('已释放 Worker：0；活跃任务：0');
  await expect(page.getByRole('button', { name: '开始转换' })).toBeEnabled();
});

test('budget protection respects occupied Workers and iterator cleanup retries explicitly', async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult, iterateResults } = await import(
      '/dist/index.js'
    );
    const factory = browserWorker('/test/fixtures/browser-worker.mjs');
    const rt = createWorkerRuntime({
      pools: { busy: { factory, size: 1 }, idle: { factory, size: 1 } },
      budgets: { scratchBytes: 100 },
      budgetWaitMs: 1,
    });
    const scope = rt.createScope();
    const opts = (pool, scratchBytes, payload) => ({
      pool,
      budget: { inputBytes: 4096, scratchBytes, outputBytes: 4096 },
      prepare: () => ({ payload }),
    });
    try {
      // Warm both physical Workers so startup speed cannot decide the assertion.
      await Promise.all(
        ['busy', 'idle'].map((pool) =>
          consumeResult(scope.enqueue('ping', opts(pool, 0, null)), () => {}),
        ),
      );
      const running = scope.enqueue('wait', opts('busy', 60, { ms: 500 }));
      while (running.state !== 'running') await new Promise((r) => setTimeout(r, 2));
      const large = scope.enqueuePrepared('ping', {
        ...opts('busy', 80, null),
        preparationScratchBytes: 80,
        prepareAsync: async () => ({ payload: null }),
      });
      await new Promise((r) => setTimeout(r, 20));
      await consumeResult(scope.enqueue('ping', opts('idle', 20, null)), () => {});
      const state = running.state;
      large.cancel();
      await large.settled;
      await consumeResult(running, () => {});
      let attempts = 0;
      const iterator = iterateResults({
        next() {
          throw Error('unexpected pull');
        },
        isDone: () => false,
        async close() {
          if (++attempts === 1) throw Error('temporary');
          await scope.dispose();
        },
      });
      let firstFailure;
      try {
        await iterator.dispose();
      } catch (error) {
        firstFailure = error.message;
      }
      await iterator.retryCleanup();
      let closedFailure;
      try {
        await iterator.closed;
      } catch (error) {
        closedFailure = error.message;
      }
      return {
        state,
        attempts,
        firstFailure,
        closedFailure,
        scopes: rt.stats.scopes,
        scratch: rt.stats.reserved.scratchBytes,
      };
    } finally {
      await rt.dispose();
    }
  });
  expect(result).toEqual({
    state: 'running',
    attempts: 2,
    firstFailure: 'temporary',
    closedFailure: 'temporary',
    scopes: 0,
    scratch: 0,
  });
});

test('resident leases, admitted Sessions, replica reclaim and sized chunks use real Workers', async ({
  page,
}) => {
  const actual = await page.evaluate(async () => {
    const {
      createWorkerRuntime,
      browserWorker,
      consumeResult,
      iterateSizedResults,
      transferOwnedBuffers,
    } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          size: 1,
          cacheBytes: 128,
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
        },
      },
      budgets: { residentBytes: 64 },
    });
    const scope = rt.createScope();
    const options = (payload, outputBytes = 1024) => ({
      budget: { inputBytes: 1024, scratchBytes: 0, outputBytes },
      prepare: () => ({ payload }),
    });
    try {
      const replica = await scope.acquireSession('cpu', { reclaimable: true, mode: 'immediate' });
      await consumeResult(replica.enqueue('resource', options(null)), () => {});
      const session = await scope.acquireSession('cpu', { timeoutMs: 5000 });
      const resized = await consumeResult(session.enqueue('sizedOpen', options(null)), (v) => v);
      const iterator = iterateSizedResults({
        session,
        task: 'sizedTake',
        maxChunkBytes: 64,
        budget: { inputBytes: 1024, scratchBytes: 0 },
        describe: (signal) => session.enqueue('sizedDescribe', { ...options(null), signal }),
        prepare: (chunk) => ({ payload: chunk.token }),
        close: () => session.dispose(),
      });
      const sizes = [],
        held = [];
      for await (const bytes of iterator) {
        const reservation = scope.resources.acquire({ kind: 'resident', bytes: bytes.byteLength });
        held.push(rt.stats.reserved.residentBytes);
        sizes.push(bytes.byteLength);
        reservation.release();
      }
      await iterator.closed;
      const retained = scope.resources.acquire({ kind: 'resident', bytes: 16 });
      const buffer = new Uint8Array(16);
      structuredClone(buffer, { transfer: transferOwnedBuffers(buffer) });
      await scope.dispose();
      return {
        sizes,
        held,
        resized,
        reclaimed: replica.reclaimed,
        reclaimedCount: rt.stats.sessionsReclaimed,
        workers: rt.stats.workers,
        leases: rt.stats.leases,
        resident: rt.stats.reserved.residentBytes,
        released: retained.released,
        detached: buffer.byteLength,
        waiting: rt.diagnostics().waiting.length,
      };
    } finally {
      await rt.dispose();
    }
  });
  expect(actual).toEqual({
    sizes: [16, 64, 8],
    held: [16, 64, 8],
    resized: 32,
    reclaimed: true,
    reclaimedCount: 1,
    workers: 0,
    leases: 0,
    resident: 0,
    released: true,
    detached: 0,
    waiting: 0,
  });
});

test('joint resident admission, binary cache and cumulative telemetry work across real Worker replacement', async ({
  page,
}) => {
  const actual = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult } = await import('/dist/index.js');
    const rt = createWorkerRuntime({
      pools: {
        cpu: {
          size: 2,
          cacheBytes: 64,
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
        },
      },
      budgets: { residentBytes: 32 },
    });
    const scope = rt.createScope();
    try {
      const replica = await scope.acquireSession('cpu', { reclaimable: true, residentBytes: 32 });
      const primary = await scope.acquireSession('cpu', { residentBytes: 32 });
      const value = await consumeResult(
        primary.enqueue('binaryCache', {
          budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 1024 },
          prepare: () => ({ payload: null }),
        }),
        (v) => v,
      );
      const bound = rt.stats.reserved.residentBytes;
      await primary.dispose();
      return {
        value,
        reclaimed: replica.reclaimed,
        released: replica.resident.released,
        bound,
        resident: rt.stats.reserved.residentBytes,
        workers: rt.stats.workers,
        cache: rt.stats.cacheStats,
        pool: rt.diagnostics().pools[0].cacheStats,
      };
    } finally {
      await rt.dispose();
    }
  });
  expect(actual).toEqual({
    value: { native: true, sameBuffer: true, value: 42, extra: false },
    reclaimed: true,
    released: true,
    bound: 32,
    resident: 0,
    workers: 0,
    cache: { hits: 1, misses: 1, evictions: 1 },
    pool: { hits: 1, misses: 1, evictions: 1 },
  });
});
