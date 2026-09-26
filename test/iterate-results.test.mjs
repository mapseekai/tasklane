import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, iterateResults, consumeResult, RuntimeError } from '../dist/index.js';
import { deferred } from '../dist/runtime/deferred.js';
import { runtime, options, sleep } from './helpers.mjs';

test('pull helper returns one lease at a time and cleans a borrowed cursor without closing its Session', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const scope = rt.createScope(),
    session = scope.session('cpu');
  let calls = 0,
    closes = 0;
  const chunks = iterateResults({
    next: (signal) => {
      calls++;
      return session.enqueue('cursorNext', options(null, { signal }));
    },
    isDone: (value) => value === null,
    close: async () => {
      closes++;
      await consumeResult(session.enqueue('cursorClose', options(null)), () => {});
    },
  });
  assert.deepEqual(await chunks.next(), { done: false, value: 0 });
  await sleep(5);
  assert.equal(calls, 1);
  assert.equal(rt.stats.leases, 1);
  assert.deepEqual(await chunks.next(), { done: false, value: 1 });
  assert.equal(rt.stats.leases, 1);
  assert.equal((await chunks.next()).done, true);
  await chunks.closed;
  await chunks.dispose();
  assert.equal(closes, 1);
  assert.equal(rt.stats.leases, 0);
  assert.equal(session.state, 'bound');
  await consumeResult(session.enqueue('cursorNext', options(null)), (value) =>
    assert.equal(value, 0),
  );
});

test('break, consumer throw and idle abort close owned scopes', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const make = (signal) => {
    const scope = rt.createScope(),
      session = scope.session('cpu');
    return iterateResults({
      signal,
      next: (signal) => session.enqueue('cursorNext', options(null, { signal })),
      isDone: (v) => v === null,
      close: () => scope.dispose(),
    });
  };
  const short = make();
  for await (const _value of short) break;
  await short.closed;
  assert.equal(rt.stats.scopes, 0);
  const error = make();
  await assert.rejects(async () => {
    for await (const _value of error) throw Error('consumer');
  }, /consumer/);
  await error.closed;
  assert.equal(rt.stats.scopes, 0);
  const controller = new AbortController(),
    idle = make(controller.signal);
  await idle.next();
  controller.abort();
  await idle.closed;
  assert.equal(rt.stats.scopes, 0);
  assert.equal(rt.stats.leases, 0);
  await assert.rejects(idle.next(), { code: 'ABORTED' });
});

for (const abortInsideNext of [false, true]) {
  test(`abort waits for physical completion, including synchronous callback abort=${abortInsideNext}`, async () => {
    const controller = new AbortController(),
      result = deferred(),
      settled = deferred();
    let closes = 0,
      cancels = 0;
    const iterator = iterateResults({
      signal: controller.signal,
      next() {
        if (abortInsideNext) controller.abort();
        return {
          result: result.promise,
          settled: settled.promise,
          cancel() {
            cancels++;
            result.reject(new RuntimeError('ABORTED', 'cancelled'));
          },
        };
      },
      isDone: () => false,
      close: () => {
        closes++;
      },
    });
    const next = iterator.next();
    const assertion = assert.rejects(next, { code: 'ABORTED' });
    await assert.rejects(iterator.next(), { code: 'INVALID_ARGUMENT' });
    if (!abortInsideNext) controller.abort();
    await sleep(5);
    assert.equal(closes, 0);
    assert.equal(cancels, 1);
    settled.resolve();
    await assertion;
    await iterator.closed;
    assert.equal(closes, 1);
  });
}

test('cleanup errors are observable, and combine with a task failure', async () => {
  const failed = iterateResults({
    next() {
      throw Error('request failed');
    },
    isDone: () => false,
    close() {
      throw Error('close failed');
    },
  });
  await assert.rejects(failed.next(), (error) => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(
      error.errors.map((e) => e.message),
      ['request failed', 'close failed'],
    );
    return true;
  });
  await assert.rejects(failed.closed, AggregateError);
  await assert.rejects(failed.dispose(), AggregateError);
});

test('completion checks can abort without delivering a late value', async () => {
  const controller = new AbortController();
  let releases = 0,
    closes = 0;
  const iterator = iterateResults({
    signal: controller.signal,
    next: () => ({
      result: Promise.resolve({
        value: 42,
        release() {
          releases++;
        },
      }),
      settled: Promise.resolve(),
      cancel() {},
    }),
    isDone: () => {
      controller.abort();
      return false;
    },
    close: () => {
      closes++;
    },
  });
  await assert.rejects(iterator.next(), { code: 'ABORTED' });
  await iterator.closed;
  assert.equal(releases, 1);
  assert.equal(closes, 1);
});

test('undefined thrown by an application callback still rejects consumption', async () => {
  let rejected = false,
    closed = false;
  const iterator = iterateResults({
    next: () => {
      throw undefined;
    },
    isDone: () => false,
    close: () => {
      closed = true;
    },
  });
  try {
    await iterator.next();
  } catch (error) {
    assert.equal(error, undefined);
    rejected = true;
  }
  assert.equal(rejected, true);
  assert.equal(closed, true);
});

test('Session loss closes the iterator and returns outstanding credits', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const scope = rt.createScope(),
    session = scope.session('cpu');
  let closes = 0;
  const iterator = iterateResults({
    next: () => session.enqueue('cursorNext', options(null)),
    isDone: (v) => v === null,
    close: () => {
      closes++;
    },
  });
  await iterator.next();
  const running = session.enqueue('wait', options({ ms: 100 }, { cancellation: 'terminate' }));
  await sleep(10);
  running.cancel();
  await assert.rejects(running.result);
  await running.settled;
  await assert.rejects(iterator.next(), { code: 'SESSION_LOST' });
  await iterator.closed;
  assert.equal(closes, 1);
  assert.equal(rt.stats.leases, 0);
});

test('explicit cleanup retry shares concurrent attempts and preserves the first closed outcome', async () => {
  const gate = deferred();
  let closes = 0,
    pulls = 0;
  const iterator = iterateResults({
    next() {
      pulls++;
      throw Error('task failed');
    },
    isDone: () => false,
    async close() {
      if (++closes === 1) throw Error('temporary close failure');
      await gate.promise;
    },
  });
  const firstClosed = iterator.closed;
  await assert.rejects(iterator.next(), AggregateError);
  await assert.rejects(iterator.dispose(), AggregateError);
  assert.equal(closes, 1);
  const retry = iterator.retryCleanup();
  assert.equal(iterator.retryCleanup(), retry);
  assert.equal(iterator.dispose(), retry);
  gate.resolve();
  await retry;
  assert.equal(iterator.closed, firstClosed);
  await assert.rejects(firstClosed, AggregateError);
  await assert.rejects(iterator.next(), /task failed/);
  await iterator.retryCleanup();
  await iterator.dispose();
  assert.equal(closes, 2);
  assert.equal(pulls, 1);
});

test('retry before iteration closes once without starting a request', async () => {
  let closes = 0;
  const iterator = iterateResults({
    next() {
      assert.fail('cleanup must not pull');
    },
    isDone: () => false,
    close() {
      closes++;
    },
  });
  await iterator.retryCleanup();
  await iterator.closed;
  await iterator.retryCleanup();
  assert.equal((await iterator.next()).done, true);
  assert.equal(closes, 1);
});

test('cleanup retry retains Session worker and resident credits until resource disposal completes', async (t) => {
  const { createLoopback } = await import('../dist/testing.js');
  const { serve, output } = await import('../dist/host.js');
  const gate = deferred();
  let disposals = 0,
    pulls = 0,
    stop;
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        cacheBytes: 64,
        idleTimeoutMs: 0,
        factory() {
          const link = createLoopback();
          stop = serve(link.host, {
            open(_value, context) {
              context.cache.setResource('cursor', {}, 16, async () => {
                if (++disposals === 1) throw Error('temporary disposer failure');
                await gate.promise;
              });
              return output(null);
            },
          });
          return link.endpoint;
        },
      },
    },
    budgets: { cacheBytes: 64 },
  });
  t.after(async () => {
    gate.resolve();
    await rt.dispose();
    stop?.();
  });
  const scope = rt.createScope(),
    session = scope.session('cpu');
  const iterator = iterateResults({
    next() {
      pulls++;
      return session.enqueue('open', options(null));
    },
    isDone: () => false,
    close: () => scope.dispose(),
  });
  await iterator.next();
  await assert.rejects(iterator.dispose(), /temporary disposer failure/);
  await assert.rejects(iterator.closed, /temporary disposer failure/);
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 64);
  const retry = iterator.retryCleanup();
  await sleep(5);
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 64);
  gate.resolve();
  await retry;
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.reserved.cacheBytes, 0);
  assert.equal(rt.stats.leases, 0);
  assert.equal(rt.stats.scopes, 0);
  assert.equal(disposals, 2);
  assert.equal(pulls, 1);
});
