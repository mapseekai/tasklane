import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { deferred } from '../dist/runtime/deferred.js';
import { Scheduler } from '../dist/runtime/scheduler.js';
import { sleep, until } from './helpers.mjs';

function setup(t, extra = {}) {
  const gate = deferred();
  const stops = [];
  const order = [];
  const pools = Object.fromEntries(
    ['holder', 'large', 'small'].map((pool) => [
      pool,
      {
        size: 1,
        idleTimeoutMs: 0,
        factory: () => {
          const link = createLoopback();
          stops.push(
            serve(link.host, {
              async hold() {
                await gate.promise;
                return output(null);
              },
              ping() {
                order.push(pool);
                return output(null);
              },
            }),
          );
          return link.endpoint;
        },
      },
    ]),
  );
  const rt = createWorkerRuntime({
    pools,
    budgets: { scratchBytes: 100 },
    budgetWaitMs: 1,
    ...extra,
  });
  t.after(async () => {
    gate.resolve();
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  const scope = rt.createScope();
  const options = (pool, scratchBytes, priority = 'foreground') => ({
    pool,
    priority,
    group: pool,
    budget: { inputBytes: 0, scratchBytes, outputBytes: 0 },
    prepare: () => ({ payload: null }),
  });
  const prepared = (pool, scratch, priority) => {
    const { prepare, ...rest } = options(pool, scratch, priority);
    return scope.enqueuePrepared('ping', {
      ...rest,
      preparationScratchBytes: scratch,
      prepareAsync: prepare,
    });
  };
  return { rt, scope, gate, order, options, prepared };
}
const take = (handle) => consumeResult(handle, () => {});

for (const occupied of ['busy', 'session']) {
  test(`prepared task behind a ${occupied} slot does not reserve away another pool's budget`, async (t) => {
    const { rt, scope, gate, options, prepared } = setup(t);
    const session = occupied === 'session' ? scope.session('large') : undefined;
    if (session) await take(session.enqueue('ping', options('large', 0)));
    const running = scope.enqueue('hold', options(session ? 'holder' : 'large', 60));
    await until(() => running.state === 'running');
    const large = prepared('large', 80);
    await sleep(10);
    const small = scope.enqueue('ping', options('small', 20));
    await until(() => small.state === 'succeeded', 200);
    await take(small);
    assert.equal(large.state, 'queued');
    large.cancel();
    await large.settled;
    gate.resolve();
    await take(running);
    await scope.dispose();
    assert.equal(rt.stats.reserved.scratchBytes, 0);
    assert.equal(rt.stats.active, 0);
  });
}

for (const policy of ['strict', 'ageing', 'equal']) {
  test(`${policy}: effective priority governs budget protection and equal-priority starvation prevention`, async (t) => {
    const { rt, scope, gate, order, options, prepared } = setup(t, {
      priorityPolicy: policy === 'ageing' ? 'ageing' : 'strict',
      ageingMs: 20,
    });
    const running = scope.enqueue('hold', options('holder', 60));
    await until(() => running.state === 'running');
    const large = prepared('large', 80, policy === 'equal' ? 'interactive' : 'background');
    await sleep(policy === 'ageing' ? 60 : 10);
    // Wake selection after the budget wait / ageing interval without consuming scratch.
    await take(scope.enqueue('ping', options('small', 0, 'interactive')));
    order.length = 0;
    const small = scope.enqueue('ping', options('small', 20, 'interactive'));
    if (policy === 'strict') {
      await until(() => small.state === 'succeeded', 200);
      await take(small);
      assert.equal(large.state, 'queued');
    } else {
      await sleep(10);
      assert.equal(small.state, 'queued');
    }
    gate.resolve();
    await take(running);
    await take(large);
    if (policy !== 'strict') {
      await take(small);
      // After the holder ends both budgets fit; physical completion order is independent.
      assert.deepEqual(order.sort(), ['large', 'small']);
    }
    await scope.dispose();
    assert.equal(rt.stats.reserved.scratchBytes, 0);
  });
}

test('a budget reservation is released when an exclusive Session takes its physical slot', async (t) => {
  const { rt, scope, gate, options, prepared } = setup(t);
  const running = scope.enqueue('hold', options('holder', 60));
  await until(() => running.state === 'running');
  const large = prepared('large', 80);
  await sleep(10);
  await take(scope.enqueue('ping', options('small', 0)));
  const session = scope.session('large');
  await take(session.enqueue('ping', options('large', 0, 'interactive')));
  const small = scope.enqueue('ping', options('small', 20));
  await until(() => small.state === 'succeeded', 200);
  await take(small);
  large.cancel();
  await large.settled;
  gate.resolve();
  await take(running);
  await scope.dispose();
  assert.equal(rt.stats.reserved.scratchBytes, 0);
});

test('scheduler exposes current promoted priority during eligibility checks', () => {
  const scheduler = new Scheduler(10, 16, 'ageing');
  const job = { groupKey: 'g', order: 1, enqueuedAt: 0, options: { priority: 'background' } };
  scheduler.add(job);
  for (const [now, rank] of [
    [0, 2],
    [10, 1],
    [20, 0],
  ]) {
    scheduler.select(now, (candidate) => {
      assert.equal(scheduler.priority(candidate), rank);
      return false;
    });
  }
});

test('cancelling a protected large request unblocks fitting work before active work ends', async (t) => {
  const { rt, scope, gate, options, prepared } = setup(t);
  const running = scope.enqueue('hold', options('holder', 60));
  await until(() => running.state === 'running');
  const large = prepared('large', 80);
  await sleep(10);
  await take(scope.enqueue('ping', options('small', 0)));
  const small = scope.enqueue('ping', options('small', 20));
  await sleep(10);
  assert.equal(small.state, 'queued');
  large.cancel();
  await assert.rejects(large.result, { code: 'ABORTED' });
  await large.settled;
  await until(() => small.state === 'succeeded', 200);
  await take(small);
  assert.equal(running.state, 'running');
  assert.equal(rt.stats.reserved.scratchBytes, 60);
  gate.resolve();
  await take(running);
  await scope.dispose();
  assert.deepEqual(rt.stats.reserved, {
    inputBytes: 0,
    scratchBytes: 0,
    outputBytes: 0,
    cacheBytes: 0,
    residentBytes: 0,
  });
});
