import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { deferred } from '../dist/runtime/deferred.js';
import { until } from './helpers.mjs';
const options = (affinity, group) => ({
  pool: 'cpu',
  group,
  affinity,
  budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 8 },
  prepare: () => ({ payload: null }),
});

test('footprint scores include shared keys on multiple Workers and remain scoped', async (t) => {
  const gate = deferred();
  let serial = 0;
  const stops = [];
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 2,
        idleTimeoutMs: 0,
        factory() {
          const id = ++serial,
            link = createLoopback();
          stops.push(
            serve(link.host, {
              id: () => output(id),
              wait: async () => {
                await gate.promise;
                return output(id);
              },
            }),
          );
          return link.endpoint;
        },
      },
    },
  });
  t.after(async () => {
    gate.resolve();
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  const scope = rt.createScope();
  const keys = ['A', 'B'];
  const first = scope.enqueue('wait', options({ keys }, 'first'));
  keys.splice(0, 2, 'mutated');
  await until(() => first.state === 'running');
  const second = await consumeResult(
    scope.enqueue('id', options({ keys: ['B', 'D'] }, 'second')),
    (id) => id,
  );
  gate.resolve();
  const initial = await consumeResult(first, (id) => id);
  assert.notEqual(initial, second);
  // First is more recently used, so a naive LRU or last-key-only placement would choose second.
  assert.equal(
    await consumeResult(scope.enqueue('id', options({ keys: ['A', 'B', 'B'] })), (id) => id),
    initial,
  );
  assert.equal(
    await consumeResult(rt.createScope().enqueue('id', options({ keys: ['A', 'B'] })), (id) => id),
    second,
  );
  assert.throws(() => scope.enqueue('id', options({ keys: Array(129).fill('A') })), {
    code: 'INVALID_ARGUMENT',
  });
  assert.throws(() => scope.enqueue('id', options({ keys: [1] })), { code: 'INVALID_ARGUMENT' });
});

test('failed work does not create affinity history', async (t) => {
  const gate = deferred();
  let serial = 0;
  const stops = [];
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 2,
        idleTimeoutMs: 0,
        factory() {
          const id = ++serial,
            link = createLoopback();
          stops.push(
            serve(link.host, {
              id: () => output(id),
              fail: () => {
                throw Error('failed');
              },
              wait: async () => {
                await gate.promise;
                return output(id);
              },
            }),
          );
          return link.endpoint;
        },
      },
    },
  });
  t.after(async () => {
    gate.resolve();
    await rt.dispose();
    stops.forEach((stop) => stop());
  });
  const scope = rt.createScope();
  const busy = scope.enqueue('wait', options(undefined, 'busy'));
  await until(() => busy.state === 'running');
  const id2 = await consumeResult(scope.enqueue('id', options()), (id) => id);
  await assert.rejects(scope.enqueue('fail', options('failed-key')).result);
  gate.resolve();
  const id1 = await consumeResult(busy, (id) => id);
  // Make worker 2 newest; the failed key must not override the older worker 1 fallback.
  await consumeResult(scope.enqueue('id', options()), (id) => assert.equal(id, id2));
  assert.equal(await consumeResult(scope.enqueue('id', options('failed-key')), (id) => id), id1);
});
