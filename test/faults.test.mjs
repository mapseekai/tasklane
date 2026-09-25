import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { header } from '../dist/protocol.js';
import { runtime, options, take, until, sleep } from './helpers.mjs';

function fake(t, mutate = (endpoint) => endpoint, extra = {}) {
  const link = createLoopback();
  let hello;
  link.host.onMessage((message) => {
    if (message.type === 'hello') hello = message;
  });
  const stop = serve(link.host, {
    ping: (v) => output(v),
    async wait(v) {
      await sleep(v);
      return output(v);
    },
  });
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        factory: () => mutate(link.endpoint),
        size: 1,
        cacheBytes: 16,
        allowHardCancel: true,
        idleTimeoutMs: 0,
      },
    },
    maxActiveTasks: 1,
    executionTimeoutMs: 300,
    startupTimeoutMs: 100,
    ...extra,
  });
  t.after(async () => {
    try {
      await rt.dispose().catch(() => {});
    } finally {
      stop();
    }
  });
  return { rt, link, epoch: () => hello.epoch };
}

test('stale worker epoch response cannot complete current task', async (t) => {
  const { rt, link, epoch } = fake(t);
  const scope = rt.createScope();
  const task = scope.enqueue('wait', options(40));
  await until(() => task.state === 'running');
  link.inject({
    ...header(epoch() + 100),
    type: 'result',
    id: task.id,
    scope: scope.id,
    value: 'stale',
    byteLength: 0,
    workerMs: 1,
    cacheBytes: 0,
  });
  assert.equal(task.state, 'running');
  assert.equal(await take(task), 40);
});
test('wrong scope response cannot cross owner boundary', async (t) => {
  const { rt, link, epoch } = fake(t);
  const task = rt.createScope().enqueue('wait', options(30));
  await until(() => task.state === 'running');
  link.inject({
    ...header(epoch()),
    type: 'result',
    id: task.id,
    scope: 'different',
    value: 'wrong',
    byteLength: 0,
    workerMs: 1,
    cacheBytes: 0,
  });
  assert.equal(await take(task), 30);
});
test('invalid envelope fails the physical worker explicitly', async (t) => {
  const { rt, link } = fake(t);
  const task = rt.createScope().enqueue('wait', options(30));
  await until(() => task.state === 'running');
  link.inject({ type: 'invalid' });
  await assert.rejects(task.result, { code: 'PROTOCOL_ERROR' });
  await task.settled;
  assert.equal(rt.stats.workers, 0);
});
test('malformed result byte accounting is rejected', async (t) => {
  const { rt, link, epoch } = fake(t);
  const scope = rt.createScope();
  const task = scope.enqueue('wait', options(30));
  await until(() => task.state === 'running');
  link.inject({
    ...header(epoch()),
    type: 'result',
    id: task.id,
    scope: scope.id,
    value: { kind: 'binary', value: new Uint8Array(8) },
    byteLength: 0,
    workerMs: 1,
    cacheBytes: 0,
  });
  await assert.rejects(task.result, { code: 'BUDGET_EXCEEDED' });
});
test('asynchronous termination retains physical slot and reservations', async (t) => {
  let finishStop;
  const { rt } = fake(t, (endpoint) => ({
    ...endpoint,
    terminate: () =>
      new Promise((resolve) => {
        finishStop = () => {
          endpoint.terminate();
          resolve();
        };
      }),
  }));
  const task = rt.createScope().enqueue(
    'wait',
    options(60, {
      cancellation: 'terminate',
      budget: { inputBytes: 8, scratchBytes: 8, outputBytes: 8 },
    }),
  );
  await until(() => task.state === 'running');
  task.cancel();
  await assert.rejects(task.result);
  assert.equal(rt.stats.closingWorkers, 1);
  assert.equal(rt.stats.active, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 16);
  finishStop();
  await task.settled;
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.reserved.cacheBytes, 0);
});
test('failed termination quarantines rather than pretending completion', async (t) => {
  const { rt } = fake(t, (endpoint) => ({
    ...endpoint,
    terminate: async () => {
      throw new Error('Stop unavailable');
    },
  }));
  const task = rt
    .createScope()
    .enqueue('wait', options(20, { cancellation: 'terminate', executionTimeoutMs: 60 }));
  await until(() => task.state === 'running');
  task.cancel();
  await sleep(2);
  assert.equal(rt.stats.closingWorkers, 1);
  assert.equal(rt.stats.active, 1);
  assert.equal(rt.stats.reserved.cacheBytes, 16);
  await assert.rejects(rt.dispose(), /quarantined/);
});
test('startup handshake timeout never prepares input', async () => {
  const link = createLoopback();
  let prepared = false;
  const rt = createWorkerRuntime({
    pools: { cpu: { factory: () => link.endpoint, size: 1 } },
    startupTimeoutMs: 15,
  });
  const task = rt.createScope().enqueue(
    'ping',
    options(null, {
      prepare: () => {
        prepared = true;
        return { payload: 1 };
      },
    }),
  );
  await assert.rejects(task.result, { code: 'STARTUP_TIMEOUT' });
  await rt.dispose();
  assert.equal(prepared, false);
});
test('factory exceptions release fixed cache reservations', async () => {
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        factory: () => {
          throw new Error('missing URL');
        },
        size: 1,
        cacheBytes: 10,
      },
    },
  });
  await assert.rejects(rt.createScope().enqueue('ping', options(null)).result, {
    code: 'WORKER_FAILED',
  });
  assert.equal(rt.stats.reserved.cacheBytes, 0);
  await rt.dispose();
});
test('failed preparation releases all task credits', async (t) => {
  const { rt } = fake(t);
  const task = rt.createScope().enqueue(
    'ping',
    options(null, {
      budget: { inputBytes: 8, scratchBytes: 8, outputBytes: 8 },
      prepare: () => {
        throw new Error('prepare failed');
      },
    }),
  );
  await assert.rejects(task.result, /prepare failed/);
  await task.settled;
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
  assert.equal(rt.stats.reserved.outputBytes, 0);
});
test('session dispose releases successful session result leases', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const session = rt.createScope().session('cpu');
  const { pool, ...opts } = options(
    { size: 16 },
    { budget: { inputBytes: 1024, scratchBytes: 0, outputBytes: 16 } },
  );
  const lease = await session.enqueue('allocate', opts).result;
  await session.dispose();
  assert.equal(lease.released, true);
  assert.equal(rt.stats.reserved.outputBytes, 0);
});
test('group scheduling metadata does not accumulate across completed groups', async (t) => {
  const { rt } = fake(t);
  const scope = rt.createScope();
  for (let i = 0; i < 100; i++)
    await take(scope.enqueue('ping', options(i, { group: `group-${i}` })));
  assert.ok(rt.scheduler.historySize <= 4096);
  await scope.dispose();
  assert.equal(rt.scheduler.historySize, 0);
});
test('runtime budget and options snapshot resists caller mutation', async (t) => {
  const { rt } = fake(t);
  const scope = rt.createScope();
  const budget = { inputBytes: 8, scratchBytes: 8, outputBytes: 8 };
  const task = scope.enqueue('wait', options(20, { budget }));
  budget.inputBytes = 2 ** 40;
  await take(task);
  assert.equal(rt.stats.peakReserved.inputBytes, 8);
});
