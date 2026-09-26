import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { sleep, until } from './helpers.mjs';

function gate() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
function setup(t, extra = {}, handlers = { echo: (value) => output(value) }) {
  const stops = [],
    gates = [];
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 2,
        factory: () => {
          const link = createLoopback();
          stops.push(serve(link.host, handlers));
          return link.endpoint;
        },
      },
    },
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
    gate: () => {
      const g = gate();
      gates.push(g);
      return g;
    },
  };
}
const options = (prepareAsync, extra = {}) => ({
  pool: 'cpu',
  budget: { inputBytes: 8, scratchBytes: 4, outputBytes: 8 },
  preparationScratchBytes: 16,
  prepareAsync,
  ...extra,
});

test('asynchronous production reserves the whole envelope before allocation and binds no Worker', async (t) => {
  const { rt, scope, gate } = setup(t, {
    maxPreparingTasks: 1,
    budgets: { inputBytes: 8, scratchBytes: 16, outputBytes: 8 },
  });
  const g = gate();
  let calls = 0;
  const a = scope.enqueuePrepared(
    'echo',
    options(
      async () => {
        calls++;
        assert.deepEqual(rt.stats.reserved, {
          inputBytes: 8,
          scratchBytes: 16,
          outputBytes: 8,
          cacheBytes: 0,
          residentBytes: 0,
        });
        assert.equal(rt.stats.workers, 0);
        await g.promise;
        return { payload: 42 };
      },
      { group: 'a' },
    ),
  );
  const b = scope.enqueuePrepared(
    'echo',
    options(
      async () => {
        calls++;
        return { payload: 43 };
      },
      { group: 'b' },
    ),
  );
  await until(() => a.state === 'preparing');
  assert.equal(calls, 1);
  assert.equal(b.state, 'queued');
  assert.equal(rt.stats.preparing, 1);
  g.resolve();
  const lease = await a.result;
  assert.equal(lease.value, 42);
  assert.equal(calls, 1); // output reservation still held by the first result
  lease.release();
  await consumeResult(b, (value) => assert.equal(value, 43));
  assert.equal(rt.stats.preparing + rt.stats.prepared, 0);
  assert.equal(rt.stats.reserved.scratchBytes, 0);
});

test('cancelled producer holds credits and scope disposal until it physically returns; late buffers stay owned', async (t) => {
  const { rt, scope, gate } = setup(t);
  const g = gate(),
    bytes = new Uint8Array(8);
  const h = scope.enqueuePrepared(
    'echo',
    options(async () => {
      await g.promise;
      return { payload: bytes, transfer: [bytes.buffer] };
    }),
  );
  await until(() => h.state === 'preparing');
  h.cancel();
  await assert.rejects(h.result, { code: 'ABORTED' });
  let settled = false;
  void h.settled.then(() => {
    settled = true;
  });
  await assert.rejects(scope.disposeWithin(10), { code: 'EXECUTION_TIMEOUT' });
  assert.equal(settled, false);
  assert.equal(rt.stats.reserved.inputBytes, 8);
  assert.deepEqual(rt.resourceDiagnostics().preparing, [h.id]);
  g.resolve();
  await h.settled;
  await scope.dispose();
  assert.equal(bytes.byteLength, 8);
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
});

test('preparation deadline rejects delivery while waiting for physical production', async (t) => {
  const { rt, scope, gate } = setup(t);
  const g = gate();
  const h = scope.enqueuePrepared(
    'echo',
    options(
      async () => {
        await g.promise;
        return { payload: 1 };
      },
      { preparationTimeoutMs: 10 },
    ),
  );
  await assert.rejects(h.result, { code: 'EXECUTION_TIMEOUT' });
  assert.equal(h.state, 'cancelling');
  assert.equal(rt.stats.reserved.inputBytes, 8);
  g.resolve();
  await h.settled;
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
});

test('preparing plus prepared inputs share a bounded window while a Session holds the Worker', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1, maxPreparingTasks: 1 });
  const session = scope.session('cpu');
  await consumeResult(
    session.enqueue('echo', {
      budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
      prepare: () => ({ payload: 0 }),
    }),
    () => {},
  );
  const a = scope.enqueuePrepared(
    'echo',
    options(async () => ({ payload: 1 }), { group: 'a' }),
  );
  let started = false;
  const b = scope.enqueuePrepared(
    'echo',
    options(
      async () => {
        started = true;
        return { payload: 2 };
      },
      { group: 'b' },
    ),
  );
  await until(() => a.state === 'prepared');
  assert.equal(started, false);
  assert.equal(rt.stats.prepared, 1);
  a.cancel();
  await assert.rejects(a.result, { code: 'ABORTED' });
  await a.settled;
  await until(() => b.state === 'prepared');
  await session.dispose();
  await consumeResult(b, (v) => assert.equal(v, 2));
  assert.equal(rt.stats.reserved.inputBytes, 0);
});

test('producer failure, invalid submission and Worker factory failure return credits once', async (t) => {
  const { rt, scope } = setup(t);
  assert.throws(
    () =>
      scope.enqueuePrepared(
        'echo',
        options(async () => ({ payload: 1 }), { preparationScratchBytes: -1 }),
      ),
    { code: 'INVALID_ARGUMENT' },
  );
  const h = scope.enqueuePrepared(
    'echo',
    options(async () => {
      throw new Error('pack failed');
    }),
  );
  await assert.rejects(h.result, /pack failed/);
  await h.settled;
  assert.equal(rt.stats.reserved.inputBytes, 0);
  assert.equal(rt.stats.preparing, 0);
  const bad = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        factory: () => {
          throw Error('factory failed');
        },
      },
    },
  });
  try {
    const fail = bad.createScope().enqueuePrepared(
      'echo',
      options(async () => ({ payload: 1 })),
    );
    await assert.rejects(fail.result, { code: 'WORKER_FAILED' });
    await fail.settled;
    assert.equal(bad.stats.reserved.inputBytes, 0);
    assert.equal(bad.stats.prepared, 0);
  } finally {
    await bad.dispose();
  }
});

test('separate groups prepare concurrently and result slots count preparation reservations', async (t) => {
  const { rt, scope, gate } = setup(t, { maxPreparingTasks: 2, maxResultLeases: 2 });
  const g = gate();
  let started = 0;
  const tasks = ['a', 'b', 'c'].map((group) =>
    scope.enqueuePrepared(
      'echo',
      options(
        async () => {
          started++;
          await g.promise;
          return { payload: 1 };
        },
        { group },
      ),
    ),
  );
  await until(() => started === 2);
  assert.equal(rt.stats.workers, 0);
  assert.equal(tasks[2].state, 'queued');
  g.resolve();
  await Promise.all(tasks.map((h) => consumeResult(h, () => {})));
  assert.equal(started, 3);
});

test('execution deadline also bounds prepared inputs waiting for a Worker', async (t) => {
  const { rt, scope } = setup(t, { maxWorkers: 1 });
  const s = scope.session('cpu');
  await consumeResult(
    s.enqueue('echo', {
      budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
      prepare: () => ({ payload: 0 }),
    }),
    () => {},
  );
  const h = scope.enqueuePrepared(
    'echo',
    options(async () => ({ payload: 1 }), { executionTimeoutMs: 20 }),
  );
  await assert.rejects(h.result, { code: 'EXECUTION_TIMEOUT' });
  await h.settled;
  assert.equal(rt.stats.prepared, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
});

test('queued cancellation skips production and producer FIFO is preserved within a group', async (t) => {
  const { scope, gate } = setup(t);
  const g = gate(),
    order = [];
  const a = scope.enqueuePrepared(
    'echo',
    options(async () => {
      order.push('a');
      await g.promise;
      return { payload: 1 };
    }),
  );
  const b = scope.enqueuePrepared(
    'echo',
    options(async () => {
      order.push('b');
      return { payload: 2 };
    }),
  );
  const c = scope.enqueuePrepared(
    'echo',
    options(async () => {
      assert.fail('cancelled callback');
    }),
  );
  c.cancel();
  await assert.rejects(c.result, { code: 'ABORTED' });
  await until(() => a.state === 'preparing');
  await sleep(5);
  assert.deepEqual(order, ['a']);
  g.resolve();
  await Promise.all([a, b].map((h) => consumeResult(h, () => {})));
  assert.deepEqual(order, ['a', 'b']);
});

test('lost Session aborts pending production but holds its credits through completion', async (t) => {
  const { rt, scope, gate } = setup(t, {
    pools: {
      cpu: {
        size: 1,
        allowHardCancel: true,
        factory: () => {
          const link = createLoopback();
          serve(link.host, {
            wait: async () => {
              await sleep(100);
              return output(1);
            },
          });
          return link.endpoint;
        },
      },
    },
  });
  const session = scope.session('cpu');
  const active = session.enqueue('wait', {
    budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 8 },
    prepare: () => ({ payload: null }),
    cancellation: 'terminate',
  });
  await until(() => active.state === 'running');
  const g = gate();
  const pending = session.enqueuePrepared(
    'wait',
    options(async () => {
      await g.promise;
      return { payload: null };
    }),
  );
  await until(() => pending.state === 'preparing');
  active.cancel();
  await assert.rejects(active.result);
  await active.settled;
  await assert.rejects(pending.result, { code: 'SESSION_LOST' });
  assert.equal(rt.stats.reserved.inputBytes, 8);
  g.resolve();
  await pending.settled;
  assert.equal(rt.stats.reserved.inputBytes, 0);
});
