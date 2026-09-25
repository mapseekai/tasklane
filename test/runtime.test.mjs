import assert from 'node:assert/strict';
import test from 'node:test';
import { transferBuffers } from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';
import { runtime, options, take, sleep, until } from './helpers.mjs';

function owned(t, extra) {
  const rt = runtime(extra);
  t.after(() => rt.dispose());
  return rt;
}

test('lazy worker start, typed task result and cleanup', async (t) => {
  const rt = owned(t);
  assert.equal(rt.stats.workers, 0);
  const result = await take(rt.createScope().enqueue('ping', options(42)));
  assert.equal(result.value, 42);
  assert.equal(rt.stats.completed, 1);
  await rt.dispose();
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.active, 0);
});
test('unique scopes do not alias equal human-readable labels', async (t) => {
  const rt = owned(t);
  const a = rt.createScope('map'),
    b = rt.createScope('map');
  assert.notEqual(a.id, b.id);
  const [x, y] = await Promise.all([
    take(a.enqueue('ping', options('a'))),
    take(b.enqueue('ping', options('b'))),
  ]);
  assert.notEqual(x.scope, y.scope);
});
test('exact input ownership is transferred, not copied', async (t) => {
  const rt = owned(t);
  const a = new Uint8Array(1024);
  a[123] = 91;
  const value = await take(
    rt.createScope().enqueue(
      'echo',
      options(null, {
        budget: { inputBytes: 2048, scratchBytes: 0, outputBytes: 1024 },
        prepare: () => ({ payload: { bytes: a }, transfer: transferBuffers(a) }),
      }),
    ),
  );
  assert.equal(a.byteLength, 0);
  assert.equal(value[123], 91);
});
test('structured-clone path preserves caller data', async (t) => {
  const rt = owned(t);
  const a = new Uint8Array(1024);
  a[99] = 77;
  const result = await take(
    rt
      .createScope()
      .enqueue(
        'echo',
        options(
          { bytes: a, transfer: false },
          { budget: { inputBytes: 2048, scratchBytes: 1024, outputBytes: 1024 } },
        ),
      ),
  );
  assert.equal(a.byteLength, 1024);
  assert.equal(result[99], 77);
  assert.notEqual(a.buffer, result.buffer);
});
test('output credits block prepare until a result is consumed', async (t) => {
  const rt = owned(t, { budgets: { outputBytes: 64 }, maxActiveTasks: 2 });
  const scope = rt.createScope();
  let prepared = 0;
  const opts = () =>
    options(null, {
      budget: { inputBytes: 1024, scratchBytes: 0, outputBytes: 64 },
      prepare: () => {
        prepared++;
        return { payload: { size: 64 } };
      },
    });
  const first = scope.enqueue('allocate', opts());
  const second = scope.enqueue('allocate', opts());
  const lease = await first.result;
  await sleep(20);
  assert.equal(prepared, 1);
  assert.equal(second.state, 'queued');
  assert.equal(rt.stats.reserved.outputBytes, 64);
  lease.release();
  const next = await second.result;
  assert.equal(prepared, 2);
  next.release();
  assert.equal(rt.stats.reserved.outputBytes, 0);
});
test('input and scratch admission precede packet allocation', async (t) => {
  const rt = owned(t, { budgets: { inputBytes: 1024, scratchBytes: 8 }, maxActiveTasks: 2 });
  const scope = rt.createScope();
  let prepared = 0;
  const opts = () =>
    options(null, {
      budget: { inputBytes: 1024, scratchBytes: 8, outputBytes: 8 },
      prepare: () => {
        prepared++;
        return { payload: { ms: 60 } };
      },
    });
  const a = scope.enqueue('wait', opts());
  const b = scope.enqueue('wait', opts());
  await until(() => a.state === 'running');
  assert.equal(prepared, 1);
  assert.equal(b.state, 'queued');
  await Promise.all([take(a), take(b)]);
  assert.equal(rt.stats.peakReserved.inputBytes, 1024);
});
test('queued cancellation never calls prepare', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const a = s.enqueue('wait', options({ ms: 40 }));
  let prepared = false;
  const b = s.enqueue(
    'ping',
    options(null, {
      prepare: () => {
        prepared = true;
        return { payload: 1 };
      },
    }),
  );
  b.cancel();
  await assert.rejects(b.result, { code: 'ABORTED' });
  await b.settled;
  await take(a);
  assert.equal(prepared, false);
});
test('pre-aborted signal causes no worker allocation', async (t) => {
  const rt = owned(t);
  const c = new AbortController();
  c.abort();
  const a = rt.createScope().enqueue('ping', options(1, { signal: c.signal }));
  await assert.rejects(a.result, { code: 'ABORTED' });
  assert.equal(rt.stats.workers, 0);
});
test('logical cancellation does not free a physically busy worker', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const a = s.enqueue('wait', options({ ms: 120 }, { cancellation: 'discard' }));
  await until(() => a.state === 'running');
  a.cancel();
  await assert.rejects(a.result, { code: 'ABORTED' });
  let prepared = false;
  const b = s.enqueue(
    'ping',
    options(null, {
      prepare: () => {
        prepared = true;
        return { payload: 1 };
      },
    }),
  );
  await sleep(20);
  assert.equal(rt.stats.active, 1);
  assert.equal(prepared, false);
  await a.settled;
  await take(b);
  assert.equal(a.state, 'cancelled');
});
test('cooperative checkpoints stop cancellable computation', async (t) => {
  const rt = owned(t);
  const s = rt.createScope();
  const a = s.enqueue('wait', options({ ms: 1000, cooperate: true }));
  await until(() => a.state === 'running');
  a.cancel();
  await a.settled;
  assert.ok(a.timing.totalMs < 700);
  assert.equal(rt.stats.workerTerminations, 0);
});
test('hard cancellation terminates non-interruptible computation and respawns', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  let started = false;
  const a = s.enqueue(
    'spin',
    options(
      { ms: 2000 },
      {
        cancellation: 'terminate',
        onProgress: () => {
          started = true;
        },
      },
    ),
  );
  await until(() => started);
  a.cancel();
  await a.settled;
  assert.equal(rt.stats.workerTerminations, 1);
  assert.equal((await take(s.enqueue('ping', options(1)))).value, 1);
  assert.equal(rt.stats.workerStarts, 2);
});
test('async prepare is rejected immediately and returns all credits', async (t) => {
  const rt = owned(t);
  const h = rt.createScope().enqueue(
    'ping',
    options(null, {
      prepare: () => new Promise(() => {}),
    }),
  );
  await assert.rejects(h.result, { code: 'INVALID_ARGUMENT' });
  await h.settled;
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.reserved.inputBytes, 0);
});
test('queue timeout is distinct from execution timeout', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const a = s.enqueue('wait', options({ ms: 100 }));
  const b = s.enqueue('ping', options(1, { queueTimeoutMs: 10 }));
  await assert.rejects(b.result, { code: 'QUEUE_TIMEOUT' });
  await take(a);
});
test('physical timeout terminates a hung worker', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  await take(s.enqueue('ping', options(1)));
  const a = s.enqueue('wait', options({ ms: 2000 }, { executionTimeoutMs: 20 }));
  await assert.rejects(a.result, { code: 'EXECUTION_TIMEOUT' });
  await a.settled;
  assert.equal(rt.stats.workerTerminations, 1);
});
test('unknown task rejected before prepare', async (t) => {
  const rt = owned(t);
  let called = false;
  const task = rt.createScope().enqueue(
    'missing',
    options(null, {
      prepare: () => {
        called = true;
        return { payload: 1 };
      },
    }),
  );
  await assert.rejects(task.result, { code: 'UNKNOWN_TASK' });
  assert.equal(called, false);
});
test('task error does not corrupt the reusable worker', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  await assert.rejects(s.enqueue('error', options(null)).result, /Deliberate/);
  await take(s.enqueue('ping', options(1)));
  assert.equal(rt.stats.workerStarts, 1);
});
test('worker crash rejects current work and allows a new generation', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const a = s.enqueue('crash', options(null));
  await assert.rejects(a.result, { code: 'WORKER_FAILED' });
  await a.settled;
  await take(s.enqueue('ping', options(2)));
  assert.equal(rt.stats.workerStarts, 2);
});
test('oversized input fails without detaching the original', async (t) => {
  const rt = owned(t);
  const bytes = new Uint8Array(20);
  const a = rt.createScope().enqueue(
    'echo',
    options(null, {
      budget: { inputBytes: 10, scratchBytes: 0, outputBytes: 20 },
      prepare: () => ({ payload: { bytes }, transfer: transferBuffers(bytes) }),
    }),
  );
  await assert.rejects(a.result, { code: 'BUDGET_EXCEEDED' });
  assert.equal(bytes.byteLength, 20);
});
test('oversized output rejected in host, credits are released', async (t) => {
  const rt = owned(t);
  const a = rt
    .createScope()
    .enqueue(
      'allocate',
      options({ size: 64 }, { budget: { inputBytes: 1024, scratchBytes: 64, outputBytes: 32 } }),
    );
  await assert.rejects(a.result, { code: 'BUDGET_EXCEEDED' });
  assert.equal(rt.stats.reserved.outputBytes, 0);
});
test('uncloneable input rejects without wedging the slot', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  await assert.rejects(
    s.enqueue(
      'ping',
      options(() => {}),
    ).result,
  );
  assert.equal((await take(s.enqueue('ping', options(1)))).value, 1);
});
test('uncloneable output becomes a remote error', async (t) => {
  const rt = owned(t);
  await assert.rejects(rt.createScope().enqueue('uncloneable', options(null)).result);
  assert.equal(rt.stats.active, 0);
});
test('observer exceptions do not interfere with completion', async (t) => {
  const rt = owned(t, {
    onDiagnostic: () => {
      throw new Error('observer');
    },
  });
  const result = await take(
    rt.createScope().enqueue(
      'progress',
      options(null, {
        onProgress: () => {
          throw new Error('progress');
        },
      }),
    ),
  );
  assert.equal(result, 3);
  assert.ok(rt.stats.observerErrors >= 1);
});
test('scope disposal releases leases, cancels children, preserves unrelated scope', async (t) => {
  const rt = owned(t);
  const a = rt.createScope(),
    child = a.createScope(),
    b = rt.createScope();
  const lease = await a.enqueue(
    'allocate',
    options({ size: 16 }, { budget: { inputBytes: 1024, scratchBytes: 0, outputBytes: 16 } }),
  ).result;
  const task = child.enqueue('wait', options({ ms: 40 }));
  await a.dispose();
  await assert.rejects(task.result);
  assert.equal(lease.released, true);
  assert.throws(() => child.enqueue('ping', options(1)), { code: 'CLOSED' });
  assert.equal((await take(b.enqueue('ping', options(2)))).value, 2);
});
test('soft affinity reuses worker-local cache in the same scope', async (t) => {
  const rt = owned(t);
  const s = rt.createScope();
  const opts = (payload) => options(payload, { affinity: 'shard-1' });
  await take(s.enqueue('cache', opts({ key: 'x', value: 12 })));
  assert.equal(await take(s.enqueue('cache', opts({ key: 'x' }))), 12);
});
test('worker-local cache does not cross scopes even on the same worker', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const a = rt.createScope(),
    b = rt.createScope();
  await take(a.enqueue('cache', options({ key: 'x', value: 12 })));
  assert.equal(await take(b.enqueue('cache', options({ key: 'x' }))), undefined);
});
test('exclusive session persists non-evictable state and keeps other tasks out', async (t) => {
  const rt = owned(t);
  const s = rt.createScope();
  const session = s.session('cpu');
  const opts = ({ pool, ...rest }) => rest;
  const first = await take(session.enqueue('ping', opts(options(1))));
  await take(session.enqueue('cache', opts(options({ key: 'db', value: 42, pinned: true }))));
  const second = await take(s.enqueue('ping', options(2)));
  assert.notEqual(first.epoch, second.epoch);
  assert.equal(await take(session.enqueue('cache', opts(options({ key: 'db' })))), 42);
  await session.dispose();
  assert.equal(session.state, 'closed');
});
test('session worker loss is fail-closed, never silently rebound', async (t) => {
  const rt = owned(t);
  const session = rt.createScope().session('cpu');
  const { pool, ...opts } = options(null);
  const a = session.enqueue('crash', opts);
  await assert.rejects(a.result);
  await a.settled;
  assert.equal(session.state, 'lost');
  assert.throws(() => session.enqueue('ping', opts), { code: 'SESSION_LOST' });
});
test('priority changes which queued task is admitted next', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const blocker = s.enqueue('wait', options({ ms: 50 }));
  await until(() => blocker.state === 'running');
  const order = [];
  const add = (name, priority) =>
    s.enqueue(
      'ping',
      options(null, {
        priority,
        prepare: () => {
          order.push(name);
          return { payload: name };
        },
      }),
    );
  const low = add('low', 'background'),
    high = add('high', 'interactive');
  await Promise.all([take(blocker), take(low), take(high)]);
  assert.deepEqual(order, ['high', 'low']);
});
test('fairness alternates queued request groups', async (t) => {
  const rt = owned(t, { maxActiveTasks: 1 });
  const s = rt.createScope();
  const order = [];
  const add = (group) =>
    s.enqueue(
      'ping',
      options(null, {
        group,
        prepare: () => {
          order.push(group);
          return { payload: group };
        },
      }),
    );
  await Promise.all([take(add('a')), take(add('a')), take(add('b')), take(add('b'))]);
  assert.deepEqual(order, ['a', 'b', 'a', 'b']);
});
test('global worker cap reclaims idle workers across different pools', async (t) => {
  const factory = nodeWorker(new URL('./fixtures/node-worker.mjs', import.meta.url));
  const rt = owned(t, {
    maxWorkers: 1,
    pools: {
      cpu: { factory, size: 1, idleTimeoutMs: 0 },
      other: { factory, size: 1, idleTimeoutMs: 0 },
    },
  });
  const s = rt.createScope();
  await take(s.enqueue('ping', options(1)));
  await take(s.enqueue('ping', options(2, { pool: 'other' })));
  assert.equal(rt.stats.workers, 1);
  assert.equal(rt.stats.workerStarts, 2);
});
test('queue is bounded before heavyweight payload preparation', async (t) => {
  const rt = owned(t, { maxQueuedTasks: 1 });
  const s = rt.createScope();
  const a = s.enqueue('ping', options(1));
  assert.throws(() => s.enqueue('ping', options(2)), { code: 'QUEUE_FULL' });
  await take(a);
});
test('invalid configuration, unknown pools and oversized reservations fail early', async (t) => {
  for (const maxWorkers of [0, -1, NaN, Infinity, 1.5])
    assert.throws(() => runtime({ maxWorkers }));
  const rt = owned(t);
  const s = rt.createScope();
  assert.throws(() => s.enqueue('ping', options(1, { pool: 'missing' })));
  assert.throws(
    () =>
      s.enqueue(
        'ping',
        options(1, { budget: { inputBytes: 2 ** 40, scratchBytes: 0, outputBytes: 0 } }),
      ),
    { code: 'BUDGET_EXCEEDED' },
  );
});
test('200 interleaved requests resolve exactly once with bounded execution', async (t) => {
  const rt = owned(t);
  const s = rt.createScope();
  const values = await Promise.all(
    Array.from({ length: 200 }, (_, i) => take(s.enqueue('ping', options(i)))),
  );
  assert.deepEqual(
    values.map((x) => x.value),
    Array.from({ length: 200 }, (_, i) => i),
  );
  assert.equal(rt.stats.completed, 200);
  assert.equal(rt.stats.active, 0);
  assert.equal(rt.stats.leases, 0);
});
