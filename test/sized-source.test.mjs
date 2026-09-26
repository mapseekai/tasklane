import assert from 'node:assert/strict';
import test from 'node:test';
import { createSizedResultSource } from '../dist/sized-source.js';
import { deferred } from '../dist/runtime/deferred.js';
const context = () => ({ signal: new AbortController().signal, outputLimit: 1024 });

test('source plans once and encodes only on take, enforcing tokens and the announced bound', async () => {
  let plans = 0,
    encodes = 0,
    disposed = 0;
  const source = createSizedResultSource({
    maxChunkBytes: 32,
    plan() {
      plans++;
      if (plans > 1) return null;
      return {
        outputBytes: 16,
        encode() {
          encodes++;
          return { value: new Uint8Array(16) };
        },
        dispose() {
          disposed++;
        },
      };
    },
  });
  const first = (await source.describe(context())).value;
  assert.deepEqual((await source.describe(context())).value, first);
  assert.equal(plans, 1);
  assert.equal(encodes, 0);
  await assert.rejects(source.take('wrong', context()), { code: 'INVALID_ARGUMENT' });
  assert.equal((await source.take(first.token, context())).value.byteLength, 16);
  assert.equal(encodes, 1);
  assert.equal(disposed, 1);
  await assert.rejects(source.take(first.token, context()), { code: 'INVALID_ARGUMENT' });
  assert.deepEqual((await source.describe(context())).value, { done: true });
  await source.close();
  assert.equal(disposed, 1);
});

test('source rejects output growth and retains the pending plan for cleanup', async () => {
  let disposed = 0;
  const source = createSizedResultSource({
    maxChunkBytes: 32,
    plan: () => ({
      outputBytes: 8,
      encode: () => ({ value: new Uint8Array(16) }),
      dispose() {
        disposed++;
      },
    }),
  });
  const chunk = (await source.describe(context())).value;
  await assert.rejects(source.take(chunk.token, context()), { code: 'BUDGET_EXCEEDED' });
  await source.close();
  assert.equal(disposed, 1);
});

test('close waits for late planning and supports failed plan cleanup retry', async () => {
  const gate = deferred();
  let disposals = 0;
  const source = createSizedResultSource({
    maxChunkBytes: 16,
    async plan() {
      await gate.promise;
      return {
        outputBytes: 8,
        encode() {
          assert.fail('closed source must not encode');
        },
        dispose() {
          if (++disposals === 1) throw Error('temporary');
        },
      };
    },
  });
  const pending = source.describe(context());
  const closed = source.close();
  gate.resolve();
  await assert.rejects(pending, { code: 'CLOSED' });
  await assert.rejects(closed, /temporary/);
  await source.close();
  assert.equal(disposals, 2);
});

test('encoding never starts without enough admitted output bytes', async () => {
  let encoded = 0;
  const source = createSizedResultSource({
    maxChunkBytes: 32,
    plan: () => ({
      outputBytes: 16,
      encode: () => {
        encoded++;
        return { value: new Uint8Array(16) };
      },
      dispose() {},
    }),
  });
  const chunk = (await source.describe(context())).value;
  await assert.rejects(source.take(chunk.token, { ...context(), outputLimit: 8 }), {
    code: 'BUDGET_EXCEEDED',
  });
  assert.equal(encoded, 0);
  await source.take(chunk.token, context());
  assert.equal(encoded, 1);
  await source.close();
});

test('sized output metadata is encoded once and remains snapshotted across plan disposal', async () => {
  const { createWorkerRuntime, consumeResult } = await import('../dist/index.js');
  const { createLoopback } = await import('../dist/testing.js');
  const { serve, output } = await import('../dist/host.js');
  const payload = { id: 1, properties: { name: 'feature' }, coordinates: new Float64Array(100) };
  const watched = new Set([payload, payload.properties]);
  const descriptor = Object.getOwnPropertyDescriptor;
  let reads = 0;
  Object.getOwnPropertyDescriptor = (object, key) => {
    if (watched.has(object)) reads++;
    return descriptor(object, key);
  };
  const source = createSizedResultSource({
    maxChunkBytes: 4096,
    plan: () => ({
      outputBytes: 4096,
      encode: () => output(payload),
      dispose() {
        payload.properties.name = 'changed';
      },
    }),
  });
  const link = createLoopback();
  const stop = serve(link.host, {
    direct: () => output(payload),
    describe: (_v, ctx) => source.describe(ctx),
    take: (token, ctx) => source.take(token, ctx),
  });
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => link.endpoint } } });
  try {
    const session = await rt.createScope().acquireSession('cpu');
    const options = (v = null) => ({
      budget: { inputBytes: 64, outputBytes: 4096, scratchBytes: 0 },
      prepare: () => ({ payload: v }),
    });
    await consumeResult(session.enqueue('direct', options()), () => {});
    const direct = reads;
    const chunk = await consumeResult(session.enqueue('describe', options()), (v) => v);
    reads = 0;
    const value = await consumeResult(session.enqueue('take', options(chunk.token)), (v) => v);
    assert.equal(reads, direct);
    assert.equal(value.properties.name, 'feature');
    assert.equal(payload.properties.name, 'changed');
  } finally {
    Object.getOwnPropertyDescriptor = descriptor;
    await rt.dispose();
    stop();
    await source.close();
  }
});

test('Host rechecks announced chunk bound after disposal grows a resizable backing store', async () => {
  const { createWorkerRuntime, consumeResult } = await import('../dist/index.js');
  const { createLoopback } = await import('../dist/testing.js');
  const { serve, output } = await import('../dist/host.js');
  const buffer = new ArrayBuffer(8, { maxByteLength: 32 });
  const source = createSizedResultSource({
    maxChunkBytes: 8,
    plan: () => ({
      outputBytes: 8,
      encode: () => output(new Uint8Array(buffer)),
      dispose() {
        buffer.resize(16);
      },
    }),
  });
  const link = createLoopback();
  const stop = serve(link.host, {
    describe: (_v, ctx) => source.describe(ctx),
    take: (token, ctx) => source.take(token, ctx),
  });
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => link.endpoint } } });
  try {
    const session = await rt.createScope().acquireSession('cpu');
    const options = (v = null) => ({
      budget: { inputBytes: 64, outputBytes: 1024, scratchBytes: 0 },
      prepare: () => ({ payload: v }),
    });
    const chunk = await consumeResult(session.enqueue('describe', options()), (v) => v);
    await assert.rejects(session.enqueue('take', options(chunk.token)).result, {
      code: 'BUDGET_EXCEEDED',
    });
  } finally {
    await rt.dispose();
    stop();
    await source.close();
  }
});
