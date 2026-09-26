import { output, createSizedResultSource } from '../../dist/host.js';
import { transferBuffers } from '../../dist/index.js';
import { convert, flatten } from '../../benchmarks/workloads.mjs';

export const handlers = {
  async maintenanceWork({ id, delay = 0 }, ctx) {
    ctx.cache.get('missing');
    ctx.cache.setBinary('working-a', new Uint8Array(384));
    ctx.cache.setBinary('working-b', new Uint8Array(384));
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    return output(id);
  },
  maintenanceResource({ trimMs = 0 }, ctx) {
    const state = { hits: 1, misses: 0, evictions: 0, keys: ['block'], usedBytes: 64 };
    const lease = ctx.cache.setResource('maintained-reader', state, 64, () => {}, {
      trim: async (bytes) => {
        // Browser harness consumes this marker before passing protocol messages to Runtime.
        globalThis.postMessage({ maintenanceTest: 'trim-started' });
        await new Promise((resolve) => setTimeout(resolve, trimMs));
        state.keys = [];
        state.usedBytes = bytes;
        state.evictions++;
        lease.report(state);
        return bytes;
      },
    });
    lease.report(state);
    return output(null);
  },
  binaryCache(_input, ctx) {
    const value = new Float32Array(16);
    value[0] = 42;
    Object.defineProperty(value, 'extra', {
      enumerable: true,
      get() {
        throw Error('custom field read');
      },
    });
    ctx.cache.get('missing');
    ctx.cache.setBinary('a', value);
    const stored = ctx.cache.get('a');
    ctx.cache.setBinary('b', new Uint8Array(64));
    return output({
      native: stored instanceof Float32Array,
      sameBuffer: stored.buffer === value.buffer,
      value: stored[0],
      extra: Object.hasOwn(stored, 'extra'),
    });
  },
  sizedOpen(_input, ctx) {
    let index = 0;
    const source = createSizedResultSource({
      maxChunkBytes: 64,
      plan() {
        const bytes = [16, 64, 8][index++];
        return bytes === undefined
          ? null
          : {
              outputBytes: bytes,
              encode: () => output(new Uint8Array(bytes)),
              dispose() {},
            };
      },
    });
    const lease = ctx.cache.setResource('sized', source, 8, (value) => value.close());
    lease.resize(32);
    return output(lease.bytes);
  },
  sizedDescribe(_input, ctx) {
    return ctx.cache.get('sized').describe(ctx);
  },
  sizedTake(token, ctx) {
    return ctx.cache.get('sized').take(token, ctx);
  },
  cursorNext(_input, ctx) {
    const value = ctx.cache.get('cursor') ?? 0;
    ctx.cache.set('cursor', value + 1, 8);
    return output(value < 2 ? value : null);
  },
  cursorClose(_input, ctx) {
    ctx.cache.delete('cursor');
    return output(null);
  },
  businessError() {
    throw Object.assign(new Error('Read limit exceeded'), {
      name: 'DataError',
      code: 'READ_BUDGET',
      details: { limit: 32 },
    });
  },
  oversizedOutput() {
    return output('x'.repeat(2_000_000));
  },
  scratch(_value, ctx) {
    ctx.scratch.allocate(9);
    return output(null);
  },
  oversizedProgress(_value, ctx) {
    ctx.progress(new Uint8Array(1024 ** 2));
    return output(null);
  },
  lateProgress(_value, ctx) {
    setTimeout(() => ctx.progress(new Uint8Array(1024 ** 2)), 30);
    return output(null);
  },
  resource(_value, ctx) {
    class Resource {}
    ctx.cache.setResource('resource', new Resource(), 8, async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
    });
    return output(null);
  },
  ping: (value, ctx) =>
    output({ value, epoch: ctx.epoch, scope: ctx.scopeId, session: ctx.sessionId }),
  footprint(keys, ctx) {
    const state = { keys, usedBytes: 64, hits: 2, misses: 1, evictions: 0 };
    const lease = ctx.cache.setResource('footprint-reader', state, 64, () => {}, {
      trim: (target) => {
        state.keys = [];
        state.usedBytes = target;
        state.evictions++;
        lease.report(state);
        return target;
      },
    });
    lease.report(state);
    return output(ctx.epoch);
  },
  async wait({ ms = 20, cooperate = false }, ctx) {
    if (cooperate) {
      const end = performance.now() + ms;
      while (performance.now() < end) await ctx.checkpoint();
    } else await new Promise((resolve) => setTimeout(resolve, ms));
    return output(ms);
  },
  spin({ ms = 50 }, ctx) {
    ctx.progress('started');
    const end = performance.now() + ms;
    while (performance.now() < end) {
      /* Deliberately non-interruptible CPU work. */
    }
    return output(ms);
  },
  async progress(_value, ctx) {
    ctx.progress(1);
    await new Promise((resolve) => setTimeout(resolve, 20));
    ctx.progress(2);
    return output(3);
  },
  echo: ({ bytes, transfer = true }) => output(bytes, transfer ? transferBuffers(bytes) : []),
  allocate: ({ size }) => {
    const bytes = new Uint8Array(size);
    return output(bytes, transferBuffers(bytes));
  },
  error: () => {
    throw new Error('Deliberate task failure');
  },
  uncloneable: () => output(() => {}),
  crash: () => {
    setTimeout(() => {
      throw new Error('Deliberate worker crash');
    }, 0);
    return new Promise(() => {});
  },
  cache({ key, value, bytes = 8, pinned = false }, ctx) {
    if (value !== undefined) {
      if (pinned) ctx.cache.setPinned(key, value, bytes);
      else ctx.cache.set(key, value, bytes);
    }
    return output(ctx.cache.get(key));
  },
  convert(payload) {
    const result = convert(payload);
    return output(
      result,
      payload.returnClone ? [] : transferBuffers(result.vertices, result.bounds),
    );
  },
  flatten(payload) {
    const result = flatten(payload);
    const transfer = payload.returnClone
      ? []
      : transferBuffers(
          result.xy,
          result.pathOffsets,
          result.featureOffsets,
          result.polygonOffsets,
          result.featurePolygonOffsets,
          result.types,
        );
    return output(result, transfer);
  },
};
