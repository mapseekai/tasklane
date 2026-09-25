import { output } from '../../dist/host.js';
import { transferBuffers } from '../../dist/index.js';
import { convert, flatten } from '../../benchmarks/workloads.mjs';

export const handlers = {
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
