import assert from 'node:assert/strict';
import test from 'node:test';
import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { iterateSizedResults } from '../dist/sized-results.js';
import { createSizedResultSource } from '../dist/sized-source.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { sleep, until } from './helpers.mjs';

function setup(t, override = {}) {
  let stop,
    planned = 0,
    encoded = 0,
    disposed = 0;
  const rt = createWorkerRuntime({
    pools: {
      cpu: {
        size: 1,
        cacheBytes: 128,
        idleTimeoutMs: 0,
        factory() {
          const link = createLoopback();
          stop = serve(link.host, {
            open(_payload, ctx) {
              const source = createSizedResultSource({
                maxChunkBytes: 32,
                plan() {
                  const size = [8, 32, 16][planned++];
                  if (size === undefined) return null;
                  return {
                    outputBytes: size,
                    encode() {
                      encoded++;
                      return output(new Uint8Array(size), []);
                    },
                    dispose() {
                      disposed++;
                    },
                  };
                },
              });
              ctx.cache.setResource('source', source, 64, (source) => source.close());
              return output(null);
            },
            describe(_payload, ctx) {
              return ctx.cache.get('source').describe(ctx);
            },
            take(token, ctx) {
              return ctx.cache.get('source').take(token, ctx);
            },
            hold() {
              return output(new Uint8Array(60));
            },
          });
          return link.endpoint;
        },
      },
    },
    budgets: { outputBytes: 512 },
    ...override,
  });
  t.after(async () => {
    await rt.dispose();
    stop?.();
  });
  const scope = rt.createScope(),
    session = scope.session('cpu');
  const opts = (payload, outputBytes = 0) => ({
    budget: { inputBytes: 128, scratchBytes: 0, outputBytes },
    prepare: () => ({ payload }),
  });
  const iterator = () =>
    iterateSizedResults({
      session,
      task: 'take',
      maxChunkBytes: 32,
      budget: { inputBytes: 128, scratchBytes: 0 },
      describe: (signal) => session.enqueue('describe', { ...opts(null, 512), signal }),
      prepare: (chunk) => ({ payload: chunk.token }),
      close: () => scope.dispose(),
    });
  return { rt, scope, session, opts, iterator, counts: () => ({ planned, encoded, disposed }) };
}

test('variable chunk iterator consumes exactly one planned chunk and releases each lease', async (t) => {
  const { rt, session, opts, iterator, counts } = setup(t);
  await consumeResult(session.enqueue('open', opts(null)), () => {});
  const chunks = iterator();
  const sizes = [];
  for await (const chunk of chunks) {
    sizes.push(chunk.byteLength);
    assert.equal(rt.stats.leases, 1);
    assert.equal(rt.stats.reserved.outputBytes, chunk.byteLength);
  }
  assert.deepEqual(sizes, [8, 32, 16]);
  assert.deepEqual(counts(), { planned: 4, encoded: 3, disposed: 3 });
  await chunks.closed;
  assert.equal(rt.stats.workers, 0);
});

test('a consumer break closes the pending source without planning another chunk', async (t) => {
  const { rt, session, opts, iterator, counts } = setup(t);
  await consumeResult(session.enqueue('open', opts(null)), () => {});
  const chunks = iterator();
  for await (const _chunk of chunks) break;
  await chunks.closed;
  assert.deepEqual(counts(), { planned: 1, encoded: 1, disposed: 1 });
  assert.equal(rt.stats.leases, 0);
});
