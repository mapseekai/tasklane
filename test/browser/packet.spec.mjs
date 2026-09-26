import { test, expect } from '@playwright/test';

test('compact arrays preserve graph semantics and transfer ownership through a real Worker', async ({
  page,
}) => {
  await page.goto('/test/browser/harness.html');
  const actual = await page.evaluate(async () => {
    const { createWorkerRuntime, browserWorker, consumeResult, packetByteLength } = await import(
      '/dist/index.js'
    );
    const runtime = createWorkerRuntime({
      pools: {
        cpu: {
          size: 1,
          factory: browserWorker('/test/fixtures/browser-worker.mjs'),
        },
      },
    });
    const numbers = Array.from({ length: 100_000 }, (_, i) => i);
    const buffer = new Float64Array([1.5, -0]);
    const special = [-0, NaN, Infinity, undefined, 9n];
    const value = [numbers, special, special, buffer];
    value.push(value);
    value.length = 8;
    value[6] = undefined;
    value.extra = special;
    const inputBytes = packetByteLength(value);
    try {
      return await consumeResult(
        runtime.createScope().enqueue('ping', {
          pool: 'cpu',
          budget: { inputBytes, scratchBytes: 0, outputBytes: inputBytes + 4096 },
          prepare: () => ({ payload: value, transfer: [buffer.buffer] }),
        }),
        ({ value: copy }) => ({
          inputBytes,
          numbers: copy[0].length === numbers.length && copy[0].every((v, i) => v === i),
          aliases: copy[1] === copy[2] && copy.extra === copy[1] && copy[4] === copy,
          special:
            Object.is(copy[1][0], -0) &&
            Number.isNaN(copy[1][1]) &&
            copy[1][2] === Infinity &&
            copy[1][3] === undefined &&
            copy[1][4] === 9n,
          holes: copy.length === 8 && !(5 in copy) && 6 in copy && !(7 in copy),
          transfer: buffer.byteLength === 0 && copy[3][0] === 1.5 && Object.is(copy[3][1], -0),
        }),
      );
    } finally {
      await runtime.dispose();
    }
  });
  expect(actual.inputBytes).toBeLessThan(1_200_000);
  expect(actual).toMatchObject({
    numbers: true,
    aliases: true,
    special: true,
    holes: true,
    transfer: true,
  });
});
