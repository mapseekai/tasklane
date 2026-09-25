import assert from 'node:assert/strict';
import test from 'node:test';
import { encodePacket, decodePacket, packetBytes, packetBlobBytes } from '../dist/packet.js';
import { createWorkerRuntime, packetByteLength } from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';
import { createLoopback } from '../dist/testing.js';
import { serve, output } from '../dist/host.js';
import { header } from '../dist/protocol.js';
import { CacheStore } from '../dist/resources/cache.js';
import { runtime, options, take } from './helpers.mjs';
import { readFileChunks } from '../examples/file-chunks/read.mjs';
import { CHUNK_BYTES, CURSOR_BYTES } from '../examples/file-chunks/handlers.mjs';

test('File/Blob roundtrip preserves aliases, metadata and bytes without reading the source', async () => {
  const file = new File(['abcdef'], '地图.tif', { type: 'image/tiff', lastModified: -123 });
  const blob = new Blob(['xyz'], { type: 'text/plain' });
  file.arrayBuffer = () => assert.fail('Encoder must not read the file');
  const input = { file, again: file, blob, map: new Map([[file, blob]]) };
  const packet = encodePacket(input, 4096, 9);
  assert.equal(packet.blobs.length, 2);
  assert.equal(packetBlobBytes(packet), 9);
  const value = decodePacket(structuredClone(packet));
  assert.equal(value.file, value.again);
  assert.equal(value.map.get(value.file), value.blob);
  assert.ok(value.file instanceof File);
  assert.equal(value.file.name, file.name);
  assert.equal(value.file.lastModified, -123);
  assert.equal(value.file.type, 'image/tiff');
  assert.equal(await value.file.slice(1, 4).text(), 'bcd');
  assert.equal(await value.blob.text(), 'xyz');
  assert.throws(() => encodePacket(input, 4096, 8), { code: 'BUDGET_EXCEEDED' });
  assert.throws(() => encodePacket(input, 10, 9), { code: 'BUDGET_EXCEEDED' });
});

test('attachment work, metadata, logical sizes and corrupt references are bounded', () => {
  assert.throws(() => encodePacket(Array.from({ length: 257 }, () => new Blob())), {
    code: 'BUDGET_EXCEEDED',
  });
  assert.throws(() => encodePacket(new File([], 'x'.repeat(4096)), 4096), {
    code: 'BUDGET_EXCEEDED',
  });
  const packet = encodePacket(new Blob(['x']));
  assert.throws(() => packetBytes({ ...packet, blobs: [{}] }), { code: 'PROTOCOL_ERROR' });
  assert.throws(() => packetBytes({ ...packet, blobs: [packet.blobs[0], packet.blobs[0]] }), {
    code: 'PROTOCOL_ERROR',
  });
  const graph = JSON.parse(packet.metadata);
  graph.nodes[0].blob = 1;
  assert.throws(() => decodePacket({ ...packet, metadata: JSON.stringify(graph) }), {
    code: 'PROTOCOL_ERROR',
  });
  assert.ok(packetByteLength(new Blob([new Uint8Array(1024 * 1024)])) < 4096);
});

test('real Node Worker preserves File results and enforces both logical limits', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  const scope = rt.createScope();
  const file = new File(['abcdef'], 'x.tif', { type: 'image/tiff', lastModified: 123 });
  for (const blobLimits of [
    undefined,
    { inputBytes: 5, outputBytes: 6 },
    { inputBytes: 6, outputBytes: 5 },
  ]) {
    await assert.rejects(scope.enqueue('ping', options(file, { blobLimits })).result, {
      code: 'BUDGET_EXCEEDED',
    });
  }
  const value = await take(
    scope.enqueue(
      'ping',
      options(file, {
        blobLimits: { inputBytes: 6, outputBytes: 6 },
      }),
    ),
  );
  assert.ok(value.value instanceof File);
  assert.equal(value.value.name, 'x.tif');
  assert.equal(value.value.lastModified, 123);
  assert.equal(await value.value.text(), 'abcdef');
  assert.equal(rt.stats.leases, 0);
  assert.equal(rt.stats.reserved.outputBytes, 0);
  await assert.rejects(
    scope.enqueue(
      'ping',
      options(file, {
        blobLimits: { inputBytes: 6, outputBytes: 6 },
        prepare: () => ({ payload: file, transfer: [file] }),
      }),
    ).result,
    { code: 'INVALID_ARGUMENT' },
  );
  for (const blobLimits of [null, 'x', {}, { inputBytes: -1, outputBytes: 0 }]) {
    assert.throws(() => scope.enqueue('ping', options(file, { blobLimits })), {
      code: 'INVALID_ARGUMENT',
    });
  }
});

test('forged output attachments are checked again at the runtime boundary', async () => {
  const link = createLoopback();
  const stop = serve(link.host, { ping: () => output(null) });
  const endpoint = {
    ...link.endpoint,
    onMessage(fn) {
      return link.endpoint.onMessage((message) => {
        if (message.type === 'result') {
          const value = encodePacket(new Blob(['unauthorized']));
          fn({ ...message, value, byteLength: packetBytes(value) });
        } else fn(message);
      });
    },
  };
  const rt = createWorkerRuntime({ pools: { cpu: { size: 1, factory: () => endpoint } } });
  try {
    await assert.rejects(rt.createScope().enqueue('ping', options(null)).result, {
      code: 'BUDGET_EXCEEDED',
    });
  } finally {
    await rt.dispose();
    stop();
  }
});

test('previous protocol hosts fail within startup timeout', async () => {
  const link = createLoopback();
  const off = link.host.onMessage((message) => {
    if (message.type === 'hello' && message.version === 3)
      link.host.postMessage({
        ...header(message.epoch),
        version: 3,
        type: 'ready',
        tasks: ['ping'],
      });
  });
  const rt = createWorkerRuntime({
    pools: { cpu: { size: 1, factory: () => link.endpoint } },
    startupTimeoutMs: 20,
  });
  try {
    await assert.rejects(rt.createScope().enqueue('ping', options(null)).result, {
      code: 'STARTUP_TIMEOUT',
    });
  } finally {
    await rt.dispose();
    off();
  }
});

test('pull example bounds a chunk, returns credits and closes on completion, break, error and abort', async () => {
  const rt = createWorkerRuntime({
    pools: {
      files: {
        size: 1,
        cacheBytes: CURSOR_BYTES,
        factory: nodeWorker(new URL('./fixtures/file-worker.mjs', import.meta.url)),
      },
    },
  });
  const bytes = new Uint8Array(CHUNK_BYTES * 2 + 7).fill(42);
  const file = new File([bytes], 'large.bin');
  try {
    let total = 0;
    for await (const chunk of readFileChunks(rt, file)) {
      assert.ok(chunk.byteLength <= CHUNK_BYTES);
      assert.ok(new Uint8Array(chunk).every((value) => value === 42));
      assert.equal(rt.stats.leases, 1);
      assert.equal(rt.stats.active, 0);
      total += chunk.byteLength;
    }
    assert.equal(total, bytes.length);
    for await (const _chunk of readFileChunks(rt, file)) break;
    await assert.rejects(async () => {
      for await (const _chunk of readFileChunks(rt, file)) throw new Error('consumer failed');
    }, /consumer failed/);
    const controller = new AbortController();
    await assert.rejects(
      async () => {
        for await (const _chunk of readFileChunks(rt, file, { signal: controller.signal }))
          controller.abort();
      },
      { code: 'ABORTED' },
    );
    assert.equal(rt.stats.workers, 0);
    assert.equal(rt.stats.scopes, 0);
    assert.equal(rt.stats.leases, 0);
    assert.equal(rt.stats.reserved.cacheBytes, 0);
  } finally {
    await rt.dispose();
  }
});

test('large numerical cache stores the complete backing buffer and recreates views', () => {
  const values = new Float32Array(1_000_001);
  values[1_000_000] = 42;
  const cache = new CacheStore(values.byteLength).scope('numerical');
  assert.throws(() => cache.set('view', values, values.byteLength), { code: 'BUDGET_EXCEEDED' });
  cache.set('buffer', values.buffer, values.byteLength);
  assert.equal(new Float32Array(cache.get('buffer'))[1_000_000], 42);
});
