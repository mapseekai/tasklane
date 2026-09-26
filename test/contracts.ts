import {
  binaryByteLength,
  createWorkerRuntime,
  iterateResults,
  type WorkerEndpoint,
  type RemoteErrorInfo,
} from '../src/index.js';
import { output, type TaskHandlers } from '../src/host.js';

type Tasks = {
  convert: { input: Float64Array; output: Float32Array };
  ping: { input: string; output: number };
};
declare const factory: () => WorkerEndpoint;
const runtime = createWorkerRuntime<Tasks>({ pools: { cpu: { factory, size: 2 } } });
const scope = runtime.createScope();
const task = scope.enqueue('convert', {
  pool: 'cpu',
  budget: { inputBytes: 16, scratchBytes: 0, outputBytes: 8 },
  prepare: () => ({ payload: new Float64Array(2) }),
});
const result: Promise<Float32Array> = task.result.then((lease) => lease.value);
void result;
const prepared = scope.enqueuePrepared('convert', {
  pool: 'cpu',
  budget: { inputBytes: 16, scratchBytes: 0, outputBytes: 8 },
  preparationScratchBytes: 32,
  prepareAsync: async ({ signal }) => {
    signal.throwIfAborted();
    return { payload: new Float64Array(2) };
  },
});
const preparedResult: Promise<Float32Array> = prepared.result.then((lease) => lease.value);
void preparedResult;
scope.session('cpu').enqueuePrepared('ping', {
  budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
  preparationScratchBytes: 0,
  prepareAsync: async () => ({ payload: 'text' }),
});
const chunks = iterateResults({
  next: (signal) =>
    scope.enqueue('ping', {
      pool: 'cpu',
      budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
      signal,
      prepare: () => ({ payload: 'text' }),
    }),
  isDone: (value) => value === 0,
  close: () => scope.dispose(),
});
const iterator: AsyncIterableIterator<number> = chunks;
void iterator;
const cleanupRetry: Promise<void> = chunks.retryCleanup();
void cleanupRetry;
const info: RemoteErrorInfo = {
  name: 'DataError',
  message: 'failed',
  code: 'DOMAIN_CODE',
  details: { limit: 10 },
};
void info;
// @ts-expect-error Unknown task names must not typecheck.
scope.enqueue('unknown', {});
scope.enqueue('convert', {
  pool: 'cpu',
  budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 0 },
  // @ts-expect-error Wrong input type must not typecheck.
  prepare: () => ({ payload: 'wrong' }),
});
const handlers: TaskHandlers<Tasks> = {
  convert: (values) => output(Float32Array.from(values)),
  ping: (text) => output(text.length),
};
void handlers;

const files = createWorkerRuntime<{ open: { input: File; output: Blob } }>({
  pools: { files: { factory, size: 1 } },
});
files
  .createScope()
  .session('files')
  .enqueue('open', {
    budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
    blobLimits: { inputBytes: 1024, outputBytes: 1024 },
    prepare: () => ({ payload: new File(['data'], 'input.tif') }),
  });

binaryByteLength(new Uint8Array(8), { maxObjects: 10, maxEntries: 100 });
// @ts-expect-error Traversal limits use the structured options API.
binaryByteLength(new Uint8Array(8), 10);

scope.enqueue('ping', {
  pool: 'cpu',
  budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
  // @ts-expect-error Main-thread preparation must be synchronous.
  prepare: async () => ({ payload: 'text' }),
});

const resident = scope.resources.acquire({ kind: 'resident', bytes: 32 });
resident.resize(64);
const heldBytes: number = resident.bytes;
void heldBytes;
resident.release();
const admission = scope.acquireSession('cpu', {
  mode: 'immediate',
  reclaimable: true,
  reclaimPriority: 10,
  residentBytes: 32,
});
void admission.then((session) => session.resident?.resize(64));
void admission;
scope.enqueue('ping', {
  pool: 'cpu',
  affinity: { keys: ['block-a', 'block-b'] },
  budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
  prepare: () => ({ payload: 'text' }),
});
// @ts-expect-error Resident budgets require an explicit supported kind.
scope.resources.acquire({ kind: 'gpu', bytes: 8 });
scope.enqueue('ping', {
  pool: 'cpu',
  // @ts-expect-error Affinity keys must be strings.
  affinity: { keys: [1] },
  budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
  prepare: () => ({ payload: 'text' }),
});

import { iterateSizedResults, type ChunkDescriptor } from '../src/index.js';
import { createSizedResultSource } from '../src/host.js';
const chunkRuntime = createWorkerRuntime<{
  describe: { input: null; output: ChunkDescriptor };
  take: { input: string; output: Uint8Array };
}>({ pools: { cpu: { factory, size: 1 } } });
const chunkSession = chunkRuntime.createScope().session('cpu');
const sizedIterator: AsyncIterableIterator<Uint8Array> = iterateSizedResults({
  session: chunkSession,
  task: 'take',
  maxChunkBytes: 1024,
  budget: { inputBytes: 256, scratchBytes: 0 },
  describe: (signal) =>
    chunkSession.enqueue('describe', {
      budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 1024 },
      signal,
      prepare: () => ({ payload: null }),
    }),
  prepare: (chunk) => ({ payload: chunk.token }),
  close: () => chunkSession.dispose(),
});
void sizedIterator;
const chunkSource = createSizedResultSource({
  maxChunkBytes: 16,
  plan: () => ({
    outputBytes: 16,
    encode: () => output(new Uint8Array(16)),
    dispose() {},
  }),
});
void chunkSource;

import type { MaintenanceReport, SessionGroup, ResourceCacheReport } from '../src/index.js';
import type { ScopedCache } from '../src/host.js';
declare const workerCache: ScopedCache;
const report: ResourceCacheReport = {
  hits: 1,
  misses: 2,
  evictions: 0,
  keys: ['block'],
  usedBytes: 16,
};
const cacheResource = workerCache.setResource('reader', {}, 16, () => {}, {
  trim: async (target) => target,
});
cacheResource.report(report);
const maintenance: Promise<MaintenanceReport> = runtime.resizePool('cpu', {
  size: 1,
  cacheBytes: 0,
});
void maintenance;
void runtime.trim({
  pool: 'cpu',
  cacheBytesPerWorker: 0,
  workersPerPool: 1,
  reclaimSessions: false,
});
void runtime.setMemoryPressure('moderate');
void admission.then((session) => {
  const group: SessionGroup<Tasks> = scope.sessionGroup([session]);
  const value: Promise<number> = group
    .enqueue('ping', {
      affinity: { keys: ['block'] },
      budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
      prepare: () => ({ payload: 'test' }),
    })
    .result.then((lease) => lease.value);
  void value;
  group.enqueuePrepared('convert', {
    affinity: 'block',
    budget: { inputBytes: 16, scratchBytes: 0, outputBytes: 8 },
    preparationScratchBytes: 0,
    prepareAsync: async () => ({ payload: new Float64Array(2) }),
  });
  group.enqueue('ping', {
    budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
    // @ts-expect-error Groups preserve task input contracts.
    prepare: () => ({ payload: 42 }),
  });
});
// @ts-expect-error Pressure signals are bounded categories.
void runtime.setMemoryPressure('extreme');
createWorkerRuntime({
  pools: {
    cpu: {
      factory,
      size: 4,
      cacheBytes: 1024,
      interactiveWorkers: 1,
      adaptive: {
        minWorkers: 2,
        minCacheBytes: 128,
        sampleMs: 1000,
        idleMs: 30000,
        missRatio: 0.25,
      },
    },
  },
  interactiveReserve: {
    workers: 1,
    activeTasks: 1,
    preparingTasks: 1,
    resultLeases: 1,
    budgets: { outputBytes: 1024 },
  },
});
