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
