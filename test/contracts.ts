import { binaryByteLength, createWorkerRuntime, type WorkerEndpoint } from '../src/index.js';
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

binaryByteLength(new Uint8Array(8), { maxObjects: 10, maxEntries: 100 });
// @ts-expect-error Traversal limits use the structured options API.
binaryByteLength(new Uint8Array(8), 10);

scope.enqueue('ping', {
  pool: 'cpu',
  budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 },
  // @ts-expect-error Main-thread preparation must be synchronous.
  prepare: async () => ({ payload: 'text' }),
});
