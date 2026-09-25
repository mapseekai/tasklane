export { browserWorker } from './adapters/browser.js';
export {
  binaryByteLength,
  dataByteLength,
  transferBuffers,
  type TraversalLimits,
} from './binary.js';
export { RuntimeError, type ErrorCode } from './errors.js';
export {
  createWorkerRuntime,
  RuntimeScope,
  WorkerRuntime,
  WorkerSession,
} from './runtime/runtime.js';
export type {
  Cancellation,
  Catalog,
  MessagePortLike,
  PoolOptions,
  PreparedInput,
  Priority,
  ResultLease,
  RuntimeBudgets,
  RuntimeOptions,
  RuntimeStats,
  SessionTaskOptions,
  TaskBudget,
  TaskHandle,
  TaskMap,
  TaskName,
  TaskOptions,
  TaskState,
  TaskTiming,
  TaskType,
  WorkerEndpoint,
} from './types.js';

/** Consume a result with deterministic credit return, including when the consumer throws. */
export async function consumeResult<T, R>(
  handle: import('./types.js').TaskHandle<T>,
  consume: (value: T) => R | Promise<R>,
): Promise<R> {
  const lease = await handle.result;
  try {
    return await consume(lease.value);
  } finally {
    lease.release();
  }
}

export { packetByteLength } from './packet.js';
