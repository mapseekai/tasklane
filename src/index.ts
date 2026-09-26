export { browserWorker } from './adapters/browser.js';
export {
  binaryByteLength,
  dataByteLength,
  transferBuffers,
  transferOwnedBuffers,
  type TraversalLimits,
} from './binary.js';
export {
  RuntimeError,
  SessionAdmissionError,
  type ErrorCode,
  type RemoteErrorInfo,
  type ErrorDetail,
} from './errors.js';
export {
  createWorkerRuntime,
  RuntimeScope,
  WorkerRuntime,
  WorkerSession,
  SessionGroup,
} from './runtime/runtime.js';
export type {
  AdaptivePoolOptions,
  InteractiveReserve,
  MaintenanceReport,
  MemoryPressure,
  PoolSizing,
  TrimOptions,
  ReclaimReason,
  ReclaimStats,
  ResourceCacheReport,
  ResourceCacheSnapshot,
  CacheStats,
  AdmissionBlocker,
  RuntimeDiagnostics,
  PoolDiagnostics,
  SessionAdmissionOptions,
  Cancellation,
  Catalog,
  MessagePortLike,
  PoolOptions,
  ResourceLease,
  ResourceOptions,
  ResourceReservations,
  SessionOptions,
  TaskAffinity,
  PreparedInput,
  PreparedTaskOptions,
  SessionPreparedTaskOptions,
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

export {
  iterateResults,
  type ResultIterator,
  type ResultIterationOptions,
} from './iterate-results.js';

export {
  iterateSizedResults,
  type SizedResultOptions,
  type ChunkDescriptor,
  type PendingChunk,
} from './sized-results.js';
