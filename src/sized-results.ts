import { integer, RuntimeError } from './errors.js';
import { iterateResults, type ResultIterator } from './iterate-results.js';
import type { WorkerSession } from './runtime/runtime.js';
import type { Catalog, PreparedInput, SessionTaskOptions, TaskHandle, TaskName } from './types.js';

export type ChunkDescriptor = { done: true } | { done: false; token: string; outputBytes: number };
export type PendingChunk = Extract<ChunkDescriptor, { done: false }>;
export interface SizedResultOptions<T extends Catalog<T>, K extends TaskName<T>> {
  session: WorkerSession<T>;
  /** A small, separately budgeted metadata task. Planning retains at most one bounded chunk plan. */
  describe(signal: AbortSignal): TaskHandle<ChunkDescriptor>;
  task: K;
  maxChunkBytes: number;
  budget: { inputBytes: number; scratchBytes: number };
  prepare(
    chunk: Readonly<PendingChunk>,
    context: { signal: AbortSignal },
  ): PreparedInput<T[K]['input']>;
  taskOptions?: Omit<
    SessionTaskOptions<T[K]['input']>,
    'prepare' | 'budget' | 'signal' | 'discardResult'
  >;
  close(): void | Promise<void>;
  signal?: AbortSignal;
}

/** Pull metadata, reserve the announced output budget, then encode and consume one chunk. */
export function iterateSizedResults<T extends Catalog<T>, K extends TaskName<T>>(
  options: SizedResultOptions<T, K>,
): ResultIterator<T[K]['output']> {
  const maximum = integer(options.maxChunkBytes, 'maxChunkBytes');
  const budget = {
    inputBytes: integer(options.budget.inputBytes, 'inputBytes'),
    scratchBytes: integer(options.budget.scratchBytes, 'scratchBytes'),
  };
  return iterateResults({
    signal: options.signal,
    isDone: () => false,
    close: options.close,
    async next(signal) {
      const task = options.describe(signal);
      const cancel = () => task.cancel(signal.reason);
      signal.addEventListener('abort', cancel, { once: true });
      if (signal.aborted) cancel();
      let chunk: PendingChunk;
      try {
        const lease = await task.result;
        try {
          signal.throwIfAborted();
          const value = lease.value;
          if (!value || typeof value.done !== 'boolean')
            throw new RuntimeError('PROTOCOL_ERROR', 'Invalid chunk descriptor');
          if (value.done) return null;
          if (typeof value.token !== 'string' || !value.token.length || value.token.length > 128)
            throw new RuntimeError('PROTOCOL_ERROR', 'Invalid chunk token');
          integer(value.outputBytes, 'chunk outputBytes');
          if (value.outputBytes > maximum)
            throw new RuntimeError('BUDGET_EXCEEDED', 'Chunk exceeds maxChunkBytes');
          chunk = Object.freeze({
            done: false,
            token: value.token,
            outputBytes: value.outputBytes,
          });
        } finally {
          lease.release();
        }
      } finally {
        await task.settled;
        signal.removeEventListener('abort', cancel);
      }
      signal.throwIfAborted();
      return options.session.enqueue(options.task, {
        ...options.taskOptions,
        budget: { ...budget, outputBytes: chunk.outputBytes },
        signal,
        prepare: (context) => options.prepare(chunk, context),
      });
    },
  });
}
