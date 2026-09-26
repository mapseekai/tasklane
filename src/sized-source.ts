import { integer, RuntimeError } from './errors.js';
import { prepareOutput } from './prepared-output.js';
import type { HostContext, TaskOutput } from './host.js';
import type { ChunkDescriptor } from './sized-results.js';

export interface SizedResultPlan<T> {
  /** Full packet upper bound, including metadata. */
  readonly outputBytes: number;
  /** Allocate/encode output only during the budgeted take task. */
  encode(context: HostContext): TaskOutput<T> | Promise<TaskOutput<T>>;
  /** Release plan-owned state. Supports retry after a partial cleanup failure. */
  dispose(): void | Promise<void>;
}
export interface SizedResultSourceOptions<T> {
  maxChunkBytes: number;
  /** Produce bounded metadata/reader state. Retained state belongs in the owning Session's resource budget. */
  plan(context: HostContext): SizedResultPlan<T> | null | Promise<SizedResultPlan<T> | null>;
}
export interface SizedResultSource<T> {
  describe(context: HostContext): Promise<TaskOutput<ChunkDescriptor>>;
  take(token: string, context: HostContext): Promise<TaskOutput<T>>;
  close(): Promise<void>;
}

/** Session-owned, single pending plan. Register close() as the cache resource disposer. */
export function createSizedResultSource<T>(
  options: SizedResultSourceOptions<T>,
): SizedResultSource<T> {
  const maximum = integer(options.maxChunkBytes, 'maxChunkBytes');
  let serial = 0,
    ended = false,
    closed = false;
  let pending:
    | { token: string; bytes: number; plan: SizedResultPlan<T>; taken: boolean }
    | undefined;
  let operation: Promise<unknown> | undefined;
  let closing: Promise<void> | undefined;
  const release = async () => {
    if (pending) {
      await pending.plan.dispose();
      pending = undefined;
    }
  };
  const run = <R>(work: () => Promise<R>): Promise<R> => {
    if (closed) return Promise.reject(new RuntimeError('CLOSED', 'Result source is closed'));
    if (operation)
      return Promise.reject(
        new RuntimeError('INVALID_ARGUMENT', 'Await the preceding source operation'),
      );
    const result = Promise.resolve()
      .then(work)
      .finally(() => {
        operation = undefined;
      });
    operation = result;
    return result;
  };
  return {
    describe(context) {
      return run(async () => {
        context.signal.throwIfAborted();
        if (!pending && !ended) {
          const plan = await options.plan(context);
          if (!plan) ended = true;
          else {
            // Retain even invalid/late plans until explicit resource cleanup can dispose them.
            pending = { token: String(++serial), bytes: plan.outputBytes, plan, taken: false };
            integer(pending.bytes, 'chunk outputBytes');
            if (pending.bytes > maximum)
              throw new RuntimeError('BUDGET_EXCEEDED', 'Planned chunk exceeds maxChunkBytes');
          }
        }
        if (pending?.taken)
          throw new RuntimeError('INVALID_ARGUMENT', 'Close the source after an unsuccessful take');
        context.signal.throwIfAborted();
        if (closed) throw new RuntimeError('CLOSED', 'Result source closed during planning');
        return {
          value: pending
            ? { done: false, token: pending.token, outputBytes: pending.bytes }
            : { done: true },
        };
      });
    },
    take(token, context) {
      return run(async () => {
        const chunk = pending;
        if (!chunk || chunk.taken || token !== chunk.token)
          throw new RuntimeError('INVALID_ARGUMENT', 'Unknown or consumed chunk token');
        context.signal.throwIfAborted();
        integer(chunk.bytes, 'chunk outputBytes');
        if (chunk.bytes > maximum)
          throw new RuntimeError('BUDGET_EXCEEDED', 'Planned chunk exceeds maxChunkBytes');
        if (chunk.bytes > integer(context.outputLimit, 'outputLimit'))
          throw new RuntimeError(
            'BUDGET_EXCEEDED',
            'Task output reservation is smaller than the planned chunk',
          );
        chunk.taken = true;
        const result = await chunk.plan.encode(context);
        context.signal.throwIfAborted();
        if (closed) throw new RuntimeError('CLOSED', 'Result source closed during encoding');
        // Host validates the task's reservation too; this enforces the immutable announced bound.
        const prepared = prepareOutput(result, chunk.bytes);
        await release();
        return prepared;
      });
    },
    close() {
      if (closing) return closing;
      closed = true;
      closing = (async () => {
        await operation?.catch(() => {});
        await release();
      })().catch((error) => {
        closing = undefined;
        throw error;
      });
      void closing.catch(() => {});
      return closing;
    },
  };
}
