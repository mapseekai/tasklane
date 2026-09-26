import { aborted, RuntimeError } from './errors.js';
import { deferred, type Deferred } from './runtime/deferred.js';
import type { ResultLease, TaskHandle } from './types.js';

export interface ResultIterationOptions<T> {
  /** Called once per pull, after the previous result's lease has been released. */
  next(signal: AbortSignal): TaskHandle<T>;
  isDone(value: T): boolean;
  /** Owned Session/Scope: dispose it. Borrowed Session: close only this cursor. */
  close(): void | Promise<void>;
  signal?: AbortSignal;
}
export interface ResultIterator<T> extends AsyncIterableIterator<T> {
  /** Outcome of the first cleanup attempt; remains rejected after an explicit retry. */
  readonly closed: Promise<void>;
  dispose(): Promise<void>;
  /** Explicitly retry failed close(), sharing any in-flight cleanup. Never pulls more data. */
  retryCleanup(): Promise<void>;
}

/** One leased result at a time. next(), return(), dispose() and abort end the current lease. */
export function iterateResults<T>(options: ResultIterationOptions<T>): ResultIterator<T> {
  const controller = new AbortController();
  const closed = deferred<void>();
  let task: TaskHandle<T> | undefined;
  let lease: ResultLease<T> | undefined;
  let busy = false,
    ended = false;
  let failure: unknown;
  let failed = false;
  let closing: Promise<void> | undefined;
  let submitting: Deferred<void> | undefined;
  let cleanupFailed = false;
  let stopReason: unknown;
  let stopFailed = false;
  const stop = (reason?: unknown, hasFailure = reason !== undefined): Promise<void> => {
    if (ended) return closing ?? closed.promise;
    ended = true;
    failure = reason;
    failed = hasFailure;
    options.signal?.removeEventListener('abort', onAbort);
    controller.abort(reason);
    task?.cancel(reason);
    lease?.release();
    lease = undefined;
    stopReason = reason;
    stopFailed = hasFailure;
    return cleanup();
  };
  const cleanup = (): Promise<void> => {
    cleanupFailed = false;
    closing = (async () => {
      try {
        await submitting?.promise;
        await task?.settled;
        await options.close();
        failure = stopReason;
        failed = stopFailed;
        closed.resolve();
      } catch (error) {
        cleanupFailed = true;
        failure = !stopFailed
          ? error
          : new AggregateError([stopReason, error], 'Iteration and cleanup failed');
        failed = true;
        closed.reject(failure);
        throw failure;
      }
    })();
    void closing.catch(() => {});
    return closing;
  };
  const onAbort = () => {
    void stop(aborted(options.signal?.reason));
  };
  options.signal?.addEventListener('abort', onAbort, { once: true });
  if (options.signal?.aborted) onAbort();
  const done = (): IteratorResult<T> => ({ done: true, value: undefined });
  return {
    closed: closed.promise,
    [Symbol.asyncIterator]() {
      return this;
    },
    async next() {
      if (busy) throw new RuntimeError('INVALID_ARGUMENT', 'Await the preceding next() call');
      if (ended) {
        await closing;
        if (failed) throw failure;
        return done();
      }
      busy = true;
      try {
        lease?.release();
        lease = undefined;
        const submitted = deferred<void>();
        submitting = submitted;
        task = undefined;
        try {
          task = options.next(controller.signal);
          if (ended) task.cancel(failure);
        } finally {
          submitted.resolve();
          submitting = undefined;
        }
        const value = await task.result;
        if (ended) {
          value.release();
          await closing;
          if (failed) throw failure;
          return done();
        }
        lease = value;
        const data = value.value;
        const complete = options.isDone(data);
        if (ended) {
          await closing;
          if (failed) throw failure;
          return done();
        }
        if (complete) {
          await stop();
          return done();
        }
        return { done: false, value: data };
      } catch (error) {
        if (!ended) await stop(error, true);
        else await closing;
        if (failed) throw failure;
        return done();
      } finally {
        busy = false;
      }
    },
    async return() {
      await stop();
      return done();
    },
    async throw(error) {
      await stop(error, true);
      throw error;
    },
    dispose: () => stop(),
    retryCleanup: () => (cleanupFailed ? cleanup() : stop()),
  };
}
