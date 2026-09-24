import { binaryByteLength } from './binary.js';
import { aborted, asError, integer, required, RuntimeError } from './errors.js';
import {
  type FromWorker,
  header,
  isHeader,
  type RequestMessage,
  type ToWorker,
  type WireError,
} from './protocol.js';
import { CacheStore, type ScopedCache } from './resources/cache.js';
import type { Catalog, MessagePortLike, TaskMap } from './types.js';

export { browserHost } from './adapters/browser.js';
export type { ScopedCache } from './resources/cache.js';

export interface TaskOutput<T> {
  value: T;
  transfer?: readonly Transferable[];
}
export function output<T>(value: T, transfer: readonly Transferable[] = []): TaskOutput<T> {
  return { value, transfer };
}
export interface HostContext {
  readonly signal: AbortSignal;
  readonly scopeId: string;
  readonly sessionId?: string;
  readonly epoch: number;
  readonly cache: ScopedCache;
  /** Throttled to at most one message per 16ms per physical task. */
  progress(value: unknown): void;
  /** A real task-queue yield. Promise.resolve() is NOT sufficient for cancellation messages. */
  checkpoint(): Promise<void>;
}
export type TaskHandler<I, O> = (
  payload: I,
  context: HostContext,
) => TaskOutput<O> | Promise<TaskOutput<O>>;
export type TaskHandlers<T extends Catalog<T>> = {
  [K in keyof T]: TaskHandler<T[K]['input'], T[K]['output']>;
};

function wireError(value: unknown): WireError {
  const error = asError(value);
  return {
    code: error instanceof RuntimeError ? error.code : 'REMOTE_ERROR',
    name: error.name,
    message: error.message,
    stack: error.stack,
  };
}

/** One physical task at a time. The host does not hide an unbounded secondary queue. */
export function serve<T extends Catalog<T> = TaskMap>(
  port: MessagePortLike,
  handlers: TaskHandlers<T>,
): () => void {
  let epoch = 0;
  let cache: CacheStore | undefined;
  let active: { request: RequestMessage; controller: AbortController } | undefined;
  let disposed = false;
  const releaseAfter = new Set<string>();
  const send = (message: FromWorker, transfer?: readonly Transferable[]) =>
    port.postMessage(message, transfer);
  const release = (scope: string) => {
    cache?.release(scope);
    send({ ...header(epoch), type: 'released', scope, cacheBytes: cache?.bytes ?? 0 });
  };
  const execute = async (request: RequestMessage) => {
    const controller = new AbortController();
    active = { request, controller };
    const started = performance.now();
    let lastProgress = -Infinity;
    try {
      if (
        typeof request.id !== 'string' ||
        typeof request.scope !== 'string' ||
        typeof request.task !== 'string' ||
        (request.session !== undefined && typeof request.session !== 'string')
      ) {
        throw new RuntimeError('PROTOCOL_ERROR', 'Malformed task request');
      }
      integer(request.maxOutputBytes, 'maxOutputBytes');
      const handler = Object.hasOwn(handlers, request.task)
        ? (handlers as Record<string, TaskHandler<unknown, unknown>>)[request.task]
        : undefined;
      if (!handler) throw new RuntimeError('UNKNOWN_TASK', `Unknown task: ${request.task}`);
      const context: HostContext = {
        signal: controller.signal,
        scopeId: request.scope,
        sessionId: request.session,
        epoch,
        cache: required(cache, 'Host cache').scope(request.scope, request.session),
        progress(value) {
          if (controller.signal.aborted || disposed || performance.now() - lastProgress < 16)
            return;
          lastProgress = performance.now();
          send({ ...header(epoch), type: 'progress', id: request.id, scope: request.scope, value });
        },
        async checkpoint() {
          controller.signal.throwIfAborted();
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
          controller.signal.throwIfAborted();
        },
      };
      const result = await handler(request.payload, context);
      controller.signal.throwIfAborted();
      if (!result || !Object.hasOwn(result, 'value')) {
        throw new RuntimeError('PROTOCOL_ERROR', 'Handler must return output(value, transfer)');
      }
      const byteLength = binaryByteLength(result.value);
      if (byteLength > request.maxOutputBytes) {
        throw new RuntimeError('BUDGET_EXCEEDED', 'Result exceeds reserved outputBytes');
      }
      if (!disposed) {
        send(
          {
            ...header(epoch),
            type: 'result',
            id: request.id,
            scope: request.scope,
            value: result.value,
            byteLength,
            workerMs: performance.now() - started,
            cacheBytes: required(cache, 'Host cache').bytes,
          },
          result.transfer,
        );
      }
    } catch (error) {
      if (!disposed) {
        send({
          ...header(epoch),
          type: controller.signal.aborted ? 'cancelled' : 'error',
          id: request.id,
          scope: request.scope,
          error: wireError(controller.signal.aborted ? aborted(controller.signal.reason) : error),
          workerMs: performance.now() - started,
          cacheBytes: cache?.bytes ?? 0,
        });
      }
    } finally {
      active = undefined;
      if (releaseAfter.delete(request.scope) && !disposed) release(request.scope);
    }
  };
  const unsubscribe = port.onMessage((raw) => {
    if (disposed) return;
    if (!isHeader(raw)) return; // Invalid/version-mismatched hello expires at the bounded startup timeout.
    const message = raw as ToWorker;
    if (message.type === 'hello') {
      if (epoch) return;
      try {
        cache = new CacheStore(message.cacheBytes, message.cacheEntries);
      } catch {
        return;
      }
      epoch = message.epoch;
      send({ ...header(epoch), type: 'ready', tasks: Object.keys(handlers) });
      return;
    }
    if (!epoch || message.epoch !== epoch) return;
    if (message.type === 'cancel') {
      if (active?.request.id === message.id && active.request.scope === message.scope) {
        active.controller.abort(aborted());
      }
      return;
    }
    if (message.type === 'release-scope') {
      if (active?.request.scope === message.scope) {
        releaseAfter.add(message.scope);
        active.controller.abort(aborted());
      } else release(message.scope);
      return;
    }
    if (message.type !== 'request') return;
    if (active) {
      // A correct runtime never sends a second physical request before the first terminal reply.
      send({
        ...header(epoch),
        type: 'error',
        id: message.id,
        scope: message.scope,
        error: wireError(new RuntimeError('PROTOCOL_ERROR', 'Worker already has an active task')),
        workerMs: 0,
        cacheBytes: required(cache, 'Host cache').bytes,
      });
      return;
    }
    // A transport failure in the error response must not become an unhandled rejection.
    void execute(message).catch(() => {
      disposed = true;
      active?.controller.abort();
      cache?.release();
    });
  });
  return () => {
    disposed = true;
    unsubscribe();
    active?.controller.abort();
    cache?.release();
  };
}
