import { encodeError as wireError } from './remote-error.js';
import { validateProgress } from './progress.js';
import { yieldTask } from './yield.js';
import { decodePacket, encodePacket, packetBytes, validateBlobTransfers } from './packet.js';
import { ScratchArena } from './resources/scratch.js';
import { aborted, integer, required, RuntimeError } from './errors.js';
import {
  type FromWorker,
  header,
  isHeader,
  type RequestMessage,
  type ToWorker,
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
  /** Task-owned, measured backing stores. Allocations outside this arena are not tracked. */
  readonly scratch: ScratchArena;
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

/** One physical task at a time. The host does not hide an unbounded secondary queue. */
export function serve<T extends Catalog<T> = TaskMap>(
  port: MessagePortLike,
  handlers: TaskHandlers<T>,
): () => void {
  let epoch = 0;
  let cache: CacheStore | undefined;
  let active:
    | { request: RequestMessage; controller: AbortController; acknowledge(): void }
    | undefined;
  let disposed = false;
  const releaseAfter = new Set<string>();
  const send = (message: FromWorker, transfer?: readonly Transferable[]) =>
    port.postMessage(message, transfer);
  const release = (scope: string) => {
    void (async () => {
      try {
        await cache?.release(scope);
        send({ ...header(epoch), type: 'released', scope, cacheBytes: cache?.bytes ?? 0 });
      } catch (error) {
        send({
          ...header(epoch),
          type: 'released',
          scope,
          cacheBytes: cache?.bytes ?? 0,
          error: wireError(error),
        });
      }
    })().catch(() => {
      disposed = true;
    });
  };
  const execute = async (request: RequestMessage) => {
    const controller = new AbortController();
    let scratch: ScratchArena | undefined;
    let closed = false,
      inFlight = false,
      hasPending = false;
    let pending: unknown;
    let progressTimer: ReturnType<typeof setTimeout> | undefined;
    const flush = () => {
      if (closed || disposed || controller.signal.aborted || inFlight || !hasPending) return;
      const wait = 16 - (performance.now() - lastProgress);
      if (wait > 0) {
        if (!progressTimer)
          progressTimer = setTimeout(() => {
            progressTimer = undefined;
            try {
              flush();
            } catch (error) {
              disposed = true;
              controller.abort(error);
              void cache?.release().catch(() => {});
            }
          }, wait);
        return;
      }
      const value = pending;
      pending = undefined;
      hasPending = false;
      inFlight = true;
      lastProgress = performance.now();
      send({ ...header(epoch), type: 'progress', id: request.id, scope: request.scope, value });
    };
    active = {
      request,
      controller,
      acknowledge() {
        inFlight = false;
        flush();
      },
    };
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
      integer(request.maxOutputBlobBytes, 'maxOutputBlobBytes');
      scratch = new ScratchArena(request.maxScratchBytes);
      const handler = Object.hasOwn(handlers, request.task)
        ? (handlers as Record<string, TaskHandler<unknown, unknown>>)[request.task]
        : undefined;
      if (!handler) throw new RuntimeError('UNKNOWN_TASK', `Unknown task: ${request.task}`);
      const context: HostContext = {
        scratch,
        signal: controller.signal,
        scopeId: request.scope,
        sessionId: request.session,
        epoch,
        cache: required(cache, 'Host cache').scope(request.scope, request.session),
        progress(value) {
          if (closed || controller.signal.aborted || disposed) return;
          validateProgress(value);
          // Snapshot while bounded: caller mutation cannot enlarge a pending message later.
          pending = structuredClone(value);
          hasPending = true;
          flush();
        },
        async checkpoint() {
          if (closed || disposed) throw new RuntimeError('CLOSED', 'Task context has completed');
          controller.signal.throwIfAborted();
          await yieldTask();
          if (closed || disposed) throw new RuntimeError('CLOSED', 'Task context has completed');
          controller.signal.throwIfAborted();
        },
      };
      const result = await handler(decodePacket(request.payload), context);
      controller.signal.throwIfAborted();
      if (!result || !Object.hasOwn(result, 'value')) {
        throw new RuntimeError('PROTOCOL_ERROR', 'Handler must return output(value, transfer)');
      }
      validateBlobTransfers(result.transfer);
      const value = encodePacket(result.value, request.maxOutputBytes, request.maxOutputBlobBytes);
      const byteLength = packetBytes(value);
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
            value,
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
      closed = true;
      scratch?.close();
      pending = undefined;
      hasPending = false;
      clearTimeout(progressTimer);
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
    if (message.type === 'progress-ack') {
      if (active?.request.id === message.id && active.request.scope === message.scope)
        try {
          active.acknowledge();
        } catch (error) {
          disposed = true;
          active.controller.abort(error);
          void cache?.release().catch(() => {});
        }
      return;
    }
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
      void cache?.release().catch(() => {});
    });
  });
  return () => {
    disposed = true;
    unsubscribe();
    active?.controller.abort();
    void cache?.release().catch(() => {});
  };
}
