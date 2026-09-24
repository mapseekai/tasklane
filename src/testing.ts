import { asError } from './errors.js';
import type { MessagePortLike, WorkerEndpoint } from './types.js';

/** Deterministic same-realm transport using REAL structuredClone/transfer semantics.
 * It is not a parallelism or performance benchmark substitute for a real Worker.
 */
export function createLoopback(): {
  endpoint: WorkerEndpoint;
  host: MessagePortLike;
  fail(error: unknown): void;
  inject(message: unknown): void;
} {
  const toHost = new Set<(message: unknown) => void>();
  const toMain = new Set<(message: unknown) => void>();
  const failures = new Set<(error: Error) => void>();
  let closed = false;
  const post = (
    listeners: Set<(message: unknown) => void>,
    value: unknown,
    transfer: readonly Transferable[] = [],
  ) => {
    if (closed) throw new Error('Loopback closed');
    const clone = structuredClone(value, { transfer: [...transfer] });
    queueMicrotask(() => {
      if (!closed) for (const listener of listeners) listener(clone);
    });
  };
  return {
    endpoint: {
      postMessage: (value, transfer) => post(toHost, value, transfer),
      onMessage(listener) {
        toMain.add(listener);
        return () => {
          toMain.delete(listener);
        };
      },
      onFailure(listener) {
        failures.add(listener);
        return () => {
          failures.delete(listener);
        };
      },
      terminate() {
        closed = true;
        toHost.clear();
        toMain.clear();
        failures.clear();
      },
    },
    host: {
      postMessage: (value, transfer) => post(toMain, value, transfer),
      onMessage(listener) {
        toHost.add(listener);
        return () => {
          toHost.delete(listener);
        };
      },
    },
    fail(error) {
      for (const listener of [...failures]) listener(asError(error));
    },
    inject(message) {
      for (const listener of [...toMain]) listener(message);
    },
  };
}
