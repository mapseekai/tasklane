import { parentPort, type TransferListItem, Worker, type WorkerOptions } from 'node:worker_threads';
import type { MessagePortLike, WorkerEndpoint } from '../types.js';

/** Separate entry point keeps node:worker_threads out of browser bundles. */
export function nodeWorker(url: string | URL, options: WorkerOptions = {}): () => WorkerEndpoint {
  return () => {
    const worker = new Worker(url, options);
    let stopping = false;
    return {
      postMessage: (value, transfer = []) =>
        worker.postMessage(value, [...transfer] as TransferListItem[]),
      onMessage(listener) {
        worker.on('message', listener);
        return () => worker.off('message', listener);
      },
      onFailure(listener) {
        const exit = (code: number) => {
          if (!stopping) listener(new Error(`Worker exited unexpectedly (code ${code})`));
        };
        worker.on('error', listener);
        worker.on('messageerror', listener);
        worker.on('exit', exit);
        return () => {
          worker.off('error', listener);
          worker.off('messageerror', listener);
          worker.off('exit', exit);
        };
      },
      async terminate() {
        stopping = true;
        await worker.terminate();
      },
    };
  };
}

export function nodeHost(): MessagePortLike {
  const port = parentPort;
  if (!port) throw new Error('nodeHost must be called inside a worker thread');
  return {
    postMessage: (value, transfer = []) =>
      port.postMessage(value, [...transfer] as TransferListItem[]),
    onMessage(listener) {
      port.on('message', listener);
      return () => port.off('message', listener);
    },
  };
}
