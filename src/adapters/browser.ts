import type { MessagePortLike, WorkerEndpoint } from '../types.js';

export function browserWorker(
  url: string | URL,
  options: WorkerOptions = { type: 'module' },
): () => WorkerEndpoint {
  return () => {
    const worker = new Worker(url, { type: 'module', ...options });
    return {
      postMessage: (value, transfer = []) => worker.postMessage(value, [...transfer]),
      onMessage(listener) {
        const receive = (event: MessageEvent<unknown>) => listener(event.data);
        worker.addEventListener('message', receive);
        return () => worker.removeEventListener('message', receive);
      },
      onFailure(listener) {
        const error = (event: ErrorEvent) => listener(new Error(event.message || 'Worker failed'));
        const decode = () => listener(new Error('Worker message deserialization failed'));
        worker.addEventListener('error', error);
        worker.addEventListener('messageerror', decode);
        return () => {
          worker.removeEventListener('error', error);
          worker.removeEventListener('messageerror', decode);
        };
      },
      terminate: () => worker.terminate(),
    };
  };
}

/** Worker-global adapter; no Window or DOM access is needed inside the host. */
export function browserHost(scope: {
  postMessage(value: unknown, transfer: Transferable[]): void;
  addEventListener(type: 'message', listener: (event: MessageEvent<unknown>) => void): void;
  removeEventListener(type: 'message', listener: (event: MessageEvent<unknown>) => void): void;
}): MessagePortLike {
  return {
    postMessage: (value, transfer = []) => scope.postMessage(value, [...transfer]),
    onMessage(listener) {
      const receive = (event: MessageEvent<unknown>) => listener(event.data);
      scope.addEventListener('message', receive);
      return () => scope.removeEventListener('message', receive);
    },
  };
}
