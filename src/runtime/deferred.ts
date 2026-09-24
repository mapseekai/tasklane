export interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}
export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  // Lifecycle cancellation may precede a caller attaching await/catch. Attaching a
  // private handler prevents global unhandled noise; awaiting the original still rejects.
  void promise.catch(() => {});
  return { promise, resolve, reject };
}
