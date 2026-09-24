import { RuntimeError } from '../errors.js';
import type { ResultLease } from '../types.js';

export class OwnedResult<T> implements ResultLease<T> {
  private data: T | undefined;
  private disposed = false;
  constructor(
    data: T,
    readonly byteLength: number,
    private onRelease: () => void,
  ) {
    this.data = data;
  }
  get value(): T {
    if (this.disposed) throw new RuntimeError('RESULT_RELEASED', 'Result lease has been released');
    return this.data as T;
  }
  get released(): boolean {
    return this.disposed;
  }
  release(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.data = undefined;
    const release = this.onRelease;
    this.onRelease = () => {};
    release();
  }
}
