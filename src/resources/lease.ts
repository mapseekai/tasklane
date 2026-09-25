import { decodePacket, type Packet } from '../packet.js';
import { RuntimeError } from '../errors.js';
import type { ResultLease } from '../types.js';

export class OwnedResult<T> implements ResultLease<T> {
  private data: T | undefined;
  private packet: Packet | undefined;
  private decoded = false;
  private disposed = false;
  constructor(
    data: Packet,
    readonly byteLength: number,
    private onRelease: () => void,
  ) {
    this.packet = data;
  }
  get value(): T {
    if (this.disposed) throw new RuntimeError('RESULT_RELEASED', 'Result lease has been released');
    if (!this.decoded) {
      try {
        this.data = decodePacket(this.packet!) as T;
        this.decoded = true;
        this.packet = undefined;
      } catch (error) {
        this.release();
        throw error;
      }
    }
    return this.data as T;
  }
  get released(): boolean {
    return this.disposed;
  }
  release(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.data = undefined;
    this.packet = undefined;
    const release = this.onRelease;
    this.onRelease = () => {};
    release();
  }
}
