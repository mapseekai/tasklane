import { integer, RuntimeError } from '../errors.js';

/** Explicit task-local backing-store ownership. release detaches every alias. */
export class ScratchArena {
  private buffers = new Map<ArrayBuffer, number>();
  private used = 0;
  private closed = false;
  constructor(readonly limit: number) {
    integer(limit, 'scratchBytes');
  }
  get bytes(): number {
    return this.used;
  }
  allocate(bytes: number): ArrayBuffer {
    if (this.closed) throw new RuntimeError('CLOSED', 'Scratch arena has closed');
    integer(bytes, 'scratch allocation');
    if (bytes > this.limit - this.used)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Scratch budget exceeded');
    const buffer = new ArrayBuffer(bytes);
    this.buffers.set(buffer, bytes);
    this.used += bytes;
    return buffer;
  }
  release(buffer: ArrayBuffer): void {
    if (!this.buffers.has(buffer)) return;
    // Transferring an arena buffer elsewhere before release violates arena ownership.
    const bytes = this.buffers.get(buffer)!;
    try {
      structuredClone(buffer, { transfer: [buffer] });
    } catch {
      if (buffer.byteLength !== 0)
        throw new RuntimeError('INVALID_ARGUMENT', 'Scratch buffer cannot be detached');
    }
    this.buffers.delete(buffer);
    this.used -= bytes;
  }
  close(): void {
    this.closed = true;
    for (const buffer of this.buffers.keys()) this.release(buffer);
    this.used = 0;
  }
}
