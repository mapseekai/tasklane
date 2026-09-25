import { output } from '../../dist/host.js';

export const CHUNK_BYTES = 64 * 1024;
// Declared cursor/reference charge, not the Blob backing storage's memory usage.
export const CURSOR_BYTES = 128;

export const fileHandlers = {
  fileOpen(file, ctx) {
    if (!(file instanceof Blob)) throw new TypeError('Expected Blob or File');
    ctx.cache.setResource('cursor', { file, offset: 0 }, CURSOR_BYTES, (cursor) => {
      cursor.file = null;
    });
    return output(null);
  },
  async fileNext(_input, ctx) {
    const cursor = ctx.cache.get('cursor');
    if (!cursor?.file) throw new Error('File is not open');
    const start = cursor.offset;
    if (start >= cursor.file.size) return output(null);
    const buffer = await cursor.file.slice(start, start + CHUNK_BYTES).arrayBuffer();
    ctx.signal.throwIfAborted();
    cursor.offset += buffer.byteLength;
    return output(buffer, [buffer]);
  },
};
