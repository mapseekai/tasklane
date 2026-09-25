import { convert, flatten } from './workloads.mjs';
import { transferBuffers } from '../dist/index.js';
export function handleRaw(data, post) {
  if (data.type === 'hello') {
    post({ type: 'ready' });
    return;
  }
  try {
    const start = performance.now();
    const result = (data.task === 'flatten' ? flatten : convert)(data.payload);
    const workerMs = performance.now() - start;
    post(
      { id: data.id, result, workerMs },
      transferBuffers(...Object.values(result).filter(ArrayBuffer.isView)),
    );
  } catch (error) {
    post({ id: data.id, error: String(error) });
  }
}
