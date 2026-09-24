import { convert, flatten } from './workloads.mjs';
import { transferBuffers } from '../dist/index.js';
self.onmessage = ({ data }) => {
  if (data.type === 'hello') {
    self.postMessage({ type: 'ready' });
    return;
  }
  try {
    const start = performance.now();
    const result = (data.task === 'flatten' ? flatten : convert)(data.payload);
    const workerMs = performance.now() - start;
    self.postMessage(
      { id: data.id, result, workerMs },
      transferBuffers(...Object.values(result).filter(ArrayBuffer.isView)),
    );
  } catch (error) {
    self.postMessage({ id: data.id, error: String(error) });
  }
};
