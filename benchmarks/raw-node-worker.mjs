import { parentPort } from 'node:worker_threads';
import { convert, flatten } from './workloads.mjs';
import { transferBuffers } from '../dist/index.js';
parentPort.on('message', (data) => {
  if (data.type === 'hello') {
    parentPort.postMessage({ type: 'ready' });
    return;
  }
  try {
    const start = performance.now();
    const result = (data.task === 'flatten' ? flatten : convert)(data.payload);
    const workerMs = performance.now() - start;
    parentPort.postMessage(
      { id: data.id, result, workerMs },
      transferBuffers(...Object.values(result).filter(ArrayBuffer.isView)),
    );
  } catch (error) {
    parentPort.postMessage({ id: data.id, error: String(error) });
  }
});
