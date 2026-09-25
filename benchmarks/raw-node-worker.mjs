import { parentPort } from 'node:worker_threads';
import { handleRaw } from './raw-handler.mjs';
parentPort.on('message', (data) =>
  handleRaw(data, (value, transfer) => parentPort.postMessage(value, transfer)),
);
