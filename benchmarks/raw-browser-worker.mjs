import { handleRaw } from './raw-handler.mjs';
self.onmessage = ({ data }) =>
  handleRaw(data, (value, transfer) => self.postMessage(value, transfer));
