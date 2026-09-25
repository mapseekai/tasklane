import { consumeResult, iterateResults } from '../../dist/index.js';
import { CHUNK_BYTES } from './handlers.mjs';

/** The caller owns Runtime and provides a pool named "files" with cacheBytes >= 128.
 * One yielded chunk is leased at a time. A break or consumer exception closes the session.
 */
export async function* readFileChunks(runtime, file, { signal } = {}) {
  const scope = runtime.createScope('file-chunks');
  const session = scope.session('files');
  try {
    await consumeResult(
      session.enqueue('fileOpen', {
        budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 0 },
        blobLimits: { inputBytes: file.size, outputBytes: 0 },
        prepare: () => ({ payload: file }),
        signal,
      }),
      () => {},
    );
    yield* iterateResults({
      signal,
      next: (signal) =>
        session.enqueue('fileNext', {
          budget: { inputBytes: 0, scratchBytes: 0, outputBytes: CHUNK_BYTES },
          prepare: () => ({ payload: null }),
          signal,
        }),
      isDone: (value) => value === null,
      close: () => scope.dispose(),
    });
  } finally {
    await scope.dispose();
  }
}
