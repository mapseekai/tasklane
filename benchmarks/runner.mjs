import { createWorkerRuntime, transferBuffers } from '../dist/index.js';
import { coordinates, convert, flatten, geojson, checksum } from './workloads.mjs';

const MiB = 1024 ** 2;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class DirectWorker {
  constructor(factory) {
    this.endpoint = factory();
    this.serial = 0;
    this.pending = null;
    this.ready = new Promise((resolve, reject) => {
      this.timer = setTimeout(() => reject(new Error('Raw worker startup timed out')), 10000);
      this.readyResolve = () => {
        clearTimeout(this.timer);
        resolve();
      };
      this.readyReject = reject;
    });
    this.offMessage = this.endpoint.onMessage((message) => {
      if (message.type === 'ready') {
        this.readyResolve();
        return;
      }
      if (this.pending && message.id === this.pending.id) {
        const pending = this.pending;
        this.pending = null;
        clearTimeout(pending.timer);
        if (message.error) pending.reject(new Error(message.error));
        else pending.resolve(message);
      }
    });
    this.offFailure = this.endpoint.onFailure((error) => {
      clearTimeout(this.timer);
      this.readyReject(error);
      this.pending?.reject(error);
    });
    this.endpoint.postMessage({ type: 'hello' });
  }
  async run(task, payload, transfer = []) {
    await this.ready;
    if (this.pending) throw new Error('Raw worker busy');
    return new Promise((resolve, reject) => {
      const id = ++this.serial;
      const timer = setTimeout(() => reject(new Error('Raw worker task timed out')), 120000);
      this.pending = { id, resolve, reject, timer };
      try {
        this.endpoint.postMessage({ id, task, payload }, transfer);
      } catch (error) {
        clearTimeout(timer);
        this.pending = null;
        reject(error);
      }
    });
  }
  async dispose() {
    clearTimeout(this.timer);
    clearTimeout(this.pending?.timer);
    this.offMessage();
    this.offFailure();
    await this.endpoint.terminate();
  }
}

/** Same algorithm, deterministic chunks and full output checksum for all modes.
 * Total time includes generation, transfers, scheduling and checksum consumption;
 * startup/warmup is reported separately. No disk I/O is included.
 */
export async function runCase(config, factory, rawFactory, sampleMemory) {
  const {
    mode,
    workload = 'project',
    inputMiB = 64,
    chunkMiB = 8,
    workers = 1,
    features = 100000,
    featureChunk = 4096,
    slowConsumerMs = 0,
  } = config;
  if (
    !['main', 'cooperative-main', 'runtime-transfer', 'runtime-clone', 'direct-transfer'].includes(
      mode,
    )
  )
    throw new Error('Unknown mode');
  if (
    ![inputMiB, chunkMiB, workers, features, featureChunk].every(
      (n) => Number.isSafeInteger(n) && n > 0,
    )
  )
    throw new Error('Positive integer configuration required');
  const isJson = workload === 'geojson';
  const totalUnits = isJson ? features : (inputMiB * MiB) / 16;
  const chunkUnits = isJson ? featureChunk : (chunkMiB * MiB) / 16;
  const chunks = Math.ceil(totalUnits / chunkUnits);
  const isRuntime = mode.startsWith('runtime-');
  const lanes = mode === 'main' || mode === 'cooperative-main' ? 1 : workers;
  const maxInput = (isJson ? featureChunk * 1400 : chunkMiB * MiB) + 4096;
  const maxOutput = isJson ? featureChunk * 400 + 4096 : maxInput + 4096;
  const maxScratch = isJson ? maxInput * 8 : mode === 'runtime-clone' ? maxInput : 0;
  const runtime = isRuntime
    ? createWorkerRuntime({
        pools: { cpu: { factory, size: workers, cacheBytes: 0, idleTimeoutMs: 0 } },
        maxWorkers: workers,
        maxActiveTasks: workers,
        budgets: {
          inputBytes: maxInput * workers,
          scratchBytes: maxScratch * workers,
          outputBytes: maxOutput * workers,
          cacheBytes: 0,
        },
        executionTimeoutMs: 120000,
      })
    : null;
  const scope = runtime?.createScope('benchmark');
  const direct =
    mode === 'direct-transfer'
      ? Array.from({ length: workers }, () => new DirectWorker(rawFactory))
      : [];
  const startup = performance.now();
  try {
    const taskName = isJson ? 'flatten' : 'convert';
    if (scope)
      await Promise.all(
        Array.from({ length: workers }, async () => {
          const lease = await scope.enqueue('ping', {
            pool: 'cpu',
            budget: { inputBytes: 0, scratchBytes: 0, outputBytes: 4096 },
            prepare: () => ({ payload: null }),
          }).result;
          lease.release();
        }),
      );
    else if (direct.length) await Promise.all(direct.map((worker) => worker.ready));
    const startupMs = performance.now() - startup;
    const warmStarted = performance.now();
    const warmPayload = () =>
      isJson
        ? { bytes: geojson(chunkUnits) }
        : { xy: coordinates(chunkUnits), operation: workload };
    if (scope)
      await Promise.all(
        Array.from({ length: workers }, async (_, lane) => {
          for (let n = 0; n < 3; n++) {
            const lease = await scope.enqueue(taskName, {
              pool: 'cpu',
              affinity: 'warm-' + lane,
              budget: { inputBytes: maxInput, scratchBytes: maxScratch, outputBytes: maxOutput },
              prepare: () => {
                const payload = warmPayload();
                return { payload, transfer: transferBuffers(isJson ? payload.bytes : payload.xy) };
              },
            }).result;
            lease.release();
          }
        }),
      );
    else if (direct.length)
      await Promise.all(
        direct.map(async (worker) => {
          for (let n = 0; n < 3; n++) {
            const payload = warmPayload();
            await worker.run(
              taskName,
              payload,
              transferBuffers(isJson ? payload.bytes : payload.xy),
            );
          }
        }),
      );
    else for (let n = 0; n < 3; n++) (isJson ? flatten : convert)(warmPayload());
    const warmupMs = performance.now() - warmStarted;
    globalThis.gc?.();
    let cursor = 0,
      inputBytes = 0,
      outputBytes = 0,
      count = 0,
      generationMs = 0,
      consumeMs = 0,
      workerMs = 0;
    let prepareMs = 0,
      queueMs = 0,
      roundTripMs = 0,
      maxTimerLagMs = 0,
      timerSamples = 0,
      peakMemoryBytes = sampleMemory?.() ?? null;
    let lastTick = performance.now();
    const timer = setInterval(() => {
      const now = performance.now();
      maxTimerLagMs = Math.max(maxTimerLagMs, now - lastTick - 8);
      lastTick = now;
      timerSamples++;
      if (sampleMemory) peakMemoryBytes = Math.max(peakMemoryBytes, sampleMemory());
    }, 8);
    const memoryBaselineBytes = sampleMemory?.() ?? null;
    const hashes = new Array(chunks);
    const make = (chunk) => {
      const started = performance.now();
      const start = chunk * chunkUnits,
        units = Math.min(chunkUnits, totalUnits - start);
      const payload = isJson
        ? { bytes: geojson(units, start), returnClone: mode === 'runtime-clone' }
        : {
            xy: coordinates(units, start),
            operation: workload,
            returnClone: mode === 'runtime-clone',
          };
      inputBytes += (isJson ? payload.bytes : payload.xy).byteLength;
      generationMs += performance.now() - started;
      return payload;
    };
    const consume = (result, chunk) => {
      const started = performance.now();
      const arrays = isJson
        ? [
            result.xy,
            result.pathOffsets,
            result.featureOffsets,
            result.polygonOffsets,
            result.featurePolygonOffsets,
            result.types,
          ]
        : [result.vertices, result.bounds];
      hashes[chunk] = arrays.map(checksum).join('|');
      outputBytes += arrays.reduce((sum, array) => sum + array.byteLength, 0);
      count += result.count;
      consumeMs += performance.now() - started;
      if (sampleMemory) peakMemoryBytes = Math.max(peakMemoryBytes, sampleMemory());
    };
    await sleep(16);
    const started = performance.now();
    try {
      if (mode === 'main' || mode === 'cooperative-main') {
        for (let i = 0; i < chunks; i++) {
          const payload = make(i),
            begin = performance.now();
          const result = (isJson ? flatten : convert)(payload);
          workerMs += performance.now() - begin;
          consume(result, i);
          if (mode === 'cooperative-main') await sleep(0);
        }
      } else {
        await Promise.all(
          Array.from({ length: lanes }, async (_, lane) => {
            while (cursor < chunks) {
              const chunk = cursor++;
              if (scope) {
                const task = scope.enqueue(taskName, {
                  pool: 'cpu',
                  budget: {
                    inputBytes: maxInput,
                    scratchBytes: maxScratch,
                    outputBytes: maxOutput,
                  },
                  prepare: () => {
                    const payload = make(chunk);
                    return {
                      payload,
                      transfer:
                        mode === 'runtime-transfer'
                          ? transferBuffers(isJson ? payload.bytes : payload.xy)
                          : [],
                    };
                  },
                });
                const lease = await task.result;
                try {
                  consume(lease.value, chunk);
                  if (slowConsumerMs) await sleep(slowConsumerMs);
                } finally {
                  lease.release();
                }
                prepareMs += task.timing.prepareMs;
                queueMs += task.timing.queueMs;
                roundTripMs += task.timing.roundTripMs;
                workerMs += task.timing.workerMs;
              } else {
                const payload = make(chunk),
                  begin = performance.now();
                const response = await direct[lane].run(
                  taskName,
                  payload,
                  transferBuffers(isJson ? payload.bytes : payload.xy),
                );
                roundTripMs += performance.now() - begin;
                workerMs += response.workerMs;
                consume(response.result, chunk);
              }
            }
          }),
        );
      }
      const totalMs = performance.now() - started;
      // Let the heartbeat observe the final synchronous block before stopping it.
      await sleep(24);
      return {
        ...config,
        chunks,
        count,
        inputBytes,
        outputBytes,
        startupMs,
        warmupMs,
        totalMs,
        generationMs,
        consumeMs,
        workerMs,
        prepareMs,
        queueMs,
        roundTripMs,
        throughputMiBs: inputBytes / MiB / (totalMs / 1000),
        maxTimerLagMs,
        timerSamples,
        memoryBaselineBytes,
        peakMemoryBytes,
        peakMemoryDeltaBytes:
          peakMemoryBytes === null ? null : Math.max(0, peakMemoryBytes - memoryBaselineBytes),
        runtimeStats: runtime?.stats ?? null,
        hashes,
      };
    } finally {
      clearInterval(timer);
    }
  } finally {
    await runtime?.dispose();
    await Promise.all(direct.map((worker) => worker.dispose()));
  }
}
