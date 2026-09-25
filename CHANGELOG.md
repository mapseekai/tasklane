# Changelog

## 0.1.0-beta.1 — 2026-09-25

First beta release of tasklane with:

- browser Web Worker and Node.js worker_threads adapters
- scoped lifecycle and bounded scheduling
- priority, ageing and fair task admission
- explicit Transferable ownership
- input, scratch, output and cache budgets
- ResultLease backpressure
- cooperative, discard and terminate cancellation modes
- worker affinity and stateful sessions
- worker-local bounded caches
- protocol generation and failure handling
- runtime metrics and timing
- browser, stress and large-data conversion verification

### Reliability and scheduling

- Indexed group scheduling, bounded fairness history and waiting-budget protection.
- Physical phase validation, bounded progress with ACKs, and confirmed scope cleanup (protocol v3).
- Explicit resident-resource disposal, discarded-result tasks, lease count limits and termination recovery.
- Independent metadata traversal limits and a real-Worker scheduler benchmark.

- Bounded wire packets account for scalar and graph metadata; results decode on consumption.
- Synchronous-only prepare rejects hanging promises; asynchronous preparation belongs in Worker handlers.
- Strict priority defaults, measured scratch arenas, and metadata-aware ordinary cache accounting.
- Deterministic Node endpoint retirement tests and real Worker error/termination races.
