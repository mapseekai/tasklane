# Changelog

## 0.1.0-beta.4 — 2026-09-26

- Budget starvation protection follows physical slot availability and effective task priority, including ageing.
- Explicit result iterator cleanup retry with observable initial failure and shared in-flight cleanup.
- Generic scheduling, Session occupancy, cancellation and cleanup regression coverage.

## 0.1.0-beta.3 — 2026-09-25

- Protocol v5: structured, bounded remote business error metadata.
- Budgeted asynchronous input preparation before Worker admission.
- Public result iteration with deterministic lease and cursor cleanup.
- Simplified task completion state and shared test/benchmark helpers.

## 0.1.0-beta.2 — 2026-09-25

- Protocol v4: bounded Blob/File attachments with preserved File metadata across browser and Node workers.
- Per-task logical input/output blob limits, separate from packet memory credits; attachments use structured cloning.
- Tested pull-based file Session example, binary cache guidance and resident Session capacity planning.
- Capability-focused usage, API and resource documentation.

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
