# Changelog

## 0.2.0-beta.1 — 2026-09-26

- Scoped resident ResourceLease budgets, bounded handle counts and atomic resize.
- Explicit Session admission with immediate/wait modes, cancellation and capacity diagnostics.
- Opt-in idle Session reclamation with graceful disposal and physical capacity retention on failure.
- Multi-key locality hints with bounded, scope-isolated successful-work history.
- Resizable Worker cache resources and large TypedArray resident accounting.
- Two-phase sized result streaming with pre-encoding output admission.
- Explicit transferOwnedBuffers alias, public types and emap migration guidance.
- Joint Worker/resident Session admission, pressure-driven replica reclaim and resize wakeups.
- Failed Worker termination no longer blocks reclamation of healthy idle Workers.
- Zero-copy binary cache view snapshots and cumulative per-pool/global cache counters.
- Sized output metadata is encoded once, with announced and task bounds rechecked before sending.
- Session admission retains ownership through reentrant factory cancellation and synchronous startup failures.
- Session groups route among caller-opened replicas using bounded replacement reader footprints.
- Optional interactive Worker, active/preparing, result-lease and byte reservations.
- Resource-indexed blocked queues with targeted wakeups and eligibility counters.
- Explicit pool resize, pressure/trim and reader trim callbacks with acknowledgement-based accounting.
- Opt-in adaptive Worker/cache targets within hard bounds, with idle hysteresis and pressure suspension.
- Reader cache counters/snapshots and per-pool reclamation attempts, failures and reasons.
- Protocol v6 adds cache control and resource telemetry; update the main package and Worker bundle together.
- Recheck idle replica eligibility after asynchronous cleanup to protect newly admitted work.
- Defer adaptive cache changes on busy Workers and reconcile at idle without blocking other pools.
- Deterministic multi-pool sampling, repeated pressure transitions and three-browser sustained lifecycle coverage.

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
