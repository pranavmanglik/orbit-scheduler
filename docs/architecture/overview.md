# Orbit Scheduler: architecture and guarantees

## Scope and ownership

`orbit-scheduler` is an optional in-process async scheduler for fixed-delay interval callbacks. It
has no runtime dependencies, storage backend, or dependency on Core. The application creates and
closes the scheduler, commonly through Core's managed application-resource lifecycle. Core does
not silently create a scheduler or persist its jobs.

The scheduler creates one task per registered job. A shared semaphore bounds how many callbacks
may be active at once. Each task runs its callback, reports an ordinary exception, then waits the
full configured interval before trying again. A callback never overlaps with itself; a slow run
shifts its next start instead of accumulating missed runs.

## Timing semantics

Intervals are fixed delays measured with asyncio's monotonic event-loop timeout behavior, not
wall-clock cron schedules. `run_immediately=True` starts the first callback as soon as the task is
scheduled. Otherwise, the first callback begins after one interval. After every success or
reported failure, the task waits a full interval before the next run. Missed intervals are not
replayed.

The interval range is 0.01 to 2,592,000 seconds (30 days). Job names are unique, bounded to 128
safe ASCII characters, and registrations are limited by `max_jobs`. Active callback concurrency is
limited by `max_concurrency`. These bounds cap scheduler-owned tasks and configuration; applications
must also bound work spawned by callbacks.

## Failure handling

An ordinary `Exception` from a callback is converted to immutable `JobFailure` metadata containing
the job name, a sanitized exception class label, and a Unix timestamp. Exception messages,
tracebacks, callback arguments, and original exception objects are not retained. The configured
`on_error` handler may be synchronous or asynchronous; a handler failure is logged with safe
metadata and does not stop future scheduling. If no handler is configured, the same safe metadata
is logged.

`asyncio.CancelledError` is control flow: it propagates and stops that job task rather than being
reported as an ordinary callback failure. Other `BaseException` subclasses are not swallowed.

## Shutdown behavior

`aclose(grace_period=...)` transitions the scheduler to `CLOSING`, prevents future callback starts,
and shares one cleanup task among concurrent callers. The grace period supplied by the first closer
controls that task. Existing work is allowed to finish within the grace period, after which pending
tasks are cancelled and allowed a short settling interval. Scheduler shutdown then becomes
`CLOSED` even if a callback suppresses cancellation; such a task can outlive close and remains
observed until eventual completion. Callbacks should cooperate with cancellation and release their
own resources in `finally` blocks.

Cancellation of a caller awaiting `aclose()` does not cancel the shared cleanup task. That caller
still receives `CancelledError`; a later close call can join the cleanup. `__aenter__` starts the
scheduler and `__aexit__` closes it.

## Deployment limits

Configuration and run state are in memory in one process. A restart loses the schedule; each
worker process starts an independent copy and can run every job. There is no persistence, leader
election, distributed lock, retry/backoff, timezone or cron rule, misfire policy, durable delivery,
or exactly-once guarantee. Use a durable task platform when execution must survive restarts or be
coordinated across replicas.

## Source layout and status

- `src/orbit_scheduler/scheduler.py`: lifecycle, bounded interval execution, reporting, and cleanup.
- `src/orbit_scheduler/__init__.py`: supported public imports.

The package declares Python `>=3.11,<3.15`, no runtime dependencies, and alpha compatibility.
