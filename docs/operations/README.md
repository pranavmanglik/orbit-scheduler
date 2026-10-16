# Orbit Scheduler: operations

`orbit-scheduler` reads no environment variables and creates no external service connections. The
application owns the scheduler's construction, startup, and shutdown. Register it once per process
through the application's managed-resource lifecycle, and decide explicitly whether running a
copy in every worker is safe.

## Capacity and job design

Choose `max_jobs` and `max_concurrency` from the amount of scheduled work and downstream capacity.
Each registered job owns a task, while at most `max_concurrency` callbacks run concurrently. Jobs
that are waiting on the shared semaphore still have tasks, so callbacks should not create large
unbounded queues of their own.

Intervals use fixed-delay semantics. The next run begins one full interval after the callback
finishes. A long or slow callback shifts future runs and does not create catch-up executions. Use
this package for periodic best-effort in-process work, not precise wall-clock schedules.

## Lifecycle wiring

Register callbacks before `start()`. Call `aclose()` from application shutdown and choose a grace
period that fits the process termination budget. The first concurrent closer determines the shared
grace period. During shutdown, callbacks may be cancelled after grace expires; make them
cancellation-cooperative and close database cursors, network streams, and temporary resources in
`finally` blocks.

If a callback suppresses cancellation, `aclose()` still returns after its bounded wait and the
scheduler enters `CLOSED`, while the callback task may continue. Do not let such callbacks own
resources that the rest of application shutdown assumes are already released. Monitor these cases
and prefer a dedicated worker system for long-running or cancellation-resistant work.

## Failure reporting and observability

Use `on_error` to send safe `JobFailure` data to application monitoring. It contains the job name,
sanitized exception class name, and occurrence timestamp, not the exception message or traceback.
The handler itself should be fast and bounded; an async handler runs inside the job task and delays
the next interval. A failed handler does not stop the schedule but is logged.

Add application metrics for callback duration, last successful run, consecutive failures, active
callbacks, and shutdown cancellation. This package does not expose a metrics backend or mark jobs
healthy automatically.

## Delivery limits

Schedules are lost on restart and repeated independently in every worker. If duplicate execution is
unsafe, guard the callback with an application-level idempotency mechanism or use a distributed job
system. This package supplies no retry, persistence, locking, leader election, or delivery
acknowledgment.

Before release, exercise process shutdown during success, failure, semaphore wait, and active I/O;
also verify multi-worker deployment behavior and callbacks' cancellation cleanup. Unit tests cannot
prove a particular application's callbacks are safe to terminate.
