# Orbit Scheduler

`orbit-scheduler` is an optional, provider-neutral in-process scheduler for bounded asynchronous
interval jobs. It is separate from Orbit Core, whose lifecycle and request timeouts continue to
govern the application. The package supports Python 3.11 through 3.14 and is alpha.

```bash
python -m pip install orbit-scheduler
```

## Fixed-delay interval jobs

Jobs are registered before startup. Each job runs alone, then waits its full interval after the
callback finishes. A slow or failed run therefore does not create an overlapping catch-up queue.
The scheduler also caps total registrations and simultaneous callbacks.

```python
from orbit_scheduler import IntervalScheduler, JobFailure


async def refresh_catalog() -> None: ...


def report_failure(failure: JobFailure) -> None:
    # The record contains only the configured job name, exception type, and timestamp.
    print(f"{failure.name} failed with {failure.error_type}")


scheduler = IntervalScheduler(max_jobs=32, max_concurrency=4, on_error=report_failure)
scheduler.add_interval("catalog-refresh", 60, refresh_catalog, run_immediately=True)


async def application_lifespan() -> None:
    await scheduler.start()
    try:
        await serve_application()
    finally:
        await scheduler.aclose(grace_period=10)
```

The callback must be an async callable. `on_error` may be synchronous or async; it receives safe
failure metadata, not the original exception or its message. Without a handler, failures are logged
with the job name and exception type only, and scheduling continues. Cancellation of a callback is
propagated as task cancellation. Shutdown stops future runs, waits up to the configured grace period
for active callbacks, then cancels remaining tasks. Callbacks should cooperate with cancellation;
callbacks that suppress it may outlive bounded shutdown and must not retain critical application
resources.

## Delivery and deployment limits

Schedules and run state exist only in this process. A restart loses schedules; multiple workers each
run their own copy. The package does not provide cron/time-zone rules, persistence, leader election,
distributed locks, retries, durable delivery, or execution guarantees. Do not use it for work that
must survive process failure or must run exactly once across workers. Those needs require a separate
durable scheduler/worker backend and an adapter that defines its delivery and acknowledgement
semantics.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Licensed under Apache-2.0.

