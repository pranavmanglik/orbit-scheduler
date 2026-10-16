# Orbit Scheduler: development

The scheduler is a small in-process async component. Keep its task count and shutdown wait
bounded, and ensure errors and cancellation cannot silently break lifecycle ownership.

## Local setup and checks

From the repository root:

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Tests use local coroutines and do not require a broker, database, or external scheduler. They cover
registration bounds, fixed-delay non-overlap, global concurrency, safe failure records, and
bounded/cancelled shutdown. They cannot validate arbitrary application callback cleanup or
multi-process coordination.

## Implementation guidance

- Validate names, intervals, capacities, callbacks, and lifecycle state before starting tasks.
- Preserve one task per bounded registration and do not add a catch-up queue implicitly.
- Keep callback failure metadata free of exception messages, traceback, callback repr, and other
  potentially sensitive values. Sanitize any labels sent to logs or monitoring.
- Keep the first shutdown caller's grace period semantics explicit if shutdown remains shared.
- Observe tasks that outlive close because they suppressed cancellation; do not leak unhandled task
  exceptions.
- Test races among startup, close, callback failure, semaphore admission, and cancellation using
  events instead of fragile timing assumptions.

Follow the repository contribution guide, governance, code of conduct, and security policy. Update
API docstrings and operations guidance whenever lifecycle or timing behavior changes. Report local
checks separately from hosted CI and application deployment validation.
