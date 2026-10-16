# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bounded, process-local asynchronous interval scheduling."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum

_LOG = logging.getLogger("orbit.scheduler")
_JOB_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_MAX_INTERVAL = 2_592_000.0
_MAX_JOBS = 10_000
_MAX_CONCURRENCY = 1_024
_MAX_SHUTDOWN_GRACE = 300.0
_CANCEL_SETTLE_GRACE = 0.1
_SAFE_ERROR_TYPE_CHARACTER = re.compile(r"[A-Za-z0-9_.-]")

JobCallback = Callable[[], Awaitable[None]]


class SchedulerError(RuntimeError):
    """Raised when a scheduler operation is invalid for its current state."""


class SchedulerState(StrEnum):
    """Observable lifecycle state for an interval scheduler."""

    NEW = "new"
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class JobFailure:
    """Safe metadata emitted after one callback failure.

    ``occurred_at`` is a Unix timestamp in seconds from the system wall clock. The original
    exception, message, traceback, and callback arguments are deliberately not retained, so this
    record is safe to pass to application monitoring without exposing exception payloads.
    """

    name: str
    error_type: str
    occurred_at: float


JobErrorHandler = Callable[[JobFailure], None | Awaitable[None]]


def _safe_error_type(error: BaseException) -> str:
    """Return a short ASCII exception class label safe for logs and monitoring records."""
    name = type(error).__name__
    safe_name = "".join(
        character if _SAFE_ERROR_TYPE_CHARACTER.fullmatch(character) else "_" for character in name
    )[:128]
    return safe_name or "Exception"


@dataclass(frozen=True, slots=True)
class IntervalJob:
    """Immutable process-local job configuration registered before scheduler startup.

    The interval is fixed delay after a run completes, not a wall-clock cadence. A job cannot
    overlap with itself, and skipped time is not replayed as catch-up executions.
    """

    name: str
    interval: float
    callback: JobCallback = field(repr=False)
    run_immediately: bool


class IntervalScheduler:
    """Run fixed-delay async jobs with bounded task count and concurrency.

    Jobs never overlap with themselves. Each next interval starts after the previous callback has
    completed, so slow jobs do not accumulate catch-up runs. Callback errors are reported to an
    optional metadata-only handler and do not stop that job. ``aclose`` stops scheduling, allows
    in-flight callbacks the configured grace period, then cancels remaining tasks. A coroutine
    that suppresses cancellation may remain detached; shutdown stays bounded and its task remains
    observed until it finishes.

    This scheduler is process-local and memory-only. It does not persist schedules or executions,
    coordinate worker processes, guarantee execution across restarts, or provide cron semantics.
    """

    def __init__(
        self,
        *,
        max_jobs: int = 256,
        max_concurrency: int = 16,
        on_error: JobErrorHandler | None = None,
    ) -> None:
        """Set hard bounds before any task is created."""
        if (
            isinstance(max_jobs, bool)
            or not isinstance(max_jobs, int)
            or not 1 <= max_jobs <= _MAX_JOBS
        ):
            raise ValueError(f"max_jobs must be an integer between 1 and {_MAX_JOBS}.")
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(max_concurrency, int)
            or not 1 <= max_concurrency <= _MAX_CONCURRENCY
        ):
            raise ValueError(
                f"max_concurrency must be an integer between 1 and {_MAX_CONCURRENCY}."
            )
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable.")
        self._max_jobs = max_jobs
        self._max_concurrency = max_concurrency
        self._on_error = on_error
        self._jobs: dict[str, IntervalJob] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._detached_tasks: set[asyncio.Task[None]] = set()
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._state = SchedulerState.NEW
        self._close_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> SchedulerState:
        """Return the scheduler lifecycle state."""
        return self._state

    @property
    def jobs(self) -> tuple[IntervalJob, ...]:
        """Return an immutable snapshot of configured jobs."""
        return tuple(self._jobs.values())

    def __repr__(self) -> str:
        """Describe bounds and lifecycle without rendering callbacks or captured values."""
        return (
            f"IntervalScheduler(state={self._state.value!r}, jobs={len(self._jobs)}, "
            f"max_jobs={self._max_jobs}, max_concurrency={self._max_concurrency})"
        )

    def add_interval(
        self,
        name: str,
        interval: float,
        callback: JobCallback,
        *,
        run_immediately: bool = False,
    ) -> None:
        """Register a named fixed-delay job before startup.

        The callback must be an async callable. Intervals are bounded to prevent an accidental
        zero-delay busy loop or an indefinitely retained schedule. Names are unique and limited to
        128 safe ASCII characters. Registrations are in-memory and are not persisted across
        scheduler instances or process restarts.
        """
        if self._state is not SchedulerState.NEW:
            raise SchedulerError("Jobs can only be added before the scheduler starts.")
        if not isinstance(name, str) or _JOB_NAME.fullmatch(name) is None:
            raise ValueError("job name must be 1 to 128 safe ASCII characters.")
        if name in self._jobs:
            raise ValueError("job name is already registered.")
        if (
            isinstance(interval, bool)
            or not isinstance(interval, (int, float))
            or not math.isfinite(interval)
            or not 0.01 <= interval <= _MAX_INTERVAL
        ):
            raise ValueError(f"interval must be between 0.01 and {_MAX_INTERVAL} seconds.")
        if not callable(callback) or not (
            inspect.iscoroutinefunction(callback)
            or inspect.iscoroutinefunction(type(callback).__call__)
        ):
            raise TypeError("callback must be an async callable.")
        if not isinstance(run_immediately, bool):
            raise TypeError("run_immediately must be a bool.")
        if len(self._jobs) >= self._max_jobs:
            raise ValueError("scheduler job limit has been reached.")
        self._jobs[name] = IntervalJob(name, float(interval), callback, run_immediately)

    async def start(self) -> None:
        """Start one background task per configured job.

        Calling ``start`` again while running is harmless. Starting after shutdown begins is an
        error. An empty scheduler is valid and starts no tasks.
        """
        async with self._lock:
            if self._state is SchedulerState.RUNNING:
                return
            if self._state is not SchedulerState.NEW:
                raise SchedulerError("A closing or closed scheduler cannot be started.")
            self._state = SchedulerState.RUNNING
            self._tasks = {
                asyncio.create_task(self._run(job), name=f"orbit-schedule-{job.name}")
                for job in self._jobs.values()
            }

    async def aclose(self, *, grace_period: float = 5.0) -> None:
        """Stop future runs and bound the wait for in-flight callbacks.

        Concurrent calls share one cleanup task; the grace period from the first caller controls
        that cleanup. Cancellation of a caller does not cancel cleanup; the caller still receives
        ``CancelledError`` while shutdown continues in the background. After the grace period,
        callbacks are cancelled and given a short settling interval. A callback that suppresses
        cancellation can outlive the scheduler's ``CLOSED`` state; its eventual failure remains
        observed to avoid an unhandled-task warning.
        """
        if (
            isinstance(grace_period, bool)
            or not isinstance(grace_period, (int, float))
            or not math.isfinite(grace_period)
            or not 0 <= grace_period <= _MAX_SHUTDOWN_GRACE
        ):
            raise ValueError(f"grace_period must be between 0 and {_MAX_SHUTDOWN_GRACE} seconds.")
        async with self._lock:
            if self._state is SchedulerState.CLOSED:
                return
            if self._close_task is None:
                self._state = SchedulerState.CLOSING
                self._stop.set()
                self._close_task = asyncio.create_task(
                    self._finish_close(float(grace_period)), name="orbit-scheduler-shutdown"
                )
            close_task = self._close_task
        await asyncio.shield(close_task)

    async def __aenter__(self) -> IntervalScheduler:
        """Start the scheduler when entering an async context."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the scheduler when leaving an async context."""
        await self.aclose()

    async def _run(self, job: IntervalJob) -> None:
        """Run one job serially, waiting a full interval after every completion."""
        if not job.run_immediately and await self._wait(job.interval):
            return
        while not self._stop.is_set():
            if self._stop.is_set():
                return
            try:
                async with self._semaphore:
                    if self._stop.is_set():
                        return
                    await job.callback()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failure = JobFailure(
                    name=job.name,
                    error_type=_safe_error_type(exc),
                    occurred_at=time.time(),
                )
                await self._report_failure(failure)
            stopped = await self._wait(job.interval)
            if stopped:
                return

    async def _report_failure(self, failure: JobFailure) -> None:
        """Report only safe metadata and isolate handler failures from scheduling."""
        if self._on_error is None:
            _LOG.error(
                "Scheduled job failed: name=%s error_type=%s", failure.name, failure.error_type
            )
            return
        try:
            result = self._on_error(failure)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOG.error(
                "Scheduled job error handler failed: name=%s error_type=%s",
                failure.name,
                _safe_error_type(exc),
            )

    async def _wait(self, delay: float) -> bool:
        """Wait for either the next fixed-delay interval or shutdown."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
            return True
        except TimeoutError:
            return False

    async def _finish_close(self, grace_period: float) -> None:
        """Wait for running work, then cancel and detach cancellation-resistant callbacks."""
        tasks = tuple(self._tasks)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace_period)
            for task in pending:
                task.cancel()
            if pending:
                _, pending = await asyncio.wait(pending, timeout=_CANCEL_SETTLE_GRACE)
                for task in pending:
                    self._detached_tasks.add(task)
                    task.add_done_callback(self._observe_detached_task)
        for task in tasks:
            if task.done() and not task.cancelled():
                with suppress(asyncio.CancelledError):
                    task.exception()
        async with self._lock:
            self._tasks.clear()
            self._state = SchedulerState.CLOSED

    def _observe_detached_task(self, task: asyncio.Task[None]) -> None:
        """Retrieve eventual task errors from callbacks that suppressed cancellation."""
        self._detached_tasks.discard(task)
        if not task.cancelled():
            with suppress(asyncio.CancelledError):
                task.exception()


__all__ = [
    "IntervalJob",
    "IntervalScheduler",
    "JobCallback",
    "JobErrorHandler",
    "JobFailure",
    "SchedulerError",
    "SchedulerState",
]
