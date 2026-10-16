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
"""Scheduler lifecycle, bounds, isolation, and shutdown behavior."""

from __future__ import annotations

import asyncio

import pytest

from orbit_scheduler import IntervalScheduler, JobFailure, SchedulerError, SchedulerState


@pytest.mark.parametrize("max_jobs", [0, -1, 10_001, True, 1.5])
def test_job_capacity_must_be_a_bounded_integer(max_jobs: object) -> None:
    with pytest.raises(ValueError, match="max_jobs"):
        IntervalScheduler(max_jobs=max_jobs)  # type: ignore[arg-type]


@pytest.mark.parametrize("interval", [0, -1, 0.009, float("inf"), float("nan"), True])
async def test_interval_must_be_finite_and_positive(interval: object) -> None:
    async def callback() -> None:
        return None

    scheduler = IntervalScheduler()
    with pytest.raises(ValueError, match="interval"):
        scheduler.add_interval("valid", interval, callback)  # type: ignore[arg-type]


async def test_jobs_are_immutable_after_start_and_close_is_idempotent() -> None:
    async def callback() -> None:
        return None

    scheduler = IntervalScheduler()
    scheduler.add_interval("job", 1, callback)
    await scheduler.start()
    assert scheduler.state is SchedulerState.RUNNING
    await scheduler.start()
    with pytest.raises(SchedulerError, match="before"):
        scheduler.add_interval("late", 1, callback)
    await scheduler.aclose()
    await scheduler.aclose()
    assert scheduler.state is SchedulerState.CLOSED


def test_repr_does_not_render_callable_configuration() -> None:
    class SensitiveCallback:
        def __init__(self) -> None:
            self.api_key = "private-api-key"

        async def __call__(self) -> None:
            return None

        def __repr__(self) -> str:
            return f"SensitiveCallback(api_key={self.api_key!r})"

    scheduler = IntervalScheduler()
    scheduler.add_interval("safe-job", 10, SensitiveCallback())

    assert "private-api-key" not in repr(scheduler)
    assert "private-api-key" not in repr(scheduler.jobs)


async def test_graceful_shutdown_waits_for_current_callback() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def callback() -> None:
        started.set()
        await release.wait()
        finished.set()

    scheduler = IntervalScheduler()
    scheduler.add_interval("graceful", 1, callback, run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(started.wait(), timeout=1)
    shutdown = asyncio.create_task(scheduler.aclose(grace_period=1))
    await asyncio.sleep(0)
    assert scheduler.state is SchedulerState.CLOSING
    assert not shutdown.done()
    release.set()
    await shutdown

    assert finished.is_set()
    assert scheduler.state is SchedulerState.CLOSED


async def test_callbacks_do_not_overlap_and_follow_fixed_delay() -> None:
    active = 0
    peak = 0
    calls = 0
    reached = asyncio.Event()

    async def callback() -> None:
        nonlocal active, peak, calls
        active += 1
        peak = max(peak, active)
        calls += 1
        await asyncio.sleep(0.015)
        active -= 1
        if calls >= 3:
            reached.set()

    scheduler = IntervalScheduler()
    scheduler.add_interval("serial", 0.01, callback, run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(reached.wait(), timeout=1)
    await scheduler.aclose()
    assert calls == 3
    assert peak == 1


async def test_global_concurrency_is_bounded_and_shutdown_cancels_after_grace() -> None:
    active = 0
    peak = 0
    started = asyncio.Queue[str]()

    def make_callback(name: str):
        async def callback() -> None:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await started.put(name)
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1

        return callback

    scheduler = IntervalScheduler(max_concurrency=1)
    scheduler.add_interval("one", 1, make_callback("one"), run_immediately=True)
    scheduler.add_interval("two", 1, make_callback("two"), run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(started.get(), timeout=1)
    await scheduler.aclose(grace_period=0)
    assert peak == 1
    assert scheduler.state is SchedulerState.CLOSED


async def test_callback_failures_are_redacted_and_do_not_stop_the_job() -> None:
    reports: list[JobFailure] = []
    ran = 0
    recovered = asyncio.Event()

    async def callback() -> None:
        nonlocal ran
        ran += 1
        if ran == 1:
            raise ValueError("private-token-value")
        recovered.set()

    async def report(failure: JobFailure) -> None:
        reports.append(failure)

    scheduler = IntervalScheduler(on_error=report)
    scheduler.add_interval("safe-name", 0.01, callback, run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(recovered.wait(), timeout=1)
    await scheduler.aclose()
    assert len(reports) == 1
    assert reports[0].name == "safe-name"
    assert reports[0].error_type == "ValueError"
    assert "private-token-value" not in repr(reports[0])


async def test_failure_type_is_sanitized_before_reporting() -> None:
    """An unusual dynamic exception class name cannot inject control characters in logs."""
    reports: list[JobFailure] = []
    error_type = type("Private\nException", (Exception,), {})

    async def callback() -> None:
        raise error_type("sensitive message")

    scheduler = IntervalScheduler(on_error=reports.append)
    scheduler.add_interval("safe-name", 0.01, callback, run_immediately=True)
    await scheduler.start()
    for _ in range(100):
        if reports:
            break
        await asyncio.sleep(0.001)
    await scheduler.aclose()

    assert len(reports) == 1
    assert reports[0].error_type == "Private_Exception"
    assert "\n" not in reports[0].error_type


async def test_cancelled_shutdown_waiter_does_not_cancel_cleanup() -> None:
    started = asyncio.Event()

    async def callback() -> None:
        started.set()
        await asyncio.Event().wait()

    scheduler = IntervalScheduler()
    scheduler.add_interval("long", 1, callback, run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(started.wait(), timeout=1)
    shutdown = asyncio.create_task(scheduler.aclose(grace_period=0))
    await asyncio.sleep(0)
    shutdown.cancel()
    with pytest.raises(asyncio.CancelledError):
        await shutdown
    for _ in range(20):
        if scheduler.state is SchedulerState.CLOSED:
            break
        await asyncio.sleep(0)
    assert scheduler.state is SchedulerState.CLOSED


async def test_cancellation_resistant_callback_cannot_extend_shutdown_forever() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def callback() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    scheduler = IntervalScheduler()
    scheduler.add_interval("stubborn", 1, callback, run_immediately=True)
    await scheduler.start()
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(scheduler.aclose(grace_period=0), timeout=1)
    assert scheduler.state is SchedulerState.CLOSED
    release.set()
    await asyncio.sleep(0)
