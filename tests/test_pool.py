"""End-to-end tests for AsyncWorkerPool (all under a ManualClock)."""
import asyncio
from typing import Any, Dict

import pytest

from async_worker_pool.clock import ManualClock, drive
from async_worker_pool.errors import (
    ConfigError,
    PermanentTaskError,
    PoolStoppedError,
    QueueFullError,
    TaskFailedError,
    TaskTimeoutError,
)
from async_worker_pool.pool import AsyncWorkerPool, PoolConfig


def _run_pool(
    config: PoolConfig,
    jobs: list[Dict[str, Any]],
) -> tuple[AsyncWorkerPool, ManualClock]:
    """Build a pool, submit ``jobs`` (dicts with id/fn), drive it to quiescence."""
    clock = ManualClock()
    pool = AsyncWorkerPool(config, clock=clock)
    handles = {}
    for spec in jobs:
        handles[spec["id"]] = pool.submit(
            spec["fn"], name=spec.get("name", "job"), job_id=spec["id"]
        )
    asyncio.run(drive(clock, pool.drain()))
    return pool, clock


def test_config_validation_rejects_bad_fields() -> None:
    with pytest.raises(ConfigError, match="workers"):
        PoolConfig(workers=0).validate()
    with pytest.raises(ConfigError, match="rate"):
        PoolConfig(rate=-1).validate()
    with pytest.raises(ConfigError, match="timeout"):
        PoolConfig(timeout=0).validate()
    with pytest.raises(ConfigError, match="backoff_max"):
        PoolConfig(backoff_base=1.0, backoff_max=0.5).validate()


def test_successful_jobs_complete_in_submission_order_bookkeeping() -> None:
    async def make(work: float, clock: ManualClock):
        async def job() -> str:
            await clock.sleep(work)
            return f"ok:{work}"

        return job

    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=2, rate=100, burst=100), clock=clock)
        for i, work in enumerate([0.1, 0.2, 0.3]):
            pool.submit(await make(work, clock), name="work", job_id=f"j{i}")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    stats = pool.stats()
    assert stats["submitted"] == 3
    assert stats["completed"] == 3
    assert stats["dead_lettered"] == 0
    assert pool.result_of("j2").result == "ok:0.3"
    assert pool.result_of("j0").attempts == 1


def test_transient_failure_is_retried_with_backoff_then_completes() -> None:
    async def scenario() -> tuple[AsyncWorkerPool, list[float]]:
        clock = ManualClock()
        pool = AsyncWorkerPool(
            PoolConfig(workers=1, rate=100, burst=100, backoff_base=0.1), clock=clock
        )
        sleeps: list[float] = []
        original_sleep = clock.sleep

        async def tracking_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            await original_sleep(seconds)

        clock.sleep = tracking_sleep  # type: ignore[method-assign]

        calls = {"n": 0}

        async def flaky() -> str:
            calls["n"] += 1
            if calls["n"] == 1:
                raise TaskFailedError("transient: db hiccup")
            return "recovered"

        pool.submit(flaky, name="flaky", job_id="f1")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool, sleeps

    pool, sleeps = asyncio.run(scenario())
    record = pool.result_of("f1")
    assert record.outcome == "completed"
    assert record.attempts == 2
    assert record.history[0].outcome == "transient_error"
    assert record.history[1].outcome == "ok"
    assert sleeps == [0.1]  # one backoff sleep of backoff_base
    assert pool.stats()["retried"] == 1


def test_max_attempts_exhaustion_dead_letters() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(
            PoolConfig(workers=1, rate=100, burst=100, max_attempts=3, backoff_base=0.0),
            clock=clock,
        )

        async def always_fails() -> None:
            raise RuntimeError("boom")

        handle = pool.submit(always_fails, name="bad", job_id="b1")
        with pytest.raises(TaskFailedError):
            await drive(clock, handle)
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    record = pool.result_of("b1")
    assert record.outcome == "dead_lettered"
    assert record.attempts == 3
    assert len(record.history) == 3
    assert record.error_code == "TASK_FAILED"
    assert len(pool.dead_letters) == 1
    assert pool.stats()["dead_lettered"] == 1


def test_permanent_error_dead_letters_on_first_attempt() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(
            PoolConfig(workers=1, rate=100, burst=100, max_attempts=5), clock=clock
        )

        async def hopeless() -> None:
            raise PermanentTaskError("bad input", detail={"job_id": "p1"})

        handle = pool.submit(hopeless, name="hopeless", job_id="p1")
        with pytest.raises(TaskFailedError):
            await drive(clock, handle)
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    record = pool.result_of("p1")
    assert record.attempts == 1
    assert record.error_code == "TASK_FAILED_PERMANENT"


def test_attempt_timeout_cancels_and_retries() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(
            PoolConfig(
                workers=1, rate=100, burst=100, timeout=0.5, backoff_base=0.0
            ),
            clock=clock,
        )
        calls = {"n": 0}

        async def slow_then_fast() -> str:
            calls["n"] += 1
            await clock.sleep(0.9 if calls["n"] == 1 else 0.1)
            return "done"

        pool.submit(slow_then_fast, name="slowish", job_id="s1")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    record = pool.result_of("s1")
    assert record.attempts == 2
    assert record.history[0].outcome == "timeout"
    assert record.history[1].outcome == "ok"
    assert pool.stats()["timeouts"] == 1


def test_rate_limiting_spreads_execution() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(
            PoolConfig(workers=4, rate=5.0, burst=5.0, backoff_base=0.0), clock=clock
        )

        async def work() -> str:
            return "w"

        for i in range(10):
            pool.submit(work, name="work", job_id=f"w{i}")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    stats = pool.stats()
    assert stats["completed"] == 10
    # 5 jobs run free from the burst; the next 5 need one 0.2s refill each.
    assert stats["clock_now"] == pytest.approx(1.0)
    assert stats["rate_limiter"]["waits"] >= 1


def test_queue_full_is_rejected_without_losing_the_pool() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1, queue_size=2), clock=clock)

        async def slow() -> None:
            await clock.sleep(10)

        pool.submit(slow, name="slow", job_id="q1")
        # Let the worker pull q1 out of the queue deterministically.
        for _ in range(4):
            await asyncio.sleep(0)
        pool.submit(slow, name="slow", job_id="q2")
        pool.submit(slow, name="slow", job_id="q3")
        with pytest.raises(QueueFullError, match="work queue is full"):
            pool.submit(slow, name="slow", job_id="q4")
        await pool.stop()

    asyncio.run(scenario())


def test_duplicate_job_id_is_rejected() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1), clock=clock)

        async def work() -> None:
            return None

        pool.submit(work, job_id="dup")
        with pytest.raises(ConfigError, match="duplicate job id"):
            pool.submit(work, job_id="dup")
        await pool.stop()

    asyncio.run(scenario())


def test_submit_after_stop_raises() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1), clock=clock)

        async def work() -> None:
            return None

        pool.submit(work, job_id="a")
        await drive(clock, pool.drain())
        await pool.stop()
        with pytest.raises(PoolStoppedError, match="after the pool was stopped"):
            pool.submit(work, job_id="b")

    asyncio.run(scenario())


def test_non_callable_submit_is_rejected() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1), clock=clock)
        with pytest.raises(ConfigError, match="callable"):
            pool.submit("not-callable")  # type: ignore[arg-type]
        await pool.stop()

    asyncio.run(scenario())


def test_result_of_unknown_job_raises() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1), clock=clock)

        async def work() -> None:
            return None

        pool.submit(work, job_id="known")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    with pytest.raises(Exception) as excinfo:  # UnknownJobError, typed loosely for 3.9
        pool.result_of("nope")
    assert getattr(excinfo.value, "code", "") == "POOL_JOB_UNKNOWN"


def test_context_manager_drains_and_stops() -> None:
    async def scenario() -> dict:
        clock = ManualClock()
        async with AsyncWorkerPool(PoolConfig(workers=2), clock=clock) as pool:

            async def work() -> str:
                await clock.sleep(0.05)
                return "done"

            pool.submit(work, job_id="ctx1")
            await drive(clock, pool.drain())
        return pool.stats()

    stats = asyncio.run(scenario())
    assert stats["completed"] == 1


def test_worker_survives_defensive_crash_path() -> None:
    async def scenario() -> AsyncWorkerPool:
        clock = ManualClock()
        pool = AsyncWorkerPool(PoolConfig(workers=1, rate=100, burst=100), clock=clock)

        async def normal() -> str:
            return "fine"

        pool.submit(normal, job_id="after-crash")
        await drive(clock, pool.drain())

        # Force the defensive path: break the limiter so _process raises.
        async def broken_acquire() -> float:
            raise ZeroDivisionError("simulated pool bug")

        pool.limiter.acquire = broken_acquire  # type: ignore[method-assign]
        pool.submit(normal, job_id="crasher")
        await drive(clock, pool.drain())
        pool.limiter.acquire = AsyncWorkerPool(
            PoolConfig(rate=1, burst=1), clock=clock
        ).limiter.acquire
        # A worker must still be alive: another job completes afterwards.
        async def late() -> str:
            return "still-alive"

        pool.submit(late, job_id="late")
        await drive(clock, pool.drain())
        await pool.stop()
        return pool

    pool = asyncio.run(scenario())
    assert pool.result_of("after-crash").outcome == "completed"
    crash = pool.result_of("crasher")
    assert crash.outcome == "dead_lettered"
    assert crash.error_code == "POOL_INTERNAL"
    assert pool.result_of("late").result == "still-alive"