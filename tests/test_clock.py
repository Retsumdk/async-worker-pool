"""Tests for the injectable clocks and the deterministic drive() helper."""
import asyncio

import pytest

from async_worker_pool.clock import ManualClock, SystemClock, drive


def test_manual_clock_starts_at_start() -> None:
    clock = ManualClock(start=1.5)
    assert clock.now() == 1.5
    assert not clock.has_waiters()


def test_sleep_registers_waiter_and_advance_fires_it() -> None:
    async def scenario() -> float:
        clock = ManualClock()
        woke_at: list[float] = []

        async def sleeper() -> None:
            await clock.sleep(0.5)
            woke_at.append(clock.now())

        task = asyncio.ensure_future(sleeper())
        await asyncio.sleep(0)  # let the sleeper start and register
        assert clock.has_waiters()
        await clock.advance(0.5)
        await task
        return woke_at[0]

    assert asyncio.run(scenario()) == 0.5


def test_advance_fires_waiters_in_wake_order() -> None:
    async def scenario() -> list[float]:
        clock = ManualClock()
        order: list[float] = []

        async def sleeper(delay: float, tag: int) -> None:
            await clock.sleep(delay)
            order.append(tag)

        tasks = [
            asyncio.ensure_future(sleeper(0.3, 1)),
            asyncio.ensure_future(sleeper(0.1, 2)),
            asyncio.ensure_future(sleeper(0.3, 3)),
        ]
        await asyncio.sleep(0)  # let the sleepers start and register
        await clock.advance(1.0)
        await asyncio.gather(*tasks)
        return order

    assert asyncio.run(scenario()) == [2, 1, 3]


def test_advance_rejects_backwards() -> None:
    async def scenario() -> None:
        clock = ManualClock(start=1.0)
        with pytest.raises(ValueError, match="cannot advance backwards"):
            await clock.advance(0.5)

    asyncio.run(scenario())


def test_pending_wakes_deduplicates() -> None:
    async def scenario() -> list[float]:
        clock = ManualClock()

        async def sleeper(delay: float) -> None:
            await clock.sleep(delay)

        for _ in range(3):
            asyncio.ensure_future(sleeper(0.25))
        await asyncio.sleep(0)  # let the sleepers start and register
        wakes = clock.pending_wakes()
        await clock.advance(1.0)
        return wakes

    assert asyncio.run(scenario()) == [0.25]


def test_zero_sleep_returns_immediately_without_waiter() -> None:
    async def scenario() -> bool:
        clock = ManualClock()
        await clock.sleep(0)
        await clock.sleep(-1)
        return clock.has_waiters()

    assert not asyncio.run(scenario())


def test_next_wake_raises_without_waiters() -> None:
    with pytest.raises(RuntimeError, match="no pending sleepers"):
        ManualClock().next_wake()


def test_system_clock_now_is_monotonic() -> None:
    clock = SystemClock()
    first = clock.now()
    second = clock.now()
    assert second >= first


def test_drive_runs_coroutine_under_virtual_time() -> None:
    async def scenario() -> float:
        clock = ManualClock()

        async def work() -> float:
            await clock.sleep(2.5)
            return clock.now()

        result = await drive(clock, work())
        return float(result)

    assert asyncio.run(scenario()) == 2.5


def test_drive_bails_out_on_deadlock() -> None:
    async def scenario() -> None:
        clock = ManualClock()

        async def never_sleeps_never_finishes() -> None:
            await asyncio.Event().wait()  # nothing the driver can advance

        with pytest.raises(RuntimeError, match="step budget"):
            await drive(clock, never_sleeps_never_finishes())

    asyncio.run(scenario())