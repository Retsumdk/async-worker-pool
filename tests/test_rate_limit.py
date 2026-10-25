"""Tests for the lazy-refill TokenBucket."""
import asyncio

import pytest

from async_worker_pool.clock import ManualClock, drive
from async_worker_pool.errors import ConfigError
from async_worker_pool.rate_limit import TokenBucket


def _pump(clock: ManualClock):
    async def pump() -> None:
        while clock.has_waiters():
            await clock.advance_to_next()

    return pump()


def test_invalid_construction_raises() -> None:
    with pytest.raises(ConfigError):
        TokenBucket(rate=0, burst=1, clock=ManualClock())
    with pytest.raises(ConfigError):
        TokenBucket(rate=1, burst=0, clock=ManualClock())
    with pytest.raises(ConfigError):
        TokenBucket(rate=1, burst=1, clock=ManualClock(), initial_tokens=-0.5)


def test_acquire_rejects_non_positive_token_count() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        bucket = TokenBucket(rate=1.0, burst=2.0, clock=clock)
        with pytest.raises(ConfigError):
            await bucket.acquire(0)

    asyncio.run(scenario())


def test_burst_tokens_are_instantly_available() -> None:
    async def scenario() -> tuple[float, float]:
        clock = ManualClock()
        bucket = TokenBucket(rate=5.0, burst=5.0, clock=clock)
        first = await bucket.acquire()
        second = await bucket.acquire()
        return first, second

    first, second = asyncio.run(scenario())
    assert first == 0.0  # no wait: burst tokens
    assert second == 0.0


def test_sustained_rate_limits_throughput() -> None:
    async def scenario() -> tuple[float, float]:
        clock = ManualClock()
        pump = asyncio.ensure_future(_pump(clock))
        bucket = TokenBucket(rate=5.0, burst=5.0, clock=clock)
        waits = []
        for _ in range(10):  # 5 free from the burst, then 1 token per 0.2s
            waits.append(await bucket.acquire())
        pump.cancel()
        return waits[5], clock.now()

    fifth_beyond_burst, end = asyncio.run(scenario())
    assert fifth_beyond_burst == pytest.approx(0.2)
    assert end == pytest.approx(1.0)  # 5 extra tokens at 5/s = 1.0 virtual s


def test_multi_token_requests_wait_for_refill() -> None:
    async def scenario() -> tuple[float, float]:
        clock = ManualClock()
        pump = asyncio.ensure_future(_pump(clock))
        bucket = TokenBucket(rate=4.0, burst=4.0, clock=clock)
        for _ in range(4):
            await bucket.acquire()
        wait = await bucket.acquire(4)  # needs a full 1.0s of refill
        pump.cancel()
        return wait, clock.now()

    wait, end = asyncio.run(scenario())
    assert wait == pytest.approx(1.0)
    assert end == pytest.approx(1.0)


def test_snapshot_reports_accounting() -> None:
    async def scenario() -> dict:
        clock = ManualClock()
        pump = asyncio.ensure_future(_pump(clock))
        bucket = TokenBucket(rate=10.0, burst=2.0, clock=clock)
        await bucket.acquire()
        await bucket.acquire()
        await bucket.acquire()  # first wait of 0.1s
        pump.cancel()
        return bucket.snapshot()

    snap = asyncio.run(scenario())
    assert snap["waits"] == 1
    assert snap["wait_total_seconds"] == pytest.approx(0.1)
    assert snap["requests"] == 3
    assert snap["tokens_available"] == pytest.approx(0.0)


def test_tokens_refill_lazily_between_calls() -> None:
    async def scenario() -> float:
        clock = ManualClock()
        bucket = TokenBucket(rate=2.0, burst=1.0, clock=clock)
        await bucket.acquire()  # empty the bucket
        pump = asyncio.ensure_future(_pump(clock))
        await clock.advance(0.5)  # idle half a second → 1 token accrues
        pump.cancel()
        wait = await bucket.acquire()
        return wait

    assert asyncio.run(scenario()) == 0.0