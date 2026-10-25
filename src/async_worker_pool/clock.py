"""Injectable clocks for async-worker-pool.

All time in the pool flows through a :class:`Clock` port — never
``time.monotonic`` or ``asyncio.sleep`` directly. Two adapters ship:

- :class:`SystemClock` — real time, for production use.
- :class:`ManualClock` — a virtual timeline whose sleeps fire only when the
  test/demo driver advances it. This makes worker scheduling, rate-limit
  waits, retry backoff and timeouts fully deterministic: the same scenario
  produces byte-identical output on every run, in milliseconds of wall time.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from typing import List, Optional, Protocol, Tuple


class Clock(Protocol):
    """Time port used by every timed operation in the pool."""

    def now(self) -> float:
        """Current clock reading, in seconds (monotonic per implementation)."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Suspend the caller for ``seconds`` of this clock's time."""
        ...


class SystemClock:
    """Wall-clock adapter backed by ``time.monotonic`` + ``asyncio.sleep``."""

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)

    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """Deterministic virtual clock.

    ``sleep()`` registers a waiter on an internal heap and suspends the
    caller; nothing resumes until the driver calls :meth:`advance` (which
    fires every waiter whose wake time has been reached, yielding control to
    each in FIFO order) or :meth:`advance_to_next`.

    Waiting on asyncio queue primitives is NOT tracked here — those resume
    independently of virtual time, which is exactly the semantics a worker
    pool needs: work availability does not consume clock time.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)
        self._waiters: List[Tuple[float, int, asyncio.Event]] = []
        self._seq = itertools.count()

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        wake = self._now + seconds
        evt = asyncio.Event()
        heapq.heappush(self._waiters, (wake, next(self._seq), evt))
        await evt.wait()

    def has_waiters(self) -> bool:
        return bool(self._waiters)

    def next_wake(self) -> float:
        if not self._waiters:
            raise RuntimeError("ManualClock has no pending sleepers")
        return self._waiters[0][0]

    def pending_wakes(self) -> List[float]:
        return sorted({wake for wake, _seq, _evt in self._waiters})

    async def advance_to_next(self) -> float:
        """Advance to the earliest pending wake and fire its sleepers."""
        return await self.advance(self.next_wake())

    async def advance(self, target: float) -> float:
        """Move virtual time to ``target``, firing all sleepers due by then.

        Fires happen strictly in wake order; after each firing the coroutine
        yields to the event loop so the woken task can run to its next await
        point (which is either another :meth:`sleep`, a queue wait, or
        completion). Returns the new current time.
        """
        if target < self._now:
            raise ValueError(
                f"cannot advance backwards: target {target:.6f} < now {self._now:.6f}"
            )
        while self._waiters and self._waiters[0][0] <= target:
            wake, _seq, evt = heapq.heappop(self._waiters)
            self._now = max(self._now, wake)
            evt.set()
            # Let the woken task(s) run until they suspend again.
            for _ in range(8):
                await asyncio.sleep(0)
        self._now = max(self._now, target)
        return self._now


async def drive(clock: ManualClock, coro: "asyncio.Future[object] | object") -> object:
    """Run ``coro`` to completion under a :class:`ManualClock`.

    The driver loop advances virtual time whenever sleepers are pending and
    otherwise yields to the event loop, so coroutines that mix pool work,
    queue waits and clock sleeps progress deterministically. Raises
    ``RuntimeError`` if the coroutine does not finish within the step budget
    (a guard against deadlock in tests, not a production timer).
    """
    task = asyncio.ensure_future(coro)  # type: ignore[arg-type]
    for _ in range(500_000):
        if task.done():
            return task.result()
        if clock.has_waiters():
            await clock.advance_to_next()
        else:
            await asyncio.sleep(0)
    task.cancel()
    raise RuntimeError("drive(): coroutine did not finish within the step budget")


def clock_summary(clock: Clock) -> str:
    return f"{clock.now():.3f}s"