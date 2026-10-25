"""Token-bucket rate limiting for async-worker-pool.

A classic lazy-refill token bucket: tokens are replenished on demand from
``clock.now()`` arithmetic, not by background timers, so behaviour depends
only on the injected clock and is fully reproducible under
:class:`~async_worker_pool.clock.ManualClock`.

Every execution attempt (including retries) must pass through the limiter,
which is what makes "rate limited" a first-class pool statistic rather than
an afterthought.
"""
from __future__ import annotations

from typing import Optional

from .clock import Clock
from .errors import ConfigError

# A sleep smaller than this can be absorbed by floating point (now + tiny ==
# now), which would stall refill arithmetic forever; clamp every limiter
# sleep to at least this quantum. 1e-9 s is 3+ orders of magnitude below any
# realistic rate-limit timing and 7+ above float absorption at ~1e0 s scales.
_MIN_SLEEP_S = 1e-9

# Defensive backstop: an acquire() should never need more than a handful of
# sleeps; if it does, the pool is pathological and should fail loudly.
_MAX_SLEEP_STEPS = 10_000


class TokenBucket:
    """Async token bucket with lazy refill.

    Args:
        rate: Sustained refill rate in tokens per second (must be > 0).
        burst: Maximum instantly-available tokens (the bucket capacity; > 0).
        clock: Any :class:`~async_worker_pool.clock.Clock` implementation.

    Attributes:
        wait_total: Cumulative seconds callers spent suspended waiting for
            tokens — surfaced by the pool as ``rate_wait_total``.
        waits: Number of ``acquire()`` calls that had to sleep at least once.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        clock: Clock,
        initial_tokens: Optional[float] = None,
    ) -> None:
        if rate <= 0:
            raise ConfigError(
                f"rate must be > 0 tokens/second, got {rate!r}",
                detail={"field": "rate", "value": rate},
            )
        if burst <= 0:
            raise ConfigError(
                f"burst must be > 0 tokens, got {burst!r}",
                detail={"field": "burst", "value": burst},
            )
        if initial_tokens is not None:
            if initial_tokens < 0:
                raise ConfigError(
                    f"initial_tokens must be >= 0, got {initial_tokens!r}",
                    detail={"field": "initial_tokens", "value": initial_tokens},
                )
            if initial_tokens > burst:
                raise ConfigError(
                    f"initial_tokens {initial_tokens!r} exceeds burst {burst!r}",
                    detail={"field": "initial_tokens", "value": initial_tokens, "burst": burst},
                )
        self.rate = float(rate)
        self.burst = float(burst)
        self.clock = clock
        self._tokens = self.burst if initial_tokens is None else float(initial_tokens)
        self._last = clock.now()
        self.wait_total = 0.0
        self.waits = 0
        self.requests = 0

    def _refill(self) -> None:
        now = self.clock.now()
        elapsed = max(0.0, now - self._last)
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last = now

    def try_acquire(self, tokens: float = 1.0) -> Optional[float]:
        """Non-blocking acquire. Returns ``None`` on success, or the number
        of seconds the caller must wait before the request can be satisfied."""
        if tokens <= 0:
            raise ConfigError(
                f"token count must be > 0, got {tokens!r}",
                detail={"field": "tokens", "value": tokens},
            )
        if tokens > self.burst:
            raise ConfigError(
                f"requested {tokens} tokens exceeds bucket burst {self.burst}",
                detail={"field": "tokens", "value": tokens, "burst": self.burst},
            )
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return None
        deficit = tokens - self._tokens
        return deficit / self.rate

    def wait_time_for(self, tokens: float = 1.0) -> float:
        """Seconds until ``tokens`` could be granted, without acquiring."""
        self._refill()
        deficit = max(0.0, tokens - self._tokens)
        return deficit / self.rate

    async def acquire(self, tokens: float = 1.0) -> float:
        """Acquire ``tokens``, suspending via the clock until affordable.

        Returns the number of seconds spent waiting (0.0 when tokens were
        immediately available).
        """
        self.requests += 1
        waited = 0.0
        need = self.try_acquire(tokens)
        if need is None:
            return 0.0
        waited += need
        self.waits += 1
        steps = 0
        while True:
            await self.clock.sleep(max(need, _MIN_SLEEP_S))
            # Re-check after sleeping: concurrent workers woken at the same
            # clock instant may have consumed the refilled tokens first, so
            # the caller simply sleeps again until its request is affordable.
            self._refill()
            if self._tokens >= tokens:
                break
            need = (tokens - self._tokens) / self.rate
            waited += need
            steps += 1
            if steps > _MAX_SLEEP_STEPS:  # pragma: no cover - defensive
                raise RuntimeError(
                    f"acquire({tokens}) could not be satisfied after "
                    f"{_MAX_SLEEP_STEPS} sleeps — check rate/burst configuration"
                )
        self._tokens -= tokens
        self.wait_total += waited
        return waited

    def snapshot(self) -> dict:
        return {
            "rate_per_second": self.rate,
            "burst": self.burst,
            "tokens_available": round(self._tokens, 6),
            "requests": self.requests,
            "waits": self.waits,
            "wait_total_seconds": round(self.wait_total, 6),
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"TokenBucket(rate={self.rate}, burst={self.burst}, "
            f"tokens={self._tokens:.3f}, waits={self.waits})"
        )