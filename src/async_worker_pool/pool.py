"""The async worker pool: bounded queue, N workers, rate limiting, retries.

Design notes
------------
* **Everything goes through the clock port.** Attempt timeouts, retry
  backoff and rate-limit waits are all ``await clock.sleep(...)`` — so the
  whole pool is reproducible under a
  :class:`~async_worker_pool.clock.ManualClock` and testable in milliseconds.
* **Queue primitives carry work availability, not time.** Workers suspended
  on ``queue.get()`` are not tracked by the clock; they resume when a job is
  submitted. Virtual time only advances while something is genuinely waiting
  on time.
* **Failures are classified, not swallowed.** User code raises
  :class:`~async_worker_pool.errors.TaskFailedError` for a transient failure
  (retried with exponential backoff) or
  :class:`~async_worker_pool.errors.PermanentTaskError` to dead-letter
  immediately. Any other exception is wrapped as a retryable transient
  failure. Jobs that exhaust ``max_attempts`` land in ``dead_letters`` with
  their full attempt history.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .clock import Clock, SystemClock
from .errors import (
    ConfigError,
    PoolError,
    PoolStoppedError,
    PermanentTaskError,
    QueueFullError,
    TaskFailedError,
    TaskTimeoutError,
    UnknownJobError,
)
from .rate_limit import TokenBucket

_OUTCOME_COMPLETED = "completed"
_OUTCOME_DEAD_LETTERED = "dead_lettered"

def _outer_cancel_pending() -> bool:
    """True when the running task itself has a pending cancellation.

    Uses ``Task.cancelling()`` (Python 3.11+); on older interpreters the
    check is unavailable and cancellation is best-effort (stop() re-cancels
    idle workers, so shutdown still converges).
    """
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    if cancelling is None:
        return False
    return cancelling() > 0



@dataclass
class PoolConfig:
    """Static pool configuration (validated at construction).

    Attributes:
        workers: Number of concurrent worker tasks (>= 1).
        queue_size: Bounded work-queue capacity; ``submit`` raises
            :class:`QueueFullError` when full (>= 1).
        timeout: Per-attempt time budget in seconds, or ``None`` for no limit.
        max_attempts: Attempts per job before dead-lettering (>= 1).
        backoff_base: First backoff sleep, seconds (>= 0).
        backoff_factor: Multiplier applied per subsequent attempt (>= 1).
        backoff_max: Ceiling on any single backoff sleep (>= backoff_base).
        rate: Sustained execution rate, tokens per second (see
            :class:`~async_worker_pool.rate_limit.TokenBucket`).
        burst: Instantly-available token burst.
        name: Pool name, used in the demo/CLI output.
    """

    workers: int = 4
    queue_size: int = 256
    timeout: Optional[float] = None
    max_attempts: int = 3
    backoff_base: float = 0.25
    backoff_factor: float = 2.0
    backoff_max: float = 30.0
    rate: float = 10.0
    burst: float = 10.0
    name: str = "pool"

    def validate(self) -> None:
        checks: List[Tuple[bool, str, str]] = [
            (self.workers >= 1, "workers", f"must be >= 1, got {self.workers!r}"),
            (self.queue_size >= 1, "queue_size", f"must be >= 1, got {self.queue_size!r}"),
            (self.timeout is None or self.timeout > 0, "timeout", f"must be > 0 or None, got {self.timeout!r}"),
            (self.max_attempts >= 1, "max_attempts", f"must be >= 1, got {self.max_attempts!r}"),
            (self.backoff_base >= 0, "backoff_base", f"must be >= 0, got {self.backoff_base!r}"),
            (self.backoff_factor >= 1, "backoff_factor", f"must be >= 1, got {self.backoff_factor!r}"),
            (self.backoff_max >= self.backoff_base, "backoff_max", f"must be >= backoff_base ({self.backoff_base}), got {self.backoff_max!r}"),
            (self.rate > 0, "rate", f"must be > 0, got {self.rate!r}"),
            (self.burst > 0, "burst", f"must be > 0, got {self.burst!r}"),
        ]
        for ok, field_name, why in checks:
            if not ok:
                raise ConfigError(
                    f"pool config field {field_name!r} {why}",
                    detail={"field": field_name},
                )


@dataclass
class AttemptRecord:
    """One execution attempt of one job."""

    attempt: int
    started_at: float
    duration: float
    rate_wait: float
    outcome: str  # "ok" | "transient_error" | "timeout"
    error: Optional[str] = None


@dataclass
class TaskResult:
    """Final record for one submitted job."""

    id: str
    name: str
    outcome: str  # completed | dead_lettered
    attempts: int
    result: Any = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    rate_wait_total: float = 0.0
    exec_total: float = 0.0
    history: List[AttemptRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "outcome": self.outcome,
            "attempts": self.attempts,
            "result": self.result,
            "error": self.error,
            "error_code": self.error_code,
            "rate_wait_total": round(self.rate_wait_total, 6),
            "exec_total": round(self.exec_total, 6),
            "history": [
                {
                    "attempt": a.attempt,
                    "duration": round(a.duration, 6),
                    "rate_wait": round(a.rate_wait, 6),
                    "outcome": a.outcome,
                    "error": a.error,
                }
                for a in self.history
            ],
        }


@dataclass
class _Job:
    id: str
    name: str
    fn: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    submitted_at: float


class TaskHandle:
    """Awaitable handle to one submitted job.

    ``await handle`` returns the job's result or raises the final
    :class:`TaskFailedError` if the job was dead-lettered. ``handle.result``
    (``None`` until finished) is the :class:`TaskResult` record either way.
    """

    def __init__(self, job_id: str) -> None:
        self.id = job_id
        self.future: "asyncio.Future[Any]" = asyncio.get_running_loop().create_future()
        self.result: Optional[TaskResult] = None

    def __await__(self):  # pragma: no cover - trivial delegation
        return self.future.__await__()


class AsyncWorkerPool:
    """Bounded-queue async worker pool with rate limiting and retries.

    Typical use::

        pool = AsyncWorkerPool(PoolConfig(workers=4, rate=10, burst=10))
        handle = pool.submit(render_thumbnail, "img-1")
        await pool.drain()
        record = handle.result            # TaskResult
        stats = pool.stats()              # counters + limiter snapshot
        await pool.stop()

    The pool starts lazily on the first ``submit()`` and is also usable as an
    async context manager (``__aexit__`` drains then stops).
    """

    def __init__(self, config: PoolConfig, clock: Optional[Clock] = None) -> None:
        config.validate()
        self.config = config
        self.clock: Clock = clock or SystemClock()
        self.limiter = TokenBucket(config.rate, config.burst, self.clock)
        self._queue: "asyncio.Queue[_Job]" = asyncio.Queue(maxsize=config.queue_size)
        self._workers: List["asyncio.Task[None]"] = []
        self._started = False
        self._stopped = False
        self._next_id = 0
        self._handles: Dict[str, TaskHandle] = {}
        self._results: Dict[str, TaskResult] = {}
        self._dead_letters: List[TaskResult] = []
        self._busy = 0
        self._submitted = 0
        self._completed = 0
        self._dead_lettered = 0
        self._retried = 0
        self._timeouts = 0

    # ------------------------------------------------------------------ core

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            job = await self._queue.get()
            try:
                self._busy += 1
                await self._process(job)
            except asyncio.CancelledError:  # pragma: no cover - shutdown path
                raise
            except Exception as exc:  # defensive: a worker must never die
                self._record_dead_letter_from_crash(job, exc)
            finally:
                self._busy -= 1
                self._queue.task_done()

    async def _process(self, job: _Job) -> None:
        handle = self._handles[job.id]
        record = TaskResult(id=job.id, name=job.name, outcome=_OUTCOME_DEAD_LETTERED, attempts=0)
        max_attempts = self.config.max_attempts
        attempt = 0
        while True:
            attempt += 1
            record.attempts = attempt
            rate_wait = await self.limiter.acquire()
            started = self.clock.now()
            try:
                value = await self._run_with_timeout(job)
            except TaskFailedError as exc:
                duration = self.clock.now() - started
                record.exec_total += duration
                record.rate_wait_total += rate_wait
                record.history.append(
                    AttemptRecord(
                        attempt=attempt,
                        started_at=started,
                        duration=duration,
                        rate_wait=rate_wait,
                        outcome="timeout" if isinstance(exc, TaskTimeoutError) else "transient_error",
                        error=exc.message,
                    )
                )
                if isinstance(exc, TaskTimeoutError):
                    self._timeouts += 1
                if exc.permanent or attempt >= max_attempts:
                    record.error = exc.message
                    record.error_code = exc.code
                    self._finish(handle, record)
                    return
                self._retried += 1
                backoff = min(
                    self.config.backoff_base * (self.config.backoff_factor ** (attempt - 1)),
                    self.config.backoff_max,
                )
                await self.clock.sleep(backoff)
                continue
            else:
                duration = self.clock.now() - started
                record.exec_total += duration
                record.rate_wait_total += rate_wait
                record.outcome = _OUTCOME_COMPLETED
                record.result = value
                record.history.append(
                    AttemptRecord(
                        attempt=attempt,
                        started_at=started,
                        duration=duration,
                        rate_wait=rate_wait,
                        outcome="ok",
                    )
                )
                self._finish(handle, record)
                return

    async def _run_with_timeout(self, job: _Job) -> Any:
        inner = asyncio.ensure_future(self._call(job))
        if self.config.timeout is None:
            try:
                return await inner
            finally:
                if not inner.done():  # pragma: no cover - defensive
                    inner.cancel()
        timer = asyncio.ensure_future(self.clock.sleep(self.config.timeout))
        try:
            done, _pending = await asyncio.wait(
                {inner, timer}, return_when=asyncio.FIRST_COMPLETED
            )
            if inner in done:
                return inner.result()
            inner.cancel()
            await self._reap(inner)
            raise TaskTimeoutError(
                f"job {job.id!r} exceeded {self.config.timeout}s attempt budget",
                detail={"job_id": job.id, "timeout": self.config.timeout},
            )
        finally:
            if not timer.done():
                timer.cancel()
            await self._reap(timer)

    @staticmethod
    async def _reap(fut: "asyncio.Future[Any]") -> None:
        """Await a cancelled future without swallowing our own stop() signal.

        The CancelledError we must tolerate is the one caused by cancelling
        ``fut`` itself; if the *worker task* is being cancelled (pool stop),
        the exception must propagate so the worker actually dies.
        """
        try:
            await fut
        except asyncio.CancelledError:
            if _outer_cancel_pending():
                raise
        except Exception:
            pass

    @staticmethod
    async def _call(job: _Job) -> Any:
        try:
            return await job.fn(*job.args, **job.kwargs)
        except PermanentTaskError:
            raise  # keep the PERMANENT classification lossless
        except TaskFailedError:
            raise
        except Exception as exc:
            raise TaskFailedError(
                f"{type(exc).__name__}: {exc}", detail={"job_id": job.id}
            ) from exc

    def _finish(self, handle: TaskHandle, record: TaskResult) -> None:
        self._results[record.id] = record
        handle.result = record
        if record.outcome == _OUTCOME_COMPLETED:
            self._completed += 1
            if not handle.future.done():
                handle.future.set_result(record.result)
        else:
            self._dead_lettered += 1
            self._dead_letters.append(record)
            if not handle.future.done():
                handle.future.set_exception(
                    TaskFailedError(
                        record.error or "job failed",
                        detail={"job_id": record.id, "attempts": record.attempts},
                    )
                )
                # Mark retrieved so an un-awaited handle never triggers the
                # asyncio "exception was never retrieved" GC warning: callers
                # read terminal outcomes via result_of()/dead_letters, and
                # awaiting later still raises normally.
                handle.future.exception()

    def _record_dead_letter_from_crash(self, job: _Job, exc: Exception) -> None:
        """Last-resort accounting if a worker bug escapes ``_process``."""
        handle = self._handles.get(job.id)
        record = TaskResult(
            id=job.id,
            name=job.name,
            outcome=_OUTCOME_DEAD_LETTERED,
            attempts=0,
            error=f"internal pool error: {type(exc).__name__}: {exc}",
            error_code="POOL_INTERNAL",
        )
        self._results[job.id] = record
        self._dead_lettered += 1
        self._dead_letters.append(record)
        if handle is not None and not handle.future.done():
            handle.future.set_exception(
                TaskFailedError(record.error or "internal pool error", detail={"job_id": job.id})
            )
            handle.future.exception()  # retrieve now: handles may never be awaited

    # ------------------------------------------------------------- public API

    def submit(
        self,
        fn: Callable[..., Any],
        *args: Any,
        name: Optional[str] = None,
        job_id: Optional[str] = None,
        **kwargs: Any,
    ) -> TaskHandle:
        """Enqueue ``fn(*args, **kwargs)`` for execution. Returns a handle.

        Raises:
            QueueFullError: the bounded queue is at capacity.
            PoolStoppedError: the pool has been stopped.
            ConfigError: ``fn`` is not callable.
        """
        if self._stopped:
            raise PoolStoppedError(
                "submit() called after the pool was stopped",
                detail={"pool": self.config.name},
            )
        if not callable(fn):
            raise ConfigError(
                "submit() requires a callable, got a non-callable",
                detail={"type": type(fn).__name__},
            )
        self._next_id += 1
        job_id = job_id or f"job-{self._next_id:04d}"
        if job_id in self._handles:
            raise ConfigError(
                f"duplicate job id {job_id!r}",
                detail={"job_id": job_id},
            )
        if not self._started:
            self._started = True
            for wid in range(self.config.workers):
                self._workers.append(asyncio.ensure_future(self._worker_loop(wid)))
        handle = TaskHandle(job_id)
        self._handles[job_id] = handle
        job = _Job(
            id=job_id,
            name=name or getattr(fn, "__name__", "job"),
            fn=fn,
            args=args,
            kwargs=kwargs,
            submitted_at=self.clock.now(),
        )
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            del self._handles[job_id]
            raise QueueFullError(
                f"work queue is full ({self.config.queue_size} jobs)",
                detail={"queue_size": self.config.queue_size},
            )
        self._submitted += 1
        return handle

    async def drain(self) -> None:
        """Wait until every submitted job has reached a terminal outcome."""
        if self._started:
            await self._queue.join()

    async def stop(self) -> None:
        """Stop workers. Unprocessed queued jobs are abandoned (call
        :meth:`drain` first for graceful shutdown)."""
        self._stopped = True
        if not self._workers:
            return
        for worker in self._workers:
            worker.cancel()
        _done, pending = await asyncio.wait(self._workers, timeout=5.0)
        for worker in pending:
            worker.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._workers.clear()

    def result_of(self, job_id: str) -> TaskResult:
        try:
            return self._results[job_id]
        except KeyError:
            raise UnknownJobError(
                f"no result recorded for job id {job_id!r}",
                detail={"job_id": job_id},
            ) from None

    @property
    def dead_letters(self) -> List[TaskResult]:
        return list(self._dead_letters)

    @property
    def busy(self) -> int:
        return self._busy

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    def stats(self) -> dict:
        return {
            "pool": self.config.name,
            "workers": self.config.workers,
            "submitted": self._submitted,
            "completed": self._completed,
            "retried": self._retried,
            "timeouts": self._timeouts,
            "dead_lettered": self._dead_lettered,
            "busy": self._busy,
            "pending": self._queue.qsize(),
            "rate_limiter": self.limiter.snapshot(),
            "clock_now": round(self.clock.now(), 6),
        }

    async def __aenter__(self) -> "AsyncWorkerPool":
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.drain()
        await self.stop()


__all__ = [
    "AsyncWorkerPool",
    "AttemptRecord",
    "PoolConfig",
    "TaskHandle",
    "TaskResult",
    "PoolError",
    "TaskFailedError",
    "PermanentTaskError",
    "TaskTimeoutError",
]