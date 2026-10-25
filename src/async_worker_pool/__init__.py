"""Public API for async-worker-pool.

    from async_worker_pool import AsyncWorkerPool, PoolConfig

    pool = AsyncWorkerPool(PoolConfig(workers=4, rate=10, burst=10))
    handle = pool.submit(work, "arg")
    await pool.drain()
    print(handle.result.outcome)
    await pool.stop()
"""
from .clock import Clock, ManualClock, SystemClock, clock_summary, drive
from .errors import (
    ConfigError,
    PermanentTaskError,
    PoolError,
    PoolStoppedError,
    QueueFullError,
    TaskFailedError,
    TaskTimeoutError,
    UnknownJobError,
    exit_code_for,
)
from .pool import (
    AsyncWorkerPool,
    AttemptRecord,
    PoolConfig,
    TaskHandle,
    TaskResult,
)
from .rate_limit import TokenBucket

__version__ = "1.0.0"

__all__ = [
    "AsyncWorkerPool",
    "AttemptRecord",
    "Clock",
    "ConfigError",
    "ManualClock",
    "PermanentTaskError",
    "PoolConfig",
    "PoolError",
    "PoolStoppedError",
    "QueueFullError",
    "SystemClock",
    "TaskFailedError",
    "TaskHandle",
    "TaskResult",
    "TaskTimeoutError",
    "TokenBucket",
    "UnknownJobError",
    "__version__",
    "clock_summary",
    "drive",
    "exit_code_for",
]