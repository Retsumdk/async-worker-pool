"""Coded errors for async-worker-pool.

Every error carries a stable machine-readable ``code`` and a CLI ``exit_code``
so callers can branch on failures programmatically instead of parsing strings.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class PoolError(Exception):
    """Base class for all async-worker-pool errors."""

    code: str = "POOL_ERROR"
    exit_code: int = 1

    def __init__(self, message: str, *, detail: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail: Dict[str, Any] = dict(detail or {})

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.detail:
            out["detail"] = self.detail
        return out

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


class ConfigError(PoolError):
    """Invalid pool or rate-limiter configuration."""

    code = "POOL_CONFIG_INVALID"
    exit_code = 2


class QueueFullError(PoolError):
    """submit() was called while the bounded work queue was full."""

    code = "POOL_QUEUE_FULL"
    exit_code = 3


class PoolStoppedError(PoolError):
    """submit() was called after the pool was stopped."""

    code = "POOL_STOPPED"
    exit_code = 4


class UnknownJobError(PoolError):
    """Reference to a job id that was never submitted."""

    code = "POOL_JOB_UNKNOWN"
    exit_code = 5


class TaskFailedError(PoolError):
    """A task raised an exception during execution.

    Raised by user code to signal a transient failure (the pool retries it
    with backoff), or constructed by the pool itself when user code raises an
    arbitrary exception. Wrap :class:`PermanentTaskError` inside it — or raise
    that class directly — to skip retries and dead-letter immediately.
    """

    code = "TASK_FAILED"
    exit_code = 7

    def __init__(
        self,
        message: str,
        *,
        permanent: bool = False,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.permanent = permanent


class TaskTimeoutError(TaskFailedError):
    """A single execution attempt exceeded its time budget.

    Subclasses :class:`TaskFailedError` so the pool's attempt loop catches
    timeouts like any other failed attempt: ``permanent`` defaults to False,
    so a timed-out attempt is retried with backoff.
    """

    code = "TASK_TIMEOUT"
    exit_code = 6

    def __init__(
        self,
        message: str,
        *,
        permanent: bool = False,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, permanent=permanent, detail=detail)


class PermanentTaskError(TaskFailedError):
    """Retryable-failure signal that must NOT be retried; dead-letter immediately."""

    code = "TASK_FAILED_PERMANENT"
    exit_code = 8

    def __init__(self, message: str, *, detail: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message, permanent=True, detail=detail)


EXIT_CODES: Dict[str, int] = {
    cls.code: cls.exit_code
    for cls in (
        PoolError,
        ConfigError,
        QueueFullError,
        PoolStoppedError,
        UnknownJobError,
        TaskTimeoutError,
        TaskFailedError,
        PermanentTaskError,
    )
}


def exit_code_for(exc: BaseException) -> int:
    """Map an exception to the CLI exit code contract (0/1/2/3/4/5/6/7/8)."""
    if isinstance(exc, PoolError):
        return exc.exit_code
    return 1