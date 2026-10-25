"""Deterministic demo: a thumbnail render farm behind one rate-limited pool.

The scenario runs entirely on a :class:`ManualClock`, so the output is
byte-identical on every machine and finishes in milliseconds of wall time
while reporting honest virtual-time numbers.

Scenario highlights:
* 16 render jobs, 3 workers, 6 tokens/second sustained rate, burst 6.
* ``poster-04`` and ``poster-11`` hit a transient upstream 503 on their
  first attempt and succeed on retry (exponential backoff).
* ``poster-07`` overshoots the 0.5s per-attempt timeout on attempt 1; the
  attempt is cancelled and the retry succeeds quickly.
* ``poster-13`` fails permanently (unsupported colorspace) and is
  dead-lettered after a single attempt.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from .clock import ManualClock, drive
from .errors import PermanentTaskError, TaskFailedError
from .pool import AsyncWorkerPool, PoolConfig, TaskResult

_JOBS: List[Dict[str, Any]] = [
    {"id": "poster-01", "work_s": 0.20},
    {"id": "poster-02", "work_s": 0.12},
    {"id": "poster-03", "work_s": 0.28},
    {"id": "poster-04", "work_s": 0.15, "fail_once": "upstream 503"},
    {"id": "poster-05", "work_s": 0.22},
    {"id": "poster-06", "work_s": 0.10},
    {"id": "poster-07", "work_s": 0.30, "slow_first_s": 0.90},
    {"id": "poster-08", "work_s": 0.18},
    {"id": "poster-09", "work_s": 0.25},
    {"id": "poster-10", "work_s": 0.14},
    {"id": "poster-11", "work_s": 0.20, "fail_once": "upstream 503"},
    {"id": "poster-12", "work_s": 0.16},
    {"id": "poster-13", "permanent": "unsupported colorspace: CMYK"},
    {"id": "poster-14", "work_s": 0.24},
    {"id": "poster-15", "work_s": 0.11},
    {"id": "poster-16", "work_s": 0.19},
]


def _make_job(spec: Dict[str, Any], clock: ManualClock, attempts: Dict[str, int]):
    async def render() -> str:
        job_id = spec["id"]
        attempts[job_id] = attempts.get(job_id, 0) + 1
        n = attempts[job_id]
        if spec.get("permanent"):
            raise PermanentTaskError(spec["permanent"], detail={"job_id": job_id})
        if n == 1 and spec.get("fail_once"):
            raise TaskFailedError(f"transient: {spec['fail_once']}", detail={"job_id": job_id})
        work = spec.get("slow_first_s") if n == 1 and spec.get("slow_first_s") else spec["work_s"]
        await clock.sleep(work)
        return f"thumb:{job_id}.png"

    return render


def _format_results(results: List[TaskResult]) -> str:
    lines = [
        f"{'job':<10}  {'name':<16}  {'att':>3}  {'outcome':<13}  "
        f"{'exec_s':>7}  {'rate_wait_s':>11}"
    ]
    for r in results:
        lines.append(
            f"{r.id:<10}  {r.name:<16}  {r.attempts:>3}  {r.outcome:<13}  "
            f"{r.exec_total:>7.3f}  {r.rate_wait_total:>11.3f}"
        )
    return "\n".join(lines)


async def run_demo() -> str:
    clock = ManualClock()
    attempts: Dict[str, int] = {}
    pool = AsyncWorkerPool(
        PoolConfig(
            workers=3,
            queue_size=64,
            rate=6.0,
            burst=6.0,
            timeout=0.5,
            max_attempts=3,
            backoff_base=0.05,
            name="render-farm",
        ),
        clock=clock,
    )

    for spec in _JOBS:
        pool.submit(_make_job(spec, clock, attempts), name="render", job_id=spec["id"])

    await drive(clock, pool.drain())
    stats = pool.stats()
    await pool.stop()

    results = [pool.result_of(spec["id"]) for spec in _JOBS]
    limiter = stats["rate_limiter"]
    out = [
        "async-worker-pool demo — render-farm",
        "3 workers | rate 6.0/s | burst 6 | per-attempt timeout 0.5s | max 3 attempts",
        "",
        _format_results(results),
        "",
        "stats:",
        f"  submitted={stats['submitted']} completed={stats['completed']} "
        f"retried={stats['retried']} timeouts={stats['timeouts']} "
        f"dead_lettered={stats['dead_lettered']}",
        f"  rate limiter: waits={limiter['waits']} "
        f"wait_total={limiter['wait_total_seconds']:.3f}s "
        f"tokens_left={limiter['tokens_available']:.3f}",
        f"  virtual time elapsed: {stats['clock_now']:.3f}s",
        "",
        "dead letters:",
    ]
    for r in pool.dead_letters:
        out.append(
            f"  {r.id}: {r.error_code} after {r.attempts} attempt(s) — {r.error}"
        )
    return "\n".join(out)


def main() -> int:
    try:
        print(asyncio.run(run_demo()))
    except Exception as exc:  # pragma: no cover - demo is deterministic
        print(f"demo failed: {type(exc).__name__}: {exc}")
        return 1
    return 0