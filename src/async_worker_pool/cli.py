"""``awpool`` CLI — deterministic demo and a JSON script runner.

Commands:
    awpool demo
        Run the built-in ManualClock-driven demo (see ``demo.py``).

    awpool run --script jobs.json [--output out.json]
        Execute a JSON job script against an in-process pool driven by a
        ManualClock. Script format:

            {"workers": 2, "rate": 4.0, "burst": 4, "timeout": 0.5,
             "jobs": [
               {"id": "a", "work_s": 0.2},
               {"id": "b", "work_s": 0.1, "fail_once": "upstream 503"},
               {"id": "c", "permanent": "bad input"}
             ]}

        Every job's "work" is simulated as ``await clock.sleep(work_s)`` —
        this is a harness for exercising the pool itself, not a general
        task runner.

    awpool version
        Print the package version.

Exit codes: 0 success · 1 runtime error · 2 usage error · 3 unreadable/
invalid script file.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List

from . import __version__
from .clock import ManualClock, drive
from .demo import run_demo
from .errors import PoolError, exit_code_for
from .pool import AsyncWorkerPool, PoolConfig

EXIT_OK = 0
EXIT_RUNTIME = 1
EXIT_USAGE = 2
EXIT_SCRIPT = 3


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="awpool",
        description="async-worker-pool — bounded-queue async worker pool "
        "with rate limiting, retries and dead-lettering.",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("demo", help="run the deterministic built-in demo")

    run_p = sub.add_parser("run", help="execute a JSON job script against an in-process pool")
    run_p.add_argument("--script", required=True, metavar="FILE", help="job script (JSON)")
    run_p.add_argument(
        "--output", metavar="FILE", default=None,
        help="also write the full JSON report to FILE",
    )

    sub.add_parser("version", help="print the package version")
    return parser


async def _run_script(spec: Dict[str, Any]) -> Dict[str, Any]:
    clock = ManualClock()
    config = PoolConfig(
        workers=int(spec.get("workers", 4)),
        rate=float(spec.get("rate", 10.0)),
        burst=float(spec.get("burst", 10.0)),
        timeout=spec.get("timeout"),
        max_attempts=int(spec.get("max_attempts", 3)),
        backoff_base=float(spec.get("backoff_base", 0.05)),
        name=str(spec.get("name", "script-pool")),
    )
    jobs: List[Dict[str, Any]] = spec.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("'jobs' must be a list")

    pool = AsyncWorkerPool(config, clock=clock)
    attempts: Dict[str, int] = {}

    def make_fn(job: Dict[str, Any]):
        job_id = str(job.get("id"))

        async def body() -> str:
            n = attempts.get(job_id, 0) + 1
            attempts[job_id] = n
            if job.get("permanent"):
                from .errors import PermanentTaskError

                raise PermanentTaskError(str(job["permanent"]), detail={"job_id": job_id})
            if n == 1 and job.get("fail_once"):
                from .errors import TaskFailedError

                raise TaskFailedError(f"transient: {job['fail_once']}", detail={"job_id": job_id})
            await clock.sleep(float(job.get("work_s", 0.0)))
            return f"done:{job_id}"

        return body

    for job in jobs:
        if not isinstance(job, dict) or "id" not in job:
            raise ValueError("every job must be an object with an 'id' field")
        pool.submit(make_fn(job), name=str(job.get("name", "job")), job_id=str(job["id"]))

    await drive(clock, pool.drain())
    stats = pool.stats()
    await pool.stop()

    results = []
    for job in jobs:
        job_id = str(job["id"])
        try:
            results.append(pool.result_of(job_id).to_dict())
        except PoolError as exc:
            results.append({"id": job_id, "outcome": "no_result", "error": exc.message,
                            "error_code": exc.code})
    report = {
        "pool": config.name,
        "stats": stats,
        "results": results,
        "dead_letters": [r.to_dict() for r in pool.dead_letters],
    }
    return report


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        with open(args.script, "r", encoding="utf-8") as fh:
            spec = json.load(fh)
    except OSError as exc:
        print(f"awpool: cannot read script file: {exc}", file=sys.stderr)
        return EXIT_SCRIPT
    except json.JSONDecodeError as exc:
        print(f"awpool: script is not valid JSON: {exc}", file=sys.stderr)
        return EXIT_SCRIPT
    if not isinstance(spec, dict):
        print("awpool: script must be a JSON object", file=sys.stderr)
        return EXIT_SCRIPT

    try:
        report = asyncio.run(_run_script(spec))
    except PoolError as exc:
        print(f"awpool: pool error [{exc.code}]: {exc.message}", file=sys.stderr)
        return exit_code_for(exc)
    except ValueError as exc:
        print(f"awpool: invalid script: {exc}", file=sys.stderr)
        return EXIT_SCRIPT

    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    print(text)
    return EXIT_OK


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return EXIT_USAGE
    if args.command == "demo":
        try:
            print(asyncio.run(run_demo()))
            return EXIT_OK
        except PoolError as exc:
            print(f"awpool: pool error [{exc.code}]: {exc.message}", file=sys.stderr)
            return exit_code_for(exc)
    if args.command == "version":
        print(f"async-worker-pool {__version__}")
        return EXIT_OK
    if args.command == "run":
        return _cmd_run(args)
    parser.print_help()  # pragma: no cover - argparse rejects unknown commands
    return EXIT_USAGE


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())