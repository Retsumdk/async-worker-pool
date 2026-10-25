# async-worker-pool

**Scalable async worker pool for distributed task execution with rate limiting — deterministic under a virtual clock.**

A batteries-included `asyncio` worker pool: bounded queue, N concurrent workers,
token-bucket rate limiting, per-attempt timeouts, exponential-backoff retries,
and a dead-letter record for every job that could not be completed. Every timing
decision flows through an injectable clock port, so the exact same code that
runs in production can be replayed bit-for-bit in tests and demos.

Built and maintained by Retsumdk. MIT licensed.

---

## The problem

Anyone wiring async workloads hits the same wall within a week:

- **Blast radius.** Unbounded `asyncio.gather()` over 10,000 jobs takes down the
  downstream API you are calling and your own process with it. You need a bound
  on concurrency *and* on the request rate — they are different limits and you
  need both.
- **Silent failures.** A job that times out, exhausts its retries, or crashes
  the worker just… disappears. You need the *full attempt history* of every
  failure, not a log line.
- **Untestable timing.** `asyncio.sleep`, `loop.time`, and `wait_for` read real
  wall-clock time. The moment your logic depends on elapsed time, your tests
  become flaky or your timeouts never fire in CI. Existing pools bake wall-clock
  reads into their internals, so you cannot replay a busy hour deterministically.

`async-worker-pool` fixes all three in ~900 lines of dependency-free Python:
concurrency and rate are first-class config, every failed job is dead-lettered
with its complete attempt history, and **all** timing (attempt timeouts, retry
backoff, rate-limit waits) goes through an injected `Clock` — swap in the
`ManualClock` and the pool becomes a deterministic simulation you can assert on.

## How it works

```
                ┌────────────────────────────────────────────────┐
 submit() ────▶ │  bounded queue (queue_size)                    │
                └───────────────┬────────────────────────────────┘
                                │ pull
                ┌───────────────▼────────────────────────────────┐
                │  worker 1..N  (asyncio tasks)                  │
                │   1. TokenBucket.acquire()  ── rate limit      │
                │   2. run attempt under timeout  ── clock       │
                │   3. on failure: backoff  ── clock ── retry    │
                │   4. attempts > max_attempts → dead letter     │
                └───────────────┬────────────────────────────────┘
                                │
                ┌───────────────▼────────────────────────────────┐
                │  TaskResult: attempts, exec time, rate waits,  │
                │  per-attempt history, error code               │
                └────────────────────────────────────────────────┘

   every wait ↑ goes through Clock ──▶ SystemClock (real) / ManualClock (virtual)
```

Key concepts:

- **Clock port.** `SystemClock` wraps the running event loop's monotonic clock;
  `ManualClock` is a virtual timeline whose sleeps fire only when the driver
  advances it. Backoff, timeouts, and rate limiting all suspend via
  `await clock.sleep(...)` — never `asyncio.sleep` — so a `ManualClock` run is
  byte-identical on every machine.
- **Lazy token bucket.** Tokens refill on demand from `clock.now()` arithmetic;
  there are no background refill timers to test around. Concurrent workers
  woken at the same virtual instant re-check and re-sleep, so the bucket never
  issues a token it does not have (its floor is exactly 0).
- **Timeouts without wall clocks.** Each attempt races its coroutine against a
  `clock.sleep(timeout)` task; the loser is cancelled. Under `ManualClock` the
  timeout fires exactly when the driver advances past the deadline.
- **Dead letters.** Jobs that exhaust `max_attempts`, raise a permanent error,
  or crash through a defensive worker boundary land in `pool.dead_letters` with
  every attempt's duration, rate-wait, and error code.
- **Coded errors.** Every failure carries a stable `code` and CLI `exit_code`
  (`POOL_QUEUE_FULL`, `TASK_TIMEOUT`, `POOL_INTERNAL`, …) so callers branch on
  values, not string matching.

## Getting started

Requires Python 3.9+. No runtime dependencies.

```bash
git clone https://github.com/Retsumdk/async-worker-pool.git
cd async-worker-pool
pip install .
```

Or install straight from GitHub:

```bash
pip install git+https://github.com/Retsumdk/async-worker-pool.git
```

For development (tests, editable install):

```bash
pip install -e ".[dev]"
pytest
```

## Usage

### Library API

```python
import asyncio
from async_worker_pool import AsyncWorkerPool, PoolConfig, PermanentTaskError

async def main() -> None:
    pool = AsyncWorkerPool(
        PoolConfig(
            workers=8,          # concurrent workers
            queue_size=256,     # bounded backlog
            rate=50.0,          # tokens/second sustained
            burst=50.0,         # instantly-available burst
            timeout=2.0,        # per-attempt budget (seconds)
            max_attempts=4,     # attempts before dead-lettering
            backoff_base=0.2,   # first retry delay
            backoff_factor=2.0, # exponential growth
            backoff_max=5.0,    # retry delay ceiling
        ),
    )

    async def fetch(url: str) -> str:
        ...  # your real work here
        return f"payload from {url}"

    for i in range(1000):
        pool.submit(fetch, f"https://example.com/{i}", name="fetch", job_id=f"u{i}")

    await pool.drain()   # wait until every job reaches a terminal outcome
    await pool.stop()    # cancel idle workers

    print(pool.stats())
    for record in pool.dead_letters:
        print(record.id, record.error_code, record.attempts)

asyncio.run(main())
```

Job functions signal retry intent by raising:

- anything → **transient failure** — retried with backoff up to `max_attempts`;
- `PermanentTaskError` → **permanent failure** — dead-lettered on the first
  attempt, no retry.

A `TaskResult` is recorded for every job either way — `pool.result_of(job_id)`
returns it, and `record.history` holds one `AttemptRecord` per attempt
(outcome `ok` / `transient_error` / `timeout`, duration, rate-limit wait).

### Prefer structured control flow? Await the handle.

`submit()` returns a `TaskHandle`; awaiting it resolves to the job's value, or
raises `TaskFailedError` when the job was dead-lettered:

```python
handle = pool.submit(fetch, url, name="fetch", job_id="u42")
try:
    payload = await handle
except TaskFailedError as exc:
    print(f"job failed permanently: {exc.message}")
```

### Context manager

```python
async with AsyncWorkerPool(PoolConfig(workers=4)) as pool:
    pool.submit(job, job_id="one")
    # __aexit__ drains, then stops
```

## Examples

### Built-in deterministic demo

```console
$ awpool demo
async-worker-pool demo — render-farm
3 workers | rate 6.0/s | burst 6 | per-attempt timeout 0.5s | max 3 attempts

job         name              att  outcome         exec_s  rate_wait_s
poster-01   render              1  completed        0.200        0.000
poster-02   render              1  completed        0.120        0.000
...
poster-07   render              2  completed        0.800        0.237
poster-11   render              2  completed        0.200        0.533
poster-13   render              1  dead_lettered    0.000        0.200

stats:
  submitted=16 completed=15 retried=3 timeouts=1 dead_lettered=1
  rate limiter: waits=11 wait_total=2.970s tokens_left=0.000
  virtual time elapsed: 2.357s

dead letters:
  poster-13: TASK_FAILED_PERMANENT after 1 attempt(s) — unsupported colorspace: CMYK
```

What the demo shows, in ~2 milliseconds of real time: 16 render jobs, 3
workers, one job that exceeds the 0.5 s attempt budget on its first attempt
and succeeds on retry, two transient failures retried, one permanent failure
(CMYK colorspace) dead-lettered with its diagnosis, and 11 waits against the
6/s rate limiter. The scenario runs entirely on `ManualClock`, so this output
is **byte-identical on every machine** — the CI workflow diffs two runs to
prove it.

### Scripted runs

`awpool run` executes a JSON job script through a real pool (same API the
library exposes), driven by the virtual clock. Each job is a work-simulation
spec — `work_s`, optional `fail_attempts` (transient failures), optional
`permanent_after` / `slow_first_s`:

```console
$ awpool run --script examples/jobs.json --output report.json && cat report.json
{
  "pool": "script-pool",
  "stats": {
    "submitted": 16,
    "completed": 15,
    "retried": 2,
    "timeouts": 0,
    "dead_lettered": 1,
    "rate_limiter": {"requests": 18, "waits": 4, "wait_total_seconds": 0.65, ...}
  },
  "results": [ ... one TaskResult per job, with full attempt history ... ],
  "dead_letters": [ ... ]
}
```

`examples/jobs.json` in this repo is a ready-made starting point.

### CLI

```console
$ awpool --help
usage: awpool [-h] [--version] {demo,run} ...

commands:
  demo  run the deterministic render-farm demo
  run   execute a JSON job script through a real pool

exit codes: 0 ok · 1 runtime error · 2 usage error · 3 unreadable/invalid script file
```

## Tests

```console
$ pytest
.............................................                [100%]
45 passed in 2.01s
```

The suite covers token-bucket refill math, wake-order fairness, retry/backoff
scheduling, timeout cancellation, queue-full rejection, duplicate ids,
dead-letter accounting, the worker-survives-crash guarantee, CLI exit codes,
and demo determinism across processes.

## API summary

| Symbol | Purpose |
| --- | --- |
| `AsyncWorkerPool(PoolConfig, clock=None)` | The pool; `clock` defaults to `SystemClock`. |
| `pool.submit(fn, *args, name=..., job_id=...)` | Enqueue a job; returns a `TaskHandle`. |
| `await handle` | Job result, or raises `TaskFailedError` if dead-lettered. |
| `pool.drain()` | Wait until every submitted job reaches a terminal outcome. |
| `pool.stop()` | Cancel workers; unprocessed queued jobs are abandoned. |
| `pool.stats()` | Counters: submitted/completed/retried/timeouts/dead_lettered, limiter snapshot. |
| `pool.result_of(job_id)` | The `TaskResult` for a finished job. |
| `pool.dead_letters` | `TaskResult`s for every job that could not be completed. |
| `PoolConfig` | workers, queue_size, timeout, max_attempts, backoff_*, rate, burst, name. |
| `TokenBucket(rate, burst, clock, initial_tokens=None)` | Standalone lazy-refill limiter. |
| `ManualClock` / `SystemClock` / `drive(clock, coro)` | Virtual-time harness for your own code. |

## License

MIT — see [LICENSE](LICENSE).
