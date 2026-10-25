"""Determinism tests: the demo must be byte-identical across in-process runs."""
import asyncio

from async_worker_pool.demo import run_demo


def test_demo_output_is_byte_identical() -> None:
    first = asyncio.run(run_demo())
    second = asyncio.run(run_demo())
    assert first == second


def test_demo_reports_expected_counts() -> None:
    out = asyncio.run(run_demo())
    assert "submitted=16 completed=15" in out
    assert "retried=3" in out
    assert "timeouts=1" in out
    assert "dead_lettered=1" in out
    assert "poster-13" in out  # the permanent failure surfaces by id
    assert "unsupported colorspace" in out