"""Tests for the awpool CLI (demo + JSON script runner) via subprocess."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def _cli(*argv: str, expect: int = 0) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [sys.executable, "-m", "async_worker_pool", *argv],
        capture_output=True,
        text=True,
        cwd=str(REPO),
        env=None,
    )
    assert proc.returncode == expect, (
        f"exit {proc.returncode} (expected {expect})\nstdout:\n{proc.stdout}\n"
        f"stderr:\n{proc.stderr}"
    )
    return proc


def test_version_command() -> None:
    proc = _cli("version")
    assert proc.stdout.strip().startswith("async-worker-pool ")


def test_no_command_prints_help_and_usage_exit() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "async_worker_pool"],
        capture_output=True,
        text=True,
        cwd=str(REPO),
    )
    assert proc.returncode == 2
    assert "usage" in proc.stdout.lower()


def test_demo_command_reports_summary() -> None:
    proc = _cli("demo")
    out = proc.stdout
    assert "render-farm" in out
    assert "submitted=16 completed=15" in out
    assert "dead_lettered=1" in out
    assert "poster-13" in out


def test_run_executes_job_script() -> None:
    script = {
        "name": "ci-script",
        "workers": 2,
        "rate": 10.0,
        "burst": 10.0,
        "jobs": [
            {"id": "a", "work_s": 0.1},
            {"id": "b", "work_s": 0.1, "fail_once": "upstream 503"},
            {"id": "c", "permanent": "bad input"},
        ],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(script, fh)
        path = fh.name
    proc = _cli("run", "--script", path)
    report = json.loads(proc.stdout)
    assert report["pool"] == "ci-script"
    assert report["stats"]["completed"] == 2
    assert report["stats"]["dead_lettered"] == 1
    by_id = {r["id"]: r for r in report["results"]}
    assert by_id["a"]["outcome"] == "completed"
    assert by_id["b"]["attempts"] == 2
    assert by_id["c"]["outcome"] == "dead_lettered"
    Path(path).unlink()


def test_run_writes_output_file() -> None:
    script = {"jobs": [{"id": "x", "work_s": 0.05}]}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(script, fh)
        path = fh.name
    out_file = path + ".out.json"
    _cli("run", "--script", path, "--output", out_file)
    saved = json.loads(Path(out_file).read_text())
    assert saved["results"][0]["id"] == "x"
    Path(path).unlink()
    Path(out_file).unlink()


def test_run_missing_script_file_exits_3() -> None:
    _cli("run", "--script", "/nonexistent/script.json", expect=3)


def test_run_invalid_json_exits_3() -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        fh.write("{not json")
        path = fh.name
    try:
        _cli("run", "--script", path, expect=3)
    finally:
        Path(path).unlink()


def test_run_non_object_script_exits_3() -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        fh.write("[1, 2, 3]")
        path = fh.name
    try:
        _cli("run", "--script", path, expect=3)
    finally:
        Path(path).unlink()


def test_demo_is_deterministic_across_processes() -> None:
    first = _cli("demo").stdout
    second = _cli("demo").stdout
    assert first == second


@pytest.mark.parametrize(
    "argv",
    [["demo"], ["run", "--script", "-"], ["version"]],
)
def test_help_mentions_commands(argv: list) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "async_worker_pool", "--help"],
        capture_output=True,
        text=True,
        cwd=str(REPO),
    )
    assert proc.returncode == 0
    for token in ("demo", "run", "version"):
        assert token in proc.stdout