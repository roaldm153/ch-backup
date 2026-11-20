"""Check that restore observers preserve bytes, failures and actual retry behavior."""

import argparse
import csv
import importlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Callable
from unittest.mock import patch

import pytest
from botocore.response import StreamingBody
from prometheus_client import CollectorRegistry, generate_latest

from ch_backup.storage.engine.s3.s3_retry import RetryExponential
from tests.benchmark.copy_memory.probe import Probe
from tests.benchmark.restore.metrics import RestoreMetrics
from tests.benchmark.restore.profiles import cpu_coverage


def test_body_bytes_and_error_preserved() -> None:
    """Read a real body and retain a truncated response error without leaking data."""
    registry = CollectorRegistry()
    metrics = RestoreMetrics(registry)
    body = StreamingBody(io.BytesIO(b"payload"), 7)
    assert metrics.wrap(body.read, "body_read")() == b"payload"
    assert registry.get_sample_value("bench_restore_body_bytes_total") == 7
    secret_error = RuntimeError("secret-object-key")

    def fail() -> None:
        """Raise the same exception that the caller must receive."""
        raise secret_error

    with pytest.raises(RuntimeError) as caught:
        metrics.wrap(fail, "body_read")()
    assert caught.value is secret_error
    assert b"secret-object-key" not in generate_latest(registry)
    assert registry.get_sample_value("bench_restore_body_bytes_total") == 7
    assert (
        registry.get_sample_value(
            "bench_restore_stage_seconds_count",
            {"stage": "body_read", "outcome": "error"},
        )
        == 1
    )


def test_outer_retry_sleep_preserves_policy() -> None:
    """Exercise the real retry loop with deterministic waits and eventual success."""
    registry = CollectorRegistry()
    metrics = RestoreMetrics(registry)
    sleeps: list[float] = []
    calls = []

    def operation() -> bytes:
        """Fail twice before returning the actual payload."""
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("transient")
        return b"restored"

    with patch.object(
        RetryExponential, "__init__", metrics.wrap_retry_init(RetryExponential.__init__)
    ):
        retry = RetryExponential(
            ValueError, max_attempts=3, sleep_function=sleeps.append, verbose=False
        )
    assert retry(operation) == b"restored"
    assert len(calls) == 3
    assert len(sleeps) == 2
    assert registry.get_sample_value("bench_restore_retry_sleep_seconds_count") == 2


def test_branch_order_rotates() -> None:
    """Keep identical membership while moving every branch through each position."""

    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.restore.driver")
    branches = ["a", "b", "c"]
    assert [driver.order(branches, repeat) for repeat in range(3)] == [
        ["a", "b", "c"],
        ["b", "c", "a"],
        ["c", "a", "b"],
    ]
    assert branches == ["a", "b", "c"]


def test_comparison_excludes_profiles_and_failed_data(tmp_path: Path) -> None:
    """Only verified timing runs contribute to branch medians."""

    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.restore.driver")
    args = argparse.Namespace(
        parts=3, version="25.8", workers=2, results=tmp_path, hours=1, native=False
    )
    observer = driver.RestoreDriver(args)
    observer.sources = {"a": tmp_path}
    records = [
        {
            "variant": "a",
            "profile": None,
            "verified": True,
            "exit_code": 0,
            "restore_seconds": 10,
        },
        {
            "variant": "a",
            "profile": None,
            "verified": True,
            "exit_code": 0,
            "restore_seconds": 20,
        },
        {
            "variant": "a",
            "profile": "cpu",
            "verified": True,
            "exit_code": 0,
            "restore_seconds": 100,
        },
        {
            "variant": "a",
            "profile": None,
            "verified": False,
            "exit_code": 0,
            "restore_seconds": 1,
        },
    ]
    observer._checkpoint(  # pylint: disable=protected-access
        tmp_path, {"runs": records}
    )  # pylint: disable=protected-access
    with (tmp_path / "comparison.csv").open() as output:
        row = next(csv.DictReader(output))
    assert row["runs"] == "2"
    assert row["median_seconds"] == "15.0"


def test_cpu_requires_observed_workers(tmp_path: Path) -> None:
    """A valid main-only capture must not claim subprocess coverage."""

    (tmp_path / "observed.json").write_text(
        json.dumps(
            {
                "1": {"host_pid": 101, "role": "main"},
                "2": {"host_pid": 102, "role": "worker"},
            }
        )
    )
    for pid in (1, 2):
        (tmp_path / f"instrumentation-{pid}.json").write_text(json.dumps({"pid": pid}))
    capture = tmp_path / "cpu-01.speedscope.json"
    profiles = [{"name": 'Process 101 Thread 101 "MainThread"', "samples": [[0]]}]
    capture.write_text(json.dumps({"profiles": profiles}))
    (tmp_path / "cpu-windows.jsonl").write_text(
        json.dumps({"file": capture.name, "exit_code": 0}) + "\n"
    )
    result = cpu_coverage(tmp_path)
    assert result["profile_status"] == "incomplete"
    assert result["missing_cpu_host_pids"] == ["102"]
    profiles.append({"name": 'Process 102 Thread 102 "MainThread"', "samples": [[0]]})
    capture.write_text(json.dumps({"profiles": profiles}))
    assert cpu_coverage(tmp_path)["profile_status"] == "complete"
    (tmp_path / "instrumentation-3.json").write_text(json.dumps({"pid": 3}))
    assert cpu_coverage(tmp_path)["unobserved_instrumented_pids"] == ["3"]
    assert cpu_coverage(tmp_path)["profile_status"] == "incomplete"


def test_phase_hook_preserves_static_method_binding(tmp_path: Path) -> None:
    """Wrapping a static restore method must work when called through an instance."""

    class Manager:
        """Expose the same descriptor shape as DatabaseBackup.restore."""

        @staticmethod
        def restore(payload: bytes) -> bytes:
            """Return bytes unchanged without accepting an instance argument."""
            return payload

    def wrap(method: Callable) -> Callable:
        """Return a regular function as the stage observer does."""

        def wrapped(*args):
            """Forward every received argument unchanged."""
            return method(*args)

        return wrapped

    observer = Probe(tmp_path, 0.5, False)
    with patch(
        "importlib.import_module", return_value=SimpleNamespace(Manager=Manager)
    ):
        observer._patch(  # pylint: disable=protected-access
            "test", "Manager", "restore", wrap
        )  # pylint: disable=protected-access
    assert Manager().restore(b"data") == b"data"
