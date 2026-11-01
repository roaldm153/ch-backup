"""Check profiler isolation, crash artifacts, bounded metrics and benchmark deadlines."""

import argparse
import importlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from prometheus_client import write_to_textfile

from tests.benchmark.copy_memory import (
    collector,
    instrumentation,
    measurements,
    probe,
    profiles,
    report,
)
from tests.benchmark.copy_memory.budget import Budget, BudgetExpired


def test_s3_metrics_preserve_calls_and_count_http_attempts(tmp_path: Path) -> None:
    """Measure retries without recording a URL, key, request body or credential."""
    metrics = instrumentation.S3Metrics()
    request = object()
    send = metrics.wrap_send(
        Mock(
            side_effect=[
                SimpleNamespace(status_code=503),
                SimpleNamespace(status_code=200),
            ]
        )
    )
    response = {"ResponseMetadata": {"RetryAttempts": 1}}

    def original(_client, _operation, _parameters):
        """Simulate one logical call with an internal retry."""
        send(None, request)
        send(None, request)
        return response

    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=SimpleNamespace(service_name="s3"))
    )
    call = metrics.wrap_call(original)
    assert (
        call(
            client,
            operation_name="CopyObject",
            api_params={"Key": "private-key", "Secret": "private-value"},
        )
        is response
    )
    write_to_textfile(str(tmp_path / "s3-1.prom"), metrics.registry)
    result = measurements.s3_stats(tmp_path)["CopyObject"]
    assert result["call"]["+Inf"] == 1
    assert result["attempt"]["+Inf"] == 2
    assert result["retries"] == 1
    assert result["http_errors"] == 1
    text = (tmp_path / "s3-1.prom").read_text()
    assert "private-key" not in text and "private-value" not in text
    assert metrics.operation.name is None


def test_s3_metrics_keep_exception_and_bound_operation_labels(tmp_path: Path) -> None:
    """Collapse unexpected names and preserve the original failure."""
    metrics = instrumentation.S3Metrics()
    failure = RuntimeError("transport failed")
    call = metrics.wrap_call(Mock(side_effect=failure))
    client = SimpleNamespace(
        meta=SimpleNamespace(service_model=SimpleNamespace(service_name="s3"))
    )
    for index in range(100):
        with pytest.raises(RuntimeError) as caught:
            call(client, f"unknown-{index}", {})
        assert caught.value is failure
    write_to_textfile(str(tmp_path / "s3-1.prom"), metrics.registry)
    result = measurements.s3_stats(tmp_path)
    assert set(result) == {"other"}
    assert result["other"]["errors"] == 100
    assert len(result["other"]["call"]) == len(instrumentation.BUCKETS) + 1


def test_histogram_quantiles_merge_workers(tmp_path: Path) -> None:
    """Use bucket bounds rather than falsely precise percentiles."""
    for pid, durations in enumerate(((0.01, 0.02), (0.2, 0.4))):
        metrics = instrumentation.S3Metrics()
        for duration in durations:
            metrics.calls.labels("CopyObject", "success").observe(duration)
        write_to_textfile(str(tmp_path / f"s3-{pid}.prom"), metrics.registry)
    histogram = measurements.s3_stats(tmp_path)["CopyObject"]["call"]
    assert histogram["+Inf"] == 4
    assert measurements.quantile_bound(histogram, 0.5) == 0.025
    assert measurements.quantile_bound(histogram, 0.99) == 0.5
    assert measurements.quantile_bound({}, 0.5) is None


def test_total_budget_interrupts_work_and_restores_handler() -> None:
    """End blocking preparation while leaving cleanup outside the signal timer."""
    previous = signal.getsignal(signal.SIGALRM)
    budget = Budget(0.02 / 3600)
    with pytest.raises(BudgetExpired):
        with budget.watch():
            time.sleep(1)
    assert signal.getsignal(signal.SIGALRM) == previous
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
    with pytest.raises(BudgetExpired):
        budget.limit(100)


def test_budget_expiry_keeps_point_and_cleanup(tmp_path: Path) -> None:
    """Persist the partial point and logs before propagating a series deadline."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    benchmark = driver.Driver(
        driver.Options(parts=[10000], versions=["26.3"], results=tmp_path)
    )
    env = Mock()
    with (
        patch.object(
            benchmark, "_measure_point", side_effect=BudgetExpired("deadline")
        ),
        pytest.raises(BudgetExpired),
    ):
        benchmark._point(env, "26.3", 10000)  # pylint: disable=protected-access
    point = json.loads((tmp_path / "26.3/10000/point.json").read_text())
    assert point["status"] == "budget_exhausted"
    env.save_diagnostics.assert_called_once()
    env.stop.assert_called_once()


@pytest.mark.parametrize("largest", [30000, 120000])
def test_overnight_controls_and_large_points(largest: int) -> None:
    """Keep heavy profiles on the small point and rotate their order across repeats."""
    # pylint: disable=protected-access
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    benchmark = driver.Driver(
        driver.Options(
            parts=[10000, 20000, largest],
            versions=["26.3"],
            overnight=True,
            profiles=True,
            profile_parts=2,
        )
    )
    inventory = {"unique_objects": 3_000_000, "metadata_format_version": 5}
    assert benchmark._parts() == [
        10000,
        20000,
        largest,
        2,
    ]
    controls = benchmark._runs(2, False, inventory)
    assert len(controls) == 9
    assert len({run.path_root for run in controls}) == 9
    assert [run.profile for run in controls[:6]] == [
        None,
        "cpu",
        "memory",
        "cpu",
        "memory",
        None,
    ]
    assert not benchmark._fits(10000, 3_000_000)
    for parts in (10000, 20000, largest):
        large = benchmark._runs(parts, False, inventory)
        assert [run.mode for run in large] == [
            "planning_only",
            "metadata_only",
            "metadata_only",
        ]
        assert all(run.kind != "tracemalloc" and run.profile is None for run in large)


def test_collector_rejects_recycled_pid(tmp_path: Path) -> None:
    """A matching namespace PID is insufficient when its birth time changed."""
    # pylint: disable=protected-access
    args = argparse.Namespace(
        point=tmp_path, cpu_window=120, cpu_windows=3, native=False
    )
    sampler = collector.Collector(args)
    sampler.containers = {"clickhouse": [100]}
    with (
        patch.object(collector, "namespace_pid", return_value=10),
        patch.object(
            collector.psutil,
            "Process",
            return_value=Mock(create_time=Mock(return_value=200)),
        ),
    ):
        assert (
            sampler._target({"pid": 10, "created": 100}) is None
        )  # pylint: disable=protected-access
        assert (
            sampler._target({"pid": 10, "created": 200}) == 100
        )  # pylint: disable=protected-access


def test_missing_rollup_is_unknown() -> None:
    """Do not report denied private-memory measurements as zero consumption."""
    assert collector.total([], "uss") == 0
    assert collector.total([{"uss": 10}, {"uss": None}], "uss") is None


def test_memory_coverage_detects_missing_spawn_worker(tmp_path: Path) -> None:
    """A parent's capture must not masquerade as coverage of its workers."""
    (tmp_path / "observed.json").write_text(
        json.dumps({"10": {"created": 100}, "11": {"created": 101}})
    )
    (tmp_path / "instrumentation-10.json").write_text(
        json.dumps({"pid": 10, "created": 100, "memory": True, "status": "completed"})
    )
    (tmp_path / "profiles.json").write_text('[{"exit_code": 0}]')
    result = profiles.coverage(tmp_path, "memory")
    assert result["profile_status"] == "incomplete"
    assert result["missing_memory_pids"] == ["11"]


def test_cpu_coverage_ignores_unfinished_json(tmp_path: Path) -> None:
    """Keep previous CPU windows usable when a later window is truncated."""
    (tmp_path / "cpu-01.speedscope.json").write_text(
        json.dumps({"profiles": [{"samples": [[0]]}]})
    )
    (tmp_path / "cpu-02.speedscope.json").write_text('{"profiles":')
    result = profiles.coverage(tmp_path, "cpu")
    assert result["cpu_captures"] == ["cpu-01.speedscope.json"]
    assert result["profile_status"] == "incomplete"


def test_memray_render_preserves_capture_after_failure(tmp_path: Path) -> None:
    """Rendering failures retain raw evidence and do not skip other views."""
    capture = tmp_path / "memory-10.bin"
    capture.write_bytes(b"partial capture")
    with patch.object(
        profiles.subprocess, "run", return_value=Mock(returncode=1)
    ) as run:
        records = profiles.render(tmp_path)
    assert run.call_count == 4
    assert all(record["exit_code"] == 1 for record in records)
    assert capture.read_bytes() == b"partial capture"
    assert len(json.loads((tmp_path / "profiles.json").read_text())) == 4


@pytest.mark.parametrize("unusable", ["timeout", "missing", "empty", "outdated"])
def test_memray_render_retries_only_unusable_views(
    tmp_path: Path, unusable: str
) -> None:
    """Reuse completed views while recovering a failed or lost report from raw data."""
    capture = tmp_path / "memory-10.bin"
    capture.write_bytes(b"saved capture")

    def render_view(command, **kwargs):
        """Write an artifact as the Memray subprocess would."""
        if "-o" in command:
            Path(command[command.index("-o") + 1]).write_text(
                "report", encoding="utf-8"
            )
        else:
            kwargs["stdout"].write("stats")
        return Mock(returncode=0)

    with patch.object(profiles.subprocess, "run", side_effect=render_view):
        records = profiles.render(tmp_path)
    destination = tmp_path / "memory-10-temporal.html"
    if unusable == "timeout":
        records[1].pop("exit_code")
        records[1]["error"] = "report_timeout"
        (tmp_path / "profiles.json").write_text(json.dumps(records))
    elif unusable == "missing":
        destination.unlink()
    elif unusable == "empty":
        destination.write_text("")
    else:
        os.utime(destination, ns=(1, 1))

    with patch.object(profiles.subprocess, "run", side_effect=render_view) as run:
        recovered = profiles.render(tmp_path, timeout=600)
    run.assert_called_once()
    assert "--temporal" in run.call_args.args[0]
    assert run.call_args.kwargs["timeout"] == 600
    assert len(recovered) == 4
    assert all(row.get("exit_code") == 0 and "error" not in row for row in recovered)
    assert json.loads((tmp_path / "profiles.json").read_text()) == recovered
    assert capture.read_bytes() == b"saved capture"
    with patch.object(profiles.subprocess, "run") as run:
        assert profiles.render(tmp_path) == recovered
    run.assert_not_called()


def test_memray_report_timeout_remains_bounded(tmp_path: Path) -> None:
    """Keep a timed-out report visible and retain its capture for a later retry."""
    capture = tmp_path / "memory-10.bin"
    capture.write_bytes(b"saved capture")
    with patch.object(
        profiles.subprocess, "run", side_effect=subprocess.TimeoutExpired("memray", 900)
    ) as run:
        records = profiles.render(tmp_path)
    assert run.call_count == 4
    assert all(call.kwargs["timeout"] == 900 for call in run.call_args_list)
    assert all(row.get("error") == "report_timeout" for row in records)
    assert capture.read_bytes() == b"saved capture"
    with pytest.raises(ValueError, match="positive"):
        profiles.render(tmp_path, timeout=0)


def test_process_series_is_bounded_and_keeps_complete_totals(tmp_path: Path) -> None:
    """Retain all observed CPU deltas even when plot points are decimated."""
    path = tmp_path / "processes.jsonl"
    with path.open("w", encoding="utf-8") as output:
        for tick in range(6000):
            for pid, role in ((1, "main"), (2, "collector")):
                output.write(
                    json.dumps(
                        {
                            "at": tick,
                            "pid": pid,
                            "created": 0,
                            "role": role,
                            "cpu_user": tick,
                            "cpu_system": 0,
                            "rss": 100,
                            "io": {"read_bytes": tick, "write_bytes": 0},
                        }
                    )
                    + "\n"
                )
        output.write('{"unfinished":')
    series, totals = measurements.process_series(path)
    assert len(series) <= 2000
    assert totals["main"]["cpu"] == 5999
    assert totals["observer"]["io"] == 5999
    assert totals["main"]["peak_rss"] == 100
    report.plot_resources(series, tmp_path / "resources.png")
    assert (tmp_path / "resources.png").stat().st_size > 1000


def test_process_series_does_not_count_recycled_pid_or_missing_io(
    tmp_path: Path,
) -> None:
    """Do not attribute another process's counters to a recycled PID."""
    path = tmp_path / "processes.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "at": at,
                    "pid": 10,
                    "created": birth,
                    "role": "worker",
                    "cpu_user": cpu,
                    "cpu_system": 0,
                    "rss": 100,
                    "io": None,
                }
            )
            for at, birth, cpu in ((0, 0, 10), (1, 1, 100))
        )
        + "\n"
    )
    _, totals = measurements.process_series(path)
    assert totals["worker"]["cpu"] == 0
    assert totals["worker"]["io_missing"]


def test_inventory_excludes_shadow_copies() -> None:
    """Remote and local inventories share the active-part scope after FREEZE."""
    with patch("docker.from_env"):
        inventory = importlib.import_module("tests.benchmark.copy_memory.inventory")
    env = Mock()
    uuid = "12345678-1234-1234-1234-123456789abc"
    env.query.side_effect = [uuid, "20 20 38", "/s3/", "all_1_1_0\nall_2_2_0"]
    env.exec.side_effect = [
        '{"files": 20, "apparent_size": 100, "disk_usage": 81920, "by_extension": {"bin": 20}}',
        "5\n",
        '{"path": "data.tar.gz", "files": 20}',
    ]
    with patch.object(inventory.s3, "S3Client") as client:
        client.return_value.get_object_size.return_value = 100
        result = inventory.collect(env, 2, "bench", "/config")
    sql = env.query.call_args_list[1].args[0]
    assert "startsWith(local_path, 'store/123/" in sql
    assert "AND active" in sql
    assert "disk_name = 's3'" in sql
    assert env.put_file.call_args.args[1].split("\0") == [
        f"/s3/store/123/{uuid}/all_1_1_0",
        f"/s3/store/123/{uuid}/all_2_2_0",
        "",
    ]
    assert result["local_files"] == result["unique_objects"] == 20
    assert result["inventory_scope"] == "active_s3_parts"


def test_report_does_not_mix_copy_modes_or_profiled_runs() -> None:
    """Forecast each measured mode independently and reject failed/profile controls."""
    stage = {"stage": "plan", "main_peak": 100, "worker_peaks": {}}
    base = {"variant": "candidate", "exit_code": 0, "stages": [stage], "summary": {}}
    points = [
        {
            "version": "26.3",
            "parts": 10000,
            "inventory": {"unique_objects": 100, "parts": 10000},
            "runs": [
                {**base, "kind": "fresh", "mode": "full"},
                {**base, "kind": "fresh_planning", "mode": "planning_only"},
                {**base, "kind": "fresh_metadata", "mode": "metadata_only"},
                {
                    **base,
                    "kind": "control_memory_1",
                    "mode": "full",
                    "profile": "memory",
                },
                {**base, "kind": "fresh", "mode": "full", "exit_code": 1},
            ],
        }
    ]
    for mode in ("full", "planning_only", "metadata_only"):
        assert len(report.series_points(points, "26.3", "main", "fresh", mode)) == 1


def test_overhead_ignores_broken_profiles() -> None:
    """Only usable profiles can represent profiling overhead."""
    runs = [
        {
            "kind": "control_plain_1",
            "variant": "candidate",
            "duration": 10,
            "exit_code": 0,
        },
        {
            "kind": "control_cpu_1",
            "variant": "candidate",
            "duration": 12,
            "exit_code": 0,
            "profile": "cpu",
            "profile_status": "complete",
        },
        {
            "kind": "control_cpu_2",
            "variant": "candidate",
            "duration": 999,
            "exit_code": 0,
            "profile": "cpu",
            "profile_status": "incomplete",
        },
    ]
    text = "\n".join(
        measurements.overhead_lines([{"version": "26.3", "parts": 50, "runs": runs}])
    )
    assert "| cpu | 1 | 12.0 | 12.0–12.0 | +20.0%" in text
    assert "999" not in text


def test_missing_mode_hook_cannot_turn_planning_into_full_copy(tmp_path: Path) -> None:
    """Reject an incompatible baseline even when optional observation hooks may be absent."""
    observer = probe.Probe(tmp_path, 1, False, strict=False)
    with patch.object(probe.importlib, "import_module", return_value=SimpleNamespace()):
        with pytest.raises(RuntimeError, match="not found"):
            observer._patch(  # pylint: disable=protected-access
                "module",
                "BackupCopier",
                "_copy_objects",
                lambda method: method,
                required=True,
            )
