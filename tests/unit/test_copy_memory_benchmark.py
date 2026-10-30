"""
Unit tests of the copy memory benchmark: wrapped methods, the dataset and the report.
"""

import importlib
import json
import shlex
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from tests.benchmark.copy_memory import dataset, probe, report


@pytest.mark.parametrize("exit_code", [0, 7])
def test_detached_command_pid_and_exit_code(tmp_path: Path, exit_code: int) -> None:
    """Save the command's own PID and exit code in a new directory."""
    with patch("docker.from_env"):
        environment = importlib.import_module("tests.benchmark.copy_memory.env")
    workdir = tmp_path / "new directory"
    command = [
        sys.executable,
        "-c",
        f"import os, sys; print(os.getpid()); sys.exit({exit_code})",
    ]
    with patch.object(environment.subprocess, "run") as run:
        environment.Environment.start_detached(Mock(), command, str(workdir))
    subprocess.run(["bash", "-c", run.call_args.args[0][-1]], check=True)

    assert (workdir / "pid").read_text() == (workdir / "output.log").read_text()
    assert int((workdir / "rc").read_text()) == exit_code


def test_detached_timeout(tmp_path: Path) -> None:
    """Stop a sleeping command and record GNU timeout's exit code."""
    executable = shutil.which("timeout") or shutil.which("gtimeout")
    if not executable:
        pytest.skip("GNU timeout is available in the Linux benchmark container")
    with patch("docker.from_env"):
        environment = importlib.import_module("tests.benchmark.copy_memory.env")
    command = [sys.executable, "-c", "import time; time.sleep(60)"]
    with patch.object(environment.subprocess, "run") as run:
        environment.Environment.start_detached(
            Mock(), command, str(tmp_path), timeout=0.1
        )
    script = run.call_args.args[0][-1].replace(
        "timeout --signal", f"{shlex.quote(executable)} --signal"
    )
    subprocess.run(["bash", "-c", script], check=True, timeout=5)
    assert int((tmp_path / "rc").read_text()) == 124


def test_backup_freeze_timeout(tmp_path: Path) -> None:
    """Pass the requested timeout to the source backup, outside the measured copy."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    options = driver.Options(
        parts=[10000], versions=["25.8.23.13"], freeze_timeout=21600
    )
    benchmark = driver.Driver(options)
    env = Mock()
    env.wait_detached.return_value = 0
    with patch.object(driver, "in_container", return_value="/code/backup"):
        benchmark._backup(env, tmp_path)  # pylint: disable=protected-access
    command = env.start_detached.call_args.args[0]
    index = command.index("--config-parameter")
    assert command[index + 1 : index + 4] == [
        "clickhouse.freeze_timeout",
        "21600",
        "backup",
    ]


def test_failed_point_saves_diagnostics_before_cleanup(tmp_path: Path) -> None:
    """Keep diagnostic files before removing a failed point's containers."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    options = driver.Options(parts=[10000], versions=["25.8.23.13"], results=tmp_path)
    benchmark = driver.Driver(options)
    env = Mock()
    with patch.object(benchmark, "_configure", side_effect=RuntimeError("failed")):
        benchmark._point(env, "25.8.23.13", 10000)  # pylint: disable=protected-access
    names = [call[0] for call in env.mock_calls]
    assert names.index("save_diagnostics") < names.index("stop")
    point = json.loads((tmp_path / "25.8.23.13/10000/point.json").read_text())
    assert point["error"] == "RuntimeError: failed"
    assert "RuntimeError: failed" in (tmp_path / "report.md").read_text()


def test_diagnostics_survive_command_timeout(tmp_path: Path) -> None:
    """Try remaining diagnostics if one command times out."""
    with patch("docker.from_env"):
        environment = importlib.import_module("tests.benchmark.copy_memory.env")
    with patch.object(
        environment.subprocess,
        "run",
        side_effect=[subprocess.TimeoutExpired("tail", 30), Mock(), Mock()],
    ) as run:
        environment.Environment.save_diagnostics(Mock(), tmp_path)
    assert run.call_count == 3
    assert "Diagnostics failed" in (tmp_path / "server-logs.tar.errors").read_text()
    assert (tmp_path / "freeze.jsonl").exists()
    assert (tmp_path / "seaweedfs.log").exists()


@pytest.mark.parametrize(
    "code,duration,outcome",
    [
        (0, 1, "completed"),
        (124, 432001, "timeout"),
        (137, 432060, "timeout"),
        (137, 1, "failed"),
    ],
)
def test_probe_budget(tmp_path: Path, code: int, duration: float, outcome: str) -> None:
    """Apply the deadline to the first copy and distinguish an early process kill."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    benchmark = driver.Driver(driver.Options(parts=[10000], versions=["26.3.12.3"]))
    env = Mock()
    env.wait_detached.return_value = code
    with (
        patch.object(driver, "in_container", return_value="/code/fresh"),
        patch.object(driver, "_oom_events", return_value=[]),
        patch.object(
            driver,
            "_read_json",
            return_value={"outcome": "completed" if code == 0 else "failed"},
        ),
        patch.object(driver.time, "monotonic", side_effect=[0, duration]),
    ):
        result = benchmark._probe(  # pylint: disable=protected-access
            env, tmp_path, driver.Run("fresh", "full", "fresh")
        )
    assert env.start_detached.call_args.kwargs["timeout"] == 120 * 3600
    assert result["outcome"] == outcome


@pytest.mark.parametrize("timeout", [False, True])
def test_point_checkpoints_and_logs(tmp_path: Path, timeout: bool) -> None:
    """Report preparation and each copy, keep logs and stop after a timeout."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    benchmark = driver.Driver(
        driver.Options(parts=[10000], versions=["26.3.12.3"], results=tmp_path)
    )
    env = Mock()
    point_dir = tmp_path / "26.3.12.3/10000"
    checkpoints = []

    def snapshot(_root):
        """Capture the persisted point at each report refresh."""
        checkpoints.append(json.loads((point_dir / "point.json").read_text()))

    result = {
        "exit_code": 124 if timeout else 0,
        "outcome": "timeout" if timeout else "completed",
        "interrupted": False,
    }
    with (
        patch.object(benchmark, "_configure"),
        patch.object(benchmark, "_backup", return_value=10),
        patch.object(benchmark, "_probe", return_value=result) as run,
        patch.object(driver.dataset, "create_table"),
        patch.object(driver.dataset, "insert_parts", return_value=1),
        patch.object(driver.dataset, "check_parts"),
        patch.object(
            driver.inventory,
            "collect",
            return_value={"unique_objects": 100, "metadata_format_version": 5},
        ),
        patch.object(driver.report, "build", side_effect=snapshot),
    ):
        benchmark._point(env, "26.3.12.3", 10000)  # pylint: disable=protected-access
    assert checkpoints[0]["status"] == "backup_completed"
    assert checkpoints[0]["timings"]["backup"] == 10
    assert checkpoints[1]["status"] == "copying"
    assert len(checkpoints[2]["runs"]) == 1
    assert checkpoints[-1]["status"] == ("timeout" if timeout else "completed")
    assert run.call_count == (1 if timeout else 4)
    assert benchmark.full_copy_timed_out == timeout
    names = [call[0] for call in env.mock_calls]
    assert names.index("save_diagnostics") < names.index("stop")
    if timeout:
        assert not benchmark._fits(20000, 200)  # pylint: disable=protected-access


def test_timeout_excluded_from_forecast(tmp_path: Path) -> None:
    """Retain partial peaks in the report without using them to forecast a complete run."""
    _write_point(tmp_path, 1000, 300000)
    _write_point(tmp_path, 3000, 900000)
    points = report.load(tmp_path)
    run = points[1]["runs"][0]
    run.update(exit_code=124, outcome="timeout")
    assert len(report.series_points(points, "25.8.23.13", "main", "fresh")) == 1
    assert not report.bytes_per_object(points, "25.8.23.13")
    assert "timeout" in report.render_markdown(points, [])


def test_live_run_records_before_start_and_after_failure(tmp_path: Path) -> None:
    """Keep the run and periodic checkpoints when the probe fails before returning."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    benchmark = driver.Driver(
        driver.Options(parts=[10000], versions=["26.3"], results=tmp_path)
    )
    point: dict = {"runs": []}
    snapshots = []

    def snapshot(_root):
        """Read the manifest as an external report reader would."""
        snapshots.append(json.loads((tmp_path / "point.json").read_text()))

    def fail(_env, _directory, _run, refresh):
        """Fail after two polls, only one of which should refresh the report."""
        assert snapshots[0]["runs"][0]["outcome"] == "running"
        refresh()
        refresh()
        raise RuntimeError("probe lost")

    with (
        patch.object(driver.report, "build", side_effect=snapshot),
        patch.object(benchmark, "_probe", side_effect=fail),
        patch.object(driver.time, "monotonic", side_effect=[0, 30, 61, 62]),
        pytest.raises(RuntimeError, match="probe lost"),
    ):
        benchmark._run(  # pylint: disable=protected-access
            Mock(), tmp_path, point, driver.Run("fresh", "full", "fresh")
        )
    assert len(snapshots) == 3
    assert snapshots[1]["runs"][0]["duration"] == 61
    assert snapshots[-1]["runs"][0]["outcome"] == "incomplete"


@pytest.mark.parametrize(
    "code,outcome", [(None, "running"), (137, "failed"), (124, "timeout")]
)
def test_report_without_finished_stages(
    tmp_path: Path, code: int | None, outcome: str
) -> None:
    """Report samples even if SIGKILL prevented stage and summary files from being written."""
    _write_point(tmp_path, 1000, 300000)
    point_file = tmp_path / "25.8.23.13/1000/point.json"
    point = json.loads(point_file.read_text())
    point["runs"] = point["runs"][:1]
    point["runs"][0].update(exit_code=None, outcome="running", duration=0)
    point_file.write_text(json.dumps(point))
    run_dir = point_file.parent / "fresh-candidate"
    (run_dir / "stages.jsonl").write_text('{"unfinished":')
    (run_dir / "summary.json").write_text('{"outcome":')
    (run_dir / "samples.csv").write_text(
        ",".join(probe.SAMPLE_FIELDS)
        + "\n"
        + f"1,copy_cloud_storage,{3 * report.GIB},0,0,0,4,{report.GIB},0,0,0\n"
        + f"2,copy_cloud_storage,{2 * report.GIB},0,0,0,4,{report.GIB},0,0,0\n"
        + "3,copy_cloud_storage,999999999999"
    )
    if code is not None:
        (run_dir / "rc").write_text(str(code))
    report.build(tmp_path)
    loaded = report.load(tmp_path)
    run = loaded[0]["runs"][0]
    assert run["outcome"] == outcome
    assert run["sample_stats"]["t"] == 2
    assert run["sample_stats"]["peak_main_rss"] == 3 * report.GIB
    assert run["sample_stats"]["main_rss"] == 2 * report.GIB
    assert not report.series_points(loaded, "25.8.23.13", "main", "fresh")
    markdown = (tmp_path / "report.md").read_text()
    assert "2.00 / 1.00" in markdown
    assert "3.00 / 1.00" in markdown
    assert "timeline_25.8.23.13_1000_fresh-candidate.png" in markdown
    assert outcome in markdown
    assert (
        tmp_path / "timeline_25.8.23.13_1000_fresh-candidate.png"
    ).stat().st_size > 0


def test_interrupted_chart_keeps_previous_file(tmp_path: Path) -> None:
    """Do not replace the last usable chart with a half-written image."""
    out = tmp_path / "chart.png"
    out.write_bytes(b"previous chart")

    def fail(path, **_kwargs):
        """Simulate a renderer dying after writing only part of a PNG."""
        path.write_bytes(b"partial chart")
        raise RuntimeError("render failed")

    with pytest.raises(RuntimeError, match="render failed"):
        report._save_figure(Mock(savefig=fail), out)  # pylint: disable=protected-access
    assert out.read_bytes() == b"previous chart"


@pytest.mark.parametrize(
    "module,owner,name",
    [stage[:3] for stage in probe.STAGES] + list(probe.HOOKS),
)
def test_probe_targets_exist(module: str, owner: str, name: str) -> None:
    assert name in vars(getattr(importlib.import_module(module), owner))


@pytest.mark.parametrize(
    "count,expected",
    [
        (
            120,
            {
                "numeric": 70,
                "String": 20,
                "Nullable(UInt32)": 15,
                "LowCardinality(String)": 10,
                "Array(UInt32)": 5,
            },
        ),
        (
            10,
            {
                "numeric": 6,
                "String": 2,
                "Nullable(UInt32)": 1,
                "LowCardinality(String)": 1,
            },
        ),
    ],
)
def test_dataset_columns(count: int, expected: dict) -> None:
    columns = dataset.columns(count)
    numeric = {column_type for column_type, _ in dataset.NUMERIC_TYPES}
    kinds = Counter(
        "numeric" if column_type in numeric else column_type
        for _, column_type, _ in columns
    )
    assert dict(kinds) == expected
    assert columns[0][:2] == ("c001", "UInt64")
    assert len({name for name, _, _ in columns}) == count


def _segment(stage: str, start: float, end: float, main: int, worker: int) -> dict:
    return {
        "stage": stage,
        "depth": 1,
        "start": start,
        "end": end,
        "rss_start": main // 2,
        "rss_end": main,
        "main_peak": main,
        "worker_peaks": {"101": worker, "102": worker // 2},
        "cpu_main": 1.0,
        "cpu_workers": 2.0,
        "counters": {"plan_entries": main // 1000},
    }


def _write_point(root: Path, parts: int, objects: int) -> None:
    point_dir = root / "25.8.23.13" / str(parts)
    runs = []
    for kind in ("fresh", "resume", "tracemalloc"):
        run_dir = point_dir / f"{kind}-candidate"
        run_dir.mkdir(parents=True)
        main = objects * 1000 * (2 if kind == "resume" else 1)
        stages = [
            _segment("other", 0, 1, main // 10, 0),
            _segment("plan_cloud_storage", 1, 5, main, objects * 10),
            _segment("copy_cloud_storage", 5, 9, main // 2, objects * 20),
        ]
        (run_dir / "stages.jsonl").write_text(
            "\n".join(json.dumps(s) for s in stages) + "\n{broken"
        )
        (run_dir / "summary.json").write_text(
            json.dumps({"outcome": "completed", "counters": {"objects_total": objects}})
        )
        (run_dir / "samples.csv").write_text(
            ",".join(probe.SAMPLE_FIELDS)
            + "\n"
            + "\n".join(
                f"{t},plan_cloud_storage,{main},{main},{main},{main},2,1,{objects},1,1"
                for t in range(10)
            )
            + "\n"
        )
        if kind == "tracemalloc":
            top = [
                {
                    "where": f"/usr/lib/ch_backup/backup/copier.py:{i}",
                    "size": i,
                    "count": 1,
                }
                for i in range(19, 0, -1)
            ]
            (run_dir / "tracemalloc.jsonl").write_text(
                json.dumps({"stage": "plan_cloud_storage", "at": 5, "top": top})
            )
        runs.append(
            {
                "kind": kind,
                "mode": "planning_only" if kind == "tracemalloc" else "full",
                "prefix": kind,
                "variant": "candidate",
                "exit_code": 0,
                "duration": 10.0,
            }
        )
    (point_dir / "point.json").write_text(
        json.dumps({"version": "25.8.23.13", "parts": parts, "runs": runs})
    )
    (point_dir / "inventory.json").write_text(
        json.dumps(
            {
                "files_per_part": 308,
                "unique_objects": objects,
                "object_refs": objects,
                "metadata_format_version": 4,
                "local_metadata_disk_usage": 1 << 20,
                "disk_metadata_tarballs": [
                    {"path": "x", "compressed": 1 << 20, "unpacked": 1 << 24}
                ],
            }
        )
    )


def test_report(tmp_path: Path) -> None:
    _write_point(tmp_path, 1000, 300_000)
    _write_point(tmp_path, 3000, 900_000)

    report.build(tmp_path)

    for name in (
        "peak_vs_objects.png",
        "stage_peaks.png",
        "structures.png",
        "timeline_25.8.23.13_1000_fresh-candidate.png",
    ):
        assert (tmp_path / name).stat().st_size > 0
    text = (tmp_path / "report.md").read_text()
    assert "| plan_cloud_storage | 1000 | 33.5 |" in text
    assert "copier.py:19" in text


def test_peaks_and_stage_table() -> None:
    stages = [
        _segment("plan_files", 0, 1, 100, 10),
        _segment("other", 1, 2, 50, 30),
        _segment("plan_files", 2, 4, 80, 20),
    ]

    assert report.peaks(stages) == {"main": 100, "worker": 30, "total": 115}
    table = report.stage_table(stages)
    assert table["plan_files"]["duration"] == 3
    assert table["plan_files"]["main_peak"] == 100
    assert table["plan_files"]["growth"] == 90
