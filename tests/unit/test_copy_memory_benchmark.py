"""
Unit tests of the copy memory benchmark: wrapped methods, the dataset and the report.
"""

import importlib
import json
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
