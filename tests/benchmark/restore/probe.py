"""Run restore with the shared collector contract and per-worker CPU/S3 metrics."""

import importlib
import json
import sys
import time
import traceback
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "copy_memory"))
base = importlib.import_module("probe")
sys.path.insert(0, str(Path(__file__).resolve().parent))
RestoreMetrics = importlib.import_module("metrics").RestoreMetrics

STAGES = (
    ("ch_backup.backup.layout", "BackupLayout", "get_backup", "metadata"),
    ("ch_backup.logic.database", "DatabaseBackup", "restore", "schema"),
    (
        "ch_backup.logic.part_restore",
        "LocalPartRestorer",
        "wait",
        "local_drain_validate",
    ),
    (
        "ch_backup.clickhouse.disks",
        "ClickHouseTemporaryDisks",
        "copy_parts",
        "cloud_copy",
    ),
    (
        "ch_backup.clickhouse.control",
        "ClickhouseCTL",
        "chown_detached_table_parts",
        "chown",
    ),
    ("ch_backup.clickhouse.control", "ClickhouseCTL", "attach_part", "attach"),
)


class RestoreProbe(base.Probe):  # type: ignore[name-defined]
    """Observe main-process restore phases with the existing progress protocol."""

    def install(self, planning_only: bool = False, metadata_only: bool = False) -> None:
        """Require actual restore hooks instead of installing copy-planning hooks."""
        if planning_only or metadata_only:
            raise ValueError("Copy-only modes are not supported")
        for module, owner, name, stage in STAGES:
            self._patch(module, owner, name, self._stage_wrapper(stage), required=True)


def instrument() -> Any:
    """Enable identical instrumentation in the main interpreter and spawn workers."""
    observer = base._instrument()  # pylint: disable=protected-access
    if observer is not None:
        RestoreMetrics(observer.metrics.registry).install()
    return observer


def main() -> None:
    """Execute a restore command, preserving its exit status and partial artifacts."""
    args = base._parse_args(sys.argv[1:])  # pylint: disable=protected-access
    if args.planning_only or args.metadata_only:
        raise ValueError("Restore does not support copy-only modes")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    observer = instrument()
    probe = RestoreProbe(out, args.interval, args.tracemalloc)
    probe.install()
    probe.start()
    code, error = 0, None
    error_frames = []
    restore_started = None
    try:
        if args.wait_collector:
            deadline = time.monotonic() + 60
            while not (out / "collector-ready").exists():
                if time.monotonic() >= deadline:
                    raise RuntimeError("Collector did not observe restore")
                time.sleep(0.1)
        restore_started = time.monotonic()
        importlib.import_module("ch_backup").main.main(
            args=args.command, prog_name="ch-backup"
        )
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else int(exc.code is not None)
    except BaseException as exc:
        code, error = 1, type(exc).__name__
        error_frames = [
            {"file": frame.filename, "line": frame.lineno, "function": frame.name}
            for frame in traceback.extract_tb(exc.__traceback__)
        ]
    finally:
        elapsed = (
            time.monotonic() - restore_started if restore_started is not None else None
        )
        probe.finish(code, "failed" if code else "completed", error)
        summary_path = out / "summary.json"
        summary = json.loads(summary_path.read_text())
        summary["restore_seconds"] = elapsed
        summary["error_frames"] = error_frames
        summary_path.write_text(json.dumps(summary))
        if observer:
            observer.close()
    sys.exit(code)


if __name__ == "__mp_main__":
    instrument()
elif __name__ == "__main__":
    main()
