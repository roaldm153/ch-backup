"""
Run ch-backup with memory probes on stages of copying.

The probe wraps methods of the copier, so production code stays unchanged. A stage ends with
peaks of the main and worker processes taken from VmHWM, which is reset when the stage starts.
Stages form a stack: a nested stage pauses the outer one, so their segments do not overlap.
A sampler thread writes memory of the processes to samples.csv. Spawned workers import this
file as __mp_main__, so only definitions are at the top level.
"""

# pylint: disable=protected-access

import argparse
import csv
import importlib
import json
import os
import sys
import threading
import time
import tracemalloc
import weakref
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import psutil

COPIER = "ch_backup.backup.copier"
CONTEXT = "ch_backup.backup.copy_context"
LAYOUT = "ch_backup.backup.layout"

STAGES = (
    (COPIER, "BackupCopier", "_select_backups", "select"),
    (COPIER, "BackupCopier", "_validate_backup_names", "select"),
    (COPIER, "BackupCopier", "_get_source_backup", "load_metadata"),
    (COPIER, "BackupCopier", "_get_target_metadata", "load_metadata"),
    (COPIER, "BackupCopier", "_make_target_metadata", "target_metadata"),
    (CONTEXT, "CopyContext", "load", "journal_load"),
    (COPIER, "BackupCopier", "_plan_files_scope", "plan_files"),
    (COPIER, "BackupCopier", "_plan_cloud_storage_scope", "plan_cloud_storage"),
    (COPIER, "BackupCopier", "_validate_destinations", "validate"),
    (CONTEXT, "CopyContext", "save_manifest", "save_manifest"),
    (COPIER, "BackupCopier", "_leave_out_confirmed_objects", "resume_filter"),
    (COPIER, "BackupCopier", "_list_target_objects", "resume_listing"),
    (COPIER, "BackupCopier", "_copy_scope", "copy"),
    (
        LAYOUT,
        "BackupLayout",
        "upload_cloud_storage_metadata_files",
        "upload_rewritten_metadata",
    ),
    (COPIER, "BackupCopier", "_record_archives", "record_archives"),
    (COPIER, "BackupCopier", "_upload_target_metadata", "publish"),
)

HOOKS = (
    (COPIER, "_ScopePlan", "__init__"),
    (CONTEXT, "CopyContext", "__init__"),
    (CONTEXT, "CopyContext", "flush"),
    (COPIER, "BackupCopier", "_transfer_backup"),
    (COPIER, "BackupCopier", "_copy_objects"),
)

SAMPLE_FIELDS = (
    "t",
    "stage",
    "main_rss",
    "main_anon",
    "main_uss",
    "main_pss",
    "workers",
    "workers_rss",
    "workers_uss",
    "workers_pss",
    "worker_max_uss",
)


class PlanningDone(BaseException):
    """Stops copying once it is planned, before anything is written to the target storage."""


def read_status(pid: int) -> dict[str, int]:
    """Return memory fields of /proc/<pid>/status in bytes."""
    fields = {}
    with open(f"/proc/{pid}/status", encoding="utf-8") as status:
        for line in status:
            key, _, value = line.partition(":")
            if key in ("VmHWM", "VmRSS", "RssAnon"):
                fields[key] = int(value.split()[0]) * 1024
    return fields


def read_rollup(pid: int) -> dict[str, int]:
    """Return RSS, PSS and USS of the process in bytes from /proc/<pid>/smaps_rollup."""
    fields = {}
    with open(f"/proc/{pid}/smaps_rollup", encoding="utf-8") as rollup:
        for line in rollup:
            key, _, value = line.partition(":")
            if key in ("Rss", "Pss", "Private_Clean", "Private_Dirty"):
                fields[key] = int(value.split()[0]) * 1024
    return {
        "rss": fields.get("Rss", 0),
        "pss": fields.get("Pss", 0),
        "uss": fields.get("Private_Clean", 0) + fields.get("Private_Dirty", 0),
    }


def reset_peak(pid: int) -> None:
    """Reset VmHWM of the process to its current RSS."""
    with open(f"/proc/{pid}/clear_refs", "w", encoding="utf-8") as clear_refs:
        clear_refs.write("5")


def cpu_seconds(pid: int) -> float:
    """Return CPU time used by the process."""
    times = psutil.Process(pid).cpu_times()
    return times.user + times.system


def worker_pids() -> list[int]:
    """Return PIDs of worker processes, without the resource tracker of multiprocessing."""
    pids = []
    for child in psutil.Process().children(recursive=True):
        try:
            if not any("resource_tracker" in arg for arg in child.cmdline()):
                pids.append(child.pid)
        except psutil.Error:
            continue
    return pids


class Probe:
    """Collects stages, peaks and samples of memory of one ch-backup run."""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self, out: Path, interval: float, trace: bool, strict: bool = True
    ) -> None:
        self.out = out
        self.interval = interval
        self.trace = trace
        self.strict = strict
        self.started = time.monotonic()
        self.stack = ["other"]
        self.lock = threading.RLock()
        self.plans: weakref.WeakSet = weakref.WeakSet()
        self.contexts: weakref.WeakSet = weakref.WeakSet()
        self.result: Any = None
        self.counters = {"listing_keys": 0, "flushes": 0, "flush_failures": 0}
        self.missing: list[str] = []
        self.errors: list[str] = []
        self.segment_start = self.started
        self.segment_rss = 0
        self.segment_cpu: dict[int, float] = {}
        self.stopped = threading.Event()
        self.sampler = threading.Thread(target=self._sample_loop, daemon=True)

    def install(self, planning_only: bool, metadata_only: bool) -> None:
        """Wrap stage methods and hooks, with switches of the run mode."""
        self._patch(COPIER, "_ScopePlan", "__init__", self._tracker(self.plans))
        self._patch(CONTEXT, "CopyContext", "__init__", self._tracker(self.contexts))
        self._patch(CONTEXT, "CopyContext", "flush", self._flush_counter)
        self._patch(COPIER, "BackupCopier", "_transfer_backup", self._result_catcher)
        if metadata_only:
            self._patch(COPIER, "BackupCopier", "_copy_objects", _without_objects)
        if planning_only:
            self._patch(
                COPIER, "BackupCopier", "_upload_target_metadata", _stop_planned
            )
        for module, owner, name, stage in STAGES:
            self._patch(module, owner, name, self._stage_wrapper(stage))

    def start(self) -> None:
        """Start the first segment and the sampler."""
        if self.trace:
            tracemalloc.start(25)
        self._open_segment()
        self.sampler.start()

    def finish(self, code: int, outcome: str, error: str | None) -> None:
        """Close the last segment, stop the sampler and save the summary of the run."""
        with self.lock:
            self._close_segment()
        self.stopped.set()
        self.sampler.join()
        summary = {
            "exit_code": code,
            "outcome": outcome,
            "error": error,
            "duration": time.monotonic() - self.started,
            "missing": self.missing,
            "errors": self.errors,
            "counters": self._counters(),
            "python": sys.version,
        }
        (self.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    def enter(self, stage: str) -> None:
        """Pause the current segment and start a segment of the stage."""
        with self.lock:
            self._close_segment()
            self.stack.append(stage)
            self._open_segment()

    def leave(self) -> None:
        """Finish the segment of the current stage and resume the outer one."""
        with self.lock:
            self._close_segment()
            self.stack.pop()
            self._open_segment()

    def _patch(
        self, module: str, owner_name: str, name: str, make: Callable[[Any], Any]
    ) -> None:
        """Replace the method with a wrapper, failing on a missing one unless allowed."""
        try:
            owner = getattr(importlib.import_module(module), owner_name, None)
        except ImportError:
            owner = None
        original = vars(owner).get(name) if owner is not None else None
        if original is None:
            target = f"{module}.{owner_name}.{name}"
            if self.strict:
                raise RuntimeError(f"Method {target} is not found")
            self.missing.append(target)
            return
        setattr(owner, name, make(original))

    def _stage_wrapper(self, stage: str) -> Callable[[Any], Any]:
        """Return a factory of wrappers that run the method as a stage."""

        def make(original: Callable) -> Callable:
            """Wrap the method into the stage."""

            @wraps(original)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                """Run the method in its own segment."""
                name = stage
                if stage == "copy":
                    name = f"copy_{getattr(args[1], 'value', args[1])}"
                self.enter(name)
                try:
                    value = original(*args, **kwargs)
                    if stage == "resume_listing":
                        self.counters["listing_keys"] = len(value)
                    return value
                finally:
                    self.leave()

            return wrapper

        return make

    @staticmethod
    def _tracker(instances: weakref.WeakSet) -> Callable[[Any], Any]:
        """Return a factory of constructors that remember created objects."""

        def make(original: Callable) -> Callable:
            """Wrap the constructor."""

            @wraps(original)
            def wrapper(obj: Any, *args: Any, **kwargs: Any) -> None:
                """Create the object and remember it."""
                original(obj, *args, **kwargs)
                instances.add(obj)

            return wrapper

        return make

    def _flush_counter(self, original: Callable) -> Callable:
        """Wrap flush of the copy context to count writes of the journal and their failures."""

        @wraps(original)
        def wrapper(context: Any, *args: Any, **kwargs: Any) -> None:
            """Flush the journal and count the attempt."""
            index = getattr(context, "_index", 0)
            original(context, *args, **kwargs)
            if getattr(context, "_index", 0) > index:
                self.counters["flushes"] += 1
                if getattr(context, "_flushed_size", 0):
                    self.counters["flush_failures"] += 1

        return wrapper

    def _result_catcher(self, original: Callable) -> Callable:
        """Wrap the transfer of the backup to keep its result for progress."""

        @wraps(original)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            """Remember the result of the copied backup and transfer it."""
            self.result = next(
                (
                    arg
                    for arg in (*args, *kwargs.values())
                    if type(arg).__name__ == "BackupCopyResult"
                ),
                None,
            )
            original(*args, **kwargs)

        return wrapper

    def _counters(self) -> dict[str, int]:
        """Return sizes of structures that grow with the number of objects."""
        plans = list(self.plans)
        contexts = list(self.contexts)
        result = self.result
        return {
            "plan_entries": sum(len(getattr(p, "entries", ())) for p in plans),
            "rewritten_archives": sum(
                len(getattr(p, "rewritten_metadata", ())) for p in plans
            ),
            "rewritten_files": sum(
                len(files)
                for p in plans
                for _, files in getattr(p, "rewritten_metadata", ())
            ),
            "journal_digests": sum(len(getattr(c, "_recorded", ())) for c in contexts),
            "journal_buffer": sum(len(getattr(c, "_buffer", ())) for c in contexts),
            "objects_total": (result.objects_total or 0) if result else 0,
            "objects_skipped": result.objects_skipped if result else 0,
            "objects_copied": result.objects_copied if result else 0,
            **self.counters,
        }

    def _open_segment(self) -> None:
        """Reset peaks of the processes and remember where the segment starts."""
        for pid in (os.getpid(), *worker_pids()):
            try:
                reset_peak(pid)
            except FileNotFoundError:
                continue
            except OSError as e:
                if len(self.errors) < 10:
                    self.errors.append(f"clear_refs of {pid}: {e}")
        if self.trace:
            tracemalloc.reset_peak()
        self.segment_cpu = self._cpu()
        self.segment_start = time.monotonic()
        self.segment_rss = read_status(os.getpid())["VmRSS"]

    def _close_segment(self) -> None:
        """Save peaks of the processes reached in the current segment."""
        end = time.monotonic()
        cpu = {
            pid: seconds - self.segment_cpu.get(pid, 0)
            for pid, seconds in self._cpu().items()
        }
        status = read_status(os.getpid())
        workers = {}
        for pid in worker_pids():
            try:
                workers[str(pid)] = read_status(pid)["VmHWM"]
            except (FileNotFoundError, KeyError):
                continue
        record = {
            "stage": self.stack[-1],
            "depth": len(self.stack) - 1,
            "start": self.segment_start - self.started,
            "end": end - self.started,
            "rss_start": self.segment_rss,
            "rss_end": status["VmRSS"],
            "main_peak": status["VmHWM"],
            "worker_peaks": workers,
            "cpu_main": cpu.pop(os.getpid(), 0),
            "cpu_workers": sum(cpu.values()),
            "counters": self._counters(),
        }
        if self.trace:
            record["traced"], record["traced_peak"] = tracemalloc.get_traced_memory()
            if end - self.segment_start >= 1:
                self._save_snapshot(self.stack[-1], end - self.started)
        _append_line(self.out / "stages.jsonl", record)

    @staticmethod
    def _cpu() -> dict[int, float]:
        """Return CPU time used by the main and worker processes."""
        cpu = {}
        for pid in (os.getpid(), *worker_pids()):
            try:
                cpu[pid] = cpu_seconds(pid)
            except psutil.Error:
                continue
        return cpu

    def _save_snapshot(self, stage: str, at: float) -> None:
        """Save lines of code holding most of the traced memory."""
        snapshot = tracemalloc.take_snapshot().filter_traces(
            (
                tracemalloc.Filter(False, tracemalloc.__file__),
                tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
                tracemalloc.Filter(False, "<unknown>"),
            )
        )
        top = [
            {
                "where": f"{stat.traceback[0].filename}:{stat.traceback[0].lineno}",
                "size": stat.size,
                "count": stat.count,
            }
            for stat in snapshot.statistics("lineno")[:30]
        ]
        _append_line(
            self.out / "tracemalloc.jsonl", {"stage": stage, "at": at, "top": top}
        )

    def _sample(self) -> list:
        """Return a row of memory of the main and worker processes."""
        memory = read_rollup(os.getpid())
        anon = read_status(os.getpid()).get("RssAnon", 0)
        workers = []
        for pid in worker_pids():
            try:
                workers.append(read_rollup(pid))
            except (FileNotFoundError, ProcessLookupError):
                continue
        return [
            round(time.monotonic() - self.started, 3),
            self.stack[-1],
            memory["rss"],
            anon,
            memory["uss"],
            memory["pss"],
            len(workers),
            sum(w["rss"] for w in workers),
            sum(w["uss"] for w in workers),
            sum(w["pss"] for w in workers),
            max((w["uss"] for w in workers), default=0),
        ]

    def _sample_loop(self) -> None:
        """Write samples and progress until the run finishes, flushing every row."""
        with open(self.out / "samples.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(SAMPLE_FIELDS)
            while not self.stopped.wait(self.interval):
                try:
                    writer.writerow(self._sample())
                    f.flush()
                    self._save_progress()
                except Exception as e:
                    if len(self.errors) < 10:
                        self.errors.append(f"sampler: {e}")

    def _save_progress(self) -> None:
        """Save copied and pending objects for the driver."""
        counters = self._counters()
        progress = {
            "t": time.monotonic() - self.started,
            "stage": self.stack[-1],
            "copied": counters["objects_copied"],
            "pending": counters["objects_total"] - counters["objects_skipped"],
        }
        path = self.out / "progress.json"
        path.with_suffix(".tmp").write_text(json.dumps(progress))
        path.with_suffix(".tmp").replace(path)


def _append_line(path: Path, record: dict) -> None:
    """Append the record to the file of JSON lines, so it survives a killed process."""
    with open(path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")


def _without_objects(original: Callable) -> Callable:
    """Wrap copying of objects to upload only rewritten disk metadata."""

    @wraps(original)
    def wrapper(copier: Any, backup: Any, _pending: Any, *args: Any) -> None:
        """Copy no objects."""
        original(copier, backup, [], *args)

    return wrapper


def _stop_planned(original: Callable) -> Callable:
    """Wrap the upload of metadata of the copy to stop the run before it."""

    @wraps(original)
    def wrapper(*_args: Any, **_kwargs: Any) -> None:
        """Stop the run."""
        raise PlanningDone()

    return wrapper


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse options of the probe and the command of ch-backup after them."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="Directory for results.")
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--tracemalloc", action="store_true")
    parser.add_argument("--planning-only", action="store_true")
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Skip methods missing in the tested code instead of failing.",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    return args


def main() -> None:
    """Run the ch-backup command under the probe and exit with its code."""
    args = _parse_args(sys.argv[1:])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    probe = Probe(out, args.interval, args.tracemalloc, strict=not args.allow_missing)
    probe.install(args.planning_only, args.metadata_only)
    cli = importlib.import_module("ch_backup").main
    code, outcome, error = 0, "completed", None
    probe.start()
    try:
        cli.main(args=args.command, prog_name="ch-backup")
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else int(e.code is not None)
    except PlanningDone:
        outcome = "planning_only"
    except BaseException as e:
        code, error = 1, f"{type(e).__name__}: {e}"
    finally:
        if code and outcome == "completed":
            outcome = "failed"
        probe.finish(code, outcome, error)
    sys.exit(code)


if __name__ == "__main__":
    main()
