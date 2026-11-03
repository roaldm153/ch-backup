"""
Driver of the benchmark.

For each version of ClickHouse the images are built once. Each number of parts gets fresh
containers: the table, its backup, the inventory and copies under the probe. A point runs full
copies while the speed of the first full copy fits the next point into the time budget, and only
plans copying after that. With metadata of version 5, rewritten disk metadata is also uploaded
without copying objects, so the resume can be measured.

Every copy has a wall-clock limit, including the first. Reports are refreshed after preparing
the backup and after each copy. Logs are saved before removing containers on success or failure.
"""

import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Callable

import yaml

from tests.benchmark.copy_memory import dataset, inventory, profiles, report
from tests.benchmark.copy_memory.budget import Budget, BudgetExpired
from tests.benchmark.copy_memory.env import TARGET_BUCKET, CleanupError, Environment
from tests.integration.modules.utils import merge

BACKUP_NAME = "bench"
CONFIG_PATH = "/tmp/bench/ch-backup.conf"
ORIGINAL_CONFIG_PATH = "/etc/yandex/ch-backup/ch-backup.conf"
PROBE = "/code/tests/benchmark/copy_memory/probe.py"
TOOLS_PYTHON = "/tmp/bench-tools/bin/python"
SOURCES_DIR = Path("staging/benchmark/sources")
SESSIONS_DIR = Path("staging/benchmark/sessions")
TARGET_CLOUD_BUCKET = "cloud-storage-02"
TRACEMALLOC_OBJECTS = 300_000
INTERRUPT_AT = 0.9
REPORT_INTERVAL = 60
CLICKHOUSE_LOG_CONFIG = """<clickhouse>
    <logger>
        <level>information</level>
    </logger>
</clickhouse>
"""


@dataclass
class Options:
    """Parameters of the benchmark."""

    # pylint: disable=too-many-instance-attributes

    parts: list[int]
    versions: list[str]
    columns: int = 120
    workers: int = 4
    baseline_ref: str | None = None
    candidate_ref: str | None = None
    sqlite_cache_mib: int | None = None
    baseline_sqlite_cache_mib: int | None = None
    ab: bool = False
    copy_mode: str = "auto"
    repeats: int = 1
    cleanup_timeout: float = 1800
    budget_hours: float = 120
    freeze_timeout: int = 21600
    planning_only_from: int | None = None
    interval: float = 0.5
    overnight: bool = False
    total_budget_hours: float | None = None
    profiles: bool = False
    profile_repeats: int = 1
    profile_budget_hours: float = 2
    memory_format: str = "aggregated"
    cpu_window: int = 120
    cpu_windows: int = 3
    native_profiles: bool = False
    trace_python_allocators: bool = False
    tracemalloc: bool = False
    min_free_gib: float = 20
    max_profile_gib: float = 4
    results: Path = field(
        default_factory=lambda: Path("staging/benchmark")
        / time.strftime("%Y%m%dT%H%M%S")
    )


@dataclass
class Run:
    """One copy under the probe."""

    kind: str
    mode: str
    prefix: str
    variant: str = "candidate"
    profile: str | None = None
    repeat: int = 0
    case: str = ""

    @property
    def name(self) -> str:
        """Return the directory name of the run."""
        suffix = f"_{self.repeat}" if self.repeat else ""
        return f"{self.kind}{suffix}-{self.variant}"

    @property
    def path_root(self) -> str:
        """Return the target prefix, separate for runs that need an empty target."""
        suffix = f"_{self.repeat}" if self.repeat else ""
        return f"bench_{self.prefix}{suffix}_{self.variant}"


def overrides(workers: int) -> dict:
    """Return changes of the ch-backup config for measured copies, without secrets."""
    return {
        "main": {"drop_privileges": False},
        "storage": {"proxy_resolver": {"uri": None}},
        "multiprocessing": {"workers": workers},
        "loguru": {
            "handlers": {
                "botocore": {
                    "filter": {
                        name: "WARNING"
                        for name in (
                            "botocore",
                            "botocore.endpoint",
                            "botocore.vendored.requests",
                            "botocore.parsers",
                        )
                    }
                },
                "urllib3.connectionpool": {"level": "WARNING"},
            }
        },
    }


def in_container(path: Path) -> str:
    """Return the path in the container, where the repository is mounted to /code."""
    relative = path.resolve().relative_to(Path.cwd().resolve())
    return f"/code/{relative}"


def plan_runs(
    smallest: bool, full: bool, metadata_v5: bool, objects: int, trace: bool = False
) -> list[Run]:
    """Return copies of the point in the order they are run."""
    runs = []
    if full:
        runs += [Run("fresh", "full", "fresh"), Run("resume", "full", "fresh")]
        if smallest:
            runs += [
                Run("interrupted", "full", "interrupted"),
                Run("resume_interrupted", "full", "interrupted"),
            ]
    else:
        runs.append(Run("fresh_planning", "planning_only", "planning"))
        if metadata_v5:
            runs += [
                Run("fresh_metadata", "metadata_only", "metadata"),
                Run("resume", "metadata_only", "metadata"),
            ]
    if trace and objects >= TRACEMALLOC_OBJECTS:
        runs.append(Run("tracemalloc", "planning_only", "tracemalloc"))
    return runs


class Driver:
    """Runs points of the benchmark and saves their results."""

    def __init__(self, options: Options) -> None:
        """Keep the run budget and collector separate from measured processes."""
        self.options = options
        self.results = options.results
        self.speed: float | None = None
        self.full_copy_timed_out = False
        self.budget = Budget(options.total_budget_hours)
        self.collector: subprocess.Popen | None = None
        self.sources: dict[str, Path] = {}

    def run(self) -> None:
        """Run all versions and points."""
        self.results.mkdir(parents=True)
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        self._save_system()
        options = {**asdict(self.options), "results": str(self.results)}
        for variant in ("baseline", "candidate"):
            ref = getattr(self.options, f"{variant}_ref")
            if ref:
                options[f"{variant}_commit"] = self._prepare_source(variant, ref)
        _write_json(self.results / "options.json", options)
        (self.results / "config-overrides.yaml").write_text(
            yaml.safe_dump(overrides(self.options.workers))
        )
        status = {"status": "running", "started": time.time()}
        _write_json(self.results / "series.json", status)
        try:
            self._versions()
            failed = any(
                (_read_json(path) or {}).get("status") != "completed"
                for path in self.results.glob("*/*/point.json")
            )
            status["status"] = "completed_with_errors" if failed else "completed"
        except BudgetExpired as error:
            status.update(status="budget_exhausted", error=str(error))
            self._skip_remaining()
        except CleanupError as error:
            status.update(status="cleanup_failed", error=str(error))
            self._skip_remaining("skipped_cleanup")
        except BaseException as error:
            status.update(status="failed", error=f"{type(error).__name__}: {error}")
            raise
        finally:
            status["ended"] = time.time()
            _write_json(self.results / "series.json", status)
            report.build(self.results)
        _log(f"Results: {self.results}")

    def _versions(self) -> None:
        """Build each version and run its points under the shared deadline."""
        for version in self.options.versions:
            slug = version.replace(".", "-")
            env = Environment(
                version,
                f"bench-{self.results.name.lower()}-{slug}",
                SESSIONS_DIR / f"{self.results.name}-{slug}.sav",
            )
            _log(f"ClickHouse {version}: building images")
            with self.budget.watch():
                env.create()
            self.speed = None
            self.full_copy_timed_out = False
            for parts in self._parts():
                self.budget.remaining()
                if (
                    shutil.disk_usage(self.results).free
                    < self.options.min_free_gib * 1024**3
                ):
                    raise RuntimeError("Insufficient free disk space for another point")
                self._point(env, version, parts)

    def _parts(self) -> list[int]:
        """Keep profiles beside their measured point, without an extra small dataset."""
        return sorted(set(self.options.parts))

    def _skip_remaining(self, status: str = "skipped_budget") -> None:
        """Make unstarted points visible after the series budget is exhausted."""
        for version in self.options.versions:
            for parts in self._parts():
                directory = self.results / version / str(parts)
                if not (directory / "point.json").exists():
                    directory.mkdir(parents=True, exist_ok=True)
                    _write_json(
                        directory / "point.json",
                        {
                            "version": version,
                            "parts": parts,
                            "runs": [],
                            "status": status,
                        },
                    )

    def _point(self, env: Environment, version: str, parts: int) -> None:
        """Measure one number of parts, saving what was done even if the point fails."""
        point_dir = self.results / version / str(parts)
        point_dir.mkdir(parents=True)
        point: dict = {"version": version, "parts": parts, "timings": {}, "runs": []}
        _log(f"ClickHouse {version}, {parts} parts")
        started = time.monotonic()
        try:
            with self.budget.watch():
                self._measure_point(env, point_dir, point, parts, started)
        except BudgetExpired as error:
            point.update(status="budget_exhausted", error=str(error))
            raise
        except (Exception, KeyboardInterrupt) as error:
            point["status"] = "failed"
            point["error"] = f"{type(error).__name__}: {error}"
            _log(f"Point failed: {point['error']}")
            if isinstance(error, KeyboardInterrupt):
                raise
        finally:
            self._finish_point(env, point_dir, point)

    def _measure_point(
        self, env: Environment, point_dir: Path, point: dict, parts: int, started: float
    ) -> None:
        """Prepare a source once and reuse it for comparable copy runs."""
        timings = point["timings"]
        env.start()
        timings["start"] = time.monotonic() - started
        self._configure(env)
        self._save_environment(env, point_dir)
        self._start_collector(env, point_dir)
        dataset.create_table(env, self.options.columns, parts)
        timings["insert"] = dataset.insert_parts(env, self.options.columns, parts)
        dataset.check_parts(env, parts)
        timings["backup"] = self._backup(env, point_dir)
        point["status"] = "backup_completed"
        self._checkpoint(point_dir, point)
        started = time.monotonic()
        inv = inventory.collect(env, parts, BACKUP_NAME, CONFIG_PATH)
        timings["inventory"] = time.monotonic() - started
        _write_json(point_dir / "inventory.json", inv)
        point["status"] = "copying"
        self._checkpoint(point_dir, point)
        objects = inv["unique_objects"]
        full = self._fits(parts, objects)
        point["mode"] = "full" if full else "planning_only"
        for run in self._runs(parts, full, inv):
            point["phase"] = "profiling" if run.profile else "measuring"
            variants = ["candidate"]
            if self.options.baseline_ref:
                variants = ["baseline", "candidate"]
                if run.repeat and run.repeat % 2 == 0:
                    variants.reverse()
            for variant in variants:
                run.variant = variant
                result = self._run(env, point_dir, point, run)
                if result.get("resource_limit"):
                    raise RuntimeError(result["resource_limit"])
                if result["outcome"] == "timeout":
                    self.budget.remaining()
                    if run.profile:
                        result.update(
                            profile_status="incomplete", profile_error="profile_timeout"
                        )
                        continue
                    self.full_copy_timed_out |= run.mode == "full"
                    point["status"] = "timeout"
                    point["error"] = (
                        f"{run.name} exceeded {self.options.budget_hours} hours"
                    )
                    return
                if result["exit_code"] and not (
                    run.kind == "interrupted" and result["interrupted"]
                ):
                    if run.profile:
                        result["profile_status"] = "incomplete"
                        continue
                    raise RuntimeError(f"{run.name} exited with {result['exit_code']}")
        point["status"] = (
            "profile_incomplete"
            if any(
                result.get("profile_status") == "incomplete" for result in point["runs"]
            )
            else "completed"
        )

    def _runs(self, parts: int, full: bool, inventory_data: dict) -> list[Run]:
        """Measure first, then profile the same source and modes with separate targets."""
        cases = plan_runs(
            parts == min(self.options.parts)
            and not (self.options.overnight or self.options.ab),
            full,
            inventory_data["metadata_format_version"] >= 5,
            inventory_data["unique_objects"],
            self.options.tracemalloc,
        )
        runs = [
            replace(run, repeat=repeat + 1 if self.options.repeats > 1 else 0)
            for repeat in range(self.options.repeats)
            for run in cases
        ]
        if self.options.profiles:
            for repeat in range(1, self.options.profile_repeats + 1):
                kinds = ("cpu", "memory") if repeat % 2 else ("memory", "cpu")
                for kind in kinds:
                    for run in cases:
                        if run.kind in (
                            "interrupted",
                            "resume_interrupted",
                            "tracemalloc",
                        ):
                            continue
                        runs.append(
                            replace(
                                run,
                                kind=f"profile_{run.kind}_{kind}",
                                case=run.kind,
                                prefix=f"profile_{run.prefix}_{kind}",
                                profile=kind,
                                repeat=repeat,
                            )
                        )
        return runs

    def _finish_point(self, env: Environment, point_dir: Path, point: dict) -> None:
        """Save logs and the report before cleaning up any completed or failed point."""
        for directory in point_dir.iterdir():
            if (
                directory.is_dir()
                and (directory / "pid").exists()
                and not (directory / "rc").exists()
            ):
                try:
                    env.signal_detached(in_container(directory))
                    deadline = time.monotonic() + 65
                    while (
                        not (directory / "rc").exists() and time.monotonic() < deadline
                    ):
                        time.sleep(1)
                except Exception as error:
                    point["termination_error"] = str(error)
        self._stop_collector(point_dir)
        try:
            env.save_diagnostics(point_dir / "diagnostics")
        except Exception as e:
            point["diagnostics_error"] = f"{type(e).__name__}: {e}"
            _log(f"Diagnostics failed: {e}")
        point["phase"] = "cleanup"
        point["cleanup"] = {"status": "running", "started": time.time()}
        try:
            self._checkpoint(point_dir, point)
        finally:
            started = time.monotonic()
            try:
                env.stop(self.options.cleanup_timeout, point_dir / "cleanup.log")
                point["cleanup"]["status"] = "completed"
            except Exception as error:
                point["cleanup"].update(
                    status=(
                        "timeout"
                        if isinstance(error.__cause__, subprocess.TimeoutExpired)
                        else "failed"
                    ),
                    error=str(error),
                )
                raise CleanupError(str(error)) from error
            finally:
                point["timings"]["cleanup"] = time.monotonic() - started
                point["cleanup"]["ended"] = time.time()
                point["phase"] = "finished"
                self._checkpoint(point_dir, point)

    def _start_collector(self, env: Environment, point_dir: Path) -> None:
        """Launch host observation before source backup preparation."""
        directory = point_dir / "telemetry"
        directory.mkdir()
        command = [
            sys.executable,
            "-m",
            "tests.benchmark.copy_memory.collector",
            "--point",
            str(point_dir.resolve()),
            "--container",
            env.container(),
            "--storage",
            env.container("seaweedfs01"),
            "--driver-pid",
            str(os.getpid()),
            "--interval",
            str(max(1, self.options.interval)),
            "--cpu-window",
            str(self.options.cpu_window),
            "--cpu-windows",
            str(self.options.cpu_windows),
            "--min-free-gib",
            str(self.options.min_free_gib),
            "--max-profile-gib",
            str(self.options.max_profile_gib),
        ]
        if self.options.native_profiles:
            command.append("--native")
        with (directory / "collector.log").open("w", encoding="utf-8") as output:
            self.collector = subprocess.Popen(  # pylint: disable=consider-using-with
                command, stdout=output, stderr=subprocess.STDOUT
            )

    def _save_environment(self, env: Environment, point_dir: Path) -> None:
        """Record versions and source hashes without serializing credentials or configuration."""
        script = (
            "import importlib.metadata as m, json, platform, sys; "
            "from ch_backup.config import Config; "
            f"config = Config({CONFIG_PATH!r}); "
            "print(json.dumps({'python': sys.version, 'platform': platform.platform(), "
            "'settings': {section: config[section] for section in "
            "('filesystem', 'compression', 'multiprocessing')}, "
            "'packages': {d.metadata['Name']: d.version for d in m.distributions()}}))"
        )
        data = json.loads(env.exec(shlex.join([TOOLS_PYTHON, "-c", script])))
        digest = hashlib.sha256()
        for path in sorted(
            [*Path("ch_backup").rglob("*.py"), *Path("tests/benchmark").rglob("*.py")]
        ):
            digest.update(str(path).encode())
            digest.update(path.read_bytes())
        data["source_sha256"] = digest.hexdigest()
        data["images"] = {}
        for name in ("clickhouse01", "seaweedfs01"):
            data["images"][name] = subprocess.run(
                ["docker", "inspect", "--format", "{{.Image}}", env.container(name)],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            ).stdout.strip()
        _write_json(point_dir / "environment.json", data)

    def _stop_collector(self, point_dir: Path) -> None:
        """Finalize CPU windows before removing the benchmark containers."""
        if self.collector:
            (point_dir / "telemetry/stop").touch()
            try:
                self.collector.wait(timeout=40)
            except subprocess.TimeoutExpired:
                self.collector.kill()
                self.collector.wait(timeout=5)
            self.collector = None

    def _checkpoint(self, point_dir: Path, point: dict) -> None:
        """Save the point and refresh the report without losing results if plotting fails."""
        _write_json(point_dir / "point.json", point)
        try:
            report.build(self.results)
        except Exception as e:
            point["report_error"] = f"{type(e).__name__}: {e}"
            _write_json(point_dir / "point.json", point)
            _log(f"Report failed: {e}")

    def _run(self, env: Environment, point_dir: Path, point: dict, run: Run) -> dict:
        """Record the run before starting it and refresh partial results while it runs."""
        result = {
            **asdict(run),
            "name": run.name,
            "exit_code": None,
            "duration": 0.0,
            "outcome": "running",
        }
        point["runs"].append(result)
        self._checkpoint(point_dir, point)
        started = last_report = time.monotonic()

        def refresh() -> None:
            """Refresh the report at most once a minute."""
            nonlocal last_report
            now = time.monotonic()
            if now - last_report >= REPORT_INTERVAL:
                result["duration"] = now - started
                self._checkpoint(point_dir, point)
                last_report = time.monotonic()

        try:
            result.update(self._probe(env, point_dir, run, refresh))
        except BaseException:
            result["outcome"] = "incomplete"
            raise
        finally:
            self._checkpoint(point_dir, point)
        return result

    def _fits(self, parts: int, objects: int) -> bool:
        """Check that full copies of the point fit into the time budget."""
        if self.full_copy_timed_out:
            return False
        if self.options.copy_mode != "auto":
            return self.options.copy_mode == "full"
        if self.options.overnight or self.options.ab:
            return False
        if self.options.planning_only_from and parts >= self.options.planning_only_from:
            return False
        if self.speed is None:
            return True
        return objects / self.speed <= self.options.budget_hours * 3600

    def _configure(self, env: Environment) -> None:
        """Put configs of ch-backup and ClickHouse into the container and restart ClickHouse."""
        base_path = Path(
            env.conf["staging_dir"], "images/clickhouse01/config/ch-backup.conf"
        )
        config = merge(
            yaml.safe_load(base_path.read_text(encoding="utf-8")),
            overrides(self.options.workers),
        )
        env.put_file(CONFIG_PATH, yaml.safe_dump(config))
        env.put_file("/etc/clickhouse-server/conf.d/bench.xml", CLICKHOUSE_LOG_CONFIG)
        env.restart_clickhouse()
        env.exec(
            "python3 -m venv --system-site-packages /tmp/bench-tools && "
            "/tmp/bench-tools/bin/pip install --disable-pip-version-check "
            "-r /code/tests/benchmark/copy_memory/requirements.txt"
        )

    def _backup(self, env: Environment, point_dir: Path) -> float:
        """Back up the table with the config of integration tests and return the duration."""
        workdir = in_container(point_dir / "backup")
        started = time.monotonic()
        env.start_detached(
            [
                "ch-backup",
                "-c",
                ORIGINAL_CONFIG_PATH,
                "--config-parameter",
                "clickhouse.freeze_timeout",
                str(self.options.freeze_timeout),
                "backup",
                "--name",
                BACKUP_NAME,
                "--databases",
                dataset.DATABASE,
            ],
            workdir,
            timeout=self.budget.limit(self.options.budget_hours * 3600),
        )
        code = env.wait_detached(workdir, lambda: self._health(env, point_dir, workdir))
        env.chown(workdir)
        if code:
            raise RuntimeError(f"Backup exited with {code}, see {point_dir}/backup")
        return time.monotonic() - started

    def _health(self, env: Environment, point_dir: Path, workdir: str) -> None:
        """Stop work if observation or the available disk budget has been lost."""
        self.budget.remaining()
        limit = _read_json(point_dir / "telemetry/limit.json")
        heartbeat = point_dir / "telemetry/heartbeat"
        stale = heartbeat.exists() and time.time() - heartbeat.stat().st_mtime > 60
        if limit or (self.collector and (self.collector.poll() is not None or stale)):
            env.signal_detached(workdir)
            raise RuntimeError(
                limit["reason"]
                if limit
                else "External collector stopped or stalled; see telemetry/collector.log"
            )

    def _probe(
        self,
        env: Environment,
        point_dir: Path,
        run: Run,
        on_update: Callable[[], None] | None = None,
    ) -> dict:
        """Run the copy under the probe and return its outcome."""
        # pylint: disable=too-many-locals,too-many-statements
        host_dir = point_dir / run.name
        workdir = in_container(host_dir)
        if (run.case or run.kind) == "resume":
            env.reset_copy_state(run.path_root, BACKUP_NAME)
        command = [TOOLS_PYTHON, PROBE, "--out", workdir]
        if self.collector:
            command.append("--wait-collector")
            _write_json(
                point_dir / "telemetry/current.json",
                {"run": run.name, "profile": run.profile},
            )
        command += ["--interval", str(self.options.interval)]
        cache = self._cache_mib(run.variant)
        if cache is not None:
            command += ["--sqlite-cache-mib", str(cache)]
        if run.mode != "full":
            command.append(f"--{run.mode.replace('_', '-')}")
        if run.kind == "tracemalloc":
            command.append("--tracemalloc")
        variables = {
            "CH_BACKUP_BENCH_INSTRUMENTATION": json.dumps(
                {
                    "out": workdir,
                    "memory": run.profile == "memory",
                    "native": self.options.native_profiles,
                    "python_allocators": self.options.trace_python_allocators,
                    "memory_format": self.options.memory_format,
                }
            )
        }
        variables["PYTHONPATH"] = self._code_path(run.variant)
        if run.variant == "baseline":
            command.append("--allow-missing")
        command += [
            "--",
            "-c",
            CONFIG_PATH,
            "copy",
            BACKUP_NAME,
            "--target-config-parameter",
            "storage.credentials.bucket",
            json.dumps(TARGET_BUCKET),
            "--target-config-parameter",
            "backup.path_root",
            json.dumps(f"{run.path_root}/"),
            "--target-cloud-storage-bucket",
            TARGET_CLOUD_BUCKET,
            "--target-cloud-storage-path",
            run.path_root,
            "--report-path",
            f"{workdir}/copy-report.json",
        ]
        _log(f"  {run.name} ({run.mode})")
        started, wall_start = time.monotonic(), time.time()
        interrupted = False

        def poll() -> None:
            """Interrupt the copy once most of its objects are copied."""
            nonlocal interrupted
            if on_update:
                on_update()
            self._health(env, point_dir, workdir)
            if run.kind != "interrupted" or interrupted:
                return
            progress = _read_json(host_dir / "progress.json")
            if (
                progress
                and progress["pending"]
                and (progress["copied"] >= INTERRUPT_AT * progress["pending"])
            ):
                env.signal_detached(workdir)
                interrupted = True

        hours = (
            self.options.profile_budget_hours
            if run.profile
            else self.options.budget_hours
        )
        timeout = self.budget.limit(hours * 3600)
        env.start_detached(command, workdir, variables, timeout=timeout)
        code = env.wait_detached(workdir, poll)
        duration = time.monotonic() - started
        env.chown(workdir)
        summary = _read_json(host_dir / "summary.json") or {}
        result = {
            **asdict(run),
            "exit_code": code,
            "duration": duration,
            "outcome": (
                "timeout"
                if code == 124 or (code == 137 and duration >= timeout)
                else summary.get("outcome")
            ),
            "interrupted": interrupted,
            "oom_events": _oom_events(wall_start),
        }
        result.update(self._profile_result(env, host_dir, workdir, run))
        if not code:
            result.update(self._source_result(host_dir, run.variant))
        limit = _read_json(point_dir / "telemetry/limit.json")
        if limit and "reason" in limit:
            result["resource_limit"] = limit["reason"]
        counters = summary.get("counters", {})
        if (
            run.kind == "fresh"
            and run.variant == "candidate"
            and not result["exit_code"]
            and counters.get("objects_copied")
        ):
            self.speed = counters["objects_copied"] / duration
            result["objects_per_second"] = self.speed
        _log(f"    exit code {code}, {duration:.0f} s, {summary.get('outcome')}")
        return result

    def _code_path(self, variant: str) -> str:
        """Choose the pinned package without silently falling back for a baseline."""
        if variant == "baseline" and variant not in self.sources:
            raise RuntimeError("Baseline worktree was not prepared")
        return (
            in_container(self.sources[variant]) if variant in self.sources else "/code"
        )

    def _cache_mib(self, variant: str) -> int | None:
        """Return the cache override for this side of the comparison."""
        return (
            self.options.baseline_sqlite_cache_mib
            if variant == "baseline"
            else self.options.sqlite_cache_mib
        )

    def _source_result(self, directory: Path, variant: str) -> dict:
        """Reject successful copies using the wrong package or an unverified cache."""
        identity = _read_json(directory / "probe.json") or {}
        if variant in self.sources:
            expected = in_container(self.sources[variant] / "ch_backup")
            if identity.get("code_root") != expected:
                return {
                    "exit_code": 1,
                    "outcome": "source_mismatch",
                    "source_error": f"Expected {expected}",
                }
        cache = self._cache_mib(variant)
        counters = (_read_json(directory / "summary.json") or {}).get("counters", {})
        if cache is not None and (
            identity.get("sqlite_cache_mib") != cache
            or not counters.get("sqlite_indexes_created")
        ):
            return {
                "exit_code": 1,
                "outcome": "cache_mismatch",
                "source_error": f"Expected verified SQLite cache of {cache} MiB",
            }
        return {}

    def _profile_result(
        self, env: Environment, host_dir: Path, workdir: str, run: Run
    ) -> dict:
        """Render and validate optional profiles outside the measured copy duration."""
        if run.profile == "memory":
            try:
                env.exec(
                    shlex.join(
                        [
                            TOOLS_PYTHON,
                            "/code/tests/benchmark/copy_memory/profiles.py",
                            workdir,
                        ]
                    )
                )
            except Exception as error:
                return {"profile_status": "incomplete", "profile_error": str(error)}
        if run.profile:
            deadline = time.monotonic() + 30
            while (host_dir / "cpu-active").exists() and time.monotonic() < deadline:
                time.sleep(0.2)
            return profiles.coverage(host_dir, run.profile)
        return {}

    def _save_system(self) -> None:
        """Save the hardware, disks and code the benchmark runs on."""
        commands = (
            ["git", "rev-parse", "HEAD"],
            ["git", "status", "--porcelain", "--untracked-files=no"],
            ["uname", "-a"],
            ["lscpu"],
            ["py-spy", "--version"],
            ["nproc"],
            ["free", "-g"],
            ["swapon", "--show"],
            ["df", "-h", "."],
            ["df", "-i", "."],
            ["docker", "info", "--format", "{{.DockerRootDir}} {{.ServerVersion}}"],
        )
        lines = []
        for command in commands:
            lines.append(f"$ {' '.join(command)}")
            try:
                process = subprocess.run(
                    command, capture_output=True, text=True, check=False
                )
                lines.append(process.stdout + process.stderr)
            except OSError as e:
                lines.append(str(e))
        (self.results / "system.txt").write_text("\n".join(lines))

    def _prepare_source(self, variant: str, ref: str) -> str:
        """Pin a measured revision in its own worktree without changing the checkout."""
        commit = subprocess.run(
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        directory = SOURCES_DIR / self.results.name / variant
        directory.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(directory), commit], check=True
        )
        shutil.copy("ch_backup/version.txt", directory / "ch_backup/version.txt")
        self.sources[variant] = directory
        return commit


def _oom_events(since: float) -> list[str]:
    """Return kernel messages about OOM since the time, if the kernel log is readable."""
    try:
        process = subprocess.run(
            ["journalctl", "-k", "--no-pager", "--since", f"@{int(since)}"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return []
    return [
        line
        for line in process.stdout.splitlines()
        if "out of memory" in line.lower() or "oom-kill" in line.lower()
    ]


def _read_json(path: Path) -> dict | None:
    """Return the JSON file, or None if it does not exist yet."""
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _write_json(path: Path, data: dict) -> None:
    """Replace a JSON file atomically so an interrupted write keeps its previous version."""
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    temporary.replace(path)


def _log(message: str) -> None:
    """Print the message with the time."""
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)
