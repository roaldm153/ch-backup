"""
Driver of the benchmark.

For each version of ClickHouse the images are built once. Each number of parts gets fresh
containers: the table, its backup, the inventory and copies under the probe. A point runs full
copies while the speed of the first full copy fits the next point into the time budget, and only
plans copying after that. With metadata of version 5, rewritten disk metadata is also uploaded
without copying objects, so the resume can be measured.
"""

import json
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml

from tests.benchmark.copy_memory import dataset, inventory
from tests.benchmark.copy_memory.env import TARGET_BUCKET, Environment
from tests.integration.modules.utils import merge

BACKUP_NAME = "bench"
CONFIG_PATH = "/tmp/bench/ch-backup.conf"
ORIGINAL_CONFIG_PATH = "/etc/yandex/ch-backup/ch-backup.conf"
PROBE = "/code/tests/benchmark/copy_memory/probe.py"
BASELINE_DIR = Path("staging/baseline")
SESSIONS_DIR = Path("staging/benchmark/sessions")
TARGET_CLOUD_BUCKET = "cloud-storage-02"
TRACEMALLOC_OBJECTS = 300_000
INTERRUPT_AT = 0.9
CLICKHOUSE_LOG_CONFIG = """<clickhouse>
    <logger>
        <level>information</level>
    </logger>
</clickhouse>
"""


@dataclass
class Options:
    """Parameters of the benchmark."""

    parts: list[int]
    versions: list[str]
    columns: int = 120
    workers: int = 4
    baseline_ref: str | None = None
    budget_hours: float = 3
    planning_only_from: int | None = None
    interval: float = 0.5
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

    @property
    def name(self) -> str:
        """Return the directory name of the run."""
        return f"{self.kind}-{self.variant}"

    @property
    def path_root(self) -> str:
        """Return the target prefix, separate for runs that need an empty target."""
        return f"bench_{self.prefix}_{self.variant}"


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


def plan_runs(smallest: bool, full: bool, metadata_v5: bool, objects: int) -> list[Run]:
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
    if objects >= TRACEMALLOC_OBJECTS:
        runs.append(Run("tracemalloc", "planning_only", "tracemalloc"))
    return runs


class Driver:
    """Runs points of the benchmark and saves their results."""

    def __init__(self, options: Options) -> None:
        self.options = options
        self.results = options.results
        self.speed: float | None = None

    def run(self) -> None:
        """Run all versions and points."""
        self.results.mkdir(parents=True)
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        self._save_system()
        options = {**asdict(self.options), "results": str(self.results)}
        if self.options.baseline_ref:
            options["baseline_commit"] = self._prepare_baseline()
        _write_json(self.results / "options.json", options)
        (self.results / "config-overrides.yaml").write_text(
            yaml.safe_dump(overrides(self.options.workers))
        )
        for version in self.options.versions:
            slug = version.replace(".", "-")
            env = Environment(
                version,
                f"bench-{self.results.name.lower()}-{slug}",
                SESSIONS_DIR / f"{self.results.name}-{slug}.sav",
            )
            _log(f"ClickHouse {version}: building images")
            env.create()
            self.speed = None
            for parts in sorted(self.options.parts):
                self._point(env, version, parts)
        _log(f"Results: {self.results}")

    def _point(self, env: Environment, version: str, parts: int) -> None:
        """Measure one number of parts, saving what was done even if the point fails."""
        point_dir = self.results / version / str(parts)
        point_dir.mkdir(parents=True)
        point: dict = {"version": version, "parts": parts, "timings": {}, "runs": []}
        timings = point["timings"]
        _log(f"ClickHouse {version}, {parts} parts")
        started = time.monotonic()
        env.start()
        try:
            timings["start"] = time.monotonic() - started
            self._configure(env)
            dataset.create_table(env, self.options.columns, parts)
            timings["insert"] = dataset.insert_parts(env, self.options.columns, parts)
            dataset.check_parts(env, parts)
            timings["backup"] = self._backup(env, point_dir)
            started = time.monotonic()
            inv = inventory.collect(env, parts, BACKUP_NAME, CONFIG_PATH)
            timings["inventory"] = time.monotonic() - started
            _write_json(point_dir / "inventory.json", inv)
            objects = inv["unique_objects"]
            full = self._fits(parts, objects)
            point["mode"] = "full" if full else "planning_only"
            for run in plan_runs(
                parts == min(self.options.parts),
                full,
                inv["metadata_format_version"] >= 5,
                objects,
            ):
                variants = ["candidate"]
                if self.options.baseline_ref:
                    variants.append("baseline")
                for variant in variants:
                    run.variant = variant
                    point["runs"].append(self._probe(env, point_dir, run))
                    _write_json(point_dir / "point.json", point)
        except Exception as e:
            point["error"] = f"{type(e).__name__}: {e}"
            _log(f"Point failed: {point['error']}")
        finally:
            _write_json(point_dir / "point.json", point)
            env.stop()

    def _fits(self, parts: int, objects: int) -> bool:
        """Check that full copies of the point fit into the time budget."""
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

    def _backup(self, env: Environment, point_dir: Path) -> float:
        """Back up the table with the config of integration tests and return the duration."""
        workdir = in_container(point_dir / "backup")
        started = time.monotonic()
        env.start_detached(
            [
                "ch-backup",
                "-c",
                ORIGINAL_CONFIG_PATH,
                "backup",
                "--name",
                BACKUP_NAME,
                "--databases",
                dataset.DATABASE,
            ],
            workdir,
        )
        code = env.wait_detached(workdir)
        env.chown(workdir)
        if code:
            raise RuntimeError(f"Backup exited with {code}, see {point_dir}/backup")
        return time.monotonic() - started

    def _probe(self, env: Environment, point_dir: Path, run: Run) -> dict:
        """Run the copy under the probe and return its outcome."""
        host_dir = point_dir / run.name
        workdir = in_container(host_dir)
        if run.kind == "resume":
            env.reset_copy_state(run.path_root, BACKUP_NAME)
        command = ["python3", PROBE, "--out", workdir]
        command += ["--interval", str(self.options.interval)]
        if run.mode != "full":
            command.append(f"--{run.mode.replace('_', '-')}")
        if run.kind == "tracemalloc":
            command.append("--tracemalloc")
        variables = {}
        if run.variant == "baseline":
            command.append("--allow-missing")
            variables["PYTHONPATH"] = f"/code/{BASELINE_DIR}"
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

        env.start_detached(command, workdir, variables)
        code = env.wait_detached(workdir, poll)
        duration = time.monotonic() - started
        env.chown(workdir)
        summary = _read_json(host_dir / "summary.json") or {}
        result = {
            **asdict(run),
            "exit_code": code,
            "duration": duration,
            "outcome": summary.get("outcome"),
            "interrupted": interrupted,
            "oom_events": _oom_events(wall_start),
        }
        counters = summary.get("counters", {})
        if (
            run.kind == "fresh"
            and run.variant == "candidate"
            and not code
            and counters.get("objects_copied")
        ):
            self.speed = counters["objects_copied"] / duration
            result["objects_per_second"] = self.speed
        _log(f"    exit code {code}, {duration:.0f} s, {summary.get('outcome')}")
        return result

    def _save_system(self) -> None:
        """Save the hardware, disks and code the benchmark runs on."""
        commands = (
            ["git", "rev-parse", "HEAD"],
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

    def _prepare_baseline(self) -> str:
        """Check out the baseline into a worktree and return its commit."""
        ref = self.options.baseline_ref
        assert ref
        if BASELINE_DIR.exists():
            subprocess.run(
                ["git", "-C", str(BASELINE_DIR), "checkout", "--detach", ref],
                check=True,
            )
        else:
            subprocess.run(
                ["git", "worktree", "add", "--detach", str(BASELINE_DIR), ref],
                check=True,
            )
        shutil.copy("ch_backup/version.txt", BASELINE_DIR / "ch_backup/version.txt")
        return subprocess.run(
            ["git", "-C", str(BASELINE_DIR), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()


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
    """Write the JSON file."""
    path.write_text(json.dumps(data, indent=2) + "\n")


def _log(message: str) -> None:
    """Print the message with the time."""
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)
