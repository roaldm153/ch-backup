"""Compare pinned branches against one backup in an isolated test environment."""

import argparse
import csv
import json
import math
import statistics
import time
import uuid
from pathlib import Path

from tests.benchmark.copy_memory.driver import (
    CONFIG_PATH,
    SESSIONS_DIR,
    TOOLS_PYTHON,
    Driver,
    Options,
    Run,
    in_container,
)
from tests.benchmark.copy_memory.env import Environment
from tests.benchmark.restore import dataset
from tests.benchmark.restore.profiles import cpu_coverage


def save(path: Path, data: dict) -> None:
    """Atomically retain progress even when the series is interrupted."""
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temporary.replace(path)


def order(variants: list[str], repeat: int) -> list[str]:
    """Rotate branches so each occupies different positions between repeats."""
    offset = repeat % len(variants)
    return variants[offset:] + variants[:offset]


class RestoreDriver(Driver):
    """Reuse the copy harness environment, budgets, telemetry and CPU profiler."""

    def __init__(self, args: argparse.Namespace) -> None:
        """Keep branch selection separate from the fixed scenario configuration."""
        super().__init__(
            Options(
                parts=[args.parts],
                versions=[args.version],
                workers=args.workers,
                results=args.results,
                total_budget_hours=args.hours,
                budget_hours=args.hours,
                profile_budget_hours=args.hours,
                cpu_window=120,
                cpu_windows=math.ceil(args.hours * 3600 / 120) + 1,
                native_profiles=args.native,
            )
        )
        self.args = args

    def run(self) -> None:
        """Pin inputs, create one backup, then restore every branch from scratch."""
        self.results.mkdir(parents=True, exist_ok=False)
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        self._save_system()
        revisions = {
            f"branch-{index}": {
                "ref": ref,
                "commit": self._prepare_source(f"branch-{index}", ref),
            }
            for index, ref in enumerate(self.args.refs)
        }
        save(self.results / "revisions.json", revisions)
        save(
            self.results / "scenario.json",
            {**vars(self.args), "results": str(self.results)},
        )
        env = Environment(
            self.args.version,
            f"restore-{uuid.uuid4().hex}",
            SESSIONS_DIR / f"{self.results.name}.sav",
        )
        point_dir = self.results / self.args.version / str(self.args.parts)
        point_dir.mkdir(parents=True)
        point: dict = {"runs": [], "timings": {}, "status": "preparing"}
        try:
            with self.budget.watch():
                env.create()
                env.start()
                self._configure(env)
                self._save_environment(env, point_dir)
                self._start_collector(env, point_dir)
                fixture = dataset.create(
                    env, self.args.parts, self.args.rows, self.args.payload_bytes
                )
                save(point_dir / "fixture.json", fixture)
                point["timings"]["backup"] = self._backup(env, point_dir)
                for profile in [None, "cpu"] if self.args.cpu else [None]:
                    repeats = 1 if profile else self.args.repeats
                    for repeat in range(repeats):
                        for variant in order(list(revisions), repeat):
                            run = Run(
                                "cpu" if profile else "timing",
                                "full",
                                "restore",
                                variant=variant,
                                profile=profile,
                                repeat=repeat + 1,
                            )
                            result = self._restore(
                                env, point_dir, run, fixture["digest"]
                            )
                            point["runs"].append(result)
                            self._checkpoint(point_dir, point)
                            if result["exit_code"] or not result["verified"]:
                                raise RuntimeError(f"Restore failed: {run.name}")
                            if result.get("profile_status") == "incomplete":
                                raise RuntimeError(
                                    f"CPU profile incomplete: {run.name}"
                                )
                point["status"] = "completed"
        except BaseException as error:
            point.update(status="failed", error=type(error).__name__)
            raise
        finally:
            self._finish_point(env, point_dir, point)

    def _checkpoint(self, point_dir: Path, point: dict) -> None:
        """Save restore results without invoking the copy-only report renderer."""
        save(point_dir / "point.json", point)
        rows = []
        for variant in self.sources:
            runs = [
                run
                for run in point["runs"]
                if run["variant"] == variant
                and not run["profile"]
                and run["verified"]
                and not run["exit_code"]
                and run.get("restore_seconds") is not None
            ]
            seconds = [run["restore_seconds"] for run in runs]
            if seconds:
                rows.append(
                    {
                        "variant": variant,
                        "runs": len(seconds),
                        "median_seconds": statistics.median(seconds),
                        "min_seconds": min(seconds),
                        "max_seconds": max(seconds),
                    }
                )
        if rows:
            revisions_file = self.results / "revisions.json"
            revisions = (
                json.loads(revisions_file.read_text())
                if revisions_file.exists()
                else {}
            )
            baseline = next(
                (
                    row["median_seconds"]
                    for row in rows
                    if row["variant"] == next(iter(self.sources))
                ),
                None,
            )
            for row in rows:
                row.update(revisions.get(row["variant"], {}))
                row["speedup_vs_first_ref"] = (
                    baseline / row["median_seconds"] if baseline else None
                )
            with (self.results / "comparison.csv").open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

    def _restore(
        self, env: Environment, point_dir: Path, run: Run, expected: str
    ) -> dict:
        """Restore into an empty test database with a unique state file, then verify."""
        host_dir = point_dir / run.name
        workdir = in_container(host_dir)
        env.query("DROP DATABASE bench SYNC")
        save(
            point_dir / "telemetry/current.json",
            {"run": run.name, "profile": run.profile},
        )
        command = [
            TOOLS_PYTHON,
            "/code/tests/benchmark/restore/probe.py",
            "--out",
            workdir,
            "--wait-collector",
            "--",
            "-c",
            CONFIG_PATH,
            "--config-parameter",
            "backup.restore_context_path",
            json.dumps(f"{workdir}/state.json"),
            "--config-parameter",
            "storage.chunk_size",
            str(self.args.chunk_mib * 1024**2),
            "restore",
            "bench",
            "--databases",
            "bench",
        ]
        variables = {
            "PYTHONPATH": self._code_path(run.variant),
            "CH_BACKUP_BENCH_INSTRUMENTATION": json.dumps(
                {"out": workdir, "memory": False}
            ),
        }
        started = time.monotonic()
        env.start_detached(
            command,
            workdir,
            variables,
            timeout=self.budget.limit(self.args.hours * 3600),
        )
        code = env.wait_detached(workdir, lambda: self._health(env, point_dir, workdir))
        env.chown(workdir)
        summary_file = host_dir / "summary.json"
        summary = json.loads(summary_file.read_text()) if summary_file.exists() else {}
        result = {
            "name": run.name,
            "variant": run.variant,
            "repeat": run.repeat,
            "profile": run.profile,
            "exit_code": code,
            "wall_seconds": time.monotonic() - started,
            "restore_seconds": summary.get("restore_seconds"),
            "summary": summary,
            "verified": False,
        }
        result.update(self._source_result(host_dir, run.variant))
        if not result["exit_code"]:
            result["verified"] = dataset.digest(env) == expected
        result.update(self._profile_result(env, host_dir, workdir, run))
        if run.profile == "cpu":
            result.update(cpu_coverage(host_dir))
        return result


def main() -> None:
    """Parse a list of branch refs and bounded, identical restore scenarios."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refs", nargs="+", required=True)
    parser.add_argument("--version", default="25.8.23.13")
    parser.add_argument("--parts", type=int, default=16)
    parser.add_argument("--rows", type=int, default=131072)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--chunk-mib", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--hours", type=float, default=12)
    parser.add_argument("--cpu", action="store_true")
    parser.add_argument("--native", action="store_true")
    parser.add_argument(
        "--results",
        type=Path,
        default=Path("staging/benchmark") / time.strftime("restore-%Y%m%dT%H%M%S"),
    )
    args = parser.parse_args()
    if (
        min(
            args.parts,
            args.rows,
            args.payload_bytes,
            args.workers,
            args.chunk_mib,
            args.repeats,
            args.hours,
        )
        <= 0
    ):
        parser.error("All sizes, counts and budgets must be positive")
    if args.payload_bytes % 8 or not math.isfinite(args.hours):
        parser.error("Payload must be divisible by eight and budget must be finite")
    RestoreDriver(args).run()
