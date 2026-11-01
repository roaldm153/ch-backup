"""Collect Linux process and container metrics independently of ch-backup."""

import argparse
import csv
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import psutil

from tests.benchmark.copy_memory.measurements import s3_stats
from tests.benchmark.copy_memory.probe import SAMPLE_FIELDS

CGROUP_FILES = (
    "memory.current",
    "memory.peak",
    "memory.stat",
    "memory.events",
    "cpu.stat",
    "cpu.max",
    "io.stat",
    "cpu.pressure",
    "memory.pressure",
    "io.pressure",
)


def read_json(path: Path) -> dict:
    """Read a completed atomic checkpoint, tolerating a missing writer."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def append(path: Path, record: dict) -> None:
    """Commit one complete JSON line without depending on target process shutdown."""
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record) + "\n")


def proc_text(pid: int, name: str) -> str:
    """Read a proc file without converting missing or forbidden metrics to zero."""
    try:
        return Path(f"/proc/{pid}/{name}").read_text(encoding="utf-8")
    except OSError:
        return ""


def namespace_pid(pid: int) -> int | None:
    """Return the PID seen by the process inside its innermost PID namespace."""
    for line in proc_text(pid, "status").splitlines():
        if line.startswith("NSpid:"):
            return int(line.split()[-1])
    return None


def cgroup_stats(pid: int) -> dict:
    """Read the container's cgroup v2 counters, explicitly identifying absent files."""
    relative = next(
        (
            line[3:]
            for line in proc_text(pid, "cgroup").splitlines()
            if line.startswith("0::")
        ),
        None,
    )
    if relative is None:
        return {"unavailable": "cgroup v2 not found"}
    root = Path("/sys/fs/cgroup") / relative.lstrip("/")
    record: dict = {"path": relative, "missing": []}
    for name in CGROUP_FILES:
        try:
            record[name] = (root / name).read_text(encoding="utf-8").strip()
        except OSError:
            record["missing"].append(name)
    return record


def process_stats(pid: int, detailed: bool) -> dict:
    """Read CPU, I/O and memory for one PID and record its birth time."""
    process = psutil.Process(pid)
    with process.oneshot():
        memory, cpu = process.memory_info(), process.cpu_times()
        record = {
            "pid": pid,
            "created": process.create_time(),
            "rss": memory.rss,
            "cpu_user": cpu.user,
            "cpu_system": cpu.system,
            "threads": process.num_threads(),
            "context_switches": process.num_ctx_switches()._asdict(),
        }
        try:
            record["io"] = process.io_counters()._asdict()
        except (psutil.Error, AttributeError):
            record["io"] = None
    if detailed:
        try:
            full = process.memory_full_info()
            record.update(uss=full.uss, pss=full.pss, detailed_at=time.monotonic())
        except (psutil.Error, AttributeError):
            record.update(uss=None, pss=None, detailed_at=None)
    return record


class CPUWindows:
    """Run bounded py-spy captures and retain every completed window separately."""

    def __init__(self, duration: int, windows: int, native: bool) -> None:
        """Keep profiler state outside the measured process tree."""
        self.duration, self.windows, self.native = duration, windows, native
        self.process: subprocess.Popen | None = None
        self.capture: dict = {}
        self.count = 0
        self.directory: Path | None = None

    def poll(self, directory: Path, pid: int, stage: str) -> None:
        """Close a finished window and start another while the target is alive."""
        if directory != self.directory:
            self.stop()
            self.directory, self.count = directory, 0
        if self.process is not None and self.process.poll() is not None:
            self.stop()
        if self.process is not None or self.count >= self.windows:
            return
        self.count += 1
        filename = directory / f"cpu-{self.count:02d}.speedscope.json"
        command = [
            "py-spy",
            "record",
            "--pid",
            str(pid),
            "--subprocesses",
            "--format",
            "speedscope",
            "--rate",
            "49",
            "--duration",
            str(self.duration),
            "--output",
            str(filename),
        ]
        if self.native:
            command.append("--native")
        self.capture = {
            "file": filename.name,
            "pid": pid,
            "stage": stage,
            "started": time.monotonic(),
        }
        with (directory / f"cpu-{self.count:02d}.log").open(
            "w", encoding="utf-8"
        ) as output:
            self.process = subprocess.Popen(  # pylint: disable=consider-using-with
                command, stdout=output, stderr=subprocess.STDOUT
            )
        (directory / "cpu-active").touch()

    def stop(self) -> None:
        """Ask py-spy to finish its output before escalating to termination."""
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.capture.update(ended=time.monotonic(), exit_code=self.process.returncode)
        assert self.directory is not None
        append(self.directory / "cpu-windows.jsonl", self.capture)
        (self.directory / "cpu-active").unlink(missing_ok=True)
        self.process = None


class Collector:
    """Observe one point, including backup preparation and all measured copies."""

    # pylint: disable=too-many-instance-attributes
    def __init__(self, args: argparse.Namespace) -> None:
        """Initialize bounded caches and one optional external CPU profiler."""
        self.args = args
        self.out = args.point / "telemetry"
        self.out.mkdir(parents=True, exist_ok=True)
        self.details: dict = {}
        self.containers: dict[str, list[int]] = {}
        self.refreshed = 0.0
        self.cpu = CPUWindows(args.cpu_window, args.cpu_windows, args.native)
        self.seen: set[tuple[int, float]] = set()
        self.coverage: dict[str, dict] = {}
        self.s3_sampled = 0.0

    def _containers(self, now: float) -> None:
        """Discover container PIDs without reading command arguments or environment."""
        if now - self.refreshed < 5:
            return
        self.refreshed = now
        for role, name in (
            ("clickhouse", self.args.container),
            ("seaweedfs", self.args.storage),
        ):
            result = subprocess.run(
                ["docker", "top", name, "-eo", "pid"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
            pids = [
                int(line.strip())
                for line in result.stdout.splitlines()[1:]
                if line.strip().isdigit()
            ]
            self.containers[role] = pids
            if pids:
                append(
                    self.out / "containers.jsonl",
                    {"at": now, "role": role, **cgroup_stats(pids[0])},
                )

    def _process(self, pid: int, role: str, now: float) -> dict | None:
        """Cache expensive rollups for five seconds, keyed by PID and birth time."""
        try:
            identity = (pid, psutil.Process(pid).create_time())
            cached = self.details.get(identity, {})
            detailed = now - cached.get("checked_at", 0) >= 5
            record = process_stats(pid, detailed)
            if detailed:
                self.details[identity] = {
                    key: record.get(key) for key in ("uss", "pss", "detailed_at")
                }
                self.details[identity]["checked_at"] = now
            record.update(self.details.get(identity, {}), at=now, role=role)
            self.seen.add(identity)
            append(self.out / "processes.jsonl", record)
            return record
        except (psutil.Error, ProcessLookupError):
            return None

    def _target(self, info: dict) -> int | None:
        """Resolve the recorded namespace PID without accepting a recycled process."""
        for pid in self.containers.get("clickhouse", []):
            try:
                if (
                    namespace_pid(pid) == info.get("pid")
                    and abs(psutil.Process(pid).create_time() - info.get("created", 0))
                    < 0.01
                ):
                    return pid
            except psutil.Error:
                continue
        return None

    def sample(self) -> None:
        """Append a sample even if ch-backup has not reached the end of its stage."""
        now = time.monotonic()
        self._containers(now)
        current = read_json(self.out / "current.json")
        directory = self.args.point / current.get("run", "backup")
        info = read_json(directory / "probe.json")
        progress = read_json(directory / "progress.json")
        target = self._target(info)
        children: set[int] = set()
        if target is not None:
            try:
                children = {
                    child.pid
                    for child in psutil.Process(target).children(recursive=True)
                    if not any("resource_tracker" in arg for arg in child.cmdline())
                }
            except psutil.Error:
                target = None
        self.seen.clear()
        measured = []
        for role, pids in self.containers.items():
            for pid in pids:
                label = (
                    "main" if pid == target else "worker" if pid in children else role
                )
                row = self._process(pid, label, now)
                if row and label in ("main", "worker"):
                    measured.append(row)
        self._process(os.getpid(), "collector", now)
        self._process(self.args.driver_pid, "driver", now)
        if self.cpu.process and self.cpu.process.poll() is None:
            self._process(self.cpu.process.pid, "profiler", now)
        self.details = {
            key: value for key, value in self.details.items() if key in self.seen
        }
        self._host(now)
        if target is not None and measured:
            self._samples(directory, info, progress, measured, now)
            coverage = self.coverage.setdefault(str(directory), {})
            for row in measured:
                inner = namespace_pid(row["pid"])
                if inner is not None:
                    coverage[str(inner)] = {
                        "created": row["created"],
                        "role": row["role"],
                    }
            temporary = directory / "observed.tmp.json"
            temporary.write_text(json.dumps(coverage), encoding="utf-8")
            temporary.replace(directory / "observed.json")
            if now - self.s3_sampled >= 5:
                append(
                    directory / "s3-timeline.jsonl",
                    {"at": now, "operations": s3_stats(directory)},
                )
                self.s3_sampled = now
            if current.get("profile") == "cpu":
                self.cpu.poll(directory, target, progress.get("stage", "other"))
            else:
                self.cpu.stop()
            (directory / "collector-ready").touch()
        else:
            self.cpu.stop()
        self._limits(directory, target)
        (self.out / "heartbeat").write_text(str(now), encoding="utf-8")

    def _samples(
        self, directory: Path, info: dict, progress: dict, rows: list[dict], now: float
    ) -> None:
        """Preserve the existing CSV format and add a near-simultaneous process total."""
        primary = next((row for row in rows if row["role"] == "main"), None)
        if primary is None:
            return
        workers = [row for row in rows if row["role"] == "worker"]
        values = {
            "t": round(now - info["started"], 3),
            "stage": progress.get("stage", "other"),
            "main_rss": primary["rss"],
            "main_anon": "",
            "main_uss": primary.get("uss"),
            "main_pss": primary.get("pss"),
            "workers": len(workers),
            "workers_rss": sum(row["rss"] for row in workers),
            "workers_uss": total(workers, "uss"),
            "workers_pss": total(workers, "pss"),
            "worker_max_uss": (
                max((row["uss"] for row in workers), default=0)
                if all(row.get("uss") is not None for row in workers)
                else None
            ),
        }
        path = directory / "samples.csv"
        new = not path.exists()
        with path.open("a", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=SAMPLE_FIELDS)
            if new:
                writer.writeheader()
            writer.writerow(values)

    def _host(self, now: float) -> None:
        """Save pressure, free space and CPU counters separately from application memory."""
        pressure: dict = {}
        for name in ("cpu", "memory", "io"):
            try:
                pressure[name] = Path(f"/proc/pressure/{name}").read_text(
                    encoding="utf-8"
                )
            except OSError:
                pressure[name] = None
        append(
            self.out / "host.jsonl",
            {
                "at": now,
                "available": psutil.virtual_memory().available,
                "swap": psutil.swap_memory()._asdict(),
                "cpu": psutil.cpu_times()._asdict(),
                "disk_free": shutil.disk_usage(self.out).free,
                "pressure": pressure,
            },
        )

    def _limits(self, directory: Path, target: int | None) -> None:
        """Stop the measured process before profiling exhausts the results filesystem."""
        size = sum(
            path.stat().st_size for path in self.args.point.glob("*/memory-*.bin")
        )
        reason = None
        if size > self.args.max_profile_gib * 1024**3:
            reason = "profile_size_limit"
        if shutil.disk_usage(self.out).free < self.args.min_free_gib * 1024**3:
            reason = "disk_space_limit"
        if reason:
            (self.out / "limit.json").write_text(
                json.dumps({"reason": reason}), encoding="utf-8"
            )
            if target and not (directory / "rc").exists():
                process = psutil.Process(target)
                info = read_json(directory / "probe.json")
                if abs(process.create_time() - info.get("created", 0)) < 0.01:
                    process.send_signal(signal.SIGTERM)

    def run(self) -> None:
        """Keep collecting until the driver asks for shutdown or disappears."""
        owner = psutil.Process(self.args.driver_pid)
        try:
            while owner.is_running() and not (self.out / "stop").exists():
                started = time.monotonic()
                try:
                    self.sample()
                except Exception as error:
                    append(
                        self.out / "errors.jsonl",
                        {"at": time.monotonic(), "error": str(error)},
                    )
                time.sleep(max(0.05, self.args.interval - (time.monotonic() - started)))
        finally:
            self.cpu.stop()


def total(rows: list[dict], name: str) -> int | None:
    """Sum complete measurements, leaving unavailable private memory unknown."""
    if any(row.get(name) is None for row in rows):
        return None
    return sum(row[name] for row in rows)


def main() -> None:
    """Start the host collector without importing Docker's integration environment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--point", type=Path, required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--storage", required=True)
    parser.add_argument("--driver-pid", type=int, required=True)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--cpu-window", type=int, default=120)
    parser.add_argument("--cpu-windows", type=int, default=3)
    parser.add_argument("--native", action="store_true")
    parser.add_argument("--min-free-gib", type=float, default=20)
    parser.add_argument("--max-profile-gib", type=float, default=4)
    Collector(parser.parse_args()).run()


if __name__ == "__main__":
    main()
