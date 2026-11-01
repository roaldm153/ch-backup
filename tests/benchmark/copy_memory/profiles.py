"""Render Memray captures with its own reporters, including partial captures."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def coverage(directory: Path, kind: str) -> dict:
    """Validate usable captures and explicitly report missing spawn workers."""
    if kind == "cpu":
        captures = []
        paths = list(directory.glob("cpu-*.speedscope.json"))
        for path in paths:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if any(profile.get("samples") for profile in data.get("profiles", [])):
                    captures.append(path.name)
            except (OSError, ValueError):
                continue
        window_file = directory / "cpu-windows.jsonl"
        try:
            windows = (
                [
                    json.loads(line)
                    for line in window_file.read_text(encoding="utf-8").splitlines()
                ]
                if window_file.exists()
                else []
            )
        except ValueError:
            windows = [{"exit_code": -1}]
        complete = (
            bool(captures)
            and len(captures) == len(paths)
            and all(
                window.get("exit_code") == 0 and window.get("file") in captures
                for window in windows
            )
            and not (directory / "cpu-active").exists()
        )
        return {
            "profile_status": ("complete" if complete else "incomplete"),
            "cpu_captures": captures,
        }
    observed_path = directory / "observed.json"
    observed = (
        json.loads(observed_path.read_text(encoding="utf-8"))
        if observed_path.exists()
        else {}
    )
    manifests = {}
    for path in directory.glob("instrumentation-*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        manifests[str(data["pid"])] = data
    missing = [
        pid
        for pid, info in observed.items()
        if pid not in manifests
        or not manifests[pid].get("memory")
        or manifests[pid].get("created") != info["created"]
    ]
    unfinished = [
        pid for pid, data in manifests.items() if data.get("status") != "completed"
    ]
    rendered = directory / "profiles.json"
    views = (
        json.loads(rendered.read_text(encoding="utf-8")) if rendered.exists() else []
    )
    complete = (
        bool(observed and manifests and views)
        and not missing
        and not unfinished
        and all(view.get("exit_code") == 0 for view in views)
        and {view.get("capture") for view in views}
        == {f"memory-{pid}.bin" for pid in manifests}
        and {(view.get("capture"), view.get("view")) for view in views}
        == {
            (f"memory-{pid}.bin", view)
            for pid in manifests
            for view in ("peak", "temporal", "temporary", "stats")
        }
    )
    return {
        "profile_status": "complete" if complete else "incomplete",
        "missing_memory_pids": missing,
        "unfinished_memory_pids": unfinished,
    }


def render(directory: Path, timeout: int = 900) -> list[dict]:
    """Render missing or failed views, reusing successful reports of unchanged captures."""
    if timeout <= 0:
        raise ValueError("Report timeout must be positive")
    manifest = directory / "profiles.json"
    previous = {
        (record["capture"], record["view"]): record
        for record in (
            json.loads(manifest.read_text(encoding="utf-8"))
            if manifest.exists()
            else []
        )
    }
    records = []
    for capture in sorted(directory.glob("memory-*.bin")):
        for view, flags in (
            ("peak", []),
            ("temporal", ["--temporal"]),
            ("temporary", ["--temporary-allocation-threshold", "100"]),
            ("stats", []),
        ):
            destination = capture.with_name(f"{capture.stem}-{view}.html")
            command = [
                sys.executable,
                "-m",
                "memray",
                "flamegraph",
                "--no-web",
                "--confidential-files",
                "all",
                "--force",
                *flags,
                "-o",
                str(destination),
                str(capture),
            ]
            if view == "stats":
                destination = capture.with_name(f"{capture.stem}-stats.txt")
                command = [sys.executable, "-m", "memray", "stats", str(capture)]
            cached = previous.get((capture.name, view), {})
            if (
                cached.get("exit_code") == 0
                and destination.is_file()
                and destination.stat().st_size
                and destination.stat().st_mtime_ns >= capture.stat().st_mtime_ns
            ):
                record = cached
            else:
                record = {
                    "capture": capture.name,
                    "view": view,
                    "output": destination.name,
                }
                log = (
                    destination if view == "stats" else destination.with_suffix(".log")
                )
                with log.open("w", encoding="utf-8") as output:
                    try:
                        result = subprocess.run(
                            command,
                            stdout=output,
                            stderr=subprocess.STDOUT,
                            timeout=timeout,
                            check=False,
                        )
                        record["exit_code"] = result.returncode
                    except subprocess.TimeoutExpired:
                        record["error"] = "report_timeout"
            records.append(record)
            temporary = directory / "profiles.tmp.json"
            temporary.write_text(json.dumps(records, indent=2), encoding="utf-8")
            temporary.replace(directory / "profiles.json")
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--timeout", type=int, default=900)
    arguments = parser.parse_args()
    if arguments.timeout <= 0:
        parser.error("--timeout must be positive")
    render(arguments.directory, arguments.timeout)
