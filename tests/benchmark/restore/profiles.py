"""Verify sampled main and worker processes against the host collector."""

import json
import re
from pathlib import Path

from tests.benchmark.copy_memory.profiles import coverage


def cpu_coverage(directory: Path) -> dict:
    """Reject captures missing any observed Python process or window manifest."""
    result = coverage(directory, "cpu")
    observed_file = directory / "observed.json"
    observed = json.loads(observed_file.read_text()) if observed_file.exists() else {}
    expected = {str(row["host_pid"]) for row in observed.values() if "host_pid" in row}
    sampled = set()
    for name in result["cpu_captures"]:
        for profile in json.loads((directory / name).read_text()).get("profiles", []):
            match = re.search(r"\bProcess (\d+)\b", profile.get("name", ""))
            if match and profile.get("samples"):
                sampled.add(match.group(1))
    manifests = [
        json.loads(path.read_text())
        for path in directory.glob("instrumentation-*.json")
    ]
    unobserved = [
        str(item["pid"])
        for item in manifests
        if str(item["pid"]) not in observed
        or observed[str(item["pid"])].get("created") != item.get("created")
    ]
    missing = sorted(expected - sampled)
    complete = (
        result["profile_status"] == "complete"
        and bool(expected)
        and bool(manifests)
        and not unobserved
        and not missing
        and len(expected) == len(observed)
        and (directory / "cpu-windows.jsonl").is_file()
    )
    return {
        **result,
        "profile_status": "complete" if complete else "incomplete",
        "sampled_host_pids": sorted(sampled),
        "missing_cpu_host_pids": missing,
        "unobserved_instrumented_pids": unobserved,
    }
