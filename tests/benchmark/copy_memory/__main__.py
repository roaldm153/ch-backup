"""
Entry of the benchmark: python -m tests.benchmark.copy_memory run|report.
"""

import argparse
import importlib
import json
import os
import shutil
import signal
import sys
from pathlib import Path
from types import FrameType

from tests.benchmark.copy_memory import probe, report


def _list(value: str) -> list[str]:
    """Split a comma-separated value."""
    return [item for item in value.split(",") if item]


def _terminate(_signum: int, _frame: FrameType | None) -> None:
    """Let the driver save its partial results when asked to stop."""
    raise KeyboardInterrupt("Benchmark terminated")


def main() -> None:
    """Run the benchmark or build the report of its results."""
    # pylint: disable=too-many-statements,too-many-branches
    parser = argparse.ArgumentParser(prog="python -m tests.benchmark.copy_memory")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run points and build the report.")
    run.add_argument(
        "--parts",
        type=_list,
        default=_list("10000,20000,30000"),
        help="E.g. 10000,20000,30000.",
    )
    run.add_argument(
        "--versions", type=_list, default=["26.3.12.3"], help="ClickHouse versions."
    )
    run.add_argument("--columns", type=int, default=120)
    run.add_argument("--workers", type=int, default=4)
    run.add_argument(
        "--freeze-timeout",
        type=int,
        default=21600,
        help="FREEZE timeout in seconds when preparing the source backup (default: 21600).",
    )
    run.add_argument(
        "--baseline-ref", help="Git ref measured next to the current code."
    )
    run.add_argument("--candidate-ref", help="Pin candidate code to this Git ref.")
    run.add_argument(
        "--sqlite-cache-mib",
        type=probe.cache_mib,
        help="Override candidate SQLite cache per index (1-64 MiB).",
    )
    run.add_argument(
        "--baseline-sqlite-cache-mib",
        type=probe.cache_mib,
        help="Override baseline SQLite cache per index (1-64 MiB).",
    )
    run.add_argument(
        "--ab",
        action="store_true",
        help="Compare two revisions at one size and version.",
    )
    run.add_argument(
        "--copy-mode",
        choices=("auto", "memory", "full"),
        default="auto",
        help="auto uses memory runs for A/B and overnight; full copies all S3 objects.",
    )
    run.add_argument("--repeats", type=int, default=1)
    run.add_argument(
        "--cleanup-timeout",
        type=float,
        default=1800,
        help="Maximum seconds waiting for environment removal, outside the work budget.",
    )
    run.add_argument(
        "--budget-hours",
        type=float,
        default=120,
        help="Limit in hours per copy run, including the first (default: 120). "
        "Allow 60 seconds after SIGTERM before SIGKILL; larger points use planning "
        "when a full copy is predicted to exceed this limit.",
    )
    run.add_argument("--planning-only-from", type=int, help="Parts to only plan from.")
    run.add_argument("--interval", type=float, default=0.5)
    run.add_argument("--results", type=Path, help="Results directory.")
    run.add_argument(
        "--overnight",
        action="store_true",
        help="Plan copies and profile each point under a 10-hour total budget.",
    )
    run.add_argument(
        "--total-budget-hours",
        type=float,
        help="Total work budget including preparation; cleanup runs afterwards.",
    )
    run.add_argument(
        "--profiles",
        action="store_true",
        help="After each point, repeat its modes with CPU and memory profiling.",
    )
    run.add_argument("--profile-repeats", type=int, default=1)
    run.add_argument("--profile-budget-hours", type=float, default=2)
    run.add_argument(
        "--memory-format", choices=("aggregated", "all"), default="aggregated"
    )
    run.add_argument("--cpu-window", type=int, default=120)
    run.add_argument("--cpu-windows", type=int, default=3)
    run.add_argument("--native-profiles", action="store_true")
    run.add_argument("--trace-python-allocators", action="store_true")
    run.add_argument(
        "--tracemalloc",
        action="store_true",
        help="Explicit additional planning profile; disabled by default.",
    )
    run.add_argument("--min-free-gib", type=float, default=20)
    run.add_argument("--max-profile-gib", type=float, default=4)
    build = commands.add_parser("report", help="Build the report of results.")
    build.add_argument("results", type=Path)
    args = parser.parse_args()

    if args.command == "report":
        report.build(args.results)
        return
    if args.freeze_timeout <= 0:
        parser.error("--freeze-timeout must be positive")
    if args.baseline_sqlite_cache_mib is not None and not args.baseline_ref:
        parser.error("--baseline-sqlite-cache-mib requires --baseline-ref")
    if not 0 < args.budget_hours < float("inf"):
        parser.error("--budget-hours must be finite and positive")
    for name in (
        "columns",
        "workers",
        "interval",
        "repeats",
        "cleanup_timeout",
        "profile_budget_hours",
        "profile_repeats",
        "cpu_window",
        "cpu_windows",
        "min_free_gib",
        "max_profile_gib",
    ):
        if not 0 < getattr(args, name) < float("inf"):
            parser.error(f"--{name.replace('_', '-')} must be finite and positive")
    if args.total_budget_hours is not None and not 0 < args.total_budget_hours < float(
        "inf"
    ):
        parser.error("--total-budget-hours must be finite and positive")
    if any(int(parts) <= 0 for parts in args.parts):
        parser.error("--parts must contain positive integers")
    if args.ab:
        if not args.baseline_ref or len(args.parts) != 1 or len(args.versions) != 1:
            parser.error(
                "--ab requires --baseline-ref, one --parts value and one --versions value"
            )
        args.candidate_ref = args.candidate_ref or "HEAD"
    if sys.platform != "linux":
        parser.error(
            "Measurements require Linux; use report to inspect results elsewhere"
        )
    if (args.profiles or args.overnight) and (
        os.geteuid() != 0 or not shutil.which("py-spy")
    ):
        parser.error(
            "Profiling requires root for host ptrace and py-spy: run with uv --group benchmark"
        )
    driver = importlib.import_module("tests.benchmark.copy_memory.driver")

    options = driver.Options(
        parts=[int(parts) for parts in args.parts],
        versions=args.versions,
        columns=args.columns,
        workers=args.workers,
        baseline_ref=args.baseline_ref,
        candidate_ref=args.candidate_ref,
        sqlite_cache_mib=args.sqlite_cache_mib,
        baseline_sqlite_cache_mib=args.baseline_sqlite_cache_mib,
        ab=args.ab,
        copy_mode=args.copy_mode,
        repeats=args.repeats,
        cleanup_timeout=args.cleanup_timeout,
        budget_hours=args.budget_hours,
        freeze_timeout=args.freeze_timeout,
        planning_only_from=args.planning_only_from,
        interval=args.interval,
        overnight=args.overnight,
        total_budget_hours=(
            args.total_budget_hours
            if args.total_budget_hours is not None
            else 10 if args.overnight else None
        ),
        profiles=args.profiles or args.overnight,
        profile_repeats=args.profile_repeats,
        profile_budget_hours=args.profile_budget_hours,
        memory_format=args.memory_format,
        cpu_window=args.cpu_window,
        cpu_windows=args.cpu_windows,
        native_profiles=args.native_profiles,
        trace_python_allocators=args.trace_python_allocators,
        tracemalloc=args.tracemalloc,
        min_free_gib=args.min_free_gib,
        max_profile_gib=args.max_profile_gib,
    )
    if args.results:
        options.results = args.results
    previous = signal.signal(signal.SIGTERM, _terminate)
    try:
        driver.Driver(options).run()
    finally:
        signal.signal(signal.SIGTERM, previous)
    status = json.loads((options.results / "series.json").read_text(encoding="utf-8"))[
        "status"
    ]
    if status != "completed":
        raise SystemExit(124 if status == "budget_exhausted" else 1)


if __name__ == "__main__":
    main()
