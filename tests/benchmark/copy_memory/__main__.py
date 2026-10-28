"""
Entry of the benchmark: python -m tests.benchmark.copy_memory run|report.
"""

import argparse
from pathlib import Path

from tests.benchmark.copy_memory import report
from tests.benchmark.copy_memory.driver import Driver, Options


def _list(value: str) -> list[str]:
    """Split a comma-separated value."""
    return [item for item in value.split(",") if item]


def main() -> None:
    """Run the benchmark or build the report of its results."""
    parser = argparse.ArgumentParser(prog="python -m tests.benchmark.copy_memory")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run points and build the report.")
    run.add_argument("--parts", type=_list, required=True, help="E.g. 1000,3000,10000.")
    run.add_argument(
        "--versions", type=_list, required=True, help="ClickHouse versions."
    )
    run.add_argument("--columns", type=int, default=120)
    run.add_argument("--workers", type=int, default=4)
    run.add_argument(
        "--baseline-ref", help="Git ref measured next to the current code."
    )
    run.add_argument(
        "--budget-hours",
        type=float,
        default=3,
        help="Time of a full copy above which a point only plans copying.",
    )
    run.add_argument("--planning-only-from", type=int, help="Parts to only plan from.")
    run.add_argument("--interval", type=float, default=0.5)
    run.add_argument("--results", type=Path, help="Results directory.")
    build = commands.add_parser("report", help="Build the report of results.")
    build.add_argument("results", type=Path)
    args = parser.parse_args()

    if args.command == "report":
        report.build(args.results)
        return
    options = Options(
        parts=[int(parts) for parts in args.parts],
        versions=args.versions,
        columns=args.columns,
        workers=args.workers,
        baseline_ref=args.baseline_ref,
        budget_hours=args.budget_hours,
        planning_only_from=args.planning_only_from,
        interval=args.interval,
    )
    if args.results:
        options.results = args.results
    Driver(options).run()
    report.build(options.results)


if __name__ == "__main__":
    main()
