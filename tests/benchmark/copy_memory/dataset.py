"""
Table of the benchmark: one Wide part per row on the S3 disk.

The mix of 120 columns gives about 308 files per part: numbers and strings have a data file and
marks, nullable columns, low cardinality strings and arrays have two streams each.
"""

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.benchmark.copy_memory.env import Environment

DATABASE = "bench"
TABLE = "parts"
BATCH = 500

COLUMN_KINDS = (
    ("numeric", 70),
    ("string", 20),
    ("nullable", 15),
    ("low_cardinality", 10),
    ("array", 5),
)

NUMERIC_TYPES = (
    ("UInt64", "toUInt64(number + {i})"),
    ("Int32", "toInt32(number * {i})"),
    ("Float64", "toFloat64(number) / {i}"),
    ("UInt8", "toUInt8((number + {i}) % 256)"),
)

KIND_TYPES = {
    "string": ("String", "concat('s', toString(number + {i}))"),
    "nullable": (
        "Nullable(UInt32)",
        "if(number % 2 = 0, NULL, toUInt32(number + {i}))",
    ),
    "low_cardinality": ("LowCardinality(String)", "toString((number + {i}) % 7)"),
    "array": ("Array(UInt32)", "[toUInt32(number), toUInt32({i})]"),
}


def columns(count: int) -> list[tuple[str, str, str]]:
    """Return names, types and expressions of columns, keeping the mix for any count."""
    total = sum(weight for _, weight in COLUMN_KINDS)
    counts = {kind: round(count * weight / total) for kind, weight in COLUMN_KINDS[1:]}
    counts["numeric"] = max(count - sum(counts.values()), 1)
    result: list[tuple[str, str, str]] = []
    for kind, _ in COLUMN_KINDS:
        for _ in range(counts[kind]):
            i = len(result) + 1
            if kind == "numeric":
                column_type, expression = NUMERIC_TYPES[(i - 1) % len(NUMERIC_TYPES)]
            else:
                column_type, expression = KIND_TYPES[kind]
            result.append((f"c{i:03d}", column_type, expression.format(i=i)))
    return result


def create_table(env: "Environment", column_count: int, parts: int) -> None:
    """Create the table with merges stopped and limits of parts above the number of parts."""
    limit = max(200_000, 2 * parts)
    definition = ", ".join(f"{name} {kind}" for name, kind, _ in columns(column_count))
    env.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    env.query(
        f"CREATE TABLE {DATABASE}.{TABLE} ({definition}) ENGINE = MergeTree ORDER BY c001 "
        "SETTINGS storage_policy = 's3', min_bytes_for_wide_part = 0, "
        "min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, "
        "max_bytes_to_merge_at_max_space_in_pool = 1, "
        f"parts_to_delay_insert = {limit}, parts_to_throw_insert = {limit}, "
        f"max_parts_in_total = {limit}"
    )
    env.query(f"SYSTEM STOP MERGES {DATABASE}.{TABLE}")


def insert_parts(env: "Environment", column_count: int, parts: int) -> float:
    """Insert rows one per part in batches and return the duration."""
    started = time.monotonic()
    expressions = ", ".join(expression for _, _, expression in columns(column_count))
    settings = {
        "max_block_size": 1,
        "min_insert_block_size_rows": 1,
        "min_insert_block_size_bytes": 0,
        "max_threads": 16,
        "max_insert_threads": 16,
    }
    for start in range(0, parts, BATCH):
        count = min(BATCH, parts - start)
        env.query(
            f"INSERT INTO {DATABASE}.{TABLE} SELECT {expressions} "
            f"FROM numbers_mt({start}, {count})",
            settings,
        )
        print(f"  inserted {start + count} of {parts} parts", flush=True)
    return time.monotonic() - started


def check_parts(env: "Environment", parts: int) -> None:
    """Check that the table has the requested number of active Wide parts."""
    active, wide = env.query(
        "SELECT count(), countIf(part_type = 'Wide') FROM system.parts "
        f"WHERE database = '{DATABASE}' AND table = '{TABLE}' AND active"
    ).split()
    if int(active) != parts or int(wide) != parts:
        raise RuntimeError(
            f"Expected {parts} Wide parts, found {active} active parts, {wide} of them Wide"
        )
