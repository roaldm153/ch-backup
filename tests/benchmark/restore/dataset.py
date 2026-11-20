"""Create and verify deterministic local data in the isolated benchmark cluster."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.benchmark.copy_memory.env import Environment

DATABASE = "bench"


def create(env: "Environment", parts: int, rows: int, payload_bytes: int) -> dict:
    """Generate distinct payload blocks and retain a digest outside measured runs."""
    if min(parts, rows, payload_bytes) < 1 or payload_bytes % 8:
        raise ValueError("Positive sizes and a payload divisible by eight are required")
    env.query("CREATE DATABASE bench")
    env.query(
        "CREATE TABLE bench.payload (id UInt64, payload String) "
        "ENGINE=MergeTree ORDER BY id SETTINGS "
        "max_bytes_to_merge_at_max_space_in_pool=1, "
        "min_bytes_for_wide_part=0, min_rows_for_wide_part=0"
    )
    env.query("SYSTEM STOP MERGES bench.payload")
    for part in range(parts):
        env.query(
            "INSERT INTO bench.payload SELECT number, "
            "arrayStringConcat(arrayMap(i -> rightPad(reinterpretAsString("
            f"cityHash64(number, toUInt64(i))), 8, char(0)), range({payload_bytes // 8}))) "
            f"FROM numbers({part * rows}, {rows})",
            {"max_threads": 1, "max_insert_threads": 1, "max_block_size": rows},
        )
    return {
        "digest": digest(env),
        "active_parts_and_bytes": env.query(
            "SELECT count(), sum(bytes_on_disk) FROM system.parts "
            "WHERE database='bench' AND table='payload' AND active"
        ).strip(),
        "parts_requested": parts,
        "rows": parts * rows,
        "payload_bytes": payload_bytes,
    }


def digest(env: "Environment") -> str:
    """Read all restored payloads and check both row count and content hashes."""
    return env.query(
        "SELECT count(), sum(id), sum(cityHash64(id, payload)), "
        "groupBitXor(cityHash64(id, payload)), sum(length(payload)) FROM bench.payload"
    ).strip()
