"""
Inventory of a point: files and objects of the table, local disk metadata and backup sizes.

The model "bytes per object" of the report is built on these numbers, so they are taken from
ClickHouse and storages instead of being derived from the number of parts.
"""

import json
import shlex

from tests.benchmark.copy_memory.dataset import DATABASE, TABLE
from tests.benchmark.copy_memory.env import PATH_ROOT, Environment
from tests.integration.modules import s3

TARBALLS = "/code/tests/benchmark/copy_memory/tarballs.py"

FILES_AWK = r"""
{
    ext = $1
    if (ext ~ /\./) sub(/.*\./, "", ext); else ext = "none"
    count[ext]++
    files++
    size += $2
    blocks += $3 * 512
}
END {
    printf "{\"files\": %.0f, \"apparent_size\": %.0f, \"disk_usage\": %.0f, \"by_extension\": {", files, size, blocks
    sep = ""
    for (ext in count) { printf "%s\"%s\": %d", sep, ext, count[ext]; sep = ", " }
    print "}}"
}
"""


def collect(env: Environment, parts: int, backup_name: str, config_path: str) -> dict:
    """Collect the inventory of the table and its backup."""
    uuid = env.query(
        "SELECT toString(uuid) FROM system.tables "
        f"WHERE database = '{DATABASE}' AND name = '{TABLE}'"
    ).strip()
    relative = f"store/{uuid[:3]}/{uuid}/"
    active = (
        f"SELECT name FROM system.parts WHERE database = '{DATABASE}' "
        f"AND table = '{TABLE}' AND active"
    )
    refs, unique, key_length = env.query(
        "SELECT count(), uniqExact(remote_path), avg(length(remote_path)) "
        "FROM system.remote_data_paths WHERE disk_name = 's3' "
        f"AND startsWith(local_path, '{relative}') "
        f"AND splitByChar('/', local_path)[4] IN ({active})"
    ).split()
    local, metadata_version = _local_inventory(env, relative, active, parts)
    if not int(unique):
        raise RuntimeError("Inventory found no active S3 objects")
    tarballs = [
        json.loads(line)
        for line in env.exec(
            shlex.join(["python3", TARBALLS, config_path, backup_name])
        ).splitlines()
        if line.startswith("{")
    ]
    client = s3.S3Client(env.context)
    for tarball in tarballs:
        tarball["compressed"] = client.get_object_size(tarball["path"].lstrip("/"))
    files = local["files"]
    return {
        "parts": parts,
        "inventory_scope": "active_s3_parts",
        "object_refs": int(refs),
        "unique_objects": int(unique),
        "source_key_length": float(key_length),
        "local_files": files,
        "files_per_part": files / parts,
        "objects_per_file": int(refs) / files if files else 0,
        "files_by_extension_per_part": {
            ext: count / parts for ext, count in local["by_extension"].items()
        },
        "local_metadata_apparent_size": local["apparent_size"],
        "local_metadata_disk_usage": local["disk_usage"],
        "metadata_format_version": metadata_version,
        "backup_struct_size": client.get_object_size(
            f"{PATH_ROOT}/{backup_name}/backup_struct.json"
        ),
        "disk_metadata_tarballs": tarballs,
    }


def _local_inventory(
    env: Environment, relative: str, active: str, parts: int
) -> tuple[dict, int]:
    """Count metadata files only below the requested active part directories."""
    disk_path = env.query("SELECT path FROM system.disks WHERE name = 's3'").strip()
    names = env.query(active).splitlines()
    if len(names) != parts:
        raise RuntimeError(
            "Inventory did not find the expected active S3 parts and objects"
        )
    paths = [f"{disk_path.rstrip('/')}/{relative}{name}" for name in names]
    env.put_file("/tmp/bench/active-parts", "\0".join(paths) + "\0")
    scan = "find \"$@\" -type f -printf '%f %s %b\\n'"
    local = json.loads(
        env.exec(
            "set -o pipefail; xargs -0 -r -a /tmp/bench/active-parts -n 128 "
            f"sh -c {shlex.quote(scan)} sh | awk {shlex.quote(FILES_AWK)}"
        )
    )
    find = f"find {shlex.quote(paths[0])} -type f"
    first_line = env.exec(f"{find} -name '*.bin' -print -quit | xargs head -n 1")
    return local, int(first_line.split()[0])
