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
    printf "{\"files\": %d, \"apparent_size\": %d, \"disk_usage\": %d, \"by_extension\": {", files, size, blocks
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
    refs, unique, key_length = env.query(
        "SELECT count(), uniqExact(remote_path), avg(length(remote_path)) "
        f"FROM system.remote_data_paths WHERE position(local_path, '{uuid}') > 0"
    ).split()
    disk_path = env.query("SELECT path FROM system.disks WHERE name = 's3'").strip()
    find = f"find {shlex.quote(disk_path)} -path {shlex.quote(f'*{uuid}*')} -type f"
    local = json.loads(
        env.exec(f"{find} -printf '%f %s %b\\n' | awk {shlex.quote(FILES_AWK)}")
    )
    first_line = env.exec(f"{find} -name '*.bin' -print -quit | xargs head -n 1")
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
        "metadata_format_version": int(first_line.split()[0]),
        "backup_struct_size": client.get_object_size(
            f"{PATH_ROOT}/{backup_name}/backup_struct.json"
        ),
        "disk_metadata_tarballs": tarballs,
    }
