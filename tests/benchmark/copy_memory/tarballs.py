"""
Print tarballs with disk metadata of the backup as JSON lines: path, files and unpacked size.

Runs in the container with ch-backup: python3 tarballs.py <config> <backup name>.
"""

import json
import sys

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.config import Config


def main() -> None:
    """
    Download every tarball with disk metadata and print its sizes.

    Logging is configured first, as ch-backup does: workers of the pool get the logger pickled,
    and the default handler on stderr cannot be pickled.
    """
    config_path, backup_name = sys.argv[1:3]
    config = Config(config_path)
    logging.configure(config["loguru"])
    layout = BackupLayout(config)
    backup = layout.get_backup(backup_name)
    if backup is None:
        raise SystemExit(f"Backup {backup_name} is not found")
    for disk in backup.cloud_storage.disks:
        for path in layout.get_cloud_storage_metadata_remote_paths(
            backup.get_sanitized_name(), disk, backup.cloud_storage.compressed, "all"
        ):
            files = layout.get_cloud_storage_metadata_files(backup, path)
            line = {
                "path": path,
                "files": len(files),
                "unpacked": sum(len(data) for _, data in files),
                "names": sum(len(name) for name, _ in files),
            }
            print(json.dumps(line), flush=True)


if __name__ == "__main__":
    main()
