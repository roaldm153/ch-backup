"""
ClickhouseBackup unit tests.
"""

from collections import defaultdict
from typing import Sequence, Tuple
from unittest.mock import MagicMock, patch

from ch_backup.backup.deduplication import DedupReferences
from ch_backup.backup.metadata import CloudStorageMetadata, PartMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import ClickhouseBackup, ClickhouseBackupError
from ch_backup.config import DEFAULT_CONFIG


def _restore_backup_with_cloud_storage(
    data_copied: bool,
) -> Tuple[MagicMock, MagicMock]:
    """Helper: restore a backup that has data on S3 disks, without a source bucket."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    backup.__dict__["_context"] = MagicMock()

    backup_meta = MagicMock()
    backup_meta.cloud_storage = CloudStorageMetadata(
        data_copied=data_copied, disks=["s3"]
    )
    backup_meta.get_databases.return_value = []

    sources = BackupSources.for_restore(False, False, False, False, False, False, False)
    assert sources.data

    with patch.object(ClickhouseBackup, "_get_backup", return_value=backup_meta):
        with patch.object(ClickhouseBackup, "_restore") as restore_mock:
            backup.restore(
                sources=sources,
                backup_name="backup",
                databases=[],
                exclude_databases=[],
            )
    return backup_meta, restore_mock


def test_restore_requires_source_bucket_when_data_is_not_copied():
    """
    Data left in the source bucket is unreachable without its coordinates.
    """
    try:
        _restore_backup_with_cloud_storage(data_copied=False)
        assert False, "Expected ClickhouseBackupError was not raised"
    except ClickhouseBackupError as exc:
        assert "Cloud storage source bucket" in str(exc)


def test_restore_of_copied_data_does_not_require_source_bucket():
    """
    Data copied into the backup is restored from the backup bucket.
    """
    _, restore_mock = _restore_backup_with_cloud_storage(data_copied=True)

    restore_mock.assert_called_once()
    assert restore_mock.call_args.kwargs["cloud_storage_source_bucket"] is None


def _delete_backup_with_cloud_storage(referenced_parts: Sequence[str]) -> MagicMock:
    """Helper: partially delete a backup holding copied cloud storage data."""
    backup = ClickhouseBackup(DEFAULT_CONFIG)  # type: ignore[arg-type]
    backup.__dict__["_context"] = MagicMock()

    part = PartMetadata(
        database="db1",
        table="table1",
        name="all_1_1_0",
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        disk_name="s3",
    )
    table = MagicMock()
    table.name = "table1"
    table.get_parts.return_value = [part]

    backup_meta = MagicMock()
    backup_meta.name = "backup"
    backup_meta.cloud_storage = CloudStorageMetadata(data_copied=True, disks=["s3"])
    backup_meta.get_databases.return_value = ["db1"]
    backup_meta.get_tables.return_value = [table]
    backup.__dict__["_context"].backup_layout.reload_backup.return_value = backup_meta

    dedup_references: DedupReferences = defaultdict(lambda: defaultdict(set))
    dedup_references["db1"]["table1"] = set(referenced_parts)

    # pylint: disable=protected-access
    backup._delete(backup_meta, dedup_references)
    return backup.__dict__["_context"].backup_layout


def test_delete_keeps_cloud_storage_data_in_use_by_other_backups():
    """
    Object keys are known only from the disk metadata inside the backup, so
    its cloud storage data is kept whole until the last reference is gone.
    """
    layout = _delete_backup_with_cloud_storage(referenced_parts=["all_1_1_0"])

    layout.delete_cloud_storage_data.assert_not_called()


def test_delete_removes_cloud_storage_data_that_is_not_referenced():
    """
    Cloud storage data of parts nobody reuses is deleted with the backup.
    """
    layout = _delete_backup_with_cloud_storage(referenced_parts=["all_2_2_0"])

    layout.delete_cloud_storage_data.assert_called_once_with("backup")
