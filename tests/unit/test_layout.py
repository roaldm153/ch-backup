"""Unit tests for backup layout cloud metadata path selection."""

import os
from collections import Counter
from unittest.mock import MagicMock, patch

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import DEFAULT_CONFIG


class TestCloudStorageMetadataRemotePaths:
    """Tests for filtered cloud metadata remote path selection."""

    # pylint: disable=protected-access

    def test_prefers_old_style_and_filters_exact_per_table_paths(self):
        with (
            patch("ch_backup.backup.layout.StorageLoader"),
            patch("ch_backup.backup.layout.get_encryption") as get_encryption,
        ):
            get_encryption.return_value.metadata_size.return_value = 0
            layout = BackupLayout(DEFAULT_CONFIG)  # type: ignore[arg-type]
        layout._storage_loader = MagicMock()
        layout._config["path_root"] = "ch_backup"

        backup_name = "backup"
        source_disk_name = "s3"
        tables = [
            TableMetadata("db1", "table1", "MergeTree", None),
            TableMetadata("db1", "table2", "MergeTree", None),
            TableMetadata("db2", "table3", "MergeTree", None),
        ]

        backup_path = layout.get_backup_path(backup_name)
        old_style_path = f"{backup_path}/disks/{source_disk_name}.tar"
        expected_paths = [
            f"{backup_path}/disks/{source_disk_name}/db1/table1.tar",
            f"{backup_path}/disks/{source_disk_name}/db2/table3.tar",
        ]

        layout._storage_loader.path_exists.side_effect = {old_style_path: False}.get
        layout._storage_loader.list_dir.side_effect = lambda _disk_path, **_kwargs: [
            *expected_paths,
            f"{backup_path}/disks/{source_disk_name}/db2/table4.tar",
        ]

        remote_paths = layout._get_cloud_storage_metadata_remote_paths(
            backup_name,
            source_disk_name,
            compression=False,
            desired_tables=tables,
        )

        assert Counter(remote_paths) == Counter(expected_paths)


class TestCloudStorageMetadataUpload:
    """Tests for reading cloud storage metadata from a given disk."""

    # pylint: disable=protected-access

    _BACKUP_NAME = "20260101T000000"

    @staticmethod
    def _make_layout() -> tuple[BackupLayout, MagicMock]:
        """Helper: build a BackupLayout with a mocked storage loader."""
        with (
            patch("ch_backup.backup.layout.StorageLoader"),
            patch("ch_backup.backup.layout.get_encryption") as get_encryption,
        ):
            get_encryption.return_value.metadata_size.return_value = 0
            layout = BackupLayout(DEFAULT_CONFIG)  # type: ignore[arg-type]
        layout._storage_loader = MagicMock()
        layout._config["path_root"] = "ch_backup"
        return layout, layout._storage_loader

    def _make_backup_meta(self) -> MagicMock:
        backup_meta = MagicMock()
        backup_meta.get_sanitized_name.return_value = self._BACKUP_NAME
        backup_meta.cloud_storage.compressed = False
        backup_meta.cloud_storage.encrypted = False
        return backup_meta

    @staticmethod
    def _make_table(disk: Disk) -> Table:
        return Table(
            "db1",
            "table1",
            "MergeTree",
            [disk],
            [os.path.join(disk.path, "store/abc/abcdef")],
            "",
            "",
            "some-uuid",
        )

    def test_has_frozen_cloud_storage_data(self):
        """
        Frozen data is looked up in the shadow directory of the given disk.
        """
        layout, _ = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty") as dir_is_empty:
            dir_is_empty.return_value = False
            assert layout.has_frozen_cloud_storage_data(
                self._make_backup_meta(), disk, table
            )

        dir_is_empty.assert_called_once_with(
            f"/var/lib/clickhouse/disks/s3/shadow/{self._BACKUP_NAME}/store/abc/abcdef",
            ["frozen_metadata.txt"],
        )

    def test_upload_reads_files_from_source_disk(self):
        """
        With source_disk set, metadata is read from it while the tarball still
        lands under the name of the original disk.
        """
        layout, loader = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        source_disk = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty", return_value=False):
            layout.upload_cloud_storage_metadata(
                self._make_backup_meta(), disk, table, source_disk=source_disk
            )

        call = loader.upload_files_tarball_scan.call_args.kwargs
        assert call["dir_path"] == (
            f"/var/lib/clickhouse/disks/s3_backup/shadow/{self._BACKUP_NAME}"
            "/store/abc/abcdef"
        )
        assert call["remote_path"] == (
            f"ch_backup/{self._BACKUP_NAME}/disks/s3/db1/table1.tar"
        )
        assert call["tar_base_dir"] == "store/abc/abcdef"

    def test_upload_reads_files_from_the_disk_itself_by_default(self):
        """
        Without source_disk the behaviour must stay as it was.
        """
        layout, loader = self._make_layout()
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        table = self._make_table(disk)

        with patch("ch_backup.backup.layout.dir_is_empty", return_value=False):
            layout.upload_cloud_storage_metadata(self._make_backup_meta(), disk, table)

        call = loader.upload_files_tarball_scan.call_args.kwargs
        assert call["dir_path"] == (
            f"/var/lib/clickhouse/disks/s3/shadow/{self._BACKUP_NAME}/store/abc/abcdef"
        )
