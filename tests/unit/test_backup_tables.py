import copy
import os
from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, Disk, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.table import TableBackup

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@dataclass
class FakeStatResult:
    st_mtime_ns: int
    st_ctime_ns: int


_STAT_UNCHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
)
_STAT_MTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000111, st_ctime_ns=16890001958000000
)
_STAT_CTIME_CHANGED = FakeStatResult(
    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000111
)


@pytest.mark.parametrize(
    "fake_stats, backups_expected_db1, backups_expected_db2",
    [
        # Metadata unchanged in both dbs -> both tables backed up
        ([_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED], 1, 1),
        # db1 table mtime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_MTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # db1 table ctime changed after freeze -> db1 skipped, db2 backed up
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED, _STAT_UNCHANGED],
            0,
            1,
        ),
        # EXCHANGE TABLES between db1 and db2: db1 backed up normally,
        # db2 table ctime changed (EXCHANGE happened) -> db2 skipped
        (
            [_STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_UNCHANGED, _STAT_CTIME_CHANGED],
            1,
            0,
        ),
    ],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    fake_stats: List[FakeStatResult],
    backups_expected_db1: int,
    backups_expected_db2: int,
) -> None:
    table_name = "table1"
    db1_name = "db1"
    db2_name = "db2"
    creation_statement = f"ATTACH TABLE {db1_name}.{table_name} UUID '{UUID}' (date Date) ENGINE = MergeTree();"

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db1 = Database(
        db1_name, "Atomic", "/var/lib/clickhouse/metadata/db1.sql", None, None
    )
    db2 = Database(
        db2_name, "Atomic", "/var/lib/clickhouse/metadata/db2.sql", None, None
    )
    table_backup = TableBackup()
    backup_meta = BackupMetadata(
        name="20181017T210300",
        # DEPRECATED: kept for backward compatibility with older versions.
        path="ch_backup/20181017T210300",
        version="1.0.100",
        ch_version="19.1.16",
        time_format="%Y-%m-%dT%H:%M:%S%Z",
        hostname="clickhouse01.test_net_711",
    )

    backup_meta.add_database(db1)
    backup_meta.add_database(db2)
    context.backup_meta = backup_meta

    # Mock external interactions
    # Each database has its own metadata path (EXCHANGE TABLES swaps inodes, not paths)
    tables_by_db = {
        db1_name: [
            Table(
                db1_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db1_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
        db2_name: [
            Table(
                db2_name,
                table_name,
                "MergeTree",
                [],
                [],
                f"/var/lib/clickhouse/metadata/{db2_name}/{table_name}.sql",
                "",
                UUID,
            )
        ],
    }
    clickhouse_ctl_mock = Mock()
    clickhouse_ctl_mock.get_tables.side_effect = lambda db_name, *a, **kw: tables_by_db[
        db_name
    ]
    clickhouse_ctl_mock.get_disks.return_value = {}
    context.ch_ctl = clickhouse_ctl_mock

    context.backup_layout = Mock()

    read_bytes_mock = Mock(return_value=creation_statement.encode())

    with (
        patch("os.stat", side_effect=fake_stats),
        patch("ch_backup.logic.table.Path", read_bytes=read_bytes_mock),
    ):
        table_backup.backup(
            context,
            [db1, db2],
            {db1_name: [table_name], db2_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    assert len(context.backup_meta.get_tables(db1_name)) == backups_expected_db1
    assert len(context.backup_meta.get_tables(db2_name)) == backups_expected_db2
    # One call after each table and one after each database is backed up
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 4


class TestValidateUploadedParts:
    """
    Tests for TableBackup._validate_uploaded_parts.
    """

    # pylint: disable=protected-access

    _BACKUP_NAME = "20181017T210300"

    def _make_part(self, name: str, link: Optional[str] = None) -> PartMetadata:
        return PartMetadata(
            database="db1",
            table="table1",
            name=name,
            checksum="abc123",
            size=1024,
            files=["data.bin"],
            tarball=True,
            link=link,
        )

    def _make_context(
        self, validate: bool, check_returns: bool
    ) -> tuple[BackupContext, MagicMock]:
        context = Mock(spec=BackupContext)
        context.config = {"validate_part_after_upload": validate}
        context.backup_meta = MagicMock()
        context.backup_meta.name = self._BACKUP_NAME
        check_data_part_mock = MagicMock(return_value=check_returns)
        layout_mock = MagicMock()
        layout_mock.check_data_part = check_data_part_mock
        context.backup_layout = layout_mock
        return context, check_data_part_mock

    def test_validate_disabled_skips_check(self):
        """When validate_part_after_upload is False, check_data_part is never called."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=False, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_not_called()

    def test_validate_calls_check_with_backup_name(self):
        """check_data_part must receive the backup *name* (not a path)."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_raises_on_broken_part(self):
        """RuntimeError is raised when check_data_part returns False."""
        part = self._make_part("all_1_1_0")
        context, _ = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError, match="all_1_1_0"):
            TableBackup._validate_uploaded_parts(context, [part])

    def test_validate_deduplicated_part_uses_backup_name(self):
        """
        For a deduplicated part (link set to a source backup name),
        _validate_uploaded_parts still passes the *current* backup name to
        check_data_part — the layout itself resolves the link internally.
        """
        source_backup = "20181010T120000"
        part = self._make_part("all_1_1_0", link=source_backup)
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_all_parts_checked_before_raising(self):
        """All invalid parts are collected before RuntimeError is raised."""
        parts = [self._make_part(f"all_{i}_1_0") for i in range(3)]
        context, check_mock = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError):
            TableBackup._validate_uploaded_parts(context, parts)

        assert check_mock.call_count == 3


class TestRestorePreprocessing:
    @pytest.mark.parametrize(
        ("backup_uuid", "expected_detached_name"),
        [
            (UUID, "detached_by_uuid"),
            ("uuid-missed", "table1"),
        ],
    )
    def test_preprocess_tables_to_restore_matches_detached_table_by_uuid_then_name(
        self,
        backup_uuid,
        expected_detached_name,
    ):
        # pylint: disable=protected-access
        table_backup = TableBackup()
        context = Mock(spec=BackupContext)
        context.ch_ctl = Mock()
        uuid_matched_table = Table(
            "db1", "detached_by_uuid", "MergeTree", [], [], "meta-uuid.sql", "", UUID
        )
        name_matched_table = Table(
            "db1", "table1", "MergeTree", [], [], "meta-name.sql", "", "uuid-other"
        )
        context.ch_ctl.get_detached_tables.return_value = [
            name_matched_table,
            uuid_matched_table,
        ]
        context.ch_ctl.get_tables.return_value = []
        context.ch_ctl.get_replicas.return_value = []
        backup_table = Table("db1", "table1", "MergeTree", [], [], "", "", backup_uuid)
        databases = {"db1": Database("db1", "Atomic", None, None, None)}

        with patch.object(
            table_backup,
            "_rewrite_table_schema",
            side_effect=lambda *_args, **_kwargs: setattr(
                backup_table, "create_statement", "CREATE TABLE"
            ),
        ):
            result, _ = table_backup._preprocess_tables_to_restore(
                context,
                databases,
                [backup_table],
                keep_going=False,
                restore_tables_in_replicated_database=True,
                metadata_cleaner=None,
            )

        attached_table = context.ch_ctl.attach_table.call_args.args[0]
        assert attached_table.name == expected_detached_name
        assert result == [backup_table]


class TestCloudStorageCopyDataFlag:
    """
    Tests that the cloud_storage.copy_data option reaches backup metadata.
    """

    @staticmethod
    def _backup_with_cloud_conf(cloud_conf: dict) -> BackupMetadata:
        """Helper: run a schema-only backup with a given cloud_storage config."""
        config: dict = copy.deepcopy(DEFAULT_CONFIG)
        config["cloud_storage"] = cloud_conf
        context = BackupContext(config)  # type: ignore[arg-type]
        context.ch_ctl = MagicMock()
        context.ch_config = MagicMock()
        context.ch_config.config = {}
        context.backup_meta = BackupMetadata(
            name="20181017T210300",
            path="ch_backup/20181017T210300",
            version="1.0.100",
            ch_version="19.1.16",
            time_format="%Y-%m-%dT%H:%M:%S%Z",
            hostname="clickhouse01.test_net_711",
        )

        TableBackup().backup(
            context,
            databases=[],
            db_tables={},
            schema_only=True,
            multiprocessing_config={},
        )

        return context.backup_meta

    def test_flag_is_set_when_copy_data_enabled(self) -> None:
        """
        With cloud_storage.copy_data enabled the backup must be marked as
        containing copied cloud storage data.
        """
        backup_meta = self._backup_with_cloud_conf({"copy_data": True})

        assert backup_meta.cloud_storage.data_copied is True

    def test_flag_is_not_set_when_copy_data_disabled(self) -> None:
        """
        With cloud_storage.copy_data disabled the backup must keep the default
        behaviour of storing references only.
        """
        backup_meta = self._backup_with_cloud_conf({"copy_data": False})

        assert backup_meta.cloud_storage.data_copied is False

    def test_flag_is_not_set_when_option_is_absent(self) -> None:
        """
        A configuration file without the option must behave as if it is disabled.
        """
        backup_meta = self._backup_with_cloud_conf({})

        assert backup_meta.cloud_storage.data_copied is False


class TestBackupCloudStorageMetadata:
    """
    Tests for TableBackup._backup_cloud_storage_metadata.
    """

    # pylint: disable=protected-access

    @staticmethod
    def _make_table(disks: List[Disk]) -> Table:
        return Table(
            "db1",
            "table1",
            "MergeTree",
            disks,
            [os.path.join(disk.path, "store/abc/abcdef") for disk in disks],
            "",
            "",
            UUID,
        )

    @staticmethod
    def _make_context(
        has_frozen_data: bool = True,
    ) -> tuple[BackupContext, MagicMock, MagicMock]:
        """Helper: build a context with mocked layout and backup metadata."""
        context = Mock(spec=BackupContext)
        context.backup_layout = MagicMock()
        context.backup_layout.has_frozen_cloud_storage_data.return_value = (
            has_frozen_data
        )
        context.backup_layout.upload_cloud_storage_metadata.return_value = True
        context.backup_meta = MagicMock()
        return context, context.backup_layout, context.backup_meta.cloud_storage

    def test_metadata_is_uploaded_from_the_disk_itself_without_copying(self):
        """
        Without backup disks nothing is copied and metadata is read from the
        disk holding the frozen data.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        context, layout, cloud_storage = self._make_context()

        TableBackup._backup_cloud_storage_metadata(context, self._make_table([disk]))

        upload_kwargs = layout.upload_cloud_storage_metadata.call_args.kwargs
        assert upload_kwargs["source_disk"] is None
        cloud_storage.add_disk.assert_called_once_with("s3")

    def test_data_is_copied_and_metadata_is_read_from_the_backup_disk(self):
        """
        With backup disks the data is copied first and metadata of the copies
        is uploaded instead of the frozen one.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disk = Disk("s3_backup", "/var/lib/clickhouse/disks/s3_backup/", "s3")
        backup_disks = MagicMock()
        backup_disks.copy_table_data.return_value = backup_disk
        context, layout, cloud_storage = self._make_context()
        table = self._make_table([disk])

        TableBackup._backup_cloud_storage_metadata(context, table, backup_disks)

        backup_disks.copy_table_data.assert_called_once_with("s3", table)
        upload_kwargs = layout.upload_cloud_storage_metadata.call_args.kwargs
        assert upload_kwargs["source_disk"] is backup_disk
        cloud_storage.add_disk.assert_called_once_with("s3")

    def test_nothing_is_copied_when_no_data_is_frozen(self):
        """
        Copying an empty shadow directory would fail, so the check must happen
        before the copy.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disks = MagicMock()
        context, layout, cloud_storage = self._make_context(has_frozen_data=False)

        TableBackup._backup_cloud_storage_metadata(
            context, self._make_table([disk]), backup_disks
        )

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()
        cloud_storage.add_disk.assert_not_called()

    def test_failed_copy_is_not_silently_ignored(self):
        """
        clickhouse-disks reports copy errors with a zero exit code, so a copy
        that produced no metadata must fail the backup.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        backup_disks = MagicMock()
        context, layout, cloud_storage = self._make_context()
        layout.upload_cloud_storage_metadata.return_value = False

        with pytest.raises(ClickhouseBackupError):
            TableBackup._backup_cloud_storage_metadata(
                context, self._make_table([disk]), backup_disks
            )

        cloud_storage.add_disk.assert_not_called()

    def test_empty_upload_without_copying_is_not_an_error(self):
        """
        Without copying an empty result only means there is nothing to store.
        """
        disk = Disk("s3", "/var/lib/clickhouse/disks/s3/", "s3")
        context, layout, cloud_storage = self._make_context()
        layout.upload_cloud_storage_metadata.return_value = False

        TableBackup._backup_cloud_storage_metadata(context, self._make_table([disk]))

        cloud_storage.add_disk.assert_not_called()

    def test_local_disks_are_skipped(self):
        """
        Only cloud storage disks are backed up here.
        """
        disk = Disk("default", "/var/lib/clickhouse/", "local")
        backup_disks = MagicMock()
        context, layout, _ = self._make_context()

        TableBackup._backup_cloud_storage_metadata(
            context, self._make_table([disk]), backup_disks
        )

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()

    def test_cached_disks_are_skipped(self):
        """
        Data of a cached disk is handled through the disk behind the cache.
        """
        disk = Disk(
            "s3", "/var/lib/clickhouse/disks/s3/", "s3", cache_path="/var/cache/s3"
        )
        backup_disks = MagicMock()
        context, layout, _ = self._make_context()

        TableBackup._backup_cloud_storage_metadata(
            context, self._make_table([disk]), backup_disks
        )

        backup_disks.copy_table_data.assert_not_called()
        layout.upload_cloud_storage_metadata.assert_not_called()
