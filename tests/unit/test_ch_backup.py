"""
ClickhouseBackup unit tests.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from ch_backup.backup.metadata import CloudStorageMetadata, TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError


def _restore_backup_with_cloud_storage(
    data_copied: bool,
) -> tuple[MagicMock, MagicMock]:
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


def _backup_with_context(
    database_engine: str = "Atomic", table_engine: str = "MergeTree"
) -> tuple[ClickhouseBackup, Mock]:
    context = Mock()
    context.config = {"force_non_replicated": False}
    context.backup_meta.get_database.return_value = Database(
        "db", database_engine, None, None, None
    )
    context.backup_meta.get_tables.return_value = [
        TableMetadata("db", "table", table_engine, None)
    ]

    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context
    return backup, context


def test_restore_checks_zookeeper_for_replicated_table() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_called_once_with()


def test_restore_checks_zookeeper_for_replicated_database() -> None:
    backup, context = _backup_with_context(database_engine="Replicated")

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_called_once_with()


def test_restore_does_not_check_zookeeper_for_non_replicated_schema() -> None:
    backup, context = _backup_with_context()

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


def test_restore_does_not_check_zookeeper_when_forcing_non_replicated() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")
    context.config["force_non_replicated"] = True

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], []
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


def test_restore_checks_only_selected_tables() -> None:
    backup, context = _backup_with_context(table_engine="ReplicatedMergeTree")
    selected_tables = [TableMetadata("db", "local_table", "MergeTree", None)]

    backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
        BackupSources(), ["db"], selected_tables
    )

    context.ch_ctl.check_zookeeper_available.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        ClickhouseError("There is no Zookeeper configuration"),
        requests.exceptions.ConnectionError("ClickHouse is unreachable"),
        requests.exceptions.ReadTimeout("ClickHouse query timed out"),
    ],
)
def test_restore_reports_failed_zookeeper_check(error: Exception) -> None:
    backup, context = _backup_with_context(
        database_engine="Replicated", table_engine="ReplicatedMergeTree"
    )
    context.ch_ctl.check_zookeeper_available.side_effect = error

    with pytest.raises(ClickhouseBackupError) as exc:
        backup._check_zookeeper_for_restore(  # pylint: disable=protected-access
            BackupSources(), ["db"], []
        )

    assert str(exc.value) == (
        "Restore requires ZooKeeper or ClickHouse Keeper because we have replicated "
        "databases: `db`, tables: `db`.`table`. Availability check through "
        f"ClickHouse failed: {error}"
    )
    assert exc.value.__cause__ is error
