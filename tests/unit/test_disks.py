"""
Unit tests disks module.
"""

import copy
import io
import os
import unittest
import unittest.mock
from contextlib import contextmanager
from typing import IO, Dict, Iterator, List, Optional, Tuple

import xmltodict

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.disks import (
    CH_DISK_CONFIG_PATH,
    ClickHouseBackupDisks,
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
)
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import DEFAULT_CONFIG, Config
from tests.unit.utils import assert_equal, parametrize


@parametrize(
    {
        "id": "No timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>3600000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                      <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
    {
        "id": "Small timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>30000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>3600000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                      <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
    {
        "id": "Large timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>7200000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>7200000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
)
def test_temporary_disk(clickhouse_config, disk_name, source, temp_config):
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.backup_layout = unittest.mock.MagicMock()
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.cloud_storage.data_copied = False
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    disk = ClickHouseTemporaryDisks(
        context.ch_ctl,
        context.backup_layout,
        context.config_root,
        context.backup_meta,
        source["bucket"],
        source["path"],
        source["endpoint"],
        context.ch_config,
    )

    with _capture_config_files() as (written, _):
        # pylint: disable=protected-access
        # Initialise _disks the same way __enter__ does
        disk._disks = (context.ch_config.config.get("storage_configuration") or {}).get(
            "disks"
        ) or {}
        disk._create_temporary_disk(
            context.backup_meta,
            disk_name,
        )

    config_path = (
        f"/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_{disk_name}_source.xml"
    )
    expected_content = xmltodict.parse(temp_config, disable_entities=False)
    actual_content = xmltodict.parse(written[config_path], disable_entities=False)
    assert_equal(actual_content, expected_content)


BACKUP_STORAGE_CREDENTIALS = {
    "endpoint_url": "https://minio:9000/",
    "access_key_id": "BackupAccessKey",
    "secret_access_key": "BackupSecretKey",
    "bucket": "backup-bucket",
}


def _make_backup_storage_config() -> dict:
    """Helper: build a config with credentials of the backup storage."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"]["path_root"] = "ch_backup/"
    config["storage"]["credentials"] = copy.deepcopy(BACKUP_STORAGE_CREDENTIALS)
    return config


def _make_temporary_disks(
    clickhouse_config_xml: str,
    cloud_storage_disks: Optional[List[str]] = None,
    data_copied: bool = False,
    source_bucket: Optional[str] = "test-bucket",
) -> ClickHouseTemporaryDisks:
    """Helper: build ClickHouseTemporaryDisks with mocked dependencies."""
    context = BackupContext(_make_backup_storage_config())  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.backup_layout = unittest.mock.MagicMock()
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.cloud_storage.disks = cloud_storage_disks or []
    context.backup_meta.cloud_storage.enabled = bool(cloud_storage_disks)
    context.backup_meta.cloud_storage.data_copied = data_copied
    context.backup_meta.name = "20260101T000000"
    context.backup_meta.get_sanitized_name.return_value = "20260101T000000"
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config_xml),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    return ClickHouseTemporaryDisks(
        context.ch_ctl,
        context.backup_layout,
        context.config_root,
        context.backup_meta,
        source_bucket,
        None,
        None,
        context.ch_config,
    )


def test_enter_without_storage_configuration():
    """
    __enter__ must not raise KeyError when the ClickHouse config has no
    storage_configuration section (valid CH config that uses the default disk).
    """
    clickhouse_config_xml = """
        <clickhouse>
            <logger>
                <level>trace</level>
            </logger>
        </clickhouse>
    """
    disk_manager = _make_temporary_disks(clickhouse_config_xml, cloud_storage_disks=[])

    with _capture_config_files() as (written, _):
        with disk_manager:
            # pylint: disable=protected-access
            assert disk_manager._disks == {}

    actual_content = xmltodict.parse(
        written[CH_DISK_CONFIG_PATH], disable_entities=False
    )
    assert_equal(
        actual_content["clickhouse"]["history-file"],
        "/tmp/.disks-file-history",
    )


def test_create_temporary_disk_missing_disk_raises():
    """
    _create_temporary_disk must raise ClickHouseDisksException with a descriptive
    message when disk_name is present in backup cloud storage metadata but absent
    from the ClickHouse storage_configuration.
    """
    clickhouse_config_xml = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <other_disk>
                        <type>s3</type>
                        <endpoint>https://localhost/bucket/path/</endpoint>
                    </other_disk>
                </disks>
            </storage_configuration>
        </clickhouse>
    """
    disk_manager = _make_temporary_disks(
        clickhouse_config_xml, cloud_storage_disks=["missing_disk"]
    )

    with unittest.mock.patch("builtins.open", new=unittest.mock.mock_open()):
        # Manually initialise _disks as __enter__ would
        # pylint: disable=protected-access
        disk_manager._disks = (
            disk_manager._ch_config.config.get("storage_configuration") or {}
        ).get("disks") or {}  # fmt: skip

        try:
            disk_manager._create_temporary_disk(
                disk_manager._backup_meta,
                "missing_disk",
            )
            assert False, "Expected ClickHouseDisksException was not raised"
        except ClickHouseDisksException as exc:
            assert "missing_disk" in str(exc)
            assert "storage_configuration" in str(exc)


BACKUP_DISK_CLICKHOUSE_CONFIG = """
    <clickhouse>
        <storage_configuration>
            <disks>
                <object_storage>
                    <type>s3</type>
                    <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                    <access_key_id>AKIAACCESSKEY</access_key_id>
                    <secret_access_key>SecretAccesskey</secret_access_key>
                </object_storage>
            </disks>
        </storage_configuration>
    </clickhouse>
"""

BACKUP_DISK_PATH = "/var/lib/clickhouse/disks/object_storage_backup/"
BACKUP_DISK_CONFIG_PATH = (
    "/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_object_storage_backup.xml"
)


def _make_backup_disks(
    clickhouse_config_xml: str,
    storage_config: Optional[dict] = None,
) -> Tuple[ClickHouseBackupDisks, unittest.mock.MagicMock]:
    """Helper: build ClickHouseBackupDisks with mocked dependencies."""
    config = _make_backup_storage_config()
    config["storage"].update(storage_config or {})
    context = BackupContext(config)  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.ch_ctl.get_disk.return_value = Disk(
        "object_storage_backup", BACKUP_DISK_PATH, "s3"
    )
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.name = "20260101T000000"
    context.backup_meta.get_sanitized_name.return_value = "20260101T000000"
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config_xml),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    disks = ClickHouseBackupDisks(
        context.ch_ctl,
        context.config_root,
        context.backup_meta,
        context.ch_config,
    )
    return disks, context.ch_ctl


@contextmanager
def _capture_config_files() -> Iterator[Tuple[Dict[str, str], unittest.mock.MagicMock]]:
    """
    Helper: collect content of rendered config files and calls removing them.

    Keeps unit tests off the filesystem, which the disks module writes to directly.
    """
    written: Dict[str, str] = {}

    @contextmanager
    def collect(path: str) -> Iterator[IO[str]]:
        buffer = io.StringIO()
        yield buffer
        written[path] = written.get(path, "") + buffer.getvalue()

    with unittest.mock.patch(
        "ch_backup.clickhouse.disks._open_config_file", new=collect
    ):
        with unittest.mock.patch("os.remove") as remove_mock:
            yield written, remove_mock


def test_backup_disk_config():
    """
    create_disk() must point the temporary disk to the backup bucket and use
    the credentials of the backup storage, not the ones of the source disk.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    expected_config = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <object_storage_backup>
                        <type>s3</type>
                        <endpoint>https://minio:9000/backup-bucket/ch_backup/20260101T000000/cloud_storage/object_storage/</endpoint>
                        <access_key_id>BackupAccessKey</access_key_id>
                        <secret_access_key>BackupSecretKey</secret_access_key>
                        <request_timeout_ms>3600000</request_timeout_ms>
                    </object_storage_backup>
                    <object_storage>
                        <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                </disks>
            </storage_configuration>
        </clickhouse>
    """

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    assert_equal(
        xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False),
        xmltodict.parse(expected_config, disable_entities=False),
    )


def _created_disk_config(
    storage_config: dict,
) -> dict:
    """Helper: build a backup disk and return its rendered configuration."""
    disk_manager, _ = _make_backup_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG, storage_config=storage_config
    )
    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    return xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False)[
        "clickhouse"
    ]["storage_configuration"]["disks"]["object_storage_backup"]


def test_backup_disk_endpoint_follows_virtual_addressing_style():
    """
    With virtual addressing the bucket is a part of the host name, addressing it
    as a path would not resolve.
    """
    disk_config = _created_disk_config(
        {"boto_config": {"addressing_style": "virtual", "region_name": "us-east-1"}}
    )

    assert_equal(
        disk_config["endpoint"],
        "https://backup-bucket.minio:9000/ch_backup/20260101T000000/cloud_storage/object_storage/",
    )


def test_backup_disk_uses_the_proxy_of_the_backup_storage():
    """
    ClickHouse must reach the backup storage the same way ch-backup does.
    """
    with unittest.mock.patch(
        "ch_backup.clickhouse.disks.resolve_proxy_host", return_value="proxy-host"
    ):
        disk_config = _created_disk_config(
            {"proxy_resolver": {"uri": "http://resolver/", "proxy_port": 8080}}
        )

    assert_equal(disk_config["proxy"], {"uri": "http://proxy-host:8080"})


def test_backup_disk_has_no_proxy_without_a_resolver():
    """
    Proxy is optional, an unset resolver must not end up in the configuration.
    """
    assert "proxy" not in _created_disk_config({})


def test_backup_disk_keeps_a_larger_request_timeout():
    """
    A timeout configured by the user must not be lowered.
    """
    clickhouse_config = BACKUP_DISK_CLICKHOUSE_CONFIG.replace(
        "</object_storage>",
        "<request_timeout_ms>7200000</request_timeout_ms></object_storage>",
    )
    disk_manager, _ = _make_backup_disks(clickhouse_config)

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    disks = xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False)[
        "clickhouse"
    ]["storage_configuration"]["disks"]

    assert_equal(disks["object_storage_backup"]["request_timeout_ms"], "7200000")
    assert "object_storage" not in disks


def test_backup_disk_is_added_to_clickhouse_disks_config():
    """
    The clickhouse-disks utility gets its own config, so it must list both the
    source disk and the created one, otherwise the copy command cannot run.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    disks = xmltodict.parse(
        written["/tmp/clickhouse-disks-config.xml"], disable_entities=False
    )["clickhouse"]["storage_configuration"]["disks"]

    assert sorted(disks) == ["object_storage", "object_storage_backup"]


def test_backup_disk_source_disk_is_not_modified():
    """
    create_disk() must not touch the configuration of the source disk.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            disk_manager.create_disk("object_storage")
            # pylint: disable=protected-access
            source_config = disk_manager._disks["object_storage"]

    assert_equal(
        source_config["endpoint"], "https://localhost/test-bucket/cluster1/shard1/"
    )
    assert_equal(source_config["access_key_id"], "AKIAACCESSKEY")


def test_backup_disk_is_created_once():
    """
    Repeated calls must reuse the disk instead of reloading the configuration.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            first = disk_manager.create_disk("object_storage")
            second = disk_manager.create_disk("object_storage")
            assert ch_ctl.reload_config.call_count == 1

    assert first is second


def test_backup_disk_missing_disk_raises():
    """
    create_disk() must raise ClickHouseDisksException when the disk is absent
    from the ClickHouse storage_configuration.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            try:
                disk_manager.create_disk("missing_disk")
                assert False, "Expected ClickHouseDisksException was not raised"
            except ClickHouseDisksException as exc:
                assert "missing_disk" in str(exc)
                assert "storage_configuration" in str(exc)


def test_backup_disk_is_cleaned_up_on_exit():
    """
    Leaving the context must remove the generated config files, tell ClickHouse
    to forget the disk and drop the local metadata written while copying.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (_, remove_mock):
        with unittest.mock.patch("shutil.rmtree") as rmtree_mock:
            with disk_manager:
                disk_manager.create_disk("object_storage")

    assert_equal(
        [call.args[0] for call in remove_mock.call_args_list],
        [BACKUP_DISK_CONFIG_PATH, CH_DISK_CONFIG_PATH],
    )
    rmtree_mock.assert_called_once_with(
        os.path.join(BACKUP_DISK_PATH, "shadow"), ignore_errors=True
    )
    assert ch_ctl.reload_config.call_count == 2
    # pylint: disable=protected-access
    assert "object_storage_backup" not in disk_manager._disks


def test_backup_disk_cleanup_is_omitted_on_error():
    """
    Configuration of a failed backup must be left in place for investigation.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (_, remove_mock):
        with unittest.mock.patch("shutil.rmtree") as rmtree_mock:
            try:
                with disk_manager:
                    disk_manager.create_disk("object_storage")
                    raise ValueError("copy failed")
            except ValueError:
                pass

    remove_mock.assert_not_called()
    rmtree_mock.assert_not_called()


def test_copy_table_data_copies_frozen_shadow_directory():
    """
    Data must be copied from the shadow directory of the source disk to the
    same path on the backup disk, so that uploaded metadata keeps its layout.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    source_disk = Disk("object_storage", "/var/lib/clickhouse/disks/s3/", "s3")
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [source_disk],
        ["/var/lib/clickhouse/disks/s3/store/abc/abcdef"],
        "",
        "",
        "some-uuid",
    )

    with _capture_config_files():
        with unittest.mock.patch("ch_backup.clickhouse.disks._exec") as exec_mock:
            with unittest.mock.patch("os.makedirs") as makedirs_mock:
                with unittest.mock.patch("shutil.rmtree"):
                    with disk_manager:
                        disk = disk_manager.copy_table_data("object_storage", table)

    assert disk.name == "object_storage_backup"
    command_args = exec_mock.call_args.kwargs["command_args"]
    assert "object_storage" in command_args
    assert "object_storage_backup" in command_args
    assert (
        command_args.count("shadow/20260101T000000/store/abc/abcdef/") == 2
    ), command_args
    # Only the parent is created: clickhouse-disks nests the copy one level
    # deeper when the destination directory already exists
    makedirs_mock.assert_called_once_with(
        os.path.join(BACKUP_DISK_PATH, "shadow/20260101T000000/store/abc"),
        exist_ok=True,
    )


def test_restore_requires_source_bucket_when_data_is_not_copied():
    """
    A backup that only references the source bucket cannot be restored without it.
    """
    try:
        _make_temporary_disks(
            BACKUP_DISK_CLICKHOUSE_CONFIG,
            cloud_storage_disks=["object_storage"],
            source_bucket=None,
        )
        assert False, "Expected RuntimeError was not raised"
    except RuntimeError as exc:
        assert "cloud-storage-source-bucket" in str(exc)


def test_restore_reads_copied_data_from_the_backup_bucket():
    """
    When data is copied into the backup, the temporary disk must point to the
    backup bucket and use the credentials of the backup storage.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
    )

    expected_config = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <object_storage_source>
                        <type>s3</type>
                        <endpoint>https://minio:9000/backup-bucket/ch_backup/20260101T000000/cloud_storage/object_storage/</endpoint>
                        <access_key_id>BackupAccessKey</access_key_id>
                        <secret_access_key>BackupSecretKey</secret_access_key>
                        <request_timeout_ms>3600000</request_timeout_ms>
                        <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                        <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                </disks>
            </storage_configuration>
        </clickhouse>
    """

    with _capture_config_files() as (written, _):
        with disk_manager:
            pass

    config_path = "/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_object_storage_source.xml"
    assert_equal(
        xmltodict.parse(written[config_path], disable_entities=False),
        xmltodict.parse(expected_config, disable_entities=False),
    )


def test_restore_and_backup_use_the_same_location():
    """
    Restore must read data from where the backup wrote it, otherwise copied
    objects are unreachable.
    """
    backup_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    restore_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
    )

    with _capture_config_files():
        with backup_manager:
            backup_manager.create_disk("object_storage")
            # pylint: disable=protected-access
            backup_endpoint = backup_manager._disks["object_storage_backup"]["endpoint"]
        with restore_manager:
            # pylint: disable=protected-access
            restore_endpoint = restore_manager._disks["object_storage_source"][
                "endpoint"
            ]

    assert_equal(restore_endpoint, backup_endpoint)
