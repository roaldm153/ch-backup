"""
Clickhouse-disks controls temporary cloud storage disks management.
"""

import copy
import os
import shutil
from contextlib import contextmanager
from functools import partial
from subprocess import PIPE, Popen
from types import TracebackType
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
)
from urllib.parse import urlparse

import xmltodict

from ch_backup import logging
from ch_backup.backup.layout import CLOUD_STORAGE_DATA_DIR, BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    PartMetadata,
    sanitize_backup_name,
)
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import Config
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.engine.s3.s3_client_factory import resolve_proxy_host
from ch_backup.util import is_equal_s3_endpoints


class ClickHouseDisksException(RuntimeError):
    """
    ClickHouse-disks call error.
    """

    pass


CH_DISK_CONFIG_PATH = "/tmp/clickhouse-disks-config.xml"
CH_DISK_HISTORY_FILE_PATH = "/tmp/.disks-file-history"
CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS = 1 * 60 * 60 * 1000


class ClickHouseTemporaryDisks:
    """
    Manages temporary cloud storage disks.
    """

    # pylint: disable=too-many-instance-attributes,too-many-positional-arguments,too-many-arguments
    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        backup_layout: BackupLayout,
        config: Config,
        backup_meta: BackupMetadata,
        source_bucket: Optional[str],
        source_path: Optional[str],
        source_endpoint: Optional[str],
        ch_config: ClickhouseConfig,
        desired_tables: Sequence[TableMetadata] | Literal["all"] = "all",
        use_local_copy: bool = False,
    ):
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
        self._config = config["backup"]
        self._config_dir = config["clickhouse"]["config_dir"]
        self._storage_config = config["storage"]
        self._backup_meta = backup_meta
        self._ch_config = ch_config
        self._use_local_copy = use_local_copy
        self._source_bucket: str = source_bucket or ""
        self._source_path: str = source_path or ""
        self._source_endpoint: str = source_endpoint or ""
        if (
            self._backup_meta.cloud_storage.enabled
            and not self._backup_meta.cloud_storage.data_copied
            and source_bucket is None
        ):
            raise RuntimeError(
                "Backup contains cloud storage data, cloud-storage-source-bucket must be set."
            )
        self._desired_tables: Sequence[TableMetadata] | Literal["all"] = desired_tables

        self._disks: Dict[str, Dict] = {}
        self._created_disks: Dict[str, Disk] = {}
        self._ch_availible_disks: Dict[str, Disk] = {}

    def __enter__(self):
        self._disks = self._ch_config.config.get("storage_configuration", {}).get(
            "disks", {}
        )
        for disk_name in self._backup_meta.cloud_storage.disks:
            self._create_temporary_disk(
                self._backup_meta,
                disk_name,
                self._desired_tables,
            )
        self._backup_layout.wait()
        self._ch_availible_disks = self._ch_ctl.get_disks()
        _render_ch_disks_config(self._disks)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        if exc_type is not None:
            logging.warning(
                f'Omitting tmp cloud storage disk cleanup due to exception: "{exc_type.__name__}: {value}"'
            )
            return False

        for disk in self._created_disks.values():
            logging.debug(f"Removing tmp disk {disk.name}")
            _remove_file(_get_config_path(self._config_dir, disk.name))
            self._disks.pop(disk.name, None)
        if self._created_disks:
            self._ch_ctl.reload_config()
        self._created_disks.clear()
        _remove_file(CH_DISK_CONFIG_PATH)
        return True

    def _create_temporary_disk(
        self,
        backup_meta: BackupMetadata,
        disk_name: str,
        desired_tables: Sequence[TableMetadata] | Literal["all"] = "all",
    ) -> None:
        tmp_disk_name = _get_tmp_disk_name(disk_name)
        logging.debug(f"Creating tmp disk {tmp_disk_name}")
        if disk_name not in self._disks:
            raise ClickHouseDisksException(
                f'Disk "{disk_name}" is present in backup cloud storage metadata'
                f" but is missing from ClickHouse storage_configuration."
                f" Add the disk to the ClickHouse configuration and retry."
            )
        disk_config = copy.copy(self._disks[disk_name])

        orig_disk_endpoint = self._disks[disk_name]["endpoint"]
        self._set_disk_source(disk_config, disk_name)
        tmp_disk_endpoint = disk_config["endpoint"]

        if self._use_local_copy and not is_equal_s3_endpoints(
            tmp_disk_endpoint, orig_disk_endpoint
        ):
            raise RuntimeError(
                f"Endpoint of tmp object storage disk is not equal to original (original {orig_disk_endpoint}  tmp: {tmp_disk_endpoint})."
                "It is required for inplace restore mode."
            )

        disks_config = {tmp_disk_name: disk_config}

        if _raise_request_timeout(disk_config):
            disks_config[disk_name] = _request_timeout_override()
            if self._disks:
                self._disks[disk_name]["request_timeout_ms"] = str(
                    CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
                )

        disks_config[tmp_disk_name]["skip_access_check"] = str(True).lower()

        _render_disks_config(
            _get_config_path(self._config_dir, tmp_disk_name),
            disks_config,
        )

        self._ch_ctl.reload_config()
        source_disk = self._ch_ctl.get_disk(tmp_disk_name)
        logging.debug(f'Restoring Cloud Storage "shadow" data of disk "{disk_name}"')
        self._backup_layout.download_cloud_storage_metadata(
            backup_meta,
            source_disk,
            disk_name,
            desired_tables,
        )

        self._created_disks[tmp_disk_name] = source_disk
        self._disks[tmp_disk_name] = disks_config[tmp_disk_name]

    def _set_disk_source(self, disk_config: Dict, disk_name: str) -> None:
        """
        Point a temporary disk configuration to the location of the data.

        Data copied into the backup is read from the backup bucket, the rest
        from the bucket of the source ClickHouse installation.
        """
        if self._backup_meta.cloud_storage.data_copied:
            _set_backup_storage(
                disk_config,
                self._storage_config,
                self._config["path_root"],
                self._backup_meta,
                disk_name,
            )
            return

        endpoint = urlparse(disk_config["endpoint"])
        disk_config["endpoint"] = os.path.join(
            f"{endpoint.scheme}://{self._source_endpoint or endpoint.netloc}",
            self._source_bucket,
            self._source_path,
            "",
        )

    def copy_parts(
        self,
        backup_meta: BackupMetadata,
        parts_to_copy: List[Tuple[Table, PartMetadata]],
        max_proccesses_count: int,
        keep_going: bool,
        part_callback: Callable,
    ) -> None:
        """
        Copy parts from temporary cloud storage disk to actual.

        If clickhouse greater or equal than 24.1 then we are able to use s3-server-side copy.
        Spawns no more than max_processes_count of clickhouse-disks subproceses to copy part from tmp disk.
        """

        if max_proccesses_count > 1 and not self._ch_ctl.ch_version_ge("23.3"):
            logging.warning(
                "It is unsafe to use cloud_storage_restore_workers > 1 with clickhouse version < 23.3"
                f"(cloud_storage_restore_workers: {max_proccesses_count}, ch_version: {self._ch_ctl.get_version()}"
            )
        with ThreadExecPool(max_proccesses_count) as executor:
            for part in parts_to_copy:
                executor.submit(
                    f"Restore of part {part[1].name}",
                    self._run_copy_command,
                    backup_meta,
                    part[0],
                    part[1],
                    callback=partial(part_callback, part[1]),
                )
            executor.wait_all(keep_going)

    def _run_copy_command(
        self, backup_meta: BackupMetadata, table: Table, part: PartMetadata
    ) -> None:
        """
        Copy data from temporary cloud storage disk to actual.
        """
        source_part_name = part.deduplicated_part_name

        routine_tag = f"{table.database}.{table.name}::{source_part_name}"
        target_disk = self._ch_availible_disks[part.disk_name]
        source_disk = self._ch_availible_disks[_get_tmp_disk_name(part.disk_name)]
        for path, disk in table.paths_with_disks:
            if disk.name == target_disk.name:
                table_path = os.path.relpath(path, target_disk.path)
                target_path = os.path.join(table_path, "detached")
                if self._ch_ctl.ch_version_ge("23.7"):
                    target_path = os.path.join(target_path, part.name, "")
                source_path = os.path.join(
                    "shadow",
                    backup_meta.get_sanitized_name(),
                    table_path,
                    source_part_name,
                    "",
                )
                self._copy_dir(
                    source_disk.name,
                    source_path,
                    target_disk.name,
                    target_path,
                    routine_tag,
                )
                return

        raise RuntimeError(
            f'Disk "{target_disk.name}" path not found for table `{table.database}`.`{table.name}`'
        )

    # pylint: disable=too-many-positional-arguments
    def _copy_dir(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        if self._use_local_copy:
            self._os_copy(from_disk, from_path, to_disk, to_path, routine_tag)
        else:
            _ch_disks_copy(
                self._ch_ctl, from_disk, from_path, to_disk, to_path, routine_tag
            )

    # pylint: disable=too-many-positional-arguments
    def _os_copy(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        from_full_path = os.path.join(self._ch_ctl.get_disk(from_disk).path, from_path)
        to_full_path = os.path.join(self._ch_ctl.get_disk(to_disk).path, to_path)

        result = _exec(
            routine_tag,
            exe="/bin/cp",
            common_args=["-rf", from_full_path, to_full_path],
        )
        logging.info(f"os copy result for {routine_tag}: {result}")


class ClickHouseBackupDisks:
    """
    Manages temporary cloud storage disks pointing to the backup bucket.
    """

    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        config: Config,
        backup_meta: BackupMetadata,
        ch_config: ClickhouseConfig,
    ) -> None:
        self._ch_ctl = ch_ctl
        self._config_dir = config["clickhouse"]["config_dir"]
        self._path_root = config["backup"]["path_root"]
        self._storage_config = config["storage"]
        self._backup_meta = backup_meta
        self._ch_config = ch_config

        self._disks: Dict[str, Dict] = {}
        self._created_disks: Dict[str, Disk] = {}

    def __enter__(self) -> "ClickHouseBackupDisks":
        """
        Read currently configured disks from the ClickHouse configuration.
        """
        self._disks = self._ch_config.config.get("storage_configuration", {}).get(
            "disks", {}
        )
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """
        Remove configuration files and local metadata of the created disks.
        """
        if exc_type is not None:
            logging.warning(
                f'Omitting backup cloud storage disk cleanup due to exception: "{exc_type.__name__}: {value}"'
            )
            return

        for disk_name, disk in self._created_disks.items():
            logging.debug(f"Removing tmp disk {disk_name}")
            _remove_file(_get_config_path(self._config_dir, disk_name))
            shutil.rmtree(os.path.join(disk.path, "shadow"), ignore_errors=True)
            self._disks.pop(disk_name, None)
        if self._created_disks:
            self._ch_ctl.reload_config()
        self._created_disks.clear()
        _remove_file(CH_DISK_CONFIG_PATH)

    def create_disk(self, disk_name: str) -> Disk:
        """
        Create a temporary disk that writes to the backup bucket.

        Returns the already created disk if called for the same disk twice.
        """
        tmp_disk_name = _get_backup_disk_name(disk_name)
        if tmp_disk_name in self._created_disks:
            return self._created_disks[tmp_disk_name]

        if disk_name not in self._disks:
            raise ClickHouseDisksException(
                f'Disk "{disk_name}" is missing from ClickHouse storage_configuration.'
            )

        logging.debug(f"Creating tmp disk {tmp_disk_name}")
        disk_config = copy.copy(self._disks[disk_name])
        _set_backup_storage(
            disk_config,
            self._storage_config,
            self._path_root,
            self._backup_meta,
            disk_name,
        )

        disks_config = {tmp_disk_name: disk_config}
        # Both sides of the copy must tolerate slow object storage requests.
        if _raise_request_timeout(disk_config):
            disks_config[disk_name] = _request_timeout_override()
            self._disks[disk_name]["request_timeout_ms"] = str(
                CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
            )

        _render_disks_config(
            _get_config_path(self._config_dir, tmp_disk_name),
            disks_config,
        )
        self._ch_ctl.reload_config()

        disk = self._ch_ctl.get_disk(tmp_disk_name)
        self._created_disks[tmp_disk_name] = disk
        self._disks[tmp_disk_name] = disk_config
        _render_ch_disks_config(self._disks)
        return disk

    def copy_table_data(self, disk_name: str, table: Table) -> Disk:
        """
        Copy frozen table data from a given disk into the backup bucket.

        Returns the temporary disk holding metadata of the copied objects.
        """
        assert table.path_on_disk, f"Table {table} doesn't store data on disk"

        backup_disk = self.create_disk(disk_name)
        shadow_path = os.path.join(
            "shadow",
            self._backup_meta.get_sanitized_name(),
            table.path_on_disk,
            "",
        )
        # clickhouse-disks creates the last directory of the destination itself,
        # fails if the rest of the path is missing and nests the copy one level
        # deeper if the directory already exists
        target_path = os.path.join(backup_disk.path, shadow_path)
        os.makedirs(os.path.dirname(target_path.rstrip("/")), exist_ok=True)
        _ch_disks_copy(
            self._ch_ctl,
            disk_name,
            shadow_path,
            backup_disk.name,
            shadow_path,
            f"Backup of {table.database}.{table.name} on disk {disk_name}",
        )
        return backup_disk


# pylint: disable=too-many-positional-arguments
def _set_backup_storage(
    disk_config: Dict,
    storage_config: Dict,
    path_root: str,
    backup_meta: BackupMetadata,
    disk_name: str,
) -> None:
    """
    Point a disk configuration to the location of a disk data in the backup.

    The same location is used when the data is copied into the backup and when it
    is read back, so the two can not drift apart.
    """
    credentials = storage_config["credentials"]
    disk_config["endpoint"] = _backup_disk_endpoint(
        storage_config, path_root, backup_meta, disk_name
    )
    disk_config["access_key_id"] = credentials["access_key_id"]
    disk_config["secret_access_key"] = credentials["secret_access_key"]

    proxy_uri = _backup_storage_proxy_uri(storage_config)
    if proxy_uri:
        disk_config["proxy"] = {"uri": proxy_uri}


def _backup_disk_endpoint(
    storage_config: Dict, path_root: str, backup_meta: BackupMetadata, disk_name: str
) -> str:
    """
    Build the backup bucket URL where data of a given disk is stored.
    """
    credentials = storage_config["credentials"]
    bucket = credentials["bucket"]
    prefix = "/".join(
        part.strip("/")
        for part in (
            path_root,
            sanitize_backup_name(backup_meta.name),
            CLOUD_STORAGE_DATA_DIR,
            disk_name,
        )
        if part
    )
    endpoint_url = credentials["endpoint_url"].rstrip("/")

    # "auto" is left path-style: it is the only style verified against the
    # object storages ch-backup is used with.
    if storage_config["boto_config"]["addressing_style"] == "virtual":
        scheme, _, host_and_path = endpoint_url.partition("://")
        return f"{scheme}://{bucket}.{host_and_path}/{prefix}/"

    return f"{endpoint_url}/{bucket}/{prefix}/"


def _backup_storage_proxy_uri(storage_config: Dict) -> Optional[str]:
    """
    Resolve the proxy ch-backup uses to reach the backup storage, if any.
    """
    proxy_resolver = storage_config.get("proxy_resolver", {})
    resolver_uri = proxy_resolver.get("uri")
    if not resolver_uri:
        return None

    host = resolve_proxy_host(resolver_uri)
    return f"http://{host}:{proxy_resolver['proxy_port']}"


def _raise_request_timeout(disk_config: Dict) -> bool:
    """
    Raise request timeout of a disk configuration, keeping a larger one intact.

    Returns whether the timeout was raised.
    """
    if int(disk_config.get("request_timeout_ms", 0)) >= (
        CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
    ):
        return False

    disk_config["request_timeout_ms"] = str(CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS)
    return True


def _request_timeout_override() -> Dict:
    """
    Build a configuration that raises request timeout of an already defined disk.
    """
    return {
        "request_timeout_ms": {
            "@replace": "replace",
            "#text": str(CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS),
        }
    }


def _remove_file(path: str) -> None:
    """
    Remove a file, ignoring a missing one.
    """
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


@contextmanager
def _open_config_file(path: str) -> Iterator[IO[str]]:
    """
    Open a config file for writing, readable by its owner only.

    Disk configurations contain object storage credentials.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yield f


def _render_disks_config(
    path: str, disks: Dict, history_file: Optional[str] = None
) -> None:
    """
    Write disks configuration as a ClickHouse config file.
    """
    config: Dict[str, Any] = {"storage_configuration": {"disks": disks}}
    if history_file is not None:
        config["history-file"] = history_file

    with _open_config_file(path) as f:
        xmltodict.unparse(
            {"clickhouse": config},
            f,
            pretty=True,
        )


def _render_ch_disks_config(disks: Dict[str, Dict]) -> None:
    """
    Write configuration of the clickhouse-disks utility.
    """
    _render_disks_config(
        CH_DISK_CONFIG_PATH,
        {
            name: conf
            for name, conf in disks.items()
            if not conf or conf.get("type") != "cache"
        },
        history_file=CH_DISK_HISTORY_FILE_PATH,
    )


# pylint: disable=too-many-positional-arguments
def _ch_disks_copy(
    ch_ctl: ClickhouseCTL,
    from_disk: str,
    from_path: str,
    to_disk: str,
    to_path: str,
    routine_tag: str,
) -> None:
    """
    Copy a directory between disks with the clickhouse-disks utility.
    """
    command = "copy"
    common_args = ["--config", CH_DISK_CONFIG_PATH]
    if ch_ctl.ch_version_ge("24.7"):
        command_args = [
            "--recursive",
            "--disk-from",
            from_disk,
            "--disk-to",
            to_disk,
            from_path,
            to_path,
            "'",
        ]
        common_args.append("--query")
        # Changes in disks interface require passing command with args in quotes
        command = "'" + command
    elif ch_ctl.ch_version_ge("23.9"):
        command_args = [
            "--disk-from",
            from_disk,
            "--disk-to",
            to_disk,
            from_path,
            to_path,
        ]
    else:
        command_args = [
            "--diskFrom",
            from_disk,
            "--diskTo",
            to_disk,
            from_path,
            to_path,
        ]

    result = _exec(
        routine_tag,
        exe="/usr/bin/clickhouse-disks",
        common_args=common_args,
        command=command,
        command_args=command_args,
    )
    logging.info(f"clickhouse-disks copy result for {routine_tag}: {result}")


def _get_config_path(config_dir: str, disk_name: str) -> str:
    """
    Return path of the config file generated for a temporary disk.
    """
    return os.path.join(config_dir, f"cloud_storage_tmp_disk_{disk_name}.xml")


def _get_tmp_disk_name(disk_name: str) -> str:
    """
    Return name of the temporary disk used to restore data of a given disk.
    """
    return f"{disk_name}_source"


def _get_backup_disk_name(disk_name: str) -> str:
    """
    Return name of the temporary disk used to back up data of a given disk.
    """
    return f"{disk_name}_backup"


def _exec(
    routine_tag: str,
    exe: str,
    common_args: List[str],
    command: Optional[str] = None,
    command_args: Optional[List[str]] = None,
) -> Any:

    proc_logger = logging.getLogger("clickhouse-disks").bind(tag=routine_tag)
    args = [
        exe,
        *common_args,
    ]
    if command:
        command_with_args = [command, *command_args] if command_args else [command]
        args += command_with_args  # type: ignore

    args = " ".join(args)  # type: ignore
    logging.debug(f'Executing "{args}"')

    with Popen(args, stdout=PIPE, stderr=PIPE, shell=True) as proc:  # nosec
        while proc.poll() is None:
            for line in proc.stderr.readlines():  # type: ignore
                proc_logger.info(line.decode("utf-8").strip())
        if proc.returncode != 0:
            raise ClickHouseDisksException(
                f"{exe} call failed with exitcode: {proc.returncode}"
            )

        return list(map(lambda b: b.decode("utf-8"), proc.stdout.readlines()))  # type: ignore
