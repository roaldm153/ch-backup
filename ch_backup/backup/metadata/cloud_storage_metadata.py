"""
Backup metadata for Cloud Storage.
"""

from typing import Any


class CloudStorageMetadata:
    """
    Backup metadata for Cloud Storage.
    """

    def __init__(
        self,
        encryption: bool = True,
        compression: bool = True,
        data_copied: bool = False,
        disks: list[str] | None = None,
    ) -> None:
        self._encryption: bool = encryption
        self._compression: bool = compression
        self._data_copied: bool = data_copied
        self._disks: list[str] = disks or []

    @property
    def enabled(self) -> bool:
        """
        Return True if Cloud Storage is enabled within the backup.
        """
        return len(self._disks) > 0

    @property
    def disks(self) -> list[str]:
        """
        Return list of backed up disks names.
        """
        return self._disks

    def add_disk(self, disk_name: str) -> None:
        """
        Add disk name in backed up disks list.
        """
        if disk_name not in self._disks:
            self._disks.append(disk_name)

    @property
    def encrypted(self) -> bool:
        """
        Return True if Cloud Storage backup is encrypted.
        """
        return self._encryption

    @property
    def compressed(self) -> bool:
        """
        Return True if Cloud Storage backup is compressed.
        """
        return self._compression

    @property
    def data_copied(self) -> bool:
        """
        Return True if Cloud Storage data is copied into the backup.
        """
        return self._data_copied

    @property
    def requires_source_bucket(self) -> bool:
        """
        Return True if restore needs the bucket of the source installation.
        """
        return self.enabled and not self._data_copied

    def encrypt(self) -> None:
        """
        Encrypt Cloud Storage data within the backup.
        """
        self._encryption = True

    def compress(self) -> None:
        """
        Compress Cloud Storage data within the backup.
        """
        self._compression = True

    def copy_data(self) -> None:
        """
        Mark that Cloud Storage data is copied into the backup.
        """
        self._data_copied = True

    @classmethod
    def load(cls, data: dict[str, Any]) -> "CloudStorageMetadata":
        """
        Deserialize Cloud Storage metadata.
        """
        return cls(
            encryption=data.get("encryption", True),
            compression=data.get("compression", False),
            data_copied=data.get("data_copied", False),
            disks=data.get("disks", []),
        )

    def dump(self) -> dict[str, Any]:
        """
        Serialize Cloud Storage metadata.
        """
        return {
            "encryption": self._encryption,
            "compression": self._compression,
            "data_copied": self._data_copied,
            "disks": self._disks,
        }
