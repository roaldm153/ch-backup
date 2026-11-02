"""Private disk indexes for copy plans, listings and journal confirmations."""

import json
import sqlite3
import threading
import weakref
from collections.abc import Iterable, Iterator, MutableMapping
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import IO, Any, Generic, TypeVar

from ch_backup.storage.object_copy import CopyEntry

K = TypeVar("K")
V = TypeVar("V")


def _close_index(connection: sqlite3.Connection, file: IO[bytes]) -> None:
    """Close the database before deleting its private file."""
    connection.close()
    file.close()


def _pack_key(key: Any) -> bytes:
    """Encode digest bytes and structured object keys without executable formats."""
    return b"b" + key if isinstance(key, bytes) else b"j" + json.dumps(key).encode()


def _unpack_key(data: bytes) -> Any:
    """Recreate hashable keys written to the private index."""
    return data[1:] if data[:1] == b"b" else _tuple_key(json.loads(data[1:]))


def _tuple_key(value):
    """Restore nested tuple identities from JSON arrays."""
    return (
        tuple(_tuple_key(item) for item in value) if isinstance(value, list) else value
    )


class CopyIndex(MutableMapping[K, V], Generic[K, V]):
    """Keep arbitrary internal records on disk with a bounded page cache."""

    def __init__(self) -> None:
        """Create an ephemeral index that is never reused as remote proof."""
        # pylint: disable-next=consider-using-with
        file = NamedTemporaryFile()
        self._db = sqlite3.connect(file.name, check_same_thread=False)
        self._lock = threading.RLock()
        self._count = 0
        self._writes = 0
        self._importing = False
        self._db.execute("PRAGMA cache_size=-2048")
        self._db.execute("PRAGMA temp_store=FILE")
        self._db.execute("PRAGMA mmap_size=0")
        self._db.execute(
            "CREATE TABLE records (key BLOB PRIMARY KEY, value BLOB NOT NULL)"
        )
        self._cleanup = weakref.finalize(self, _close_index, self._db, file)

    def _encode(self, value: V) -> bytes:
        """Serialize an internal value into the private scratch database."""
        return json.dumps(value).encode()

    def _decode(self, value: bytes) -> V:
        """Read only records written by this process to its own index."""
        return json.loads(value)

    def __getitem__(self, key: K) -> V:
        """Look up a record without caching all keys."""
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM records WHERE key=?",
                (_pack_key(key),),
            ).fetchone()
        if row is None:
            raise KeyError(key)
        return self._decode(row[0])

    def __setitem__(self, key: K, value: V) -> None:
        """Store a record and commit bounded groups of writes."""
        encoded = _pack_key(key)
        with self._lock:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?)",
                (encoded, self._encode(value)),
            )
            self._count += cursor.rowcount
            if not cursor.rowcount:
                self._db.execute(
                    "UPDATE records SET value=? WHERE key=?",
                    (self._encode(value), encoded),
                )
            self._writes += 1
            if self._writes >= 512 and not self._importing:
                self._db.commit()
                self._writes = 0

    def __delitem__(self, key: K) -> None:
        """Remove one record."""
        with self._lock:
            cursor = self._db.execute(
                "DELETE FROM records WHERE key=?", (_pack_key(key),)
            )
            if not cursor.rowcount:
                raise KeyError(key)
            self._count -= 1

    def __iter__(self) -> Iterator[K]:
        """Read keys in insertion order."""
        for key, _ in self._rows():
            yield _unpack_key(key)

    def _rows(self) -> Iterator[tuple[bytes, bytes]]:
        """Release the connection lock between bounded result batches."""
        position = 0
        while True:
            with self._lock:
                cursor = self._db.execute(
                    "SELECT rowid, key, value FROM records WHERE rowid>? ORDER BY rowid LIMIT 128",
                    (position,),
                )
                rows = cursor.fetchall()
                cursor.close()
            if not rows:
                return
            for position, key, value in rows:
                yield key, value

    def __len__(self) -> int:
        """Return the maintained row count without querying from sampler threads."""
        return self._count

    def clear(self) -> None:
        """Forget all records after verification."""
        with self._lock:
            self._db.execute("DELETE FROM records")
            self._db.commit()
            self._count = self._writes = 0

    @contextmanager
    def importing(self):
        """Discard a whole damaged journal chunk, including its valid prefix."""
        self._db.commit()
        count = self._count
        self._importing = True
        try:
            yield
            self._db.commit()
        except BaseException:
            self._db.rollback()
            self._count = count
            raise
        finally:
            self._importing = False
            self._writes = 0


class EntryIndex(CopyIndex[K, CopyEntry]):
    """Store paths and sizes while keeping storage credentials only in memory."""

    def __init__(self) -> None:
        """Share each storage configuration across all of its entries."""
        super().__init__()
        self._storages: list[dict] = []
        self._storage_ids: dict[int, int] = {}

    def _storage(self, storage: dict) -> int:
        """Assign a process-local identifier without persisting secrets."""
        key = id(storage)
        if key not in self._storage_ids:
            self._storage_ids[key] = len(self._storages)
            self._storages.append(storage)
        return self._storage_ids[key]

    def _encode(self, value: CopyEntry) -> bytes:
        """Keep only identifiers, object keys and size on disk."""
        return json.dumps(
            (
                self._storage(value.src_storage),
                self._storage(value.dst_storage),
                value.src_path,
                value.dst_path,
                value.size,
                value.server_side,
            ),
        ).encode()

    def _decode(self, value: bytes) -> CopyEntry:
        """Attach the original in-memory configurations to one entry."""
        source, target, *fields = json.loads(value)
        return CopyEntry(self._storages[source], self._storages[target], *fields)


class CopyEntries:
    """Keep an ordered plan on disk and expose only one batch at a time."""

    def __init__(self, entries: Iterable[CopyEntry] = ()) -> None:
        """Append initial entries without collecting the iterable."""
        self._index: EntryIndex[int] = EntryIndex()
        self._segments: list[EntryIndex] = [self._index]
        self.extend(entries)

    def append(self, entry: CopyEntry) -> None:
        """Append one plan entry."""
        if self._segments[-1] is not self._index:
            self._index = EntryIndex()
            self._segments.append(self._index)
        self._index[len(self._index)] = entry

    def extend(self, entries: Iterable[CopyEntry]) -> None:
        """Append entries in order."""
        for entry in entries:
            self.append(entry)

    def extend_index(self, entries: EntryIndex) -> None:
        """Reuse a planned object index without serializing every entry again."""
        self._segments.append(entries)

    def __len__(self) -> int:
        """Return the number of planned objects."""
        return sum(len(segment) for segment in self._segments)

    def __iter__(self) -> Iterator[CopyEntry]:
        """Read bounded batches from the plan."""
        for segment in self._segments:
            yield from segment.values()
