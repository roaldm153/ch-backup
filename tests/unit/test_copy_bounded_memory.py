"""Check bounded copy structures and preservation of backup safety rules."""

# pylint: disable=protected-access

import copy
import gc
import gzip
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ch_backup.backup.copier import _add_cloud_storage_copy_entry
from ch_backup.backup.copy_context import _archive_record
from ch_backup.backup.copy_index import CopyEntries, CopyIndex, EntryIndex
from ch_backup.compression.gzip import GZIPCompression
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ClickhouseBackupError, StorageError
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import run_and_collect_all
from ch_backup.storage.metadata_archive import MetadataArchive, spool_metadata
from ch_backup.storage.object_copy import CopyEntry
from tests.unit.test_backup_copy import _backup, _part
from tests.unit.test_copy_context import STORAGE, MemoryEngine, _context, _entry


@pytest.mark.parametrize("split", [1, 37, 1000000])
def test_decompression_limits_each_queued_chunk(split: int) -> None:
    """Expansion must remain bounded before reaching the rechunking stage."""
    data = b"a" * (3 * 1024 * 1024 + 31)
    compressed = gzip.compress(data)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["filesystem"]["chunk_size"] = 4096
    builder = PipelineBuilder(config).build_iterable_stage(
        compressed[i : i + split] for i in range(0, len(compressed), split)
    )
    builder.build_decompress_stage()
    chunks = run_and_collect_all(builder.pipeline())
    assert b"".join(chunks) == data
    assert max(map(len, chunks)) <= 4096


def test_decompression_keeps_full_flush_stream_tail() -> None:
    """Keep the existing gzip format, including a stream finished by Z_FULL_FLUSH."""
    compression = GZIPCompression()
    data = b"metadata" * 17000
    encoded = compression.compress(data) + compression.flush_compress()
    chunks = list(compression.decompress_chunks(encoded, 1024))
    chunks.extend(compression.decompress_chunks(b"", 1024))
    assert b"".join(chunks) + compression.flush_decompress() == data
    assert max(map(len, chunks)) <= 1024
    with pytest.raises(ValueError):
        list(GZIPCompression().decompress_chunks(encoded, 0))


def test_metadata_spool_preserves_long_names_empty_files_and_digest() -> None:
    """Preserve tar members and the old resume digest without retaining file contents."""
    files = [("part/empty", b""), ("part/" + "long" * 50, b"data" * 300000)]
    archive = spool_metadata(iter(files))
    assert len(archive) == 2
    assert list(archive) == files
    assert archive[-1] == files[-1]
    assert _archive_record(STORAGE, "archive", archive, 123) == _archive_record(
        STORAGE, "archive", files, 123
    )
    path = Path(archive.path())
    assert path.stat().st_mode & 0o077 == 0
    del archive
    gc.collect()
    assert not path.exists()


def test_downloaded_metadata_archive_is_reiterable() -> None:
    """Read a downloaded tar without a cached member list."""
    original = spool_metadata([("part/data", b"payload")])
    downloaded = MetadataArchive(downloaded=True)
    Path(downloaded.path()).write_bytes(Path(original.path()).read_bytes())
    assert len(downloaded) == 1
    assert list(downloaded) == list(downloaded) == [("part/data", b"payload")]


def test_entry_index_rejects_cross_part_conflicts_and_keeps_secrets_in_memory() -> None:
    """A global disk index must detect conflicting sources and avoid persisting credentials."""
    storage: dict = copy.deepcopy(STORAGE)
    storage["credentials"]["secret_access_key"] = "DO_NOT_PERSIST"
    index: EntryIndex[str] = EntryIndex()
    first = CopyEntry(storage, storage, "source/a", "target/shared", 1)
    _add_cloud_storage_copy_entry(index, first)
    _add_cloud_storage_copy_entry(index, first)
    assert len(index) == 1
    with pytest.raises(ClickhouseBackupError):
        _add_cloud_storage_copy_entry(index, first._replace(src_path="source/b"))
    assert index[first.dst_path] == first
    index._db.commit()
    path = Path(index._db.execute("PRAGMA database_list").fetchone()[2])
    assert b"DO_NOT_PERSIST" not in path.read_bytes()
    assert index._db.execute("PRAGMA cache_size").fetchone()[0] == -2048
    assert index._db.execute("PRAGMA mmap_size").fetchone()[0] == 0
    del index
    gc.collect()
    assert not path.exists()


def test_plan_and_listing_iterate_in_order_and_roll_back_incomplete_import() -> None:
    """Keep existing confirmations when a later journal chunk is corrupt."""
    entries = CopyEntries(_entry(str(i)) for i in range(300))
    assert len(entries) == 300
    assert [entry.src_path for entry in entries] == [f"src/{i}" for i in range(300)]
    records: CopyIndex[bytes, bool] = CopyIndex()
    records[b"old"] = True
    with pytest.raises(ValueError):
        with records.importing():
            for i in range(1000):
                records[str(i).encode()] = True
            raise ValueError("truncated gzip")
    assert list(records) == [b"old"]
    assert len(records) == 1


def test_failed_journal_uploads_keep_bounded_retry_chunks() -> None:
    """An unavailable target must not grow an in-memory list or retry one giant gzip."""
    engine = MemoryEngine()
    context = _context(engine, flush_size=3)
    engine.upload_error = StorageError("unavailable")
    for i in range(40):
        context.record(_entry(str(i)))
        context.flush()
    assert len(context._buffer) == 40
    engine.upload_error = None
    context.flush(force=True)
    assert not context._buffer
    for data in engine.objects.values():
        assert len(gzip.decompress(data).splitlines()) <= 3
    resumed = _context(engine)
    resumed.load()
    assert all(resumed.is_recorded(_entry(str(i))) for i in range(40))


def test_corrupt_journal_does_not_confirm_its_readable_prefix() -> None:
    """Skip all records from a gzip whose trailer fails validation."""
    engine = MemoryEngine()
    context = _context(engine)
    for i in range(1000):
        context.record(_entry(str(i)))
    context.flush(force=True)
    path = next(iter(engine.objects))
    engine.objects[path] = engine.objects[path][:-8]
    resumed = _context(engine)
    resumed.load()
    assert not resumed.has_records()


def test_streamed_backup_json_preserves_legacy_links_without_mutating_source() -> None:
    """Publish the same metadata without copying the complete databases tree."""
    backup = _backup("b1", parts={"all_1_1_0": _part(link="older")})
    before = copy.deepcopy(backup._databases)
    expected = json.loads(backup.dump_json())
    with patch.object(
        backup, "_databases_with_legacy_part_links", side_effect=AssertionError
    ):
        actual = json.loads("".join(backup.iter_json()))
    assert actual == expected
    assert backup._databases == before
