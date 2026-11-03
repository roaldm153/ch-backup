"""Check isolated SQLite cache experiments without launching the benchmark."""

# pylint: disable=protected-access

import gc
import importlib
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ch_backup.backup.copy_index import CopyEntries, CopyIndex, EntryIndex
from tests.benchmark.copy_memory import probe, report


@pytest.mark.parametrize("mib", [2, 8, 16, 32, 64])
def test_cache_override_preserves_indexes_and_rollback(
    tmp_path: Path, mib: int
) -> None:
    """Apply the cache to inherited indexes without changing rollback or their lifetime."""
    observer = probe.Probe(tmp_path, 1, False, sqlite_cache_mib=mib)
    with patch.object(CopyIndex, "__init__", observer._index_cache(CopyIndex.__init__)):
        records: CopyIndex[str, bool] = CopyIndex()
        entries: EntryIndex[str] = EntryIndex()
        plan = CopyEntries()
        for connection in (records._db, entries._db, plan._index._db):
            assert connection.execute("PRAGMA cache_size").fetchone()[0] == -mib * 1024
            assert connection.execute("PRAGMA mmap_size").fetchone()[0] == 0
            assert connection.execute("PRAGMA temp_store").fetchone()[0] == 1
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        records["confirmed"] = True
        with pytest.raises(ValueError):
            with records.importing():
                records["incomplete"] = True
                raise ValueError("damaged journal")
        assert list(records) == ["confirmed"]
        assert observer._counters()["sqlite_cache_budget_kib"] == 3 * mib * 1024
        del records, entries, plan
        gc.collect()
        counters = observer._counters()
        assert counters["sqlite_indexes"] == 0
        assert counters["sqlite_cache_budget_kib"] == 0
        assert counters["sqlite_indexes_created"] == 3
        assert counters["sqlite_indexes_peak"] == 3
        assert counters["sqlite_cache_budget_peak_kib"] == 3 * mib * 1024
    default_index: CopyIndex[str, bool] = CopyIndex()
    assert default_index._db.execute("PRAGMA cache_size").fetchone()[0] == -2048


def test_cache_override_must_be_applied(tmp_path: Path) -> None:
    """Fail instead of reporting a requested cache that SQLite did not accept."""
    observer = probe.Probe(tmp_path, 1, False, sqlite_cache_mib=64)
    index = Mock()
    index._db.execute.return_value.fetchone.return_value = (-2048,)
    with pytest.raises(RuntimeError, match="not applied"):
        observer._index_cache(Mock())(index)
    assert not observer.indexes


def test_cache_override_requires_supported_code(tmp_path: Path) -> None:
    """An explicitly requested experiment cannot silently skip an unsupported hook."""
    observer = probe.Probe(tmp_path, 1, False, strict=False, sqlite_cache_mib=16)
    with (
        patch.object(probe.importlib, "import_module", side_effect=ImportError),
        pytest.raises(RuntimeError, match="CopyIndex"),
    ):
        observer.install(False, False)


def test_regular_probe_keeps_original_cache_behavior(tmp_path: Path) -> None:
    """Do not add cache hooks or invented zero counters to ordinary benchmarks."""
    observer = probe.Probe(tmp_path, 1, False)
    with patch.object(observer, "_patch") as install:
        observer.install(False, False)
    assert all(call.args[0] != probe.INDEX for call in install.call_args_list)
    assert "sqlite_indexes" not in observer._counters()


@pytest.mark.parametrize("value", ["0", "-2", "65", "1.5", "nan"])
def test_cache_cli_rejects_invalid_size(tmp_path: Path, value: str) -> None:
    """Reject invalid budgets before importing or running the measured code."""
    with pytest.raises(SystemExit) as error:
        probe._parse_args(["--out", str(tmp_path), "--sqlite-cache-mib", value])
    assert error.value.code == 2


@pytest.mark.parametrize("variant", ["baseline", "candidate"])
@pytest.mark.parametrize("mismatch", [None, "size", "unused"])
def test_driver_checks_cache_for_each_variant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str, mismatch: str | None
) -> None:
    """Keep both cache settings distinct and reject successful but invalid experiments."""
    with patch("docker.from_env"):
        driver = importlib.import_module("tests.benchmark.copy_memory.driver")
    monkeypatch.chdir(tmp_path)
    benchmark = driver.Driver(
        driver.Options(
            parts=[10],
            versions=["26.3"],
            sqlite_cache_mib=64,
            baseline_sqlite_cache_mib=2,
        )
    )
    benchmark.sources[variant] = tmp_path / "sources" / variant
    expected = 2 if variant == "baseline" else 64
    run = driver.Run("fresh_planning", "planning_only", "planning", variant=variant)
    directory = tmp_path / run.name
    directory.mkdir()
    (directory / "probe.json").write_text(
        json.dumps(
            {
                "sqlite_cache_mib": 8 if mismatch == "size" else expected,
                "code_root": f"/code/sources/{variant}/ch_backup",
            }
        )
    )
    (directory / "summary.json").write_text(
        json.dumps(
            {
                "outcome": "planning_only",
                "counters": {
                    "sqlite_indexes_created": 0 if mismatch == "unused" else 3
                },
            }
        )
    )
    env = Mock()
    env.wait_detached.return_value = 0
    with patch.object(driver, "_oom_events", return_value=[]):
        result = benchmark._probe(env, tmp_path, run)
    command = env.start_detached.call_args.args[0]
    assert command[command.index("--sqlite-cache-mib") + 1] == str(expected)
    assert result["exit_code"] == int(mismatch is not None)
    assert result["outcome"] == ("cache_mismatch" if mismatch else "planning_only")


def test_cache_report_separates_budget_from_measured_memory() -> None:
    """Report configured cache totals separately from measured process peaks."""
    runs = [
        {
            "kind": "fresh_planning",
            "mode": "planning_only",
            "variant": variant,
            "exit_code": 0,
            "duration": 10,
            "summary": {
                "counters": {
                    "sqlite_indexes_peak": 3,
                    "sqlite_cache_budget_peak_kib": mib * 3 * 1024,
                }
            },
        }
        for variant, mib in (("baseline", 2), ("candidate", 64))
    ]
    rows = report.comparison_rows([{"version": "26.3", "parts": 10, "runs": runs}])
    budget = next(row for row in rows if row["metric"] == "sqlite_cache_budget_mib")
    assert (budget["baseline"], budget["candidate"]) == (6, 192)
    assert not any(row["metric"] == "main_gib" for row in rows)
