"""Summarize standard profiler artifacts and bounded Prometheus histograms."""

import json
import math
import statistics
from collections import defaultdict
from itertools import groupby
from pathlib import Path
from typing import Iterator

from prometheus_client.parser import text_string_to_metric_families


def records(path: Path) -> Iterator[dict]:
    """Stream complete records while allowing a writer to append its last line."""
    if not path.exists():
        return
    with path.open(encoding="utf-8") as source:
        for line in source:
            if not line.endswith("\n"):
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue


def process_series(path: Path) -> tuple[list[dict], dict]:
    """Stream resource samples, keeping bounded plotting data and complete observed totals."""
    # pylint: disable=too-many-locals
    previous: dict = {}
    series: list[dict] = []
    totals: dict = {}
    stride = 1
    for index, (at, rows) in enumerate(
        groupby(records(path), key=lambda row: row["at"])
    ):
        point: dict = {"at": at, "roles": {}}
        current = {}
        for row in rows:
            identity = (row["pid"], row["created"])
            old = previous.get(identity, row)
            current[identity] = row
            role = row["role"]
            if role in ("driver", "collector", "profiler"):
                role = "observer"
            values = point["roles"].setdefault(
                role, {"rss": 0, "cpu": 0, "io": 0, "io_missing": False}
            )
            cpu = max(
                0,
                row["cpu_user"]
                + row["cpu_system"]
                - old["cpu_user"]
                - old["cpu_system"],
            )
            io = sum(
                max(
                    0,
                    (row.get("io") or {}).get(key, 0)
                    - (old.get("io") or {}).get(key, 0),
                )
                for key in ("read_bytes", "write_bytes")
            )
            elapsed = max(at - old["at"], 0.001)
            values["rss"] += row["rss"]
            values["cpu"] += cpu / elapsed
            values["io"] += io / elapsed
            values["io_missing"] |= row.get("io") is None or old.get("io") is None
            total = totals.setdefault(
                role, {"cpu": 0, "io": 0, "peak_rss": 0, "io_missing": False}
            )
            total["cpu"] += cpu
            total["io"] += io
            total["io_missing"] |= values["io_missing"]
        for role, values in point["roles"].items():
            totals[role]["peak_rss"] = max(totals[role]["peak_rss"], values["rss"])
        previous = current
        if index % stride == 0:
            series.append(point)
            if len(series) > 2000:
                series = series[::2]
                stride *= 2
    return series, totals


def resource_lines(points: list[dict]) -> list[str]:
    """Report observed process-tree consumption separately from container caches."""
    lines = [
        "",
        "## Ресурсы процессов",
        "",
        "CPU и I/O — приращения между доступными отсчётами, короткие процессы могут не попасть в выборку. "
        "RSS суммируется в одном отсчёте и может повторно учитывать общие страницы. "
        "clickhouse включает подготовку backup и прочие процессы его контейнера; observer — драйвер, сборщик и py-spy. "
        "Page cache, лимиты, throttling и PSI сохранены отдельно в telemetry/containers.jsonl и host.jsonl.",
        "",
        "| Версия / парты | Процессы | CPU, с | I/O, ГиБ | Пик суммы RSS, ГиБ |",
        "|---|---|---|---|---|",
    ]
    for point in points:
        for role, values in point.get("resource_totals", {}).items():
            io = "—" if values["io_missing"] else f"{values['io'] / 1024**3:.3f}"
            lines.append(
                f"| {point['version']} / {point['parts']} | {role} | {values['cpu']:.1f} | {io} | {values['peak_rss'] / 1024**3:.3f} |"
            )
        if point.get("resource_totals"):
            lines += [
                "",
                f"![Ресурсы](resources_{point['version']}_{point['parts']}.png)",
                "",
            ]
    return lines


def s3_stats(directory: Path) -> dict:
    """Merge cumulative per-process histograms without retaining per-request data."""
    result: dict = {}
    for path in directory.glob("s3-*.prom"):
        for family in text_string_to_metric_families(path.read_text(encoding="utf-8")):
            for sample in family.samples:
                if sample.name.endswith("_created"):
                    continue
                operation = sample.labels["operation"]
                row = result.setdefault(
                    operation,
                    {
                        "call": {},
                        "attempt": {},
                        "errors": 0,
                        "retries": 0,
                        "http_errors": 0,
                    },
                )
                if sample.name == "bench_s3_retries_total":
                    row["retries"] += sample.value
                    continue
                kind = "attempt" if "_attempt_" in sample.name else "call"
                if sample.name.endswith("_bucket"):
                    bound = sample.labels["le"]
                    row[kind][bound] = row[kind].get(bound, 0) + sample.value
                elif (
                    sample.name.endswith("_count")
                    and kind == "call"
                    and sample.labels["outcome"] == "error"
                ):
                    row["errors"] += sample.value
                elif (
                    sample.name.endswith("_count")
                    and kind == "attempt"
                    and sample.labels["outcome"] in ("4xx", "5xx", "transport_error")
                ):
                    row["http_errors"] += sample.value
    return result


def quantile_bound(buckets: dict, quantile: float) -> float | None:
    """Return the containing bucket's upper bound, rather than an invented exact quantile."""
    count = buckets.get("+Inf", 0)
    if not count:
        return None
    for bound, cumulative in sorted(buckets.items(), key=lambda pair: float(pair[0])):
        if cumulative >= quantile * count:
            return float(bound)
    return None


def _bound(value: float | None) -> str:
    """Label histogram resolution and the open final bucket explicitly."""
    if value is None:
        return "—"
    return "более 300" if math.isinf(value) else f"≤ {value:g}"


def s3_lines(points: list[dict]) -> list[str]:
    """Report logical calls separately from actual HTTP attempts and failures."""
    lines = [
        "",
        "## S3: задержки и попытки",
        "",
        "p50/p95/p99 — верхние границы бакетов, секунды. Задержка вызова включает retries; "
        "скачивание тела GetObject после возврата не входит. Счётчики частичных прогонов неполные.",
        "",
        "| Версия / парты / прогон | Операция | Вызовы | HTTP-попытки | Ошибки вызовов | Ошибки HTTP | Retries ответа | Вызовы/с | p50 | p95 | p99 |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for point in points:
        for run in point["runs"]:
            for operation, row in s3_stats(run["dir"]).items():
                count = row["call"].get("+Inf", 0)
                rate = count / run["duration"] if run.get("duration") else 0
                bounds = [
                    _bound(quantile_bound(row["call"], q)) for q in (0.5, 0.95, 0.99)
                ]
                lines.append(
                    f"| {point['version']} / {point['parts']} / {run['kind']}-{run['variant']} "
                    f"| {operation} | {count:g} | {row['attempt'].get('+Inf', 0):g} "
                    f"| {row['errors']:g} | {row['http_errors']:g} | {row['retries']:g} | {rate:.1f} | {' | '.join(bounds)} |"
                )
    return lines


def overhead_lines(points: list[dict]) -> list[str]:
    """Compare only repeated controls of the same point, variant and copy mode."""
    lines = [
        "",
        "## Накладные расходы профилирования",
        "",
        "Контроли используют один исходный backup и разные пустые target-префиксы. "
        "Порядок профайлеров чередуется; кеши ОС не очищаются. CPU-профиль охватывает только записанные окна.",
        "",
        "| Версия / парты / вариант | Профиль | N | Медиана, с | Мин–макс, с | Δ времени | Медиана суммы RSS, ГиБ | Δ RSS |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for point in points:
        groups: dict = defaultdict(list)
        for run in point["runs"]:
            if (
                run["kind"].startswith("control_")
                and run.get("exit_code") == 0
                and run.get("profile_status", "complete") == "complete"
            ):
                groups[(run["variant"], run.get("profile") or "plain")].append(run)
        for (variant, kind), runs in sorted(groups.items()):
            plain = groups.get((variant, "plain"), [])
            baseline = statistics.median(r["duration"] for r in plain) if plain else 0
            duration = statistics.median(r["duration"] for r in runs)
            memory = [
                r["sample_stats"]["peak_total_rss"]
                for r in runs
                if r.get("sample_stats", {}).get("peak_total_rss")
            ]
            base_memory = [
                r["sample_stats"]["peak_total_rss"]
                for r in plain
                if r.get("sample_stats", {}).get("peak_total_rss")
            ]
            median = statistics.median(memory) if memory else None
            delta = f"{100 * (duration / baseline - 1):+.1f}%" if baseline else "—"
            ram = f"{median / 1024**3:.3f}" if median is not None else "—"
            ram_delta = (
                f"{100 * (median / statistics.median(base_memory) - 1):+.1f}%"
                if median is not None and base_memory
                else "—"
            )
            times = [r["duration"] for r in runs]
            lines.append(
                f"| {point['version']} / {point['parts']} / {variant} | {kind} | {len(runs)} | {duration:.1f} | {min(times):.1f}–{max(times):.1f} | {delta} | {ram} | {ram_delta} |"
            )
    return lines


def profile_lines(points: list[dict]) -> list[str]:
    """Link ready-made profiler views and expose coverage and rendering errors."""
    lines = [
        "",
        "## Профили функций",
        "",
        "CPU JSON открывается в Speedscope. Memray HTML показывает heap-выделения, не RSS. "
        "Peak — живые выделения на пике; temporal — выбор интервала; temporary — освобождённые "
        "в пределах 100 последующих выделений. Это разные показатели, их не складывают.",
        "",
    ]
    for point in points:
        for run in point["runs"]:
            directory = run["dir"]
            files = sorted(
                [
                    *directory.glob("cpu-*.speedscope.json"),
                    *directory.glob("memory-*.html"),
                    *directory.glob("memory-*-stats.txt"),
                ]
            )
            if not files and not run.get("profile"):
                continue
            name = f"{point['version']}/{point['parts']}/{directory.name}"
            lines.append(
                f"{name}: {run.get('profile_status', 'не проверено')}. {run.get('profile_error', '')}"
            )
            if run.get("missing_memory_pids") or run.get("unfinished_memory_pids"):
                lines.append(
                    f"Без профиля: {run.get('missing_memory_pids', [])}; без завершения: {run.get('unfinished_memory_pids', [])}."
                )
            lines.extend(f"- [{path.name}]({name}/{path.name})" for path in files)
            lines.append("")
    return lines
