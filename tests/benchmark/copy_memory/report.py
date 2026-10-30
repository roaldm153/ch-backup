"""
Report of the benchmark: charts and tables from results of the driver.

Peaks are fitted with a line over the number of planned objects and extrapolated to the
table of 120k parts. Charts follow the dataviz rules: series in the fixed order of the palette,
thin marks, hairline grid, small multiples instead of a second axis. The tables in report.md
carry every charted value.
"""

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

from matplotlib.axes import Axes
from matplotlib.figure import Figure

TARGET_PARTS = 120_000
GIB = 1024**3
MIB = 1024**2

SERIES = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100")
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BAND = "#f0efec"

PEAK_SERIES = (
    ("main", "fresh", "основной процесс, свежий copy"),
    ("main", "resume", "основной процесс, resume"),
    ("worker", "fresh", "максимум воркера, свежий copy"),
    ("total", "fresh", "сумма пиков, свежий copy"),
)

FRESH_RUNS = ("fresh", "fresh_planning")
RESUME_RUNS = ("resume",)


def load(root: Path) -> list[dict]:
    """Load points of the run with stages and summaries of their copies."""
    points = []
    for point_file in sorted(root.glob("*/*/point.json")):
        point = json.loads(point_file.read_text())
        inventory_file = point_file.parent / "inventory.json"
        point["inventory"] = (
            json.loads(inventory_file.read_text()) if inventory_file.exists() else {}
        )
        for run in point["runs"]:
            run_dir = point_file.parent / f"{run['kind']}-{run['variant']}"
            run["dir"] = run_dir
            run["stages"] = _read_lines(run_dir / "stages.jsonl")
            summary_file = run_dir / "summary.json"
            run["summary"] = (
                json.loads(summary_file.read_text()) if summary_file.exists() else {}
            )
        points.append(point)
    points.sort(key=lambda p: (p["version"], p["parts"]))
    return points


def peaks(stages: list[dict]) -> dict[str, int]:
    """Return peaks of the main process, of one worker and their sum over the run."""
    return {
        "main": max((s["main_peak"] for s in stages), default=0),
        "worker": max(
            (max(s["worker_peaks"].values(), default=0) for s in stages), default=0
        ),
        "total": max(
            (s["main_peak"] + sum(s["worker_peaks"].values()) for s in stages),
            default=0,
        ),
    }


def stage_table(stages: list[dict]) -> dict[str, dict[str, float]]:
    """Aggregate segments by stage: time, peaks, growth of RSS and CPU."""
    fields = (
        "duration",
        "main_peak",
        "worker_peak",
        "growth",
        "cpu_main",
        "cpu_workers",
        "plan_entries",
    )
    table: dict[str, dict[str, float]] = {}
    for segment in stages:
        row = table.setdefault(segment["stage"], dict.fromkeys(fields, 0.0))
        row["duration"] += segment["end"] - segment["start"]
        row["main_peak"] = max(row["main_peak"], segment["main_peak"])
        row["worker_peak"] = max(
            row["worker_peak"], max(segment["worker_peaks"].values(), default=0)
        )
        row["growth"] += segment["rss_end"] - segment["rss_start"]
        row["cpu_main"] += segment.get("cpu_main", 0)
        row["cpu_workers"] += segment.get("cpu_workers", 0)
        row["plan_entries"] = max(
            row["plan_entries"], segment["counters"].get("plan_entries", 0)
        )
    return table


def objects(point: dict, run: dict) -> int:
    """Return the number of planned objects, falling back to unique objects of the table."""
    total = run["summary"].get("counters", {}).get("objects_total")
    return total or point["inventory"].get("unique_objects", 0)


def target_objects(point: dict, run: dict) -> float:
    """Return the number of objects of the table of 120k parts."""
    return objects(point, run) / point["parts"] * TARGET_PARTS


def fit(xs: list[float], ys: list[float]) -> tuple[float, float] | None:
    """Return slope and intercept of the line through points, if there are enough of them."""
    if len(set(xs)) < 2:
        return None
    line = statistics.linear_regression(xs, ys)
    return line.slope, line.intercept


def series_points(
    points: list[dict], version: str, measure: str, kind: str
) -> list[tuple[float, float, float]]:
    """Return objects, peak and objects at 120k parts of candidate runs of the version."""
    names = FRESH_RUNS if kind == "fresh" else RESUME_RUNS
    result: list[tuple[float, float, float]] = []
    for point in points:
        if point["version"] != version:
            continue
        for run in point["runs"]:
            if (
                run["variant"] == "candidate"
                and run["kind"] in names
                and run["stages"]
                and run["exit_code"] == 0
            ):
                result.append(
                    (
                        objects(point, run),
                        peaks(run["stages"])[measure],
                        target_objects(point, run),
                    )
                )
    return result


def _style(ax: Axes) -> None:
    """Apply recessive chrome to the axes."""
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(1)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY, length=0)
    ax.grid(True, color=GRID, linewidth=1, linestyle="-")
    ax.set_axisbelow(True)


def _figure(columns: int, height: float = 4.5) -> tuple[Figure, list[Axes]]:
    """Create a figure with small multiples in one row."""
    fig = Figure(figsize=(6.5 * columns, height), facecolor=SURFACE)
    axes = list(fig.subplots(1, columns, squeeze=False)[0])
    for ax in axes:
        _style(ax)
    return fig, axes


def _plot_version_peaks(ax: Axes, points: list[dict], version: str) -> None:
    """Plot peak series of one version with their fits."""
    forecasts: list[float] = []
    ends: list[float] = []
    for color, (measure, kind, label) in zip(SERIES, PEAK_SERIES):
        data = series_points(points, version, measure, kind)
        if not data:
            continue
        xs = [x / 1e6 for x, _, _ in data]
        ys = [y / GIB for _, y, _ in data]
        ax.plot(
            xs,
            ys,
            color=color,
            linewidth=2,
            marker="o",
            markersize=8,
            markeredgecolor=SURFACE,
            markeredgewidth=2,
            label=label,
            solid_capstyle="round",
        )
        line = fit(xs, ys)
        if line:
            end = max(t for _, _, t in data) / 1e6
            ax.plot(
                [xs[-1], end],
                [line[0] * xs[-1] + line[1], line[0] * end + line[1]],
                color=color,
                linewidth=2,
                linestyle=":",
            )
            forecasts.append(line[0] * end + line[1])
            ends.append(end)
    _label_ends(ax, ends, forecasts)
    ax.set_title(f"ClickHouse {version}", color=INK, loc="left", fontsize=11)
    ax.set_xlabel("объектов в плане, млн", color=INK_SECONDARY)
    ax.set_ylabel("пик RSS, ГиБ", color=INK_SECONDARY)
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)


def plot_peaks(points: list[dict], out: Path) -> None:
    """Plot peaks against planned objects with fits extrapolated to 120k parts."""
    versions = sorted({p["version"] for p in points})
    fig, axes = _figure(len(versions))
    for ax, version in zip(axes, versions):
        _plot_version_peaks(ax, points, version)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper left",
            bbox_to_anchor=(0, 0),
            ncol=2,
            frameon=False,
            labelcolor=INK_SECONDARY,
        )
    fig.suptitle(
        "Пик памяти copy от числа объектов, пунктир до 120k партов",
        color=INK,
        x=0.01,
        ha="left",
        y=1.02,
    )
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=SURFACE)


def largest_fresh_runs(points: list[dict]) -> dict[str, tuple[dict, dict]]:
    """Return the fresh candidate run of the largest point of each version."""
    result: dict[str, tuple[dict, dict]] = {}
    for point in points:
        for run in point["runs"]:
            if (
                run["variant"] == "candidate"
                and run["kind"] in FRESH_RUNS
                and run["stages"]
            ):
                current = result.get(point["version"])
                if current is None or current[0]["parts"] <= point["parts"]:
                    result[point["version"]] = (point, run)
    return result


def plot_stage_peaks(points: list[dict], out: Path) -> None:
    """Plot peaks of the main process by stage for the largest point of each version."""
    runs = largest_fresh_runs(points)
    if not runs:
        return
    count = max(len(stage_table(run["stages"])) for _, run in runs.values())
    fig, axes = _figure(len(runs), height=_bars_height(count))
    for ax, (version, (point, run)) in zip(axes, sorted(runs.items())):
        table = stage_table(run["stages"])
        rows = sorted(table.items(), key=lambda item: item[1]["main_peak"])
        stages = [stage for stage, _ in rows]
        values = [row["main_peak"] / GIB for _, row in rows]
        ax.barh(stages, values, height=0.4, color=SERIES[0])
        for y, value in enumerate(values):
            ax.annotate(
                f"{value:.2f}",
                (value, y),
                textcoords="offset points",
                xytext=(4, 0),
                va="center",
                color=INK_SECONDARY,
                fontsize=8,
            )
        ax.grid(axis="y", visible=False)
        ax.set_title(
            f"ClickHouse {version}, {point['parts']} партов, {run['kind']}",
            color=INK,
            loc="left",
            fontsize=11,
        )
        ax.set_xlabel("пик RSS основного процесса, ГиБ", color=INK_SECONDARY)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=SURFACE)


def plot_timeline(point: dict, run: dict, out: Path) -> None:
    """Plot memory of the main process and workers over time with bands of stages."""
    samples = _read_samples(run["dir"] / "samples.csv")
    if not samples:
        return
    fig, axes = _figure(1)
    ax = axes[0]
    ts = [float(row["t"]) for row in samples]
    for color, column, label in (
        (SERIES[0], "main_rss", "основной процесс, RSS"),
        (SERIES[1], "workers_uss", "воркеры, USS в сумме"),
    ):
        ax.plot(
            ts,
            [int(row[column]) / GIB for row in samples],
            color=color,
            linewidth=2,
            label=label,
        )
    shaded = False
    for segment in _merged_segments(run["stages"]):
        if segment["stage"] != "other":
            if shaded:
                ax.axvspan(
                    segment["start"], segment["end"], color=BAND, linewidth=0, zorder=0
                )
            shaded = not shaded
            if segment["end"] - segment["start"] > (ts[-1] - ts[0]) / 40:
                ax.annotate(
                    segment["stage"],
                    (segment["start"], 1),
                    xycoords=("data", "axes fraction"),
                    rotation=90,
                    va="top",
                    fontsize=7,
                    color=INK_MUTED,
                )
    ax.set_ylim(bottom=0)
    ax.set_xlabel("секунды", color=INK_SECONDARY)
    ax.set_ylabel("ГиБ", color=INK_SECONDARY)
    ax.legend(
        frameon=False,
        labelcolor=INK_SECONDARY,
        loc="upper left",
        bbox_to_anchor=(0, -0.15),
        ncol=2,
    )
    ax.set_title(
        f"ClickHouse {point['version']}, {point['parts']} партов, {run['kind']} "
        f"({run['variant']})",
        color=INK,
        loc="left",
        fontsize=11,
    )
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=SURFACE)


def top_structures(run: dict) -> tuple[str, list[dict]] | None:
    """Return the stage and lines of code of the tracemalloc snapshot with most memory."""
    snapshots = _read_lines(run["dir"] / "tracemalloc.jsonl")
    if not snapshots:
        return None
    snapshot = max(snapshots, key=lambda s: sum(line["size"] for line in s["top"]))
    return snapshot["stage"], snapshot["top"][:12]


def plot_structures(points: list[dict], out: Path) -> list[tuple[dict, str, list]]:
    """Plot lines of code holding most memory in tracemalloc runs and return them."""
    found = []
    for point in points:
        for run in point["runs"]:
            if run["kind"] == "tracemalloc" and run["variant"] == "candidate":
                structures = top_structures(run)
                if structures:
                    found.append((point, *structures))
    if not found:
        return []
    fig, axes = _figure(
        len(found), height=_bars_height(max(len(t) for _, _, t in found))
    )
    for ax, (point, stage, top) in zip(axes, found):
        labels = [_short_where(line["where"]) for line in reversed(top)]
        values = [line["size"] / MIB for line in reversed(top)]
        ax.barh(labels, values, height=0.4, color=SERIES[0])
        ax.grid(axis="y", visible=False)
        ax.tick_params(axis="y", labelsize=7)
        ax.set_xlabel("МиБ", color=INK_SECONDARY)
        ax.set_title(
            f"ClickHouse {point['version']}, {point['parts']} партов, этап {stage}",
            color=INK,
            loc="left",
            fontsize=11,
        )
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=SURFACE)
    return found


def bytes_per_object(
    points: list[dict], version: str
) -> dict[str, tuple[float, float]]:
    """Return slope in bytes per object and forecast at 120k parts of stage peaks of fresh runs."""
    by_stage: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for point in points:
        if point["version"] != version:
            continue
        for run in point["runs"]:
            if (
                run["variant"] != "candidate"
                or run["kind"] not in FRESH_RUNS
                or run["exit_code"] != 0
            ):
                continue
            for stage, row in stage_table(run["stages"]).items():
                by_stage[stage].append(
                    (objects(point, run), row["main_peak"], target_objects(point, run))
                )
    result = {}
    for stage, data in by_stage.items():
        line = fit([x for x, _, _ in data], [y for _, y, _ in data])
        if line:
            target = max(t for _, _, t in data)
            result[stage] = (line[0], line[0] * target + line[1])
    return result


def _inventory_lines(points: list[dict]) -> list[str]:
    """Return the table of inventories of points."""
    lines: list[str] = []
    lines += ["## Инвентаризация", ""]
    lines.append(
        "| Версия | Партов | Файлов на парт | Объектов (уник.) | Ссылок | Формат metadata "
        "| Локальные metadata, МиБ | Tar disk metadata сжат./распак., МиБ |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for point in points:
        inv = point["inventory"]
        if not inv:
            continue
        tarballs = inv.get("disk_metadata_tarballs", [])
        lines.append(
            f"| {point['version']} | {point['parts']} | {inv['files_per_part']:.0f} "
            f"| {inv['unique_objects']} | {inv['object_refs']} "
            f"| {inv['metadata_format_version']} "
            f"| {inv['local_metadata_disk_usage'] / MIB:.0f} "
            f"| {sum(t.get('compressed', 0) for t in tarballs) / MIB:.1f} / "
            f"{sum(t['unpacked'] for t in tarballs) / MIB:.1f} |"
        )
    return lines


def _run_lines(points: list[dict]) -> list[str]:
    """Return the table of peaks of runs."""
    lines: list[str] = []
    lines += ["", "## Пики по прогонам", ""]
    lines.append(
        "| Версия | Партов | Прогон | Режим | Статус | Код | Время, с | Объектов | "
        "Основной, ГиБ | Воркер, ГиБ | Сумма, ГиБ | Байт на объект (основной) |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for point in points:
        for run in point["runs"]:
            run_peaks = peaks(run["stages"])
            count = objects(point, run)
            lines.append(
                f"| {point['version']} | {point['parts']} | {run['kind']}-{run['variant']} "
                f"| {run['mode']} | {run.get('outcome', 'unknown')} "
                f"| {run['exit_code']} | {run['duration']:.0f} | {count} "
                f"| {run_peaks['main'] / GIB:.2f} | {run_peaks['worker'] / GIB:.2f} "
                f"| {run_peaks['total'] / GIB:.2f} "
                f"| {run_peaks['main'] / count if count else 0:.0f} |"
            )
    return lines


def _status_lines(points: list[dict]) -> list[str]:
    """Show preparation results and failures even when no copy has finished."""
    lines = [
        "## Состояние точек",
        "",
        "| Версия | Партов | Статус | Backup, с | Ошибка |",
        "|---|---|---|---|---|",
    ]
    for point in points:
        errors = "; ".join(
            str(point[key])
            for key in ("error", "diagnostics_error", "report_error")
            if point.get(key)
        )
        errors = errors.replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {point['version']} | {point['parts']} | {point.get('status', 'unknown')} "
            f"| {point.get('timings', {}).get('backup', 0):.0f} | {errors} |"
        )
    return lines + [""]


def _stage_lines(points: list[dict]) -> list[str]:
    """Return tables of stages of candidate runs."""
    lines: list[str] = []
    for point in points:
        for run in point["runs"]:
            if run["variant"] != "candidate" or not run["stages"]:
                continue
            lines += [
                "",
                f"## Этапы: {point['version']}, {point['parts']} партов, {run['kind']}",
                "",
                "| Этап | Время, с | Пик основного, МиБ | Прирост RSS, МиБ "
                "| Пик воркера, МиБ | CPU основного, с | CPU воркеров, с | Записей плана |",
                "|---|---|---|---|---|---|---|---|",
            ]
            table = stage_table(run["stages"])
            for stage, row in sorted(
                table.items(), key=lambda item: -item[1]["main_peak"]
            ):
                lines.append(
                    f"| {stage} | {row['duration']:.1f} | {row['main_peak'] / MIB:.0f} "
                    f"| {row['growth'] / MIB:.0f} | {row['worker_peak'] / MIB:.0f} "
                    f"| {row['cpu_main']:.1f} | {row['cpu_workers']:.1f} "
                    f"| {row['plan_entries']:.0f} |"
                )
    return lines


def _forecast_lines(points: list[dict]) -> list[str]:
    """Return tables of bytes per object and the forecast by stage."""
    lines: list[str] = []
    lines += ["", f"## Прогноз на {TARGET_PARTS} партов", ""]
    for version in sorted({p["version"] for p in points}):
        model = bytes_per_object(points, version)
        if not model:
            lines.append(f"{version}: для подгонки нужно хотя бы две точки.")
            continue
        lines += [
            f"ClickHouse {version}, пик основного процесса по этапам свежего copy:",
            "",
            "| Этап | Байт на объект | Прогноз, ГиБ |",
            "|---|---|---|",
        ]
        for stage, (slope, forecast) in sorted(model.items(), key=lambda i: -i[1][1]):
            lines.append(f"| {stage} | {slope:.0f} | {forecast / GIB:.1f} |")
        lines.append("")
    return lines


def _structure_lines(structures: list) -> list[str]:
    """Return tables of lines of code holding most memory."""
    lines: list[str] = []
    for point, stage, top in structures:
        lines += [
            f"{point['version']}, {point['parts']} партов, этап {stage}:",
            "",
            "| Строка | МиБ | Блоков |",
            "|---|---|---|",
        ]
        lines += [
            f"| `{line['where']}` | {line['size'] / MIB:.1f} | {line['count']} |"
            for line in top
        ]
        lines.append("")
    return lines


def render_markdown(points: list[dict], structures: list) -> str:
    """Return report.md with inventory, peaks, stages and the forecast."""
    lines = ["# Память ch-backup copy", ""]
    lines += _status_lines(points)
    lines += _inventory_lines(points)
    lines += _run_lines(points)
    lines += _stage_lines(points)
    lines += _forecast_lines(points)
    if structures:
        lines += ["## Структуры по tracemalloc", ""]
        lines += _structure_lines(structures)
    lines += [
        "",
        "Сумма пиков складывает пики основного процесса и воркеров внутри этапа, "
        "это верхняя оценка: пики могли прийтись на разное время.",
        "Незавершённые прогоны содержат только успевшие записаться замеры "
        "и не участвуют в экстраполяции; ноль при отсутствии замеров не означает нулевую память.",
        "",
        "![Пики от числа объектов](peak_vs_objects.png)",
        "",
        "![Пики по этапам](stage_peaks.png)",
        "",
    ]
    if structures:
        lines += ["![Структуры](structures.png)", ""]
    return "\n".join(lines)


def build(root: Path) -> None:
    """Build charts and report.md in the results directory."""
    points = load(root)
    plot_peaks(points, root / "peak_vs_objects.png")
    plot_stage_peaks(points, root / "stage_peaks.png")
    for point in points:
        for run in point["runs"]:
            plot_timeline(
                point,
                run,
                root
                / f"timeline_{point['version']}_{point['parts']}_{run['kind']}-{run['variant']}.png",
            )
    structures = plot_structures(points, root / "structures.png")
    (root / "report.md").write_text(render_markdown(points, structures))


def _bars_height(bars: int) -> float:
    """Return the height of a figure of horizontal bars that keeps bars thin."""
    return max(3, 0.35 * bars + 1.5)


def _label_ends(ax: Axes, ends: list[float], values: list[float]) -> None:
    """Label ends of extrapolated lines, skipping a label too close to a labeled one."""
    span = max(values, default=0) or 1
    labeled: list[float] = []
    for end, value in sorted(zip(ends, values), key=lambda item: -item[1]):
        if any(abs(value - other) < span * 0.05 for other in labeled):
            continue
        labeled.append(value)
        ax.annotate(
            f"{value:.1f} ГиБ",
            (end, value),
            textcoords="offset points",
            xytext=(6, 0),
            va="center",
            color=INK_SECONDARY,
            fontsize=9,
        )


def _merged_segments(stages: list[dict]) -> list[dict]:
    """Join adjacent segments of the same stage for bands of the timeline."""
    merged: list[dict] = []
    for segment in stages:
        if merged and merged[-1]["stage"] == segment["stage"]:
            merged[-1]["end"] = segment["end"]
        else:
            merged.append(
                {
                    "stage": segment["stage"],
                    "start": segment["start"],
                    "end": segment["end"],
                }
            )
    return merged


def _short_where(where: str) -> str:
    """Shorten the path of the source line to the package."""
    path, _, line = where.rpartition(":")
    parts = Path(path).parts
    for anchor in ("ch_backup", "site-packages", "lib"):
        if anchor in parts:
            parts = parts[parts.index(anchor) :]
            break
    return f"{'/'.join(parts[-3:])}:{line}"


def _read_lines(path: Path) -> list[dict]:
    """Read JSON lines, skipping a line cut by a killed process."""
    if not path.exists():
        return []
    result = []
    for line in path.read_text().splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return result


def _read_samples(path: Path) -> list[dict]:
    """Read samples of memory."""
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return [row for row in csv.DictReader(f) if row.get("main_rss")]
