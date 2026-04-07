#!/usr/bin/env python3

import argparse
import csv
import html
import math
from pathlib import Path


IODEPTHS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
MODE_ORDER = ["phxfs", "native"]
MODE_LABELS = {
    "phxfs": "PhxFS",
    "native": "Native (NVMe->host->HBM)",
}
MODE_COLORS = {
    "phxfs": "#c75b12",
    "native": "#157f6b",
}
MODE_LIGHT_COLORS = {
    "phxfs": "#f3b489",
    "native": "#8dd5c7",
}


def latest_result_dir(results_root):
    if not results_root.exists():
        return None
    candidates = [path for path in results_root.iterdir() if path.is_dir() and (path / "summary.csv").exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def parse_args():
    script_dir = Path(__file__).resolve().parent
    default_result_dir = latest_result_dir(script_dir / "results")

    parser = argparse.ArgumentParser(
        description="Render a no-dependency SVG overview from fio sweep CSV outputs."
    )
    parser.add_argument(
        "--result-dir",
        default=str(default_result_dir) if default_result_dir else None,
        help="Directory containing summary.csv, all_runs.csv, and max_iops.csv.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output SVG path. Defaults to <result-dir>/plots/randread_4k_steady_overview.svg.",
    )
    parser.add_argument(
        "--title",
        default="4KB Randread Steady-State Comparison",
        help="Title rendered at the top of the SVG.",
    )
    args = parser.parse_args()
    if not args.result_dir:
        parser.error("No result directory found. Pass --result-dir explicitly.")
    return args


def read_csv_rows(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_summary(path):
    rows = []
    for row in read_csv_rows(path):
        rows.append(
            {
                "mode": row["mode"],
                "iodepth": int(row["iodepth"]),
                "read_iops": float(row["read_iops"]),
                "read_iops_stddev": float(row["read_iops_stddev"]),
                "bw": float(row["bw"]),
                "clat_mean": float(row["clat_mean"]),
                "clat_p95": float(row["clat_p95"]),
                "clat_p99": float(row["clat_p99"]),
                "runtime": float(row["runtime"]),
                "max_iops_point": int(row["max_iops_point"]),
            }
        )
    return rows


def load_all_runs(path):
    if not Path(path).exists():
        return []
    rows = []
    for row in read_csv_rows(path):
        rows.append(
            {
                "mode": row["mode"],
                "iodepth": int(row["iodepth"]),
                "rep": int(row["rep"]),
                "read_iops": float(row["read_iops"]),
                "bw": float(row["bw"]),
                "clat_mean": float(row["clat_mean"]),
                "clat_p95": float(row["clat_p95"]),
                "clat_p99": float(row["clat_p99"]),
            }
        )
    return rows


def rows_by_mode(rows):
    grouped = {mode: {} for mode in MODE_ORDER}
    for row in rows:
        grouped.setdefault(row["mode"], {})
        grouped[row["mode"]][row["iodepth"]] = row
    return grouped


def runs_by_mode(rows):
    grouped = {mode: [] for mode in MODE_ORDER}
    for row in rows:
        grouped.setdefault(row["mode"], [])
        grouped[row["mode"]].append(row)
    return grouped


def nice_axis(max_value, tick_count=5):
    if max_value <= 0:
        return 1.0, float(tick_count)
    raw_step = max_value / tick_count
    magnitude = 10 ** math.floor(math.log10(raw_step))
    residual = raw_step / magnitude
    if residual <= 1:
        nice = 1
    elif residual <= 2:
        nice = 2
    elif residual <= 2.5:
        nice = 2.5
    elif residual <= 5:
        nice = 5
    else:
        nice = 10
    step = nice * magnitude
    upper = step * tick_count
    while upper < max_value:
        upper += step
    return step, upper


def fmt_tick(value):
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def x_for_iodepth(iodepth, left, width):
    index = IODEPTHS.index(iodepth)
    if len(IODEPTHS) == 1:
        return left + width / 2.0
    return left + index * width / (len(IODEPTHS) - 1)


def y_for_value(value, top, height, y_max):
    if y_max <= 0:
        return top + height
    return top + height - (value / y_max) * height


def svg_text(x, y, text, css_class="", anchor="start"):
    class_attr = f' class="{css_class}"' if css_class else ""
    return (
        f'<text x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}"{class_attr}>'
        f"{html.escape(text)}</text>"
    )


def svg_line(x1, y1, x2, y2, stroke, stroke_width=1.0, dash=None, opacity=None):
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    opacity_attr = f' opacity="{opacity}"' if opacity is not None else ""
    return (
        f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
        f'stroke="{stroke}" stroke-width="{stroke_width:.2f}"{dash_attr}{opacity_attr}/>'
    )


def svg_polyline(points, stroke, stroke_width=3.0, fill="none", opacity=None):
    opacity_attr = f' opacity="{opacity}"' if opacity is not None else ""
    encoded = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    return (
        f'<polyline points="{encoded}" fill="{fill}" stroke="{stroke}" '
        f'stroke-width="{stroke_width:.2f}" stroke-linejoin="round" '
        f'stroke-linecap="round"{opacity_attr}/>'
    )


def svg_circle(x, y, radius, fill, stroke=None, stroke_width=1.0, opacity=None):
    stroke_attr = f' stroke="{stroke}" stroke-width="{stroke_width:.2f}"' if stroke else ""
    opacity_attr = f' opacity="{opacity}"' if opacity is not None else ""
    return (
        f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" fill="{fill}"'
        f"{stroke_attr}{opacity_attr}/>"
    )


def draw_panel(panel, title, unit, summary, all_runs, key, transform, show_stddev=False, show_scatter=False):
    left = panel["x"]
    top = panel["y"]
    width = panel["w"]
    height = panel["h"]
    plot_left = left + 68
    plot_top = top + 38
    plot_width = width - 92
    plot_height = height - 88

    mode_rows = rows_by_mode(summary)
    all_run_rows = runs_by_mode(all_runs)
    y_values = []

    for mode in MODE_ORDER:
        for iodepth in IODEPTHS:
            row = mode_rows.get(mode, {}).get(iodepth)
            if row:
                y_values.append(transform(row))
                if show_stddev:
                    y_values.append(transform(row) + row["read_iops_stddev"] / 1000.0)

    y_step, y_max = nice_axis(max(y_values) * 1.12 if y_values else 1.0)
    y_ticks = [y_step * tick for tick in range(0, int(round(y_max / y_step)) + 1)]

    elements = [
        f'<rect x="{left:.2f}" y="{top:.2f}" width="{width:.2f}" height="{height:.2f}" rx="18" fill="#fffdf8" stroke="#eadfce"/>',
        svg_text(left + 20, top + 28, title, "panel-title"),
        svg_text(left + width - 16, top + 28, unit, "panel-unit", anchor="end"),
    ]

    for tick in y_ticks:
        y = y_for_value(tick, plot_top, plot_height, y_max)
        elements.append(svg_line(plot_left, y, plot_left + plot_width, y, "#efe7db", 1))
        elements.append(svg_text(plot_left - 10, y + 4, fmt_tick(tick), "tick", anchor="end"))

    for iodepth in IODEPTHS:
        x = x_for_iodepth(iodepth, plot_left, plot_width)
        elements.append(svg_line(x, plot_top, x, plot_top + plot_height, "#f6f0e8", 1))
        elements.append(svg_text(x, plot_top + plot_height + 22, str(iodepth), "tick", anchor="middle"))

    elements.append(svg_line(plot_left, plot_top, plot_left, plot_top + plot_height, "#6b6258", 1.4))
    elements.append(svg_line(plot_left, plot_top + plot_height, plot_left + plot_width, plot_top + plot_height, "#6b6258", 1.4))
    elements.append(svg_text(plot_left + plot_width / 2.0, plot_top + plot_height + 46, "iodepth", "axis-label", anchor="middle"))

    if show_scatter:
        rep_jitter = {1: -7.0, 2: 0.0, 3: 7.0}
        for mode in MODE_ORDER:
            color = MODE_COLORS[mode]
            for row in all_run_rows.get(mode, []):
                x = x_for_iodepth(row["iodepth"], plot_left, plot_width) + rep_jitter.get(row["rep"], 0.0)
                y = y_for_value(transform(row), plot_top, plot_height, y_max)
                elements.append(svg_circle(x, y, 3.6, color, opacity=0.28))

    for mode in MODE_ORDER:
        color = MODE_COLORS[mode]
        light = MODE_LIGHT_COLORS[mode]
        rows = []
        for iodepth in IODEPTHS:
            row = mode_rows.get(mode, {}).get(iodepth)
            if row:
                rows.append(row)
        if not rows:
            continue

        points = []
        for row in rows:
            x = x_for_iodepth(row["iodepth"], plot_left, plot_width)
            y = y_for_value(transform(row), plot_top, plot_height, y_max)
            points.append((x, y))
            if show_stddev:
                high = y_for_value(transform(row) + row["read_iops_stddev"] / 1000.0, plot_top, plot_height, y_max)
                low = y_for_value(max(transform(row) - row["read_iops_stddev"] / 1000.0, 0.0), plot_top, plot_height, y_max)
                elements.append(svg_line(x, high, x, low, light, 2.0))
                elements.append(svg_line(x - 5, high, x + 5, high, light, 2.0))
                elements.append(svg_line(x - 5, low, x + 5, low, light, 2.0))

        elements.append(svg_polyline(points, color, 3.2))
        for row in rows:
            x = x_for_iodepth(row["iodepth"], plot_left, plot_width)
            y = y_for_value(transform(row), plot_top, plot_height, y_max)
            peak = row.get("max_iops_point", 0) if key == "read_iops" else 0
            radius = 6.5 if peak else 5.0
            fill = "#fffdf8" if peak else "#fff6ed"
            elements.append(svg_circle(x, y, radius, fill, stroke=color, stroke_width=2.4))

    return "\n".join(elements)


def render_svg(summary_rows, all_runs, title, result_dir):
    peaks = {}
    for row in summary_rows:
        mode = row["mode"]
        if mode not in peaks or row["read_iops"] > peaks[mode]["read_iops"]:
            peaks[mode] = row

    phxfs_peak = peaks.get("phxfs", {}).get("read_iops", 0.0)
    native_peak = peaks.get("native", {}).get("read_iops", 0.0)
    uplift = ((phxfs_peak / native_peak) - 1.0) * 100.0 if native_peak else 0.0

    width = 1520
    height = 700
    gap = 28
    outer_margin = 56
    legend_width = 280
    chart_area_width = width - outer_margin * 2 - legend_width - gap
    panel_width = (chart_area_width - gap) / 2.0
    panel_height = 430
    top_row_y = 180

    panels = [
        {"x": outer_margin, "y": top_row_y, "w": panel_width, "h": panel_height},
        {"x": outer_margin + panel_width + gap, "y": top_row_y, "w": panel_width, "h": panel_height},
    ]
    legend_box = {
        "x": outer_margin + chart_area_width + gap,
        "y": top_row_y,
        "w": legend_width,
        "h": panel_height,
    }

    subtitle = (
        f"Result set: {result_dir.name} | "
        f"PhxFS peak {phxfs_peak / 1000.0:.1f} kIOPS | "
        f"Native peak {native_peak / 1000.0:.1f} kIOPS | "
        f"Uplift {uplift:.1f}%"
    )

    legend_items = [
        f'<rect x="{legend_box["x"]:.2f}" y="{legend_box["y"]:.2f}" width="{legend_box["w"]:.2f}" height="{legend_box["h"]:.2f}" rx="18" fill="#fffdf8" stroke="#eadfce"/>',
        svg_text(legend_box["x"] + 22, legend_box["y"] + 32, "Legend", "panel-title"),
    ]
    legend_y = legend_box["y"] + 66
    for index, mode in enumerate(MODE_ORDER):
        y = legend_y + index * 34
        color = MODE_COLORS[mode]
        legend_items.append(svg_line(legend_box["x"] + 22, y, legend_box["x"] + 50, y, color, 4.0))
        legend_items.append(svg_text(legend_box["x"] + 62, y + 5, MODE_LABELS[mode], "legend"))
    note_y = legend_y + len(MODE_ORDER) * 34 + 18
    legend_items.append(svg_text(legend_box["x"] + 22, note_y, "Notes", "panel-title"))
    legend_items.append(svg_text(legend_box["x"] + 22, note_y + 28, "Dots on IOPS show repetitions", "legend-note"))
    legend_items.append(svg_text(legend_box["x"] + 22, note_y + 50, f"PhxFS peak: {phxfs_peak / 1000.0:.1f} kIOPS", "legend-note"))
    legend_items.append(svg_text(legend_box["x"] + 22, note_y + 72, f"Native peak: {native_peak / 1000.0:.1f} kIOPS", "legend-note"))
    legend_items.append(svg_text(legend_box["x"] + 22, note_y + 94, f"Uplift: {uplift:.1f}%", "legend-note"))

    body = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text { fill: #2c241d; }",
        ".title { font: 700 30px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; }",
        ".subtitle { font: 400 14px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; fill: #65584b; }",
        ".panel-title { font: 700 18px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; }",
        ".panel-unit { font: 600 12px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; fill: #7b6e62; }",
        ".tick { font: 400 12px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; fill: #6b6258; }",
        ".axis-label { font: 600 12px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; fill: #5f554a; }",
        ".legend { font: 600 14px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; }",
        ".legend-note { font: 400 13px 'IBM Plex Sans', 'DejaVu Sans', sans-serif; fill: #6b6258; }",
        "</style>",
        '<rect width="100%" height="100%" fill="#f7f1e8"/>',
        svg_text(outer_margin, 66, title, "title"),
        svg_text(outer_margin, 92, subtitle, "subtitle"),
        *legend_items,
        draw_panel(
            panels[0],
            "Aggregate Read IOPS",
            "kIOPS",
            summary_rows,
            all_runs,
            "read_iops",
            lambda row: row["read_iops"] / 1000.0,
            show_stddev=True,
            show_scatter=True,
        ),
        draw_panel(
            panels[1],
            "Tail Completion Latency (p99)",
            "us",
            summary_rows,
            all_runs,
            "clat_p99",
            lambda row: row["clat_p99"],
        ),
        "</svg>",
    ]
    return "\n".join(body)


def main():
    args = parse_args()
    result_dir = Path(args.result_dir).resolve()
    summary_path = result_dir / "summary.csv"
    all_runs_path = result_dir / "all_runs.csv"
    if not summary_path.exists():
        raise SystemExit(f"summary.csv not found under {result_dir}")

    output_path = Path(args.output).resolve() if args.output else result_dir / "plots" / "randread_4k_steady_overview.svg"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = load_summary(summary_path)
    all_runs = load_all_runs(all_runs_path)
    svg = render_svg(summary_rows, all_runs, args.title, result_dir)
    output_path.write_text(svg, encoding="utf-8")
    print(f"Wrote plot to {output_path}")


if __name__ == "__main__":
    main()
