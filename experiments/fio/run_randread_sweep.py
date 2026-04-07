#!/usr/bin/env python3

import argparse
import csv
import json
import math
import os
import subprocess
from pathlib import Path


IODEPTHS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
MODES = [("phxfs", 1), ("native", 0)]


def parse_args():
    root = Path(__file__).resolve().parent
    repo_root = root.parent.parent
    default_engine = repo_root / "benchmarks" / "fio" / "phoenix_fio.so"
    default_template = root / "templates" / "randread_4k_steady.fio"
    default_output = root / "results" / "randread_4k_steady"

    parser = argparse.ArgumentParser(
        description="Run steady-state 4KB randread sweeps for phxfs and native fio paths."
    )
    parser.add_argument("--filename", required=True, help="Target file opened with O_DIRECT.")
    parser.add_argument("--size", required=True, help="fio size=... value, e.g. 64G.")
    parser.add_argument("--runtime", type=int, default=20, help="Steady-state runtime per run in seconds.")
    parser.add_argument("--repetitions", type=int, default=3, help="Number of repetitions per iodepth.")
    parser.add_argument("--device-id", type=int, default=0, help="Phoenix device id.")
    parser.add_argument("--fio-bin", default="fio", help="fio binary to invoke.")
    parser.add_argument("--engine", default=str(default_engine), help="Path to phoenix_fio.so.")
    parser.add_argument("--template", default=str(default_template), help="fio template path.")
    parser.add_argument("--output-dir", default=str(default_output), help="Directory for JSON and CSV outputs.")
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="Reuse already-complete JSON outputs in output-dir and only rerun missing/corrupt runs.",
    )
    return parser.parse_args()


def read_template(path):
    return Path(path).read_text(encoding="utf-8")


def render_template(template, replacements):
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(key, str(value))
    return rendered


def clat_section(read_stats):
    for unit in ("clat_ns", "clat_us", "clat_ms"):
        section = read_stats.get(unit)
        if section:
            return unit, section
    return None, {}


def latency_scale_to_us(unit):
    if unit == "clat_ns":
        return 1.0 / 1000.0
    if unit == "clat_ms":
        return 1000.0
    return 1.0


def percentile_value(percentiles, candidates):
    for candidate in candidates:
        if candidate in percentiles:
            return percentiles[candidate]
    return ""


def extract_metrics(json_path, mode, iodepth, rep):
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    job = data["jobs"][0]
    read_stats = job["read"]
    unit, lat = clat_section(read_stats)
    scale = latency_scale_to_us(unit)
    percentiles = lat.get("percentile", {})

    runtime_s = job.get("job_runtime", 0) / 1000.0
    return {
        "mode": mode,
        "iodepth": iodepth,
        "rep": rep,
        "read_iops": float(read_stats.get("iops", 0.0)),
        "bw": float(read_stats.get("bw_bytes", 0.0)),
        "clat_mean": float(lat.get("mean", 0.0)) * scale if lat else 0.0,
        "clat_p95": float(percentile_value(percentiles, ("95.000000", "95.00", "95.0", "95"))) * scale if percentiles else 0.0,
        "clat_p99": float(percentile_value(percentiles, ("99.000000", "99.00", "99.0", "99"))) * scale if percentiles else 0.0,
        "runtime": runtime_s,
        "max_iops_point": 0,
        "json_path": str(json_path),
    }


def try_extract_existing_metrics(json_path, mode, iodepth, rep):
    path = Path(json_path)
    if not path.exists():
        return None
    try:
        return extract_metrics(path, mode, iodepth, rep)
    except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
        return None


def mean(values):
    return sum(values) / len(values) if values else 0.0


def stdev(values):
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def write_csv(path, rows, fieldnames):
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    template = read_template(args.template)
    output_dir = Path(args.output_dir)
    configs_dir = output_dir / "configs"
    json_dir = output_dir / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    all_runs = []
    summary_rows = []

    for mode, enable_phoenix in MODES:
        for iodepth in IODEPTHS:
            for rep in range(1, args.repetitions + 1):
                job_name = f"{mode}-qd{iodepth}-rep{rep}"
                config_path = configs_dir / f"{job_name}.fio"
                json_path = json_dir / f"{job_name}.json"
                rendered = render_template(
                    template,
                    {
                        "__ENGINE__": os.path.abspath(args.engine),
                        "__JOB_NAME__": job_name,
                        "__FILENAME__": args.filename,
                        "__SIZE__": args.size,
                        "__RUNTIME__": args.runtime,
                        "__IODEPTH__": iodepth,
                        "__DEVICE_ID__": args.device_id,
                        "__ENABLE_PHOENIX__": enable_phoenix,
                    },
                )
                config_path.write_text(rendered, encoding="utf-8")

                existing_metrics = None
                if args.resume_existing:
                    existing_metrics = try_extract_existing_metrics(json_path, mode, iodepth, rep)
                if existing_metrics is not None:
                    print(f"Reusing existing result: {json_path}", flush=True)
                    all_runs.append(existing_metrics)
                    continue

                cmd = [
                    args.fio_bin,
                    f"--output={json_path}",
                    "--output-format=json+",
                    str(config_path),
                ]
                print("Running:", " ".join(cmd), flush=True)
                subprocess.run(cmd, check=True)
                all_runs.append(extract_metrics(json_path, mode, iodepth, rep))

    max_iops_by_mode = {}
    for mode, _ in MODES:
        mode_rows = [row for row in all_runs if row["mode"] == mode]
        for iodepth in IODEPTHS:
            rows = [row for row in mode_rows if row["iodepth"] == iodepth]
            if not rows:
                continue
            summary_rows.append(
                {
                    "mode": mode,
                    "iodepth": iodepth,
                    "read_iops": mean([row["read_iops"] for row in rows]),
                    "read_iops_stddev": stdev([row["read_iops"] for row in rows]),
                    "bw": mean([row["bw"] for row in rows]),
                    "clat_mean": mean([row["clat_mean"] for row in rows]),
                    "clat_p95": mean([row["clat_p95"] for row in rows]),
                    "clat_p99": mean([row["clat_p99"] for row in rows]),
                    "runtime": mean([row["runtime"] for row in rows]),
                    "max_iops_point": 0,
                }
            )

        mode_summary = [row for row in summary_rows if row["mode"] == mode]
        if mode_summary:
            max_iops_by_mode[mode] = max(mode_summary, key=lambda row: row["read_iops"])["iodepth"]

    for row in all_runs:
        row["max_iops_point"] = int(row["iodepth"] == max_iops_by_mode.get(row["mode"]))

    for row in summary_rows:
        row["max_iops_point"] = int(row["iodepth"] == max_iops_by_mode.get(row["mode"]))

    max_iops_rows = [
        row for row in summary_rows if row["iodepth"] == max_iops_by_mode.get(row["mode"])
    ]

    write_csv(
        output_dir / "all_runs.csv",
        all_runs,
        [
            "mode",
            "iodepth",
            "rep",
            "read_iops",
            "bw",
            "clat_mean",
            "clat_p95",
            "clat_p99",
            "runtime",
            "max_iops_point",
            "json_path",
        ],
    )
    write_csv(
        output_dir / "summary.csv",
        summary_rows,
        [
            "mode",
            "iodepth",
            "read_iops",
            "read_iops_stddev",
            "bw",
            "clat_mean",
            "clat_p95",
            "clat_p99",
            "runtime",
            "max_iops_point",
        ],
    )
    write_csv(
        output_dir / "max_iops.csv",
        max_iops_rows,
        [
            "mode",
            "iodepth",
            "read_iops",
            "read_iops_stddev",
            "bw",
            "clat_mean",
            "clat_p95",
            "clat_p99",
            "runtime",
            "max_iops_point",
        ],
    )

    print(f"Wrote results to {output_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(f"fio command failed with exit code {exc.returncode}", file=sys.stderr)
        raise
