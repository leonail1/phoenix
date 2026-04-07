#!/usr/bin/env python3

import argparse
import csv
import json
import math
import os
import subprocess
from pathlib import Path


DEFAULT_NUMJOBS = [1, 2, 4, 8]
ENGINE_MODES = [("phxfs", 1), ("native", 0)]


def parse_int_list(value):
    items = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        items.append(int(part))
    if not items:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return items


def parse_args():
    root = Path(__file__).resolve().parent
    repo_root = root.parent.parent
    default_engine = repo_root / "benchmarks" / "fio" / "phoenix_fio.so"
    default_template = root / "templates" / "randread_4k_numjobs_scaling.fio"
    default_raw_template = root / "templates" / "randread_4k_numjobs_scaling_raw_nvme.fio"
    default_output = root / "results" / "randread_4k_numjobs_scaling"

    parser = argparse.ArgumentParser(
        description=(
            "Run steady-state 4KB randread numjobs scaling sweeps for phxfs/native fio paths "
            "and an optional raw NVMe block-device ceiling."
        )
    )
    parser.add_argument("--filename", required=True, help="Target file opened with O_DIRECT.")
    parser.add_argument("--size", required=True, help="fio size=... value, e.g. 64G.")
    parser.add_argument("--runtime", type=int, default=20, help="Steady-state runtime per run in seconds.")
    parser.add_argument("--repetitions", type=int, default=1, help="Number of repetitions per numjobs point.")
    parser.add_argument("--device-id", type=int, default=0, help="Phoenix device id.")
    parser.add_argument("--fio-bin", default="fio", help="fio binary to invoke.")
    parser.add_argument("--engine", default=str(default_engine), help="Path to phoenix_fio.so.")
    parser.add_argument("--template", default=str(default_template), help="fio template path for phxfs/native.")
    parser.add_argument(
        "--raw-template",
        default=str(default_raw_template),
        help="fio template path for the raw block-device baseline.",
    )
    parser.add_argument("--output-dir", default=str(default_output), help="Directory for JSON and CSV outputs.")
    parser.add_argument("--iodepth", type=int, default=256, help="Per-job iodepth used for each numjobs point.")
    parser.add_argument(
        "--numjobs-list",
        type=parse_int_list,
        default=DEFAULT_NUMJOBS,
        help="Comma-separated numjobs values, e.g. 1,2,4,8.",
    )
    parser.add_argument(
        "--raw-device",
        default=None,
        help=(
            "Optional raw block device or partition to benchmark as an upper bound. "
            "Pass 'auto' to derive it from the mount backing --filename."
        ),
    )
    parser.add_argument(
        "--raw-size",
        default=None,
        help="fio size=... value for raw-device runs. Defaults to --size.",
    )
    parser.add_argument(
        "--raw-ioengine",
        default="io_uring",
        help="Built-in fio ioengine used for the raw-device baseline.",
    )
    parser.add_argument(
        "--raw-sudo",
        action="store_true",
        help="Run raw-device fio commands with 'sudo -n' so block-device opens succeed without an interactive password.",
    )
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


def discover_mount_source(path):
    result = subprocess.run(
        ["findmnt", "-T", path, "-o", "SOURCE", "-n"],
        check=True,
        capture_output=True,
        text=True,
    )
    source = result.stdout.strip()
    if not source.startswith("/dev/"):
        raise SystemExit(f"Could not derive a raw block device from {path}: got {source!r}")
    return source


def resolve_raw_device(raw_device_arg, filename):
    if not raw_device_arg:
        return None
    if raw_device_arg == "auto":
        return discover_mount_source(filename)
    return raw_device_arg


def build_mode_specs(args):
    phx_template = read_template(args.template)
    mode_specs = []

    for mode, enable_phoenix in ENGINE_MODES:
        mode_specs.append(
            {
                "mode": mode,
                "template": phx_template,
                "target": args.filename,
                "size": args.size,
                "static_replacements": {
                    "__ENGINE__": os.path.abspath(args.engine),
                    "__DEVICE_ID__": args.device_id,
                    "__ENABLE_PHOENIX__": enable_phoenix,
                    "__ENABLE_CUFILE__": 0,
                },
                "command_prefix": [],
            }
        )

    raw_device = resolve_raw_device(args.raw_device, args.filename)
    if raw_device:
        raw_size = args.raw_size or args.size
        mode_specs.append(
            {
                "mode": "nvme_raw",
                "template": read_template(args.raw_template),
                "target": raw_device,
                "size": raw_size,
                "static_replacements": {
                    "__RAW_IOENGINE__": args.raw_ioengine,
                },
                "command_prefix": ["sudo", "-n"] if args.raw_sudo else [],
            }
        )
        print(
            f"Including raw NVMe baseline: target={raw_device} ioengine={args.raw_ioengine} size={raw_size} sudo={args.raw_sudo}",
            flush=True,
        )

    return mode_specs


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


def extract_metrics(json_path, mode, numjobs, iodepth, rep):
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    job = data["jobs"][0]
    read_stats = job["read"]
    unit, lat = clat_section(read_stats)
    scale = latency_scale_to_us(unit)
    percentiles = lat.get("percentile", {})

    runtime_s = job.get("job_runtime", 0) / 1000.0
    return {
        "mode": mode,
        "numjobs": numjobs,
        "iodepth": iodepth,
        "total_qd": numjobs * iodepth,
        "rep": rep,
        "read_iops": float(read_stats.get("iops", 0.0)),
        "bw": float(read_stats.get("bw_bytes", 0.0)),
        "clat_mean": float(lat.get("mean", 0.0)) * scale if lat else 0.0,
        "clat_p95": float(percentile_value(percentiles, ("95.000000", "95.00", "95.0", "95"))) * scale if percentiles else 0.0,
        "clat_p99": float(percentile_value(percentiles, ("99.000000", "99.00", "99.0", "99"))) * scale if percentiles else 0.0,
        "runtime": runtime_s,
        "speedup_vs_numjobs1": 0.0,
        "efficiency_vs_numjobs1": 0.0,
        "per_job_iops": 0.0,
        "max_iops_point": 0,
        "json_path": str(json_path),
    }


def try_extract_existing_metrics(json_path, mode, numjobs, iodepth, rep):
    path = Path(json_path)
    if not path.exists():
        return None
    try:
        return extract_metrics(path, mode, numjobs, iodepth, rep)
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
    mode_specs = build_mode_specs(args)
    mode_names = [spec["mode"] for spec in mode_specs]

    output_dir = Path(args.output_dir)
    configs_dir = output_dir / "configs"
    json_dir = output_dir / "json"
    output_dir.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    all_runs = []
    summary_rows = []

    for mode_spec in mode_specs:
        mode = mode_spec["mode"]
        for numjobs in args.numjobs_list:
            for rep in range(1, args.repetitions + 1):
                job_name = f"{mode}-nj{numjobs}-qd{args.iodepth}-rep{rep}"
                config_path = configs_dir / f"{job_name}.fio"
                json_path = json_dir / f"{job_name}.json"
                replacements = dict(mode_spec["static_replacements"])
                replacements.update(
                    {
                        "__JOB_NAME__": job_name,
                        "__FILENAME__": mode_spec["target"],
                        "__SIZE__": mode_spec["size"],
                        "__RUNTIME__": args.runtime,
                        "__IODEPTH__": args.iodepth,
                        "__NUMJOBS__": numjobs,
                    }
                )
                rendered = render_template(mode_spec["template"], replacements)
                config_path.write_text(rendered, encoding="utf-8")

                existing_metrics = None
                if args.resume_existing:
                    existing_metrics = try_extract_existing_metrics(json_path, mode, numjobs, args.iodepth, rep)
                if existing_metrics is not None:
                    print(f"Reusing existing result: {json_path}", flush=True)
                    all_runs.append(existing_metrics)
                    continue

                cmd = list(mode_spec["command_prefix"]) + [
                    args.fio_bin,
                    f"--output={json_path}",
                    "--output-format=json+",
                    str(config_path),
                ]
                print("Running:", " ".join(cmd), flush=True)
                subprocess.run(cmd, check=True)
                all_runs.append(extract_metrics(json_path, mode, numjobs, args.iodepth, rep))

    baseline_iops = {}
    max_iops_by_mode = {}
    for mode in mode_names:
        mode_rows = [row for row in all_runs if row["mode"] == mode]
        for numjobs in args.numjobs_list:
            rows = [row for row in mode_rows if row["numjobs"] == numjobs]
            if not rows:
                continue
            summary_rows.append(
                {
                    "mode": mode,
                    "numjobs": numjobs,
                    "iodepth": args.iodepth,
                    "total_qd": numjobs * args.iodepth,
                    "read_iops": mean([row["read_iops"] for row in rows]),
                    "read_iops_stddev": stdev([row["read_iops"] for row in rows]),
                    "bw": mean([row["bw"] for row in rows]),
                    "clat_mean": mean([row["clat_mean"] for row in rows]),
                    "clat_p95": mean([row["clat_p95"] for row in rows]),
                    "clat_p99": mean([row["clat_p99"] for row in rows]),
                    "runtime": mean([row["runtime"] for row in rows]),
                    "speedup_vs_numjobs1": 0.0,
                    "efficiency_vs_numjobs1": 0.0,
                    "per_job_iops": 0.0,
                    "max_iops_point": 0,
                }
            )

        mode_summary = [row for row in summary_rows if row["mode"] == mode]
        baseline_row = next((row for row in mode_summary if row["numjobs"] == 1), None)
        if baseline_row:
            baseline_iops[mode] = baseline_row["read_iops"]
        if mode_summary:
            max_iops_by_mode[mode] = max(mode_summary, key=lambda row: row["read_iops"])["numjobs"]

    for row in all_runs:
        baseline = baseline_iops.get(row["mode"], 0.0)
        row["speedup_vs_numjobs1"] = row["read_iops"] / baseline if baseline else 0.0
        row["efficiency_vs_numjobs1"] = row["speedup_vs_numjobs1"] / row["numjobs"] if row["numjobs"] else 0.0
        row["per_job_iops"] = row["read_iops"] / row["numjobs"] if row["numjobs"] else 0.0
        row["max_iops_point"] = int(row["numjobs"] == max_iops_by_mode.get(row["mode"]))

    for row in summary_rows:
        baseline = baseline_iops.get(row["mode"], 0.0)
        row["speedup_vs_numjobs1"] = row["read_iops"] / baseline if baseline else 0.0
        row["efficiency_vs_numjobs1"] = row["speedup_vs_numjobs1"] / row["numjobs"] if row["numjobs"] else 0.0
        row["per_job_iops"] = row["read_iops"] / row["numjobs"] if row["numjobs"] else 0.0
        row["max_iops_point"] = int(row["numjobs"] == max_iops_by_mode.get(row["mode"]))

    max_iops_rows = [row for row in summary_rows if row["numjobs"] == max_iops_by_mode.get(row["mode"])]

    common_fields = [
        "mode",
        "numjobs",
        "iodepth",
        "total_qd",
        "read_iops",
        "read_iops_stddev",
        "bw",
        "clat_mean",
        "clat_p95",
        "clat_p99",
        "runtime",
        "speedup_vs_numjobs1",
        "efficiency_vs_numjobs1",
        "per_job_iops",
        "max_iops_point",
    ]

    write_csv(
        output_dir / "all_runs.csv",
        all_runs,
        [
            "mode",
            "numjobs",
            "iodepth",
            "total_qd",
            "rep",
            "read_iops",
            "bw",
            "clat_mean",
            "clat_p95",
            "clat_p99",
            "runtime",
            "speedup_vs_numjobs1",
            "efficiency_vs_numjobs1",
            "per_job_iops",
            "max_iops_point",
            "json_path",
        ],
    )
    write_csv(output_dir / "summary.csv", summary_rows, common_fields)
    write_csv(output_dir / "max_iops.csv", max_iops_rows, common_fields)
    print(f"Wrote results to {output_dir}")


if __name__ == "__main__":
    main()
