#!/usr/bin/env python3

import argparse
import csv
import re
import subprocess
import time
from pathlib import Path


DEFAULT_NUMJOBS = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32]
MODE_ORDER = ["phxfs", "native", "nvme_raw"]


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


def latest_result_dirs(results_root):
    candidates = []
    for qd in (4, 8):
        path = results_root / f"randread_4k_numjobs_scaling_sparse_raw_qd{qd}"
        if (path / "summary.csv").exists():
            candidates.append(path)
    return candidates


def parse_args():
    script_dir = Path(__file__).resolve().parent
    results_root = script_dir / "results"
    default_dirs = latest_result_dirs(results_root)

    parser = argparse.ArgumentParser(
        description=(
            "Run the sync POSIX staged GPU benchmark (microbenchmark xfer_mode=2) "
            "and replace the existing native rows in numjobs scaling result sets."
        )
    )
    parser.add_argument(
        "--result-dirs",
        nargs="+",
        default=[str(path) for path in default_dirs],
        help="Existing numjobs result directories whose native rows should be replaced.",
    )
    parser.add_argument(
        "--microbenchmark-bin",
        default=str(Path("build/bin/microbenchmark")),
        help="Path to the built microbenchmark binary.",
    )
    parser.add_argument(
        "--filename",
        default="/mnt/nvme_data/lzg/phoenix_bench/randread_4k_steady_64g.bin",
        help="Benchmark file path.",
    )
    parser.add_argument(
        "--length",
        default="1G",
        help="Total bytes read per numjobs point. Must fit on the GPU because xfer_mode=2 allocates one GPU buffer per thread.",
    )
    parser.add_argument("--io-size", default="4K", help="Per-I/O size. Keep this at 4K to match the existing plots.")
    parser.add_argument("--device-id", type=int, default=0, help="GPU device id.")
    parser.add_argument(
        "--numjobs-list",
        type=parse_int_list,
        default=DEFAULT_NUMJOBS,
        help="Comma-separated numjobs list. Defaults to the sparse schedule already used by the fio runs.",
    )
    parser.add_argument(
        "--baseline-output-dir",
        default=str(results_root / "posix_staged_sync_1g"),
        help="Directory to store the raw POSIX staged logs and summary CSV.",
    )
    parser.add_argument(
        "--plotter",
        default=str(script_dir / "plot_numjobs_scaling.py"),
        help="Path to the SVG plotter.",
    )
    parser.add_argument(
        "--reuse-existing-baseline",
        action="store_true",
        help="Reuse baseline-output-dir/all_runs.csv if present instead of rerunning the POSIX staged baseline.",
    )
    args = parser.parse_args()
    if not args.result_dirs:
        parser.error("No result dirs found. Pass --result-dirs explicitly.")
    return args


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fieldnames):
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_existing_baseline_rows(path):
    rows = []
    for row in read_csv(path):
        rows.append(
            {
                "mode": row["mode"],
                "numjobs": int(row["numjobs"]),
                "iodepth": int(row["iodepth"]),
                "total_qd": int(row["total_qd"]),
                "rep": int(row["rep"]),
                "read_iops": float(row["read_iops"]),
                "bw": float(row["bw"]),
                "clat_mean": float(row["clat_mean"]),
                "clat_p95": float(row["clat_p95"]),
                "clat_p99": float(row["clat_p99"]),
                "runtime": float(row["runtime"]),
                "speedup_vs_numjobs1": float(row["speedup_vs_numjobs1"]),
                "efficiency_vs_numjobs1": float(row["efficiency_vs_numjobs1"]),
                "per_job_iops": float(row["per_job_iops"]),
                "max_iops_point": int(row["max_iops_point"]),
                "json_path": row["json_path"],
            }
        )
    return sort_rows(rows)


def sort_rows(rows):
    mode_rank = {mode: index for index, mode in enumerate(MODE_ORDER)}
    return sorted(
        rows,
        key=lambda row: (
            mode_rank.get(row["mode"], len(mode_rank)),
            int(row["numjobs"]),
            int(row.get("rep", 1)),
        ),
    )


def parse_metric(text, pattern, cast=float):
    match = re.search(pattern, text)
    if not match:
        raise RuntimeError(f"Could not parse pattern {pattern!r} from benchmark output")
    return cast(match.group(1))


def run_posix_point(args, numjobs, log_path):
    cmd = [
        args.microbenchmark_bin,
        "-m",
        "read",
        "-a",
        "0",
        "-f",
        args.filename,
        "-l",
        args.length,
        "-s",
        args.io_size,
        "-d",
        str(args.device_id),
        "-t",
        str(numjobs),
        "-x",
        "2",
        "-i",
        "1",
    ]

    start = time.perf_counter()
    completed = subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    elapsed = time.perf_counter() - start
    output = completed.stdout
    log_path.write_text(output, encoding="utf-8")

    total_ops = parse_metric(output, r"Total IO operations:\s*([0-9]+)", int)
    clat_mean = parse_metric(output, r"Average IO latency:\s*([0-9.]+)\s*us")
    clat_p95 = parse_metric(output, r"95th percentile latency:\s*([0-9.]+)\s*us")
    clat_p99 = parse_metric(output, r"99th percentile latency:\s*([0-9.]+)\s*us")
    io_size_bytes = 4096
    iops = total_ops / elapsed if elapsed > 0 else 0.0
    bw_bytes = iops * io_size_bytes

    return {
        "mode": "native",
        "numjobs": numjobs,
        "iodepth": 1,
        "total_qd": numjobs,
        "rep": 1,
        "read_iops": iops,
        "bw": bw_bytes,
        "clat_mean": clat_mean,
        "clat_p95": clat_p95,
        "clat_p99": clat_p99,
        "runtime": elapsed,
        "speedup_vs_numjobs1": 0.0,
        "efficiency_vs_numjobs1": 0.0,
        "per_job_iops": 0.0,
        "max_iops_point": 0,
        "json_path": str(log_path),
    }


def recompute_derived(summary_rows, all_runs):
    baseline_iops = {}
    max_iops_by_mode = {}

    for mode in {row["mode"] for row in summary_rows}:
        mode_rows = [row for row in summary_rows if row["mode"] == mode]
        baseline_row = next((row for row in mode_rows if int(row["numjobs"]) == 1), None)
        if baseline_row is not None:
            baseline_iops[mode] = float(baseline_row["read_iops"])
        if mode_rows:
            max_iops_by_mode[mode] = max(mode_rows, key=lambda row: float(row["read_iops"]))["numjobs"]

    for row in summary_rows:
        numjobs = int(row["numjobs"])
        baseline = baseline_iops.get(row["mode"], 0.0)
        read_iops = float(row["read_iops"])
        row["speedup_vs_numjobs1"] = read_iops / baseline if baseline else 0.0
        row["efficiency_vs_numjobs1"] = row["speedup_vs_numjobs1"] / numjobs if numjobs else 0.0
        row["per_job_iops"] = read_iops / numjobs if numjobs else 0.0
        row["max_iops_point"] = int(str(numjobs) == str(max_iops_by_mode.get(row["mode"], "")))

    for row in all_runs:
        numjobs = int(row["numjobs"])
        baseline = baseline_iops.get(row["mode"], 0.0)
        read_iops = float(row["read_iops"])
        row["speedup_vs_numjobs1"] = read_iops / baseline if baseline else 0.0
        row["efficiency_vs_numjobs1"] = row["speedup_vs_numjobs1"] / numjobs if numjobs else 0.0
        row["per_job_iops"] = read_iops / numjobs if numjobs else 0.0
        row["max_iops_point"] = int(str(numjobs) == str(max_iops_by_mode.get(row["mode"], "")))


def baseline_rows_for_qd(base_rows, iodepth):
    summary_rows = []
    all_runs_rows = []
    for row in base_rows:
        total_qd = row["numjobs"] * iodepth
        summary_rows.append(
            {
                "mode": "native",
                "numjobs": row["numjobs"],
                "iodepth": iodepth,
                "total_qd": total_qd,
                "read_iops": row["read_iops"],
                "read_iops_stddev": 0.0,
                "bw": row["bw"],
                "clat_mean": row["clat_mean"],
                "clat_p95": row["clat_p95"],
                "clat_p99": row["clat_p99"],
                "runtime": row["runtime"],
                "speedup_vs_numjobs1": 0.0,
                "efficiency_vs_numjobs1": 0.0,
                "per_job_iops": 0.0,
                "max_iops_point": 0,
            }
        )
        all_runs_rows.append(
            {
                "mode": "native",
                "numjobs": row["numjobs"],
                "iodepth": iodepth,
                "total_qd": total_qd,
                "rep": 1,
                "read_iops": row["read_iops"],
                "bw": row["bw"],
                "clat_mean": row["clat_mean"],
                "clat_p95": row["clat_p95"],
                "clat_p99": row["clat_p99"],
                "runtime": row["runtime"],
                "speedup_vs_numjobs1": 0.0,
                "efficiency_vs_numjobs1": 0.0,
                "per_job_iops": 0.0,
                "max_iops_point": 0,
                "json_path": row["json_path"],
            }
        )
    return summary_rows, all_runs_rows


def overlay_result_dir(result_dir, base_rows, plotter):
    result_dir = Path(result_dir).resolve()
    summary_path = result_dir / "summary.csv"
    all_runs_path = result_dir / "all_runs.csv"
    if not summary_path.exists() or not all_runs_path.exists():
        raise FileNotFoundError(f"{result_dir} is missing summary.csv or all_runs.csv")

    summary_rows = [row for row in read_csv(summary_path) if row["mode"] != "native"]
    all_runs = [row for row in read_csv(all_runs_path) if row["mode"] != "native"]
    iodepths = {int(row["iodepth"]) for row in summary_rows}
    if len(iodepths) != 1:
        raise RuntimeError(f"Expected exactly one iodepth in {summary_path}, got {sorted(iodepths)}")
    iodepth = next(iter(iodepths))

    native_summary, native_all_runs = baseline_rows_for_qd(base_rows, iodepth)
    summary_rows.extend(native_summary)
    all_runs.extend(native_all_runs)
    recompute_derived(summary_rows, all_runs)

    summary_rows = sort_rows(summary_rows)
    all_runs = sort_rows(all_runs)

    write_csv(
        summary_path,
        summary_rows,
        [
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
        ],
    )
    write_csv(
        all_runs_path,
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

    subprocess.run(["python3", str(plotter), "--result-dir", str(result_dir)], check=True)


def main():
    args = parse_args()
    baseline_output_dir = Path(args.baseline_output_dir).resolve()
    logs_dir = baseline_output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    existing_all_runs = baseline_output_dir / "all_runs.csv"
    if args.reuse_existing_baseline and existing_all_runs.exists():
        print(f"Reusing existing POSIX staged baseline from {existing_all_runs}", flush=True)
        base_rows = load_existing_baseline_rows(existing_all_runs)
        requested = set(args.numjobs_list)
        available = {row["numjobs"] for row in base_rows}
        missing = sorted(requested - available)
        if missing:
            raise RuntimeError(
                f"Existing baseline is missing numjobs points required for this overlay: {missing}"
            )
        base_rows = [row for row in base_rows if row["numjobs"] in requested]
    else:
        base_rows = []
        for numjobs in args.numjobs_list:
            log_path = logs_dir / f"native-posix-staged-sync-nj{numjobs}.log"
            print(f"Running POSIX staged sync baseline for numjobs={numjobs}", flush=True)
            row = run_posix_point(args, numjobs, log_path)
            base_rows.append(row)

    baseline_summary = []
    for row in base_rows:
        baseline_summary.append(
            {
                "mode": "native",
                "numjobs": row["numjobs"],
                "iodepth": 1,
                "total_qd": row["numjobs"],
                "read_iops": row["read_iops"],
                "read_iops_stddev": 0.0,
                "bw": row["bw"],
                "clat_mean": row["clat_mean"],
                "clat_p95": row["clat_p95"],
                "clat_p99": row["clat_p99"],
                "runtime": row["runtime"],
                "speedup_vs_numjobs1": row["speedup_vs_numjobs1"],
                "efficiency_vs_numjobs1": row["efficiency_vs_numjobs1"],
                "per_job_iops": row["per_job_iops"],
                "max_iops_point": row["max_iops_point"],
            }
        )

    recompute_derived(baseline_summary, base_rows)
    for index, row in enumerate(base_rows):
        row["speedup_vs_numjobs1"] = baseline_summary[index]["speedup_vs_numjobs1"]
        row["efficiency_vs_numjobs1"] = baseline_summary[index]["efficiency_vs_numjobs1"]
        row["per_job_iops"] = baseline_summary[index]["per_job_iops"]
        row["max_iops_point"] = baseline_summary[index]["max_iops_point"]

    write_csv(
        baseline_output_dir / "summary.csv",
        sort_rows(baseline_summary),
        [
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
        ],
    )
    write_csv(
        baseline_output_dir / "all_runs.csv",
        sort_rows(base_rows),
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

    for result_dir in args.result_dirs:
        print(f"Overlaying POSIX staged sync rows into {result_dir}", flush=True)
        overlay_result_dir(result_dir, base_rows, Path(args.plotter).resolve())

    print(f"Wrote POSIX staged sync baseline to {baseline_output_dir}", flush=True)


if __name__ == "__main__":
    main()
