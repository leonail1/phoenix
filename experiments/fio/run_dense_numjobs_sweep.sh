#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

FILENAME="/mnt/nvme_data/lzg/phoenix_bench/randread_4k_steady_64g.bin"
SIZE="64G"
RUNTIME="20"
REPETITIONS="1"
DEVICE_ID="0"
NUMJOBS_MAX="$(nproc --all)"
NUMJOBS_LIST=""
NUMJOBS_EXP_CUTOFF="8"
NUMJOBS_LINEAR_STEP="4"
IODEPTHS="4,8,16"
RAW_DEVICE="auto"
RAW_SIZE=""
RAW_IOENGINE="io_uring"
RAW_SUDO="1"
FIO_BIN="$REPO_ROOT/third-party/fio/fio"
ENGINE="$REPO_ROOT/benchmarks/fio/phoenix_fio.so"
RUNNER="$SCRIPT_DIR/run_numjobs_scaling.py"
PLOTTER="$SCRIPT_DIR/plot_numjobs_scaling.py"
RESULT_ROOT="$SCRIPT_DIR/results"
RESULT_PREFIX="randread_4k_numjobs_scaling_sparse_raw"

usage() {
  cat <<'EOF'
Usage: run_dense_numjobs_sweep.sh [options]

Options:
  --filename PATH         Target fio file.
  --size SIZE             fio size=... value.
  --runtime SEC           Steady-state runtime per point.
  --repetitions N         Repetitions per point.
  --device-id ID          Phoenix device id.
  --numjobs-max N         Maximum numjobs used by the sparse schedule.
  --numjobs-list LIST     Explicit comma-separated numjobs list. Overrides schedule generation.
  --numjobs-exp-cutoff N  Exponential growth cutoff. Defaults to 8.
  --numjobs-linear-step N Linear step after the cutoff. Defaults to 4.
  --iodepths LIST         Comma-separated per-job iodepths, e.g. 4,8,16.
  --raw-device PATH       Raw block device baseline. Use 'auto' to derive from --filename. Use 'none' to disable.
  --raw-size SIZE         fio size=... for raw block-device runs. Defaults to --size.
  --raw-ioengine NAME     fio ioengine for raw block-device runs. Defaults to io_uring.
  --raw-no-sudo           Do not prepend 'sudo -n' for raw block-device runs.
  --fio-bin PATH          fio binary path.
  --engine PATH           phoenix_fio.so path.
  --result-root PATH      Root directory for result directories.
  -h, --help              Show this help.
EOF
}

build_numjobs_list() {
  local max_jobs=$1
  local cutoff=$2
  local linear_step=$3
  local -a values=()
  local current=1

  while (( current <= max_jobs && current <= cutoff )); do
    values+=("$current")
    if (( current == cutoff )); then
      break
    fi
    current=$(( current * 2 ))
    if (( current > cutoff )); then
      current=$cutoff
    fi
  done

  if (( max_jobs > cutoff )); then
    local next=$(( cutoff + linear_step ))
    while (( next <= max_jobs )); do
      values+=("$next")
      next=$(( next + linear_step ))
    done
    if (( values[${#values[@]} - 1] != max_jobs )); then
      values+=("$max_jobs")
    fi
  fi

  local list=""
  local value=""
  for value in "${values[@]}"; do
    if [[ -n "$list" ]]; then
      list+=","
    fi
    list+="$value"
  done
  printf '%s\n' "$list"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --filename)
      FILENAME="$2"
      shift 2
      ;;
    --size)
      SIZE="$2"
      shift 2
      ;;
    --runtime)
      RUNTIME="$2"
      shift 2
      ;;
    --repetitions)
      REPETITIONS="$2"
      shift 2
      ;;
    --device-id)
      DEVICE_ID="$2"
      shift 2
      ;;
    --numjobs-max)
      NUMJOBS_MAX="$2"
      shift 2
      ;;
    --numjobs-list)
      NUMJOBS_LIST="$2"
      shift 2
      ;;
    --numjobs-exp-cutoff)
      NUMJOBS_EXP_CUTOFF="$2"
      shift 2
      ;;
    --numjobs-linear-step)
      NUMJOBS_LINEAR_STEP="$2"
      shift 2
      ;;
    --iodepths)
      IODEPTHS="$2"
      shift 2
      ;;
    --raw-device)
      RAW_DEVICE="$2"
      shift 2
      ;;
    --raw-size)
      RAW_SIZE="$2"
      shift 2
      ;;
    --raw-ioengine)
      RAW_IOENGINE="$2"
      shift 2
      ;;
    --raw-no-sudo)
      RAW_SUDO="0"
      shift 1
      ;;
    --fio-bin)
      FIO_BIN="$2"
      shift 2
      ;;
    --engine)
      ENGINE="$2"
      shift 2
      ;;
    --result-root)
      RESULT_ROOT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$NUMJOBS_LIST" ]]; then
  NUMJOBS_LIST="$(build_numjobs_list "$NUMJOBS_MAX" "$NUMJOBS_EXP_CUTOFF" "$NUMJOBS_LINEAR_STEP")"
fi

runner_pid_for_dir() {
  local dir_name=$1
  ps -eo pid=,comm=,args= | awk -v target="$dir_name" '$2=="python3" && $0 ~ /run_numjobs_scaling.py/ && index($0, target) {print $1}' | head -n 1
}

run_qd() {
  local qd=$1
  local result_dir="$RESULT_ROOT/${RESULT_PREFIX}_qd${qd}"
  local result_tag
  result_tag="$(basename "$result_dir")"

  echo "=== qd${qd} => $result_dir ==="
  echo "numjobs: $NUMJOBS_LIST"
  mkdir -p "$RESULT_ROOT"

  while true; do
    if [[ -f "$result_dir/summary.csv" ]]; then
      echo "summary.csv already present for qd${qd}"
      break
    fi

    local pid=""
    pid="$(runner_pid_for_dir "$result_tag" || true)"
    if [[ -n "$pid" ]]; then
      echo "Waiting for existing qd${qd} runner pid=$pid"
      sleep 60
      continue
    fi

    echo "Starting/resuming qd${qd}"
    local -a cmd=(
      python3 "$RUNNER"
      --filename "$FILENAME"
      --size "$SIZE"
      --runtime "$RUNTIME"
      --repetitions "$REPETITIONS"
      --iodepth "$qd"
      --numjobs-list "$NUMJOBS_LIST"
      --device-id "$DEVICE_ID"
      --fio-bin "$FIO_BIN"
      --engine "$ENGINE"
      --output-dir "$result_dir"
      --raw-ioengine "$RAW_IOENGINE"
      --resume-existing
    )

    if [[ "$RAW_DEVICE" != "none" ]]; then
      cmd+=(--raw-device "$RAW_DEVICE")
      if [[ "$RAW_SUDO" == "1" ]]; then
        cmd+=(--raw-sudo)
      fi
    fi
    if [[ -n "$RAW_SIZE" ]]; then
      cmd+=(--raw-size "$RAW_SIZE")
    fi

    "${cmd[@]}"

    if [[ -f "$result_dir/summary.csv" ]]; then
      break
    fi

    echo "qd${qd} exited without summary.csv; retrying in 10s"
    sleep 10
  done

  python3 "$PLOTTER" --result-dir "$result_dir"
  echo "Plot ready for qd${qd}: $result_dir/plots/numjobs_scaling_overview.svg"
}

IFS=',' read -r -a qd_array <<<"$IODEPTHS"
for qd in "${qd_array[@]}"; do
  run_qd "$qd"
done

echo "All sparse numjobs sweeps finished."
