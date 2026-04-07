# fio Experiments

This directory holds the experiment-side tooling for the Phoenix fio engine.
The core ioengine source remains under `benchmarks/fio/`.

Tracked files here are intended to stay lightweight:

- sweep runners
- plotting scripts
- fio templates
- example job files
- short experiment notes

Generated outputs are local-only and gitignored:

- `experiments/fio/results/`
- `experiments/fio/logs/`
- `experiments/fio/__pycache__/`

## Build

Build fio with dynamic engine support, then build the Phoenix external engine:

```bash
cd /mnt/nvme1n1/lzg/phoenix/third-party/fio
./configure --dynamic-libengines
make -j

cd /mnt/nvme1n1/lzg/phoenix/benchmarks/fio
make
```

## Layout

- steady-state sweep runner: `experiments/fio/run_randread_sweep.py`
- steady-state plotter: `experiments/fio/plot_randread_results.py`
- numjobs scaling runner: `experiments/fio/run_numjobs_scaling.py`
- sparse multi-QD wrapper: `experiments/fio/run_dense_numjobs_sweep.sh`
- numjobs plotter: `experiments/fio/plot_numjobs_scaling.py`
- native baseline overlay helper: `experiments/fio/run_posix_staged_overlay.py`
- fio templates: `experiments/fio/templates/`
- example fio job: `experiments/fio/examples/test.fio`

## Steady-State Sweep

```bash
cd /mnt/nvme1n1/lzg/phoenix
python3 experiments/fio/run_randread_sweep.py \
  --filename /path/to/testfile \
  --size 64G \
  --runtime 20 \
  --repetitions 3 \
  --device-id 0 \
  --fio-bin /mnt/nvme1n1/lzg/phoenix/third-party/fio/fio \
  --engine /mnt/nvme1n1/lzg/phoenix/benchmarks/fio/phoenix_fio.so
```

Plot an existing steady-state result directory:

```bash
cd /mnt/nvme1n1/lzg/phoenix
python3 experiments/fio/plot_randread_results.py \
  --result-dir /mnt/nvme1n1/lzg/phoenix/experiments/fio/results/randread_4k_steady_20260405_1706
```

## Numjobs Scaling Sweep

The sparse schedule used in the current experiments is:

- `1,2,4,8,12,16,20,24,28,32`

Run a single fixed-`iodepth` numjobs sweep:

```bash
cd /mnt/nvme1n1/lzg/phoenix
python3 experiments/fio/run_numjobs_scaling.py \
  --filename /path/to/testfile \
  --size 64G \
  --runtime 20 \
  --repetitions 1 \
  --iodepth 4 \
  --numjobs-list 1,2,4,8,12,16,20,24,28,32 \
  --device-id 0 \
  --raw-device auto \
  --raw-sudo \
  --fio-bin /mnt/nvme1n1/lzg/phoenix/third-party/fio/fio \
  --engine /mnt/nvme1n1/lzg/phoenix/benchmarks/fio/phoenix_fio.so
```

Run the current sparse sweep wrapper for `iodepth=4,8,16`:

```bash
cd /mnt/nvme1n1/lzg/phoenix
bash experiments/fio/run_dense_numjobs_sweep.sh --iodepths 4,8,16
```

Plot an existing numjobs result directory:

```bash
cd /mnt/nvme1n1/lzg/phoenix
python3 experiments/fio/plot_numjobs_scaling.py \
  --result-dir /mnt/nvme1n1/lzg/phoenix/experiments/fio/results/randread_4k_numjobs_scaling_sparse_raw_qd4
```

## Native Baseline Overlay

The numjobs plots currently use three labels:

- `PhxFS`: Phoenix fio path
- `Native (NVMe->host->HBM)`: sync staged baseline overlaid from `benchmarks/micro-benchmarks`
- `Ideal`: raw NVMe block-device ceiling using fio built-in `io_uring`

Important note:
The overlaid `Native` line is not the same thing as a fio native run at the same per-job `iodepth`.
It is a `numjobs`-threaded staged baseline with one in-flight I/O per thread, then remapped onto each plot's `iodepth` for comparison.

Reuse an existing staged baseline and redraw selected result directories:

```bash
cd /mnt/nvme1n1/lzg/phoenix
python3 experiments/fio/run_posix_staged_overlay.py \
  --result-dirs \
    experiments/fio/results/randread_4k_numjobs_scaling_sparse_raw_qd4 \
    experiments/fio/results/randread_4k_numjobs_scaling_sparse_raw_qd8 \
    experiments/fio/results/randread_4k_numjobs_scaling_sparse_raw_qd16 \
  --reuse-existing-baseline
```
