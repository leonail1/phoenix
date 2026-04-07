# FIO engine for phoenix

This directory contains the core fio external engine used for benchmarking phoenix.
> Note: We referred to the implementation of [3FS USRBIO](https://github.com/deepseek-ai/3FS/tree/main/benchmarks/fio_usrbio)

Experiment-side assets now live under `experiments/fio/`, including:

- sweep scripts
- fio templates
- example jobs
- experiment notes
- plotting utilities

## How to Build

### 1. build fio

```shell
git submodule update --init --recursive
cd third-party/fio
./configure --dynamic-libengines
make -j
```

### 2. build ioengine

```shell
cd benchmarks/fio && make
```

The shared object is generated as `benchmarks/fio/phoenix_fio.so`.

## Usage

An example fio job now lives at `experiments/fio/examples/test.fio`.
Run it from the repository root so the relative engine path resolves correctly:

```shell
cd /mnt/nvme1n1/lzg/phoenix
/mnt/nvme1n1/lzg/phoenix/third-party/fio/fio experiments/fio/examples/test.fio
```

To use the io_uring backend, set these four parameters together:

```shell
iodepth=1024
iodepth_batch_submit=1024
iodepth_batch_complete_min=1024
iodepth_batch_complete_max=1024
```

## Experiments

See `experiments/fio/README.md` for:

- steady-state randread sweep commands
- numjobs scaling commands
- baseline/overlay notes
- SVG plotting commands
