#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <ctime>
#include <atomic>
#include <errno.h>
#include <vector>
#include <sys/stat.h>

#include "phoenix.h"
#include <cufile.h>
#include <liburing.h>
#include <liburing/io_uring.h>

extern "C" {
#include "fio.h"
#include "optgroup.h"
}

static std::atomic<bool> driver_inited{false};
static std::atomic<int> cufile_users{0};
static constexpr size_t kStagingBytes = 4UL << 20;

struct phoenix_options {
    void *pad;
    int dummy;
    int enable_phoenix;
    int enable_cufile;
    int device_id;
    int enable_iouring;
};

static struct fio_option options[] = {
    {
        .name       = "enable_iouring",
        .lname      = "Enable io_uring backend",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct phoenix_options, enable_iouring),
        .help       = "Set to 1 to enable io_uring backend",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name       = "enable_phoenix",
        .lname      = "Enable phoenix optimizations",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct phoenix_options, enable_phoenix),
        .help       = "Set to 1 to enable phoenix optimizations",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name       = "enable_cufile",
        .lname      = "Enable cuFile GPU I/O path",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct phoenix_options, enable_cufile),
        .help       = "Set to 1 to enable cuFile reads/writes into GPU memory",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {
        .name       = "device_id",
        .lname      = "phoenix Device ID",
        .type       = FIO_OPT_INT,
        .off1       = offsetof(struct phoenix_options, device_id),
        .help       = "Set the phoenix device ID to use",
        .def        = "0",
        .category = FIO_OPT_C_ENGINE,
        .group = FIO_OPT_G_NETIO,
    },
    {NULL}
};


#define LAST_POS(f) ((f)->engine_pos)

struct phoenix_data {
    struct io_uring *read_ring;
    struct io_uring *write_ring;
    cudaStream_t read_stream;
    cudaStream_t write_stream;
    void *dev_buf;
    void *host_buf;
    size_t buf_size;
    std::vector<struct io_u *> io_us;
    int queued;
    int events;
    CUfileHandle_t cufile_handle;
    bool cufile_handle_registered;
    bool cufile_buffer_registered;
    bool cufile_read_stream_registered;
    bool cufile_write_stream_registered;
    enum fio_ddir last_ddir;
};

static inline uint64_t host_buffer_offset(const phoenix_data *sd, const void *host_ptr) {
    return (uint64_t)host_ptr - (uint64_t)sd->host_buf;
}

static inline void *device_buffer_ptr(const phoenix_data *sd, const void *host_ptr) {
    return (void *)((uint64_t)sd->dev_buf + host_buffer_offset(sd, host_ptr));
}

static int create_ring(struct io_uring **ring, unsigned int depth) {
    *ring = new io_uring();

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = 0;
    params.cq_entries = params.sq_entries = depth;

    return io_uring_queue_init_params(depth, *ring, &params);
}

static int wait_for_completions(struct io_uring *ring, size_t total_submitted) {
    int res;
    size_t nr_completions = 0;

    while (nr_completions < total_submitted) {
        struct io_uring_cqe *cqe;
        res = io_uring_wait_cqe(ring, &cqe);
        if (res < 0) {
            std::cerr << "io_uring_wait_cqe failed: " << strerror(-res) << std::endl;
            return res;
        }
        if (cqe->res < 0) {
            std::cerr << "io_uring completion failed: " << strerror(-cqe->res) << std::endl;
            io_uring_cqe_seen(ring, cqe);
            return cqe->res;
        }
        if ((uint64_t)cqe->res != cqe->user_data) {
            std::cerr << "short io_uring completion: expected " << cqe->user_data
                      << " got " << cqe->res << std::endl;
            io_uring_cqe_seen(ring, cqe);
            return -EIO;
        }
        io_uring_cqe_seen(ring, cqe);
        nr_completions++;
    }

    return 0;
}

static int sync_stream(cudaStream_t stream, const char *label) {
    auto err = cudaStreamSynchronize(stream);
    if (err != cudaSuccess) {
        std::cerr << label << " failed: " << cudaGetErrorString(err) << std::endl;
        return -EIO;
    }
    return 0;
}


static int phoenix_init(struct thread_data *td) {
    td->io_ops_data = static_cast<void *>(new phoenix_data);
    struct phoenix_options * opts = (struct phoenix_options *)td->eo;
    auto data = static_cast<phoenix_data *>(td->io_ops_data);
    data->read_ring = nullptr;
    data->write_ring = nullptr;
    data->read_stream = nullptr;
    data->write_stream = nullptr;
    data->dev_buf = nullptr;
    data->host_buf = nullptr;
    data->buf_size = 0;
    data->last_ddir = DDIR_READ;
    data->cufile_handle_registered = false;
    data->cufile_buffer_registered = false;
    data->cufile_read_stream_registered = false;
    data->cufile_write_stream_registered = false;

    if (opts->enable_phoenix) {
        if (!driver_inited) {
            if (phxfs_open(opts->device_id) != 0) {
                std::cerr << "Failed to initialize Phoenix driver" << std::endl;
                return -EIO;
            }
            driver_inited = true;
        }
    }
    if (opts->enable_cufile) {
        if (cufile_users.fetch_add(1) == 0) {
            CUfileError_t status = cuFileDriverOpen();
            if (status.err != CU_FILE_SUCCESS) {
                cufile_users.fetch_sub(1);
                std::cerr << "cuFileDriverOpen failed: " << status.err << std::endl;
                return -EIO;
            }
        }
    }

    std::cout << "phoenix_init: enable_phoenix=" << opts->enable_phoenix
              << ", enable_cufile=" << opts->enable_cufile
              << ", enable_iouring=" << opts->enable_iouring
              << ", device_id=" << opts->device_id << std::endl;

    if (opts->enable_iouring && !opts->enable_cufile) {
        std::cout << "Initializing io_uring with iodepth=" << td->o.iodepth << std::endl;
        if (create_ring(&data->read_ring, td->o.iodepth)) {
            std::cerr << "io_uring_queue_init_params read failed" << std::endl;
            return -ENOMEM;
        }
        if (create_ring(&data->write_ring, td->o.iodepth)) {
            std::cerr << "io_uring_queue_init_params write failed" << std::endl;
            return -ENOMEM;
        }
    }

    if (!opts->enable_phoenix) {
        auto err = cudaStreamCreateWithFlags(&data->read_stream, cudaStreamNonBlocking);
        if (err != cudaSuccess) {
            std::cerr << "cudaStreamCreateWithFlags read failed: " << cudaGetErrorString(err) << std::endl;
            return -ENOMEM;
        }
        err = cudaStreamCreateWithFlags(&data->write_stream, cudaStreamNonBlocking);
        if (err != cudaSuccess) {
            std::cerr << "cudaStreamCreateWithFlags write failed: " << cudaGetErrorString(err) << std::endl;
            return -ENOMEM;
        }
        if (opts->enable_cufile) {
            CUfileError_t status = cuFileStreamRegister(data->read_stream, 15);
            if (status.err != CU_FILE_SUCCESS) {
                std::cerr << "cuFileStreamRegister read failed: " << status.err << std::endl;
                return -EIO;
            }
            data->cufile_read_stream_registered = true;

            status = cuFileStreamRegister(data->write_stream, 15);
            if (status.err != CU_FILE_SUCCESS) {
                std::cerr << "cuFileStreamRegister write failed: " << status.err << std::endl;
                return -EIO;
            }
            data->cufile_write_stream_registered = true;
        }
    }

    data->io_us.resize(td->o.iodepth);
    data->queued = 0;
    data->events = 0;
    return 0;
}

static int fio_io_end(struct thread_data *td, struct io_u *io_u, int ret) {
    if (io_u->file && ret >= 0 && ddir_rw(io_u->ddir)) {
        LAST_POS(io_u->file) = io_u->offset + ret;
    }

    if (ret != (int) io_u->xfer_buflen) {
        if (ret >= 0) {
            io_u->resid = io_u->xfer_buflen - ret;
            io_u->error = 0;
            return FIO_Q_COMPLETED;
        } else {
            io_u->error = errno;
        }
    }

    if (io_u->error) {
        io_u_log_error(td, io_u);
        td_verror(td, io_u->error, "xfer");
    }

    return FIO_Q_COMPLETED;
}

static enum fio_q_status phoenix_queue(struct thread_data *td, struct io_u *io_u) {
    auto &vec = static_cast<phoenix_data *>(td->io_ops_data)->io_us;
    auto *sd = static_cast<phoenix_data *>(td->io_ops_data);

    if (io_u->ddir != sd->last_ddir) {
        if (sd->queued != 0) {
            return FIO_Q_BUSY;
        } else {
            vec[sd->queued++] = io_u;
            sd->last_ddir = io_u->ddir;
            return FIO_Q_QUEUED;
        }
    } else {
        if (sd->queued == td->o.iodepth) {
            return FIO_Q_BUSY;
        }
        vec[sd->queued++] = io_u;
        return FIO_Q_QUEUED;
    }
}

static int phoenix_iouring_commit(struct phoenix_options *opts, phoenix_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    auto ring = read ? sd->read_ring : sd->write_ring;
    int res;

    size_t total_submitted = 0;
    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto buf_offset = host_buffer_offset(sd, io_u->xfer_buf);
        phxfs_xfer_addr *xfer_addr = NULL;

        xfer_addr = phxfs_do_xfer_addr(opts->device_id, sd->dev_buf, buf_offset, io_u->xfer_buflen);
        if (!xfer_addr) {
            std::cerr << "phxfs_do_xfer_addr failed" << std::endl;
            return -EIO;
        }

        size_t internal_bytes = 0;
        for (int j = 0; j < xfer_addr->nr_xfer_addrs; j++) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                std::cerr << "io_uring_get_sqe failed" << std::endl;
                return -ENOMEM;
            }
                io_uring_prep_rw(
                    read ? IORING_OP_READ : IORING_OP_WRITE,
                    sqe,
                    io_u->file->fd,
                    (char *)xfer_addr->x_addrs[j].target_addr,
                    xfer_addr->x_addrs[j].nbyte,
                    io_u->offset + internal_bytes
                );
                sqe->user_data = xfer_addr->x_addrs[j].nbyte;
                internal_bytes += xfer_addr->x_addrs[j].nbyte;
                total_submitted ++;   
        }
        std::free(xfer_addr);

        if (internal_bytes != io_u->xfer_buflen) {
            std::cerr << "internal_bytes != xfer_buflen" << std::endl;
            return -EIO;
        }
    }
    res = io_uring_submit(ring);
    if (res < 0) {
        std::cerr << "io_uring_submit failed: " << strerror(-res) << std::endl;
        return res;
    }

    return wait_for_completions(ring, total_submitted);
}

static int phoenix_sync_commit(struct phoenix_options *opts, phoenix_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    int res;

    auto ioOp = read ? phxfs_read : phxfs_write;
    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto buffer_offset = (uint64_t)io_u->xfer_buf - (uint64_t)sd->host_buf;
        res = ioOp(
            {io_u->file->fd, opts->device_id},
            sd->dev_buf,
            buffer_offset,
            io_u->xfer_buflen,
            io_u->offset
        );
        if (res < 0) {
            std::cerr << "phxfs_read/phxfs_write failed: " << strerror(-res) << std::endl;
            return res;
        }
    }
    return 0;
}

static int phoenix_native_sync_commit(struct phoenix_options *opts, phoenix_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    int res;

    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        auto dev_ptr = device_buffer_ptr(sd, io_u->xfer_buf);

        res = read ? pread(io_u->file->fd, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset)
                   : pwrite(io_u->file->fd, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset);
        
        if (res < 0) {
            std::cerr << "pread/pwrite failed: " << strerror(errno) << std::endl;
            return -errno;
        }

        auto err = read ? cudaMemcpy(dev_ptr, io_u->xfer_buf, io_u->xfer_buflen, cudaMemcpyHostToDevice)
                                     : cudaMemcpy(io_u->xfer_buf, dev_ptr, io_u->xfer_buflen, cudaMemcpyDeviceToHost);
        if (err != cudaSuccess) {
            std::cerr << "cudaMemcpy failed: " << cudaGetErrorString(err) << std::endl;
            return -EIO;
        }
    }
    
    return 0;
}

static int phoenix_native_iouring_commit(struct phoenix_options *opts, phoenix_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    auto ring = read ? sd->read_ring : sd->write_ring;
    auto stream = read ? sd->read_stream : sd->write_stream;

    if (!read) {
        for (int i = 0; i < sd->queued; i++) {
            struct io_u *io_u = vec[i];
            auto err = cudaMemcpyAsync(
                io_u->xfer_buf,
                device_buffer_ptr(sd, io_u->xfer_buf),
                io_u->xfer_buflen,
                cudaMemcpyDeviceToHost,
                stream);
            if (err != cudaSuccess) {
                std::cerr << "cudaMemcpyAsync D2H failed: " << cudaGetErrorString(err) << std::endl;
                return -EIO;
            }
        }
        int res = sync_stream(stream, "cudaStreamSynchronize before native write");
        if (res < 0) {
            return res;
        }
    }

    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            std::cerr << "io_uring_get_sqe failed" << std::endl;
            return -ENOMEM;
        }
        io_uring_prep_rw(
            read ? IORING_OP_READ : IORING_OP_WRITE,
            sqe,
            io_u->file->fd,
            io_u->xfer_buf,
            io_u->xfer_buflen,
            io_u->offset);
        sqe->user_data = io_u->xfer_buflen;
    }

    int res = io_uring_submit(ring);
    if (res < 0) {
        std::cerr << "io_uring_submit failed: " << strerror(-res) << std::endl;
        return res;
    }

    res = wait_for_completions(ring, sd->queued);
    if (res < 0) {
        return res;
    }

    if (read) {
        for (int i = 0; i < sd->queued; i++) {
            struct io_u *io_u = vec[i];
            auto err = cudaMemcpyAsync(
                device_buffer_ptr(sd, io_u->xfer_buf),
                io_u->xfer_buf,
                io_u->xfer_buflen,
                cudaMemcpyHostToDevice,
                stream);
            if (err != cudaSuccess) {
                std::cerr << "cudaMemcpyAsync H2D failed: " << cudaGetErrorString(err) << std::endl;
                return -EIO;
            }
        }
        res = sync_stream(stream, "cudaStreamSynchronize after native read");
        if (res < 0) {
            return res;
        }
    }

    return 0;
}

static int phoenix_cufile_commit(struct thread_data *td, struct phoenix_options *opts, phoenix_data *sd) {
    bool read = (sd->last_ddir == DDIR_READ);
    auto &vec = sd->io_us;
    auto stream = read ? sd->read_stream : sd->write_stream;

    if (!sd->cufile_handle_registered) {
        std::cerr << "cuFile handle not registered" << std::endl;
        return -EIO;
    }

    if (!read) {
        for (int i = 0; i < sd->queued; i++) {
            struct io_u *io_u = vec[i];
            auto err = cudaMemcpyAsync(
                device_buffer_ptr(sd, io_u->xfer_buf),
                io_u->xfer_buf,
                io_u->xfer_buflen,
                cudaMemcpyHostToDevice,
                stream);
            if (err != cudaSuccess) {
                std::cerr << "cudaMemcpyAsync H2D before cuFileWrite failed: "
                          << cudaGetErrorString(err) << std::endl;
                return -EIO;
            }
        }
        int res = sync_stream(stream, "cudaStreamSynchronize before cufile write");
        if (res < 0) {
            return res;
        }
    }

    std::vector<size_t> io_sizes(sd->queued);
    std::vector<off_t> file_offsets(sd->queued);
    std::vector<off_t> buf_offsets(sd->queued);
    std::vector<ssize_t> bytes_done(sd->queued, 0);

    for (int i = 0; i < sd->queued; i++) {
        struct io_u *io_u = vec[i];
        io_sizes[i] = io_u->xfer_buflen;
        file_offsets[i] = io_u->offset;
        buf_offsets[i] = host_buffer_offset(sd, io_u->xfer_buf);

        CUfileError_t status = read
            ? cuFileReadAsync(
                  sd->cufile_handle,
                  sd->dev_buf,
                  &io_sizes[i],
                  &file_offsets[i],
                  &buf_offsets[i],
                  &bytes_done[i],
                  stream)
            : cuFileWriteAsync(
                  sd->cufile_handle,
                  sd->dev_buf,
                  &io_sizes[i],
                  &file_offsets[i],
                  &buf_offsets[i],
                  &bytes_done[i],
                  stream);
        if (status.err != CU_FILE_SUCCESS) {
            std::cerr << (read ? "cuFileReadAsync" : "cuFileWriteAsync")
                      << " failed: " << status.err << std::endl;
            return -EIO;
        }
    }

    int res = sync_stream(stream, read ? "cudaStreamSynchronize after cufile read"
                                       : "cudaStreamSynchronize after cufile write");
    if (res < 0) {
        return res;
    }

    if (read && td->o.verify) {
        for (int i = 0; i < sd->queued; i++) {
            struct io_u *io_u = vec[i];
            auto err = cudaMemcpyAsync(
                io_u->xfer_buf,
                device_buffer_ptr(sd, io_u->xfer_buf),
                io_u->xfer_buflen,
                cudaMemcpyDeviceToHost,
                stream);
            if (err != cudaSuccess) {
                std::cerr << "cudaMemcpyAsync D2H after cufile read failed: "
                          << cudaGetErrorString(err) << std::endl;
                return -EIO;
            }
        }
        res = sync_stream(stream, "cudaStreamSynchronize after cufile verify copy");
        if (res < 0) {
            return res;
        }
    }

    for (int i = 0; i < sd->queued; i++) {
        if (bytes_done[i] != (ssize_t)vec[i]->xfer_buflen) {
            std::cerr << "short cuFile completion: expected " << vec[i]->xfer_buflen
                      << " got " << bytes_done[i] << std::endl;
            return -EIO;
        }
    }

    return 0;
}


static int phoenix_commit(struct thread_data *td) {
    auto sd = static_cast<phoenix_data *>(td->io_ops_data);
    auto &vec = sd->io_us;
    auto opts = (struct phoenix_options *)td->eo;

    if (sd->queued == 0) {
        return 0;
    }

    io_u_mark_submit(td, sd->queued);
    int res = 0;
    if (opts->enable_phoenix && opts->enable_iouring) {
        res = phoenix_iouring_commit(opts, sd);
    } else if (opts->enable_phoenix && !opts->enable_iouring) {
        res = phoenix_sync_commit(opts, sd);
    } else if (opts->enable_cufile) {
        res = phoenix_cufile_commit(td, opts, sd);
    } else if (!opts->enable_phoenix && opts->enable_iouring) {
        res = phoenix_native_iouring_commit(opts, sd);
    } else {
        res = phoenix_native_sync_commit(opts, sd);
    }

    if (res < 0) {
        std::cerr << "Commit failed" << std::endl;
        return res;
    }

    sd->events = sd->queued;
    sd->queued = 0;

    return 0;
}

static int phoenix_getevents(struct thread_data *td, unsigned int min, unsigned int max, const struct timespec *ts) {
    auto &vec = static_cast<phoenix_data *>(td->io_ops_data)->io_us;
    auto *sd = static_cast<phoenix_data *>(td->io_ops_data);
    int ret = 0;
    if (min) {
        ret = sd->events;
        sd->events = 0;
    }

    return ret;
}

static struct io_u *phoenix_event(struct thread_data *td, int event) {
    auto &vec = static_cast<phoenix_data *>(td->io_ops_data)->io_us;
    return vec[event];
}

static void phoenix_cleanup(struct thread_data *td) {
    auto opts = (struct phoenix_options *)td->eo;
    auto data = static_cast<phoenix_data *>(td->io_ops_data);
    if (opts->enable_phoenix && driver_inited) {
        phxfs_close(opts->device_id);
        driver_inited = false;
    }

    if (data->read_ring) {
        io_uring_queue_exit(data->read_ring);
        delete data->read_ring;
    }

    if (data->write_ring) {
        io_uring_queue_exit(data->write_ring);
        delete data->write_ring;
    }

    if (data->cufile_read_stream_registered && data->read_stream) {
        cuFileStreamDeregister(data->read_stream);
    }

    if (data->cufile_write_stream_registered && data->write_stream) {
        cuFileStreamDeregister(data->write_stream);
    }

    if (data->read_stream) {
        cudaStreamDestroy(data->read_stream);
    }

    if (data->write_stream) {
        cudaStreamDestroy(data->write_stream);
    }

    if (opts->enable_cufile && cufile_users.fetch_sub(1) == 1) {
        cuFileDriverClose();
    }

    delete data;
}

static int phoenix_file_open(struct thread_data *td, struct fio_file *f) {
    int flags = 0;
    if (td_write(td)) {
        if (!read_only) {
            flags = O_RDWR;
        }
    } else if (td_read(td)) {
        if (!read_only) {
            flags = O_RDWR;
        } else {
            flags = O_RDONLY;
        }
    }

    f->fd = open(f->file_name, flags | O_DIRECT);
    if (f->fd < 0) {
        auto err = errno;
        std::cerr << "phoenix open file failed: " << f->file_name
                  << " error: " << strerror(err) << std::endl;
        return -err;
    }
    if (static_cast<phoenix_options *>(td->eo)->enable_cufile) {
        auto data = static_cast<phoenix_data *>(td->io_ops_data);
        CUfileDescr_t desc;
        memset(&desc, 0, sizeof(desc));
        desc.handle.fd = f->fd;
        desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
        CUfileError_t status = cuFileHandleRegister(&data->cufile_handle, &desc);
        if (status.err != CU_FILE_SUCCESS) {
            auto err = errno;
            close(f->fd);
            f->fd = -1;
            std::cerr << "cuFileHandleRegister failed: " << status.err
                      << " errno=" << err << std::endl;
            return -EIO;
        }
        data->cufile_handle_registered = true;
    }
    std::cout << "phoenix open file: " << f->file_name << " fd: " << f->fd << std::endl;
    td->o.open_files++;
    return 0;
}

static int phoenix_file_close(struct thread_data *td, struct fio_file *f) {
    auto data = static_cast<phoenix_data *>(td->io_ops_data);
    if (data->cufile_handle_registered) {
        cuFileHandleDeregister(data->cufile_handle);
        data->cufile_handle_registered = false;
    }
    close(f->fd);
    f->fd = -1;
    return 0;
}

static int phoenix_buffer_alloc(struct thread_data *td, size_t total_mem) {
    struct phoenix_options *options = static_cast<phoenix_options *>(td->eo);
    auto data = static_cast<phoenix_data *>(td->io_ops_data);
    auto &dev_buf = data->dev_buf;
    auto &host_buf = data->host_buf;

    if (total_mem < kStagingBytes) {
        total_mem = kStagingBytes;
    }

    // Keep the staging area stable and GPU-page aligned for steady-state tests.
    if (total_mem % (64 * 1024) != 0) {
        total_mem = ((total_mem / (64 * 1024)) + 1) * (64 * 1024);
    }

    data->buf_size = total_mem;
    cudaError_t err = cudaMalloc(&dev_buf, total_mem);
    if (err != cudaSuccess) {
        std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
        return -ENOMEM;
    }
    if (options->enable_phoenix) {
        void *host_ptr = nullptr;
        if (phxfs_regmem(options->device_id, dev_buf, total_mem, &host_ptr) != 0) {
            std::cerr << "phxfs_regmem failed" << std::endl;
            cudaFree(dev_buf);
            dev_buf = nullptr;
            data->buf_size = 0;
            return -ENOMEM;
        }
        host_buf = host_ptr;
    } else {
        err = cudaMallocHost(&host_buf, total_mem);
        if (err != cudaSuccess) {
            std::cerr << "cudaMallocHost failed: " << cudaGetErrorString(err) << std::endl;
            cudaFree(dev_buf);
            dev_buf = nullptr;
            data->buf_size = 0;
            return -ENOMEM;
        }
        if (options->enable_cufile) {
            CUfileError_t status = cuFileBufRegister(dev_buf, total_mem, 0);
            if (status.err != CU_FILE_SUCCESS) {
                std::cerr << "cuFileBufRegister failed: " << status.err << std::endl;
                cudaFreeHost(host_buf);
                host_buf = nullptr;
                cudaFree(dev_buf);
                dev_buf = nullptr;
                data->buf_size = 0;
                return -ENOMEM;
            }
            data->cufile_buffer_registered = true;
        }
    }

    td->orig_buffer = (char *)host_buf;
    return 0;
}

static void phoenix_buffer_free(struct thread_data *td) {
    auto option = (struct phoenix_options *)td->eo;
    auto data = static_cast<phoenix_data *>(td->io_ops_data);
    auto &dev_buf = data->dev_buf;
    auto &host_buf = data->host_buf;

    if (option->enable_phoenix && driver_inited && dev_buf && host_buf && data->buf_size) {
        phxfs_deregmem(option->device_id, dev_buf, data->buf_size);
    } else if (option->enable_cufile && data->cufile_buffer_registered && dev_buf) {
        cuFileBufDeregister(dev_buf);
        data->cufile_buffer_registered = false;
    }

    if (!option->enable_phoenix && host_buf) {
        cudaFreeHost(host_buf);
    }
    if (dev_buf) {
        cudaFree(dev_buf);
    }
    host_buf = nullptr;
    dev_buf = nullptr;
    data->buf_size = 0;
    td->orig_buffer = nullptr;
}

static int phoenix_invalidate(struct thread_data *td, struct fio_file *f) {
    return 0;
}


extern "C" {
struct ioengine_ops ioengine = {
    .name               = "phoenix_ioengine",
    .version            = FIO_IOOPS_VERSION,
    .flags               = FIO_NODISKUTIL,
    .init               = phoenix_init,
    .queue              = phoenix_queue,
    .commit             = phoenix_commit,
    .getevents          = phoenix_getevents,
    .event              = phoenix_event,
    .cleanup            = phoenix_cleanup,
    .open_file           = phoenix_file_open,
    .close_file          = phoenix_file_close,
    .invalidate         = phoenix_invalidate,
    .get_file_size       = generic_get_file_size,
    .iomem_alloc        = phoenix_buffer_alloc,
    .iomem_free         = phoenix_buffer_free,
    .option_struct_size = sizeof(struct phoenix_options),
    .options            = options,
};

void get_ioengine(struct ioengine_ops **ioengine_ptr) {
    *ioengine_ptr = &ioengine;
}

}
