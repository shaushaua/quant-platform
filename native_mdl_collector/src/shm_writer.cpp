#include "shm_writer.h"
#include "schema.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <filesystem>
#include <iostream>

namespace quant::native_mdl {

using namespace schema;

// ── Atomic helpers for generation field ──────────────────────────────

static inline std::uint64_t atomic_load_u64(const void* addr) {
    std::uint64_t val;
    __atomic_load(reinterpret_cast<const std::uint64_t*>(addr), &val, __ATOMIC_ACQUIRE);
    return val;
}

static inline void atomic_store_u64(void* addr, std::uint64_t val) {
    __atomic_store(reinterpret_cast<std::uint64_t*>(addr), &val, __ATOMIC_RELEASE);
}

static inline std::uint64_t atomic_add_u64(void* addr, std::uint64_t delta) {
    return __atomic_fetch_add(reinterpret_cast<std::uint64_t*>(addr), delta, __ATOMIC_ACQ_REL);
}

// ── StockMmap ───────────────────────────────────────────────────────

StockMmap::StockMmap(const std::string& path, DataKind kind,
                     std::size_t capacity, std::size_t n_cols,
                     std::uint64_t trading_day)
    : path_(path), kind_(kind), capacity_(capacity), n_cols_(n_cols),
      trading_day_(trading_day), row_count_(0) {
    open_or_create();
}

StockMmap::~StockMmap() {
    close_mapping();
}

void StockMmap::open_or_create() {
    namespace fs = std::filesystem;
    bool exists = fs::exists(path_);
    std::size_t required_size = kShmHeaderBytes + capacity_ * n_cols_ * sizeof(double);

    if (exists) {
        // Check if existing file has compatible header
        auto fsize = fs::file_size(path_);
        int fd = ::open(path_.c_str(), O_RDWR, 0644);
        if (fd >= 0 && fsize >= kShmHeaderBytes) {
            void* map = ::mmap(nullptr, fsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (map != MAP_FAILED) {
                auto* hdr = reinterpret_cast<ShmHeader*>(map);
                if (hdr->magic == kShmMagic && hdr->version == kShmVersion &&
                    static_cast<DataKind>(hdr->kind) == kind_ &&
                    hdr->n_cols == n_cols_ &&
                    (trading_day_ == 0 || hdr->trading_day == trading_day_) &&
                    fsize >= required_size) {
                    // Reuse existing file (same trading day, or trading_day_ == 0)
                    fd_ = fd;
                    base_ = reinterpret_cast<std::uint8_t*>(map);
                    file_size_ = fsize;
                    row_count_ = static_cast<std::size_t>(hdr->row_count);
                    auto gen = atomic_load_u64(&hdr->generation);
                    if (gen % 2 == 1) {
                        atomic_store_u64(&hdr->generation, gen + 1);
                        ::msync(hdr, kShmHeaderBytes, MS_SYNC);
                        std::cerr << "[shm] recovered odd generation " << gen
                                  << " -> " << (gen + 1)
                                  << " for " << path_ << "\n";
                    }
                    return;
                }
                ::munmap(map, fsize);
            }
            ::close(fd);
        }
        // Incompatible, corrupt, or different trading day — recreate
        fs::remove(path_);
    }

    // Create new file
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) {
        std::cerr << "[shm] ERROR: cannot create " << path_ << ": " << strerror(errno) << "\n";
        return;
    }
    if (::ftruncate(fd_, static_cast<off_t>(required_size)) < 0) {
        std::cerr << "[shm] ERROR: ftruncate " << path_ << ": " << strerror(errno) << "\n";
        ::close(fd_);
        fd_ = -1;
        return;
    }
    base_ = reinterpret_cast<std::uint8_t*>(
        ::mmap(nullptr, required_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    if (base_ == MAP_FAILED) {
        std::cerr << "[shm] ERROR: mmap " << path_ << ": " << strerror(errno) << "\n";
        ::close(fd_);
        fd_ = -1;
        base_ = nullptr;
        return;
    }
    file_size_ = required_size;

    // Initialize header
    auto* hdr = reinterpret_cast<ShmHeader*>(base_);
    std::memset(hdr, 0, kShmHeaderBytes);
    hdr->magic     = kShmMagic;
    hdr->version   = kShmVersion;
    hdr->kind      = static_cast<std::uint64_t>(kind_);
    hdr->capacity  = capacity_;
    hdr->n_cols    = n_cols_;
    hdr->row_count = 0;
    hdr->trading_day = trading_day_;
    hdr->generation = 0;
    row_count_ = 0;
}

void StockMmap::close_mapping() {
    if (base_) {
        ::munmap(base_, file_size_);
        base_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool StockMmap::append(const double* row) {
    if (!base_ || fd_ < 0) return true;  // no-op, not a drop

    std::lock_guard<std::mutex> lock(append_mutex_);

    if (row_count_ >= capacity_) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
        return false;  // dropped
    }

    auto* hdr = reinterpret_cast<ShmHeader*>(base_);

    // Generation: odd = writing
    atomic_add_u64(&hdr->generation, 1);

    // Copy row data
    std::size_t data_offset = kShmHeaderBytes + row_count_ * n_cols_ * sizeof(double);
    std::memcpy(base_ + data_offset, row, n_cols_ * sizeof(double));

    // Update row count
    row_count_++;
    hdr->row_count = row_count_;

    // Generation: even = committed
    atomic_add_u64(&hdr->generation, 1);
    return true;
}

// ── ShmWriter ───────────────────────────────────────────────────────

ShmWriter::ShmWriter(std::string root, std::size_t tick_capacity,
                     std::size_t order_capacity, std::size_t deal_capacity,
                     std::uint64_t trading_day)
    : root_(std::move(root))
    , tick_capacity_(tick_capacity)
    , order_capacity_(order_capacity)
    , deal_capacity_(deal_capacity)
    , trading_day_(trading_day) {
    // Ensure directory exists
    std::filesystem::create_directories(root_);
}

std::string ShmWriter::path_for(DataKind kind, const std::string& code) const {
    // code = "600000.XSHG" → "600000_XSHG"
    std::string safe_code = code;
    std::replace(safe_code.begin(), safe_code.end(), '.', '_');

    const char* kind_str = "";
    switch (kind) {
        case DataKind::Tick:  kind_str = "tick";  break;
        case DataKind::Order: kind_str = "order"; break;
        case DataKind::Deal:  kind_str = "deal";  break;
    }
    return root_ + "/quant_" + kind_str + "_" + safe_code + ".mmap";
}

StockMmap& ShmWriter::get_buffer(DataKind kind, const std::string& code, std::size_t n_cols) {
    // Key = kind_code (e.g., "tick_600000_XSHG")
    const char* kind_str = "";
    std::size_t capacity = 0;
    switch (kind) {
        case DataKind::Tick:  kind_str = "tick";  capacity = tick_capacity_;  break;
        case DataKind::Order: kind_str = "order"; capacity = order_capacity_; break;
        case DataKind::Deal:  kind_str = "deal";  capacity = deal_capacity_;  break;
    }

    std::string key = std::string(kind_str) + "_" + code;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buffers_.find(key);
    if (it != buffers_.end()) {
        return *it->second;
    }

    auto path = path_for(kind, code);
    auto buf = std::make_unique<StockMmap>(path, kind, capacity, n_cols, trading_day_);
    auto& ref = *buf;
    buffers_.emplace(std::move(key), std::move(buf));
    return ref;
}

void ShmWriter::append_tick(const std::string& code, const std::vector<double>& row) {
    auto& buf = get_buffer(DataKind::Tick, code, schema::kTickCols);
    if (!buf.append(row.data())) total_dropped_.fetch_add(1, std::memory_order_relaxed);
}

void ShmWriter::append_order(const std::string& code, const std::vector<double>& row) {
    auto& buf = get_buffer(DataKind::Order, code, schema::kOrderCols);
    if (!buf.append(row.data())) total_dropped_.fetch_add(1, std::memory_order_relaxed);
}

void ShmWriter::append_deal(const std::string& code, const std::vector<double>& row) {
    auto& buf = get_buffer(DataKind::Deal, code, schema::kDealCols);
    if (!buf.append(row.data())) total_dropped_.fetch_add(1, std::memory_order_relaxed);
}

} // namespace quant::native_mdl
