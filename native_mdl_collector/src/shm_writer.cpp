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
#include <cstdio>

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

const char* ShmWriter::kind_name(DataKind kind) {
    switch (kind) {
        case DataKind::Tick:  return "tick";
        case DataKind::Order: return "order";
        case DataKind::Deal:  return "deal";
    }
    return "unknown";
}

std::string ShmWriter::path_for(DataKind kind, const std::string& code, std::size_t part) const {
    // code = "600000.XSHG" → "600000_XSHG"
    std::string safe_code = code;
    std::replace(safe_code.begin(), safe_code.end(), '.', '_');

    std::string base = root_ + "/quant_" + kind_name(kind) + "_" + safe_code;
    if (part == 0) {
        return base + ".mmap";
    }
    char suffix[32];
    std::snprintf(suffix, sizeof(suffix), "_part%03zu.mmap", part);
    return base + suffix;
}

std::size_t ShmWriter::latest_existing_part(DataKind kind, const std::string& code) const {
    namespace fs = std::filesystem;
    std::string safe_code = code;
    std::replace(safe_code.begin(), safe_code.end(), '.', '_');

    const std::string legacy_name = std::string("quant_") + kind_name(kind) + "_" + safe_code + ".mmap";
    const std::string prefix = std::string("quant_") + kind_name(kind) + "_" + safe_code + "_part";
    std::size_t latest = 0;
    bool found = false;
    fs::path root_path(root_);
    if (!fs::exists(root_path)) {
        return 0;
    }
    for (const auto& entry : fs::directory_iterator(root_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::string name = entry.path().filename().string();
        if (name == legacy_name) {
            found = true;
            latest = std::max<std::size_t>(latest, 0);
            continue;
        }
        if (name.rfind(prefix, 0) == 0 && name.size() == prefix.size() + 8 &&
            name.substr(name.size() - 5) == ".mmap") {
            try {
                std::size_t part = static_cast<std::size_t>(
                    std::stoul(name.substr(prefix.size(), 3)));
                found = true;
                latest = std::max(latest, part);
            } catch (...) {
            }
        }
    }
    return found ? latest : 0;
}

ShmWriter::BufferSlot ShmWriter::create_buffer_slot(
        DataKind kind, const std::string& code, std::size_t n_cols, std::size_t part) const {
    BufferSlot slot;
    slot.part = part;
    slot.mmap = std::make_unique<StockMmap>(
        path_for(kind, code, part),
        kind,
        kind == DataKind::Tick ? tick_capacity_ : (kind == DataKind::Order ? order_capacity_ : deal_capacity_),
        n_cols,
        trading_day_);
    return slot;
}

StockMmap& ShmWriter::get_buffer(DataKind kind, const std::string& code, std::size_t n_cols) {
    // Key = kind_code (e.g., "tick_600000_XSHG")
    std::string key = std::string(kind_name(kind)) + "_" + code;

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buffers_.find(key);
    if (it != buffers_.end()) {
        return *it->second.mmap;
    }

    // If the collector process restarts after an overflow file was already
    // created, resume from the latest part instead of reopening part0.  This
    // keeps the naming scheme append-only across process restarts.
    std::size_t part = latest_existing_part(kind, code);
    auto slot = create_buffer_slot(kind, code, n_cols, part);
    auto& ref = *slot.mmap;
    buffers_.emplace(std::move(key), std::move(slot));
    return ref;
}

bool ShmWriter::append_with_overflow(
        DataKind kind, const std::string& code,
        const std::vector<double>& row, std::size_t n_cols) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto& buf = get_buffer(kind, code, n_cols);
        if (buf.append(row.data())) {
            return true;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        std::string key = std::string(kind_name(kind)) + "_" + code;
        auto it = buffers_.find(key);
        if (it == buffers_.end()) {
            continue;
        }
        if (!it->second.mmap->full()) {
            continue;
        }
        // New stocks can hit the fixed per-code mmap capacity before close.
        // Rolling to quant_<kind>_<code>_partNNN.mmap preserves every row while
        // keeping the original part0 filename backward-compatible.
        std::size_t next_part = it->second.part + 1;
        std::cerr << "[shm] rollover kind=" << kind_name(kind)
                  << " code=" << code
                  << " part=" << it->second.part
                  << " rows=" << it->second.mmap->row_count()
                  << " capacity=" << it->second.mmap->capacity()
                  << " next_part=" << next_part << "\n";
        it->second = create_buffer_slot(kind, code, n_cols, next_part);
    }
    total_dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void ShmWriter::append_tick(const std::string& code, const std::vector<double>& row) {
    append_with_overflow(DataKind::Tick, code, row, schema::kTickCols);
}

void ShmWriter::append_order(const std::string& code, const std::vector<double>& row) {
    append_with_overflow(DataKind::Order, code, row, schema::kOrderCols);
}

void ShmWriter::append_deal(const std::string& code, const std::vector<double>& row) {
    append_with_overflow(DataKind::Deal, code, row, schema::kDealCols);
}

} // namespace quant::native_mdl
