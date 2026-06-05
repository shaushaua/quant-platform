#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace quant::native_mdl {

constexpr std::uint64_t kShmMagic = 0x514d444c53484d31ULL; // QMDLSHM1
constexpr std::uint64_t kShmVersion = 2;  // v2: replaced write_pos with trading_day
constexpr std::size_t kShmHeaderBytes = 64;

enum class DataKind : std::uint64_t {
    Tick = 1,
    Order = 2,
    Deal = 3,
};

struct ShmHeader {
    std::uint64_t magic;
    std::uint64_t version;
    std::uint64_t kind;
    std::uint64_t capacity;
    std::uint64_t n_cols;
    std::uint64_t row_count;
    std::uint64_t trading_day;  // YYYYMMDD as uint64 (e.g. 20260605)
    std::uint64_t generation;
};

class StockMmap {
public:
    StockMmap(const std::string& path, DataKind kind, std::size_t capacity, std::size_t n_cols,
              std::uint64_t trading_day = 0);
    ~StockMmap();

    StockMmap(const StockMmap&) = delete;
    StockMmap& operator=(const StockMmap&) = delete;

    bool append(const double* row);  // returns false if dropped
    std::uint64_t row_count() const { return row_count_; }
    std::uint64_t dropped()   const { return dropped_.load(std::memory_order_relaxed); }

private:
    void open_or_create();
    void close_mapping();

    std::string path_;
    DataKind kind_;
    std::size_t capacity_;
    std::size_t n_cols_;
    std::uint64_t trading_day_;
    int fd_{-1};
    std::uint8_t* base_{nullptr};
    std::size_t file_size_{0};
    std::uint64_t row_count_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::mutex append_mutex_;  // protects row_count_ + memcpy
};

class ShmWriter {
public:
    ShmWriter(std::string root, std::size_t tick_capacity, std::size_t order_capacity, std::size_t deal_capacity,
              std::uint64_t trading_day = 0);

    void append_tick(const std::string& code, const std::vector<double>& row);
    void append_order(const std::string& code, const std::vector<double>& row);
    void append_deal(const std::string& code, const std::vector<double>& row);

    // Total rows dropped across all buffers (thread-safe)
    std::uint64_t total_dropped() const { return total_dropped_.load(std::memory_order_relaxed); }

private:
    StockMmap& get_buffer(DataKind kind, const std::string& code, std::size_t n_cols);
    std::string path_for(DataKind kind, const std::string& code) const;

    std::string root_;
    std::size_t tick_capacity_;
    std::size_t order_capacity_;
    std::size_t deal_capacity_;
    std::uint64_t trading_day_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<StockMmap>> buffers_;
    std::atomic<std::uint64_t> total_dropped_{0};
};

} // namespace quant::native_mdl
