#pragma once
// Column index constants matching quant_platform/core/constants.py exactly.
// C++ writer skips TradingDay and Code (string columns), writing only numeric columns.
// Python reader reconstructs TradingDay and Code from filename and trading day.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <cmath>

namespace quant::native_mdl::schema {

// Numeric column counts (Python TICK/ORDER/DEAL_COLUMNS minus TradingDay and Code)
constexpr std::size_t kTickCols  = 79;  // 81 - 2
constexpr std::size_t kOrderCols = 9;   // 11 - 2
constexpr std::size_t kDealCols  = 10;  // 12 - 2

// ── Tick column indices (TICK_COLUMNS[2:81]) ──────────────────────
namespace tick {
    constexpr std::size_t Time           = 0;   // seconds since midnight
    constexpr std::size_t UpdateTime     = 1;   // same as Time
    constexpr std::size_t CurrentPrice   = 2;
    constexpr std::size_t TotalVolume    = 3;
    constexpr std::size_t TotalMoney     = 4;
    constexpr std::size_t PreClosePrice  = 5;
    constexpr std::size_t OpenPrice      = 6;
    constexpr std::size_t HighestPrice   = 7;
    constexpr std::size_t LowestPrice    = 8;
    constexpr std::size_t HighLimitPrice = 9;
    constexpr std::size_t LowLimitPrice  = 10;
    constexpr std::size_t IOPV           = 11;
    constexpr std::size_t TradeNum       = 12;
    constexpr std::size_t TotalBidVolume = 13;
    constexpr std::size_t TotalAskVolume = 14;
    constexpr std::size_t AvgBidPrice    = 15;
    constexpr std::size_t AvgAskPrice    = 16;
    // AskPrice1-10  = 17..26
    // AskVolume1-10 = 27..36
    // AskNum1-10    = 37..46
    // BidPrice1-10  = 47..56
    // BidVolume1-10 = 57..66
    // BidNum1-10    = 67..76
    constexpr std::size_t AskPrice1   = 17;
    constexpr std::size_t AskVolume1  = 27;
    constexpr std::size_t AskNum1     = 37;
    constexpr std::size_t BidPrice1   = 47;
    constexpr std::size_t BidVolume1  = 57;
    constexpr std::size_t BidNum1     = 67;
    constexpr std::size_t Channel     = 77;
    constexpr std::size_t SeqNum      = 78;
}

// ── Order column indices (ORDER_COLUMNS[2:11]) ────────────────────
namespace order {
    constexpr std::size_t Time      = 0;
    constexpr std::size_t UpdateTime = 1;
    constexpr std::size_t OrderID   = 2;
    constexpr std::size_t Side      = 3;
    constexpr std::size_t Price     = 4;
    constexpr std::size_t Volume    = 5;
    constexpr std::size_t OrderType = 6;
    constexpr std::size_t Channel   = 7;
    constexpr std::size_t SeqNum    = 8;
}

// ── Deal column indices (DEAL_COLUMNS[2:12]) ──────────────────────
namespace deal {
    constexpr std::size_t Time       = 0;
    constexpr std::size_t UpdateTime  = 1;
    constexpr std::size_t SaleOrderID = 2;
    constexpr std::size_t BuyOrderID  = 3;
    constexpr std::size_t Side       = 4;
    constexpr std::size_t Price      = 5;
    constexpr std::size_t Volume     = 6;
    constexpr std::size_t Money      = 7;
    constexpr std::size_t Channel    = 8;
    constexpr std::size_t SeqNum     = 9;
}

// ── Helper: convert MDLTime (u32 hhmmssmmm) to seconds since midnight ──
inline double mdl_time_to_seconds(std::uint32_t t) {
    double h  = (t / 10000000u) % 100;
    double m  = (t / 100000u)   % 100;
    double s  = (t / 1000u)     % 100;
    double ms = t % 1000;
    return h * 3600.0 + m * 60.0 + s + ms / 1000.0;
}

// ── Helper: MDLFloatT<dec> → double ──
// MDLFloatT stores int32_t; sentinel INT32_MIN → 0.0; else divide by 10^dec
inline double mdl_float_to_f64(std::int32_t raw, std::uint32_t dec) {
    if (raw == std::numeric_limits<std::int32_t>::min()) return 0.0;
    return static_cast<double>(raw) / std::pow(10.0, static_cast<double>(dec));
}

// ── Helper: MDLDoubleT<dec> → double ──
// MDLDoubleT stores int64_t; sentinel INT64_MIN → 0.0; else divide by 10^dec
inline double mdl_double_to_f64(std::int64_t raw, std::uint32_t dec) {
    if (raw == std::numeric_limits<std::int64_t>::min()) return 0.0;
    return static_cast<double>(raw) / std::pow(10.0, static_cast<double>(dec));
}

// ── Helper: format code to "600000.XSHG" style ──
inline std::string format_code(const std::string& raw, const char* market) {
    std::string code = raw;
    // Trim trailing whitespace
    while (!code.empty() && (code.back() == ' ' || code.back() == '\0')) code.pop_back();
    // Left-pad to 6 chars with spaces (will be parsed by Python reader)
    if (code.size() < 6) {
        code = std::string(6 - code.size(), '0') + code;
    }
    return code + "." + market;
}

// ── Helper: stock + index filter ──
// Accept A-share stocks and indices; reject ETF / funds / bonds / convertible bonds.
// Rationale: 接收全证券会把 ETF/可转债/国债等灌进来,导致磁盘和 shm 写爆。
// 指数数据(用于 IDX_CONS_CODES 000300/000905/000852/000985/932000)需要保留。
inline bool is_stock_or_index_sh(const char* code, std::size_t len) {
    if (len == 0) return false;
    // Trim leading whitespace / null
    while (len > 0 && (*code == ' ' || *code == '\0')) { ++code; --len; }
    if (len < 3) return false;
    char c0 = code[0], c1 = code[1], c2 = code[2];
    // 6: 600/601/603/605/688/689 stocks
    if (c0 == '6') return true;
    // 9: 900 B股 / 999 old indices
    if (c0 == '9') return true;
    // 000: indices (上证综指 000001 / 沪深300 000300 / 中证500 000905 / 中证1000 000852 / 中证全指 000985 / 中证2000 932000 not here)
    if (c0 == '0' && c1 == '0' && c2 == '0') return true;
    // 880: 行业指数
    if (c0 == '8' && c1 == '8' && c2 == '0') return true;
    // REJECT: 5xx (ETF 510-529/588/基金 500-550/LOF 560-569), 1xx (bonds 110-127), 019 (国债)
    return false;
}

inline bool is_stock_or_index_sz(const char* code, std::size_t len) {
    if (len == 0) return false;
    while (len > 0 && (*code == ' ' || *code == '\0')) { ++code; --len; }
    if (len == 0) return false;
    char c0 = *code;
    // 0: 000/001/002/003 stocks
    // 3: 300/301 stocks + 399 indices
    // REJECT: 1xx (ETF 159 / 可转债 123 / 分级 150 / LOF 16x/184), 2xx (国债逆回购 204)
    return c0 == '0' || c0 == '3';
}

} // namespace quant::native_mdl::schema
