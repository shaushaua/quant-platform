#pragma once

#include "shm_writer.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace quant::native_mdl {

// Result of parsing a single message into one row
struct ParseResult {
    std::string code;           // "600000.XSHG" format
    DataKind    kind;
    std::vector<double> row;    // n_cols float64 values
    bool        valid = false;
};

// Result of parsing an NGTS message (may produce order, deal, or both)
struct NgtsResult {
    std::string code;
    ParseResult order;
    ParseResult deal;
    bool has_order = false;
    bool has_deal  = false;
};

// Parse SH tick (MID=4, SHL2MarketData)
// msg_data: pointer to message body (cast from MDLMessage->GetBody())
// msg_len: body size in bytes
// seq_id: sequence ID from message header
ParseResult parse_sh_tick(const void* msg_data, std::size_t msg_len,
                          std::int64_t seq_id, double recv_sec);

// Parse SZ tick (MID=28, Snapshot300111_v2)
ParseResult parse_sz_tick(const void* msg_data, std::size_t msg_len,
                          std::int64_t seq_id, double recv_sec);

// Parse SH NGTS (MID=24, NGTSTick) -> may produce order and/or deal
NgtsResult parse_sh_ngts(const void* msg_data, std::size_t msg_len,
                         double recv_sec);

// Parse SZ order (MID=33, Order300192_v2)
ParseResult parse_sz_order(const void* msg_data, std::size_t msg_len,
                           double recv_sec);

// Parse SZ deal (MID=36, Transaction300191_v2)
ParseResult parse_sz_deal(const void* msg_data, std::size_t msg_len,
                          double recv_sec);

// ── Post-close (盘后定价交易) parsers ──────────────────────────────

// SH 盘后定价行情 (MID=16, ATPMarketData) → tick
// 字段比 SHL2MarketData 少（无 OpenPrice/HighPrice/LowPrice/涨跌停/IOPV），缺的列填 0
ParseResult parse_sh_atp_tick(const void* msg_data, std::size_t msg_len,
                              std::int64_t seq_id, double recv_sec);

// SH 盘后定价逐笔成交 (MID=17, ATPTransaction) → deal
ParseResult parse_sh_atp_deal(const void* msg_data, std::size_t msg_len,
                              double recv_sec);

// SZ 盘后定价行情 (MID=31, Snapshot300611_v2) → tick
// 字段比 Snapshot300111_v2 少（无 LastPrice/OpenPrice/HighPrice/涨跌停/IOPV/Avg），缺的列填 0
ParseResult parse_sz_post_close_tick(const void* msg_data, std::size_t msg_len,
                                     std::int64_t seq_id, double recv_sec);

} // namespace quant::native_mdl
