#include "parsers.h"
#include "schema.h"

// MDL SDK headers — access typed struct fields directly
#include <mdl_shl2_msg.h>
#include <mdl_api_types.h>

#include <cstring>

using namespace datayes::mdl;

namespace quant::native_mdl {

using namespace schema;

// ── SH Tick (MID=4, SHL2MarketData) ─────────────────────────────────
//
// Maps mdl_shl2_msg::SHL2MarketData → 79-col float64 row.
// Native parser for SHL2MarketData tick messages.
//
// SHL2MarketData field → schema column mapping:
//   UpdateTime      → tick::Time (seconds), tick::UpdateTime
//   LastPrice       → tick::CurrentPrice
//   TradVolume      → tick::TotalVolume
//   Turnover        → tick::TotalMoney
//   PreCloPrice     → tick::PreClosePrice
//   OpenPrice       → tick::OpenPrice
//   HighPrice       → tick::HighestPrice
//   LowPrice        → tick::LowestPrice
//   IOPV            → tick::IOPV
//   TradNumber      → tick::TradeNum
//   TotalBidVol     → tick::TotalBidVolume
//   TotalAskVol     → tick::TotalAskVolume
//   WAvgBidPri      → tick::AvgBidPrice
//   WAvgAskPri      → tick::AvgAskPrice
//   BidLevels[i]    → BidPrice/BidVolume/BidNum 1-10
//   SellLevels[i]   → AskPrice/AskVolume/AskNum 1-10
//   (no HighLimit/LowLimit in SH) → 0.0
//   Channel → 0

ParseResult parse_sh_tick(const void* msg_data, std::size_t msg_len, std::int64_t seq_id, double recv_sec) {
    ParseResult result;
    result.kind = DataKind::Tick;
    result.row.resize(kTickCols, 0.0);

    if (msg_len < sizeof(mdl_shl2_msg::SHL2MarketData)) {
        return result;
    }

    const auto* msg = reinterpret_cast<const mdl_shl2_msg::SHL2MarketData*>(msg_data);

    // Filter: stocks + indices only (reject ETF / funds / bonds)
    const char* code_raw = msg->SecurityID.c_str();
    auto code_len = msg->SecurityID.Length;
    if (!is_stock_or_index_sh(code_raw, code_len)) {
        return result;
    }

    // Format code
    result.code = format_code(std::string(code_raw, code_len), "XSHG");

    // Time
    double time_sec = mdl_time_to_seconds(msg->UpdateTime.m_Value);
    result.row[tick::Time]       = time_sec;
    result.row[tick::UpdateTime] = recv_sec;

    // Scalar fields
    result.row[tick::CurrentPrice]   = mdl_float_to_f64(msg->LastPrice.m_Value, 3);
    result.row[tick::TotalVolume]    = mdl_double_to_f64(msg->TradVolume.m_Value, 3);
    result.row[tick::TotalMoney]     = mdl_double_to_f64(msg->Turnover.m_Value, 5);
    result.row[tick::PreClosePrice]  = mdl_float_to_f64(msg->PreCloPrice.m_Value, 3);
    result.row[tick::OpenPrice]      = mdl_float_to_f64(msg->OpenPrice.m_Value, 3);
    result.row[tick::HighestPrice]   = mdl_float_to_f64(msg->HighPrice.m_Value, 3);
    result.row[tick::LowestPrice]    = mdl_float_to_f64(msg->LowPrice.m_Value, 3);
    // HighLimit/LowLimit not available in SH tick
    result.row[tick::HighLimitPrice] = 0.0;
    result.row[tick::LowLimitPrice]  = 0.0;
    result.row[tick::IOPV]           = mdl_float_to_f64(msg->IOPV.m_Value, 3);
    result.row[tick::TradeNum]       = static_cast<double>(msg->TradNumber);
    result.row[tick::TotalBidVolume] = mdl_double_to_f64(msg->TotalBidVol.m_Value, 3);
    result.row[tick::TotalAskVolume] = mdl_double_to_f64(msg->TotalAskVol.m_Value, 3);
    result.row[tick::AvgBidPrice]    = mdl_float_to_f64(msg->WAvgBidPri.m_Value, 3);
    result.row[tick::AvgAskPrice]    = mdl_float_to_f64(msg->WAvgAskPri.m_Value, 3);

    // Bid levels
    std::size_t bid_len = msg->BidLevels.Length;
    for (std::size_t i = 0; i < 10 && i < bid_len; ++i) {
        const auto& item = *msg->BidLevels[i];
        result.row[tick::BidPrice1 + i]  = mdl_float_to_f64(item.OrderPrice.m_Value, 3);
        result.row[tick::BidVolume1 + i] = mdl_double_to_f64(item.OrderVol.m_Value, 3);
        result.row[tick::BidNum1 + i]    = static_cast<double>(item.OrderNum);
    }

    // Ask levels (SellLevels in SDK)
    std::size_t ask_len = msg->SellLevels.Length;
    for (std::size_t i = 0; i < 10 && i < ask_len; ++i) {
        const auto& item = *msg->SellLevels[i];
        result.row[tick::AskPrice1 + i]  = mdl_float_to_f64(item.OrderPrice.m_Value, 3);
        result.row[tick::AskVolume1 + i] = mdl_double_to_f64(item.OrderVol.m_Value, 3);
        result.row[tick::AskNum1 + i]    = static_cast<double>(item.OrderNum);
    }

    result.row[tick::Channel] = 0.0;
    result.row[tick::SeqNum]  = static_cast<double>(seq_id);

    result.valid = true;
    return result;
}

// ── SH NGTS (MID=24, NGTSTick) ──────────────────────────────────────
//
// Maps mdl_shl2_msg::NGTSTick → order row and/or deal row.
// Native parser for SHL2Transaction/ngts order-deal messages.
//
// Type field:
//   "A" or "D" → order (add/delete)
//   "T"        → deal (trade)
//
// TickBSFlag: "B"→0 (buy), "S"→1 (sell), else→10

NgtsResult parse_sh_ngts(const void* msg_data, std::size_t msg_len, double recv_sec) {
    NgtsResult result;

    if (msg_len < sizeof(mdl_shl2_msg::NGTSTick)) {
        return result;
    }

    const auto* msg = reinterpret_cast<const mdl_shl2_msg::NGTSTick*>(msg_data);

    // Filter: stocks + indices only
    const char* code_raw = msg->SecurityID.c_str();
    auto code_len = msg->SecurityID.Length;
    if (!is_stock_or_index_sh(code_raw, code_len)) {
        return result;
    }
    result.code = format_code(std::string(code_raw, code_len), "XSHG");

    double time_sec = mdl_time_to_seconds(msg->TickTime.m_Value);
    double price    = mdl_float_to_f64(msg->Price.m_Value, 3);
    double qty      = static_cast<double>(msg->Qty);
    double money    = mdl_double_to_f64(msg->TradeMoney.m_Value, 3);
    std::int64_t channel = static_cast<std::int64_t>(msg->Channel);
    std::int64_t biz_idx = msg->BizIndex;

    // Parse TickBSFlag
    const char* flag_raw = msg->TickBSFlag.c_str();
    auto flag_len = msg->TickBSFlag.Length;
    std::int64_t side = 10; // unknown
    if (flag_len > 0) {
        if (flag_raw[0] == 'B') side = 0;
        else if (flag_raw[0] == 'S') side = 1;
    }

    // Parse Type
    const char* typ = msg->Type.c_str();
    auto typ_len = msg->Type.Length;

    std::int64_t buy_no  = msg->BuyOrderNO;
    std::int64_t sell_no = msg->SellOrderNO;

    // Order: Type == "A" (add) or "D" (delete)
    if (typ_len > 0 && (typ[0] == 'A' || typ[0] == 'D')) {
        result.has_order = true;
        result.order.code = result.code;
        result.order.kind = DataKind::Order;
        result.order.row.resize(kOrderCols, 0.0);
        result.order.valid = true;

        result.order.row[order::Time]      = time_sec;
        result.order.row[order::UpdateTime] = recv_sec;
        result.order.row[order::OrderID]   = static_cast<double>(buy_no + sell_no);
        result.order.row[order::Side]      = static_cast<double>(side);
        result.order.row[order::Price]     = price;
        result.order.row[order::Volume]    = qty;
        result.order.row[order::OrderType] = (typ[0] == 'A') ? 2.0 : 5.0;
        result.order.row[order::Channel]   = static_cast<double>(channel);
        result.order.row[order::SeqNum]    = static_cast<double>(biz_idx);
    }

    // Deal: Type == "T" (trade)
    if (typ_len > 0 && typ[0] == 'T') {
        result.has_deal = true;
        result.deal.code = result.code;
        result.deal.kind = DataKind::Deal;
        result.deal.row.resize(kDealCols, 0.0);
        result.deal.valid = true;

        double deal_money = (money != 0.0) ? money : (price * qty);

        result.deal.row[deal::Time]        = time_sec;
        result.deal.row[deal::UpdateTime]   = recv_sec;
        result.deal.row[deal::SaleOrderID]  = static_cast<double>(sell_no);
        result.deal.row[deal::BuyOrderID]   = static_cast<double>(buy_no);
        result.deal.row[deal::Side]         = static_cast<double>(side);
        result.deal.row[deal::Price]        = price;
        result.deal.row[deal::Volume]       = qty;
        result.deal.row[deal::Money]        = deal_money;
        result.deal.row[deal::Channel]      = static_cast<double>(channel);
        result.deal.row[deal::SeqNum]       = static_cast<double>(biz_idx);
    }

    return result;
}

} // namespace quant::native_mdl
