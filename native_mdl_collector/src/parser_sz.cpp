#include "parsers.h"
#include "schema.h"

// MDL SDK headers
#include <mdl_szl2_msg.h>
#include <mdl_api_types.h>

#include <cstring>

using namespace datayes::mdl;

namespace quant::native_mdl {

using namespace schema;

// ── SZ Tick (MID=28, Snapshot300111_v2) ──────────────────────────────
//
// Maps mdl_szl2_msg::Snapshot300111_v2 → 79-col float64 row.
// Native parser for SZ Snapshot300111_v2 tick messages.
//
// Key differences from SH tick:
//   - Price fields use MDLDoubleT<6> (÷1e6), not MDLFloatT<3> (÷1e3)
//   - BidPriceLevelItem: Volume(i64) at +0, Price(MDLDoubleT<6>) at +8
//   - Has HighLimitPrice and LowLimitPrice
//   - Has Channel (ChannelNo)
//   - TotalAskVolume = TotalOfferQty (i64, not MDLDoubleT)

ParseResult parse_sz_tick(const void* msg_data, std::size_t msg_len, std::int64_t seq_id, double recv_sec) {
    ParseResult result;
    result.kind = DataKind::Tick;
    result.row.resize(kTickCols, 0.0);

    if (msg_len < sizeof(mdl_szl2_msg::Snapshot300111_v2)) {
        return result;
    }

    const auto* msg = reinterpret_cast<const mdl_szl2_msg::Snapshot300111_v2*>(msg_data);

    // Filter: only stocks
    const char* code_raw = msg->SecurityID.c_str();
    auto code_len = msg->SecurityID.Length;
    if (!is_stock_sz(code_raw, code_len)) {
        return result;
    }
    result.code = format_code(std::string(code_raw, code_len), "XSHE");

    // Time
    double time_sec = mdl_time_to_seconds(msg->UpdateTime.m_Value);
    result.row[tick::Time]       = time_sec;
    result.row[tick::UpdateTime] = recv_sec;

    // Channel
    result.row[tick::Channel] = static_cast<double>(msg->ChannelNo);

    // Scalar fields (note: SZ uses MDLDoubleT<6> for prices, MDLDoubleT<4> for some)
    result.row[tick::PreClosePrice]  = mdl_double_to_f64(msg->PreCloPrice.m_Value, 4);
    result.row[tick::TradeNum]       = static_cast<double>(msg->TurnNum);
    result.row[tick::TotalVolume]    = static_cast<double>(msg->Volume);
    result.row[tick::TotalMoney]     = mdl_double_to_f64(msg->Turnover.m_Value, 4);
    result.row[tick::CurrentPrice]   = mdl_double_to_f64(msg->LastPrice.m_Value, 6);
    result.row[tick::OpenPrice]      = mdl_double_to_f64(msg->OpenPrice.m_Value, 6);
    result.row[tick::HighestPrice]   = mdl_double_to_f64(msg->HighPrice.m_Value, 6);
    result.row[tick::LowestPrice]    = mdl_double_to_f64(msg->LowPrice.m_Value, 6);
    result.row[tick::HighLimitPrice] = mdl_double_to_f64(msg->HighLimitPrice.m_Value, 6);
    result.row[tick::LowLimitPrice]  = mdl_double_to_f64(msg->LowLimitPrice.m_Value, 6);
    result.row[tick::IOPV]           = mdl_double_to_f64(msg->IOPV.m_Value, 6);
    result.row[tick::TotalAskVolume] = static_cast<double>(msg->TotalOfferQty);
    result.row[tick::AvgAskPrice]    = mdl_double_to_f64(msg->WeightedAvgOfferPx.m_Value, 6);
    result.row[tick::TotalBidVolume] = static_cast<double>(msg->TotalBidQty);
    result.row[tick::AvgBidPrice]    = mdl_double_to_f64(msg->WeightedAvgBidPx.m_Value, 6);

    // Bid price levels
    std::size_t bid_len = msg->BidPriceLevel.Length;
    for (std::size_t i = 0; i < 10 && i < bid_len; ++i) {
        const auto& item = *msg->BidPriceLevel[i];
        result.row[tick::BidVolume1 + i] = static_cast<double>(item.Volume);
        result.row[tick::BidPrice1 + i]  = mdl_double_to_f64(item.Price.m_Value, 6);
        result.row[tick::BidNum1 + i]    = static_cast<double>(item.NumOrders);
    }

    // Ask price levels
    std::size_t ask_len = msg->AskPriceLevel.Length;
    for (std::size_t i = 0; i < 10 && i < ask_len; ++i) {
        const auto& item = *msg->AskPriceLevel[i];
        result.row[tick::AskVolume1 + i] = static_cast<double>(item.Volume);
        result.row[tick::AskPrice1 + i]  = mdl_double_to_f64(item.Price.m_Value, 6);
        result.row[tick::AskNum1 + i]    = static_cast<double>(item.NumOrders);
    }

    result.row[tick::SeqNum] = static_cast<double>(seq_id);

    result.valid = true;
    return result;
}

// ── SZ Order (MID=33, Order300192_v2) ───────────────────────────────
//
// Maps mdl_szl2_msg::Order300192_v2 → 9-col float64 row.
// Native parser for SZ order messages.
//
// Side: 49('1')→0(buy), 50('2')→1(sell), else→10
// OrdType: 49→1(limit), 50→2(market), 85→3(best), else→0
// SeqNum = ApplSeqNum

ParseResult parse_sz_order(const void* msg_data, std::size_t msg_len, double recv_sec) {
    ParseResult result;
    result.kind = DataKind::Order;
    result.row.resize(kOrderCols, 0.0);

    if (msg_len < sizeof(mdl_szl2_msg::Order300192_v2)) {
        return result;
    }

    const auto* msg = reinterpret_cast<const mdl_szl2_msg::Order300192_v2*>(msg_data);

    // Filter: only stocks
    const char* code_raw = msg->SecurityID.c_str();
    auto code_len = msg->SecurityID.Length;
    if (!is_stock_sz(code_raw, code_len)) {
        return result;
    }
    result.code = format_code(std::string(code_raw, code_len), "XSHE");

    double time_sec = mdl_time_to_seconds(msg->TransactTime.m_Value);

    // Side mapping
    std::int64_t side = 10;
    switch (msg->Side) {
        case 49: side = 0; break;  // '1' → buy
        case 50: side = 1; break;  // '2' → sell
        default: side = 10; break;
    }

    // OrderType mapping
    std::int64_t ord_type = 0;
    switch (msg->OrdType) {
        case 49: ord_type = 1; break;  // '1' → limit
        case 50: ord_type = 2; break;  // '2' → market
        case 85: ord_type = 3; break;  // 'A' → best
        default: ord_type = 0; break;
    }

    result.row[order::Time]      = time_sec;
    result.row[order::UpdateTime] = recv_sec;
    result.row[order::OrderID]   = static_cast<double>(msg->ApplSeqNum);
    result.row[order::Side]      = static_cast<double>(side);
    result.row[order::Price]     = mdl_double_to_f64(msg->Price.m_Value, 4);
    result.row[order::Volume]    = static_cast<double>(msg->OrderQty);
    result.row[order::OrderType] = static_cast<double>(ord_type);
    result.row[order::Channel]   = static_cast<double>(msg->ChannelNo);
    result.row[order::SeqNum]    = static_cast<double>(msg->ApplSeqNum);

    result.valid = true;
    return result;
}

// ── SZ Deal (MID=36, Transaction300191_v2) ──────────────────────────
//
// Maps mdl_szl2_msg::Transaction300191_v2 → 10-col float64 row.
// Native parser for SZ deal messages.
//
// Side: buy_id > sell_id → 0(buy), else → 1(sell); ExecType==52 → 4
// Money = LastPx × LastQty
// SaleOrderID = OfferApplSeqNum, BuyOrderID = BidApplSeqNum

ParseResult parse_sz_deal(const void* msg_data, std::size_t msg_len, double recv_sec) {
    ParseResult result;
    result.kind = DataKind::Deal;
    result.row.resize(kDealCols, 0.0);

    if (msg_len < sizeof(mdl_szl2_msg::Transaction300191_v2)) {
        return result;
    }

    const auto* msg = reinterpret_cast<const mdl_szl2_msg::Transaction300191_v2*>(msg_data);

    // Filter: only stocks
    const char* code_raw = msg->SecurityID.c_str();
    auto code_len = msg->SecurityID.Length;
    if (!is_stock_sz(code_raw, code_len)) {
        return result;
    }
    result.code = format_code(std::string(code_raw, code_len), "XSHE");

    double time_sec = mdl_time_to_seconds(msg->TransactTime.m_Value);
    double last_px  = mdl_double_to_f64(msg->LastPx.m_Value, 4);
    double last_qty = static_cast<double>(msg->LastQty);

    std::int64_t buy_id  = msg->BidApplSeqNum;
    std::int64_t sell_id = msg->OfferApplSeqNum;

    // Side logic
    std::int64_t side = (buy_id > sell_id) ? 0 : 1;
    if (msg->ExecType == 52) side = 4;

    result.row[deal::Time]        = time_sec;
    result.row[deal::UpdateTime]   = recv_sec;
    result.row[deal::SaleOrderID]  = static_cast<double>(sell_id);
    result.row[deal::BuyOrderID]   = static_cast<double>(buy_id);
    result.row[deal::Side]         = static_cast<double>(side);
    result.row[deal::Price]        = last_px;
    result.row[deal::Volume]       = last_qty;
    result.row[deal::Money]        = last_px * last_qty;
    result.row[deal::Channel]      = static_cast<double>(msg->ChannelNo);
    result.row[deal::SeqNum]       = static_cast<double>(msg->ApplSeqNum);

    result.valid = true;
    return result;
}

} // namespace quant::native_mdl
