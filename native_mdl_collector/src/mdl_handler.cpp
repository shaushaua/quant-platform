#include "mdl_handler.h"
#include "schema.h"

#include <mdl_api.h>
#include <mdl_api_msg.h>
#include <mdl_shl2_msg.h>
#include <mdl_szl2_msg.h>

#include <iostream>
#include <cstring>

using namespace datayes::mdl;

namespace quant::native_mdl {

using namespace datayes::mdl;

MdlHandler::MdlHandler(ShmWriter& writer) : writer_(writer) {}

void MdlHandler::check_seq(std::uint8_t sid, std::uint16_t mid, std::uint64_t seq) {
    std::lock_guard<std::mutex> lock(seq_mutex_);
    SeqKey key{sid, mid};
    auto it = last_seq_.find(key);
    if (it != last_seq_.end()) {
        std::uint64_t expected = it->second + 1;
        if (seq != expected) {
            std::uint64_t gap = (seq > expected) ? (seq - expected) : 0;
            seq_gaps_.fetch_add(gap, std::memory_order_relaxed);
            if (gap > 100) {
                std::cerr << "[handler] seq-gap sid=" << (int)sid << " mid=" << mid
                          << " expected=" << expected << " actual=" << seq
                          << " gap=" << gap << "\n";
            }
        }
    }
    last_seq_[key] = seq;
}

void MdlHandler::OnMDLAPIMessage(const datayes::mdl::MDLMessage* msg) {
    auto* head = msg->GetHead();
    if (head->MessageID == mdl_api_msg::MDLMID_MDL_API_DisconnectedEvent) {
        // Parse DisconnectedEvent body
        const char* body = msg->GetBody();
        auto body_size = msg->GetBodySize();
        if (body_size >= 6) {
            // MDLAnsiString ErrorMessage at offset 0
            std::uint16_t len = *reinterpret_cast<const std::uint16_t*>(body);
            std::uint32_t off = *reinterpret_cast<const std::uint32_t*>(body + 2);
            std::string err_msg;
            if (len > 0 && off + len <= body_size) {
                err_msg = std::string(body + off, len);
            }
            std::cerr << "[handler] disconnected: " << err_msg << "\n";
        }
    } else if (head->MessageID == mdl_api_msg::MDLMID_MDL_API_ConnectingEvent) {
        std::cerr << "[handler] connecting...\n";
    } else if (head->MessageID == mdl_api_msg::MDLMID_MDL_API_ConnectErrorEvent) {
        std::cerr << "[handler] connect error\n";
        error_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void MdlHandler::OnMDLSHL2Message(const datayes::mdl::MDLMessage* msg) {
    auto* head = msg->GetHead();
    std::uint16_t mid = head->MessageID;
    std::uint64_t seq = head->SequenceID;
    const char* body = msg->GetBody();
    auto body_size = msg->GetBodySize();

    check_seq(head->ServiceID, mid, seq);

    if (mid == mdl_shl2_msg::SHL2MarketData::MessageID) {
        // SH tick (MID=4)
        auto result = parse_sh_tick(body, body_size, static_cast<std::int64_t>(seq));
        if (result.valid) {
            writer_.append_tick(result.code, result.row);
            tick_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else if (mid == mdl_shl2_msg::NGTSTick::MessageID) {
        // SH NGTS (MID=24) → order +/or deal
        auto result = parse_sh_ngts(body, body_size);
        if (!result.code.empty()) {
            if (result.has_order) {
                writer_.append_order(result.code, result.order.row);
                order_count_.fetch_add(1, std::memory_order_relaxed);
            }
            if (result.has_deal) {
                writer_.append_deal(result.code, result.deal.row);
                deal_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    msg_count_.fetch_add(1, std::memory_order_relaxed);
}

void MdlHandler::OnMDLSZL2Message(const datayes::mdl::MDLMessage* msg) {
    auto* head = msg->GetHead();
    std::uint16_t mid = head->MessageID;
    std::uint64_t seq = head->SequenceID;
    const char* body = msg->GetBody();
    auto body_size = msg->GetBodySize();

    check_seq(head->ServiceID, mid, seq);

    if (mid == mdl_szl2_msg::Snapshot300111_v2::MessageID) {
        // SZ tick (MID=28)
        auto result = parse_sz_tick(body, body_size, static_cast<std::int64_t>(seq));
        if (result.valid) {
            writer_.append_tick(result.code, result.row);
            tick_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else if (mid == mdl_szl2_msg::Order300192_v2::MessageID) {
        // SZ order (MID=33)
        auto result = parse_sz_order(body, body_size);
        if (result.valid) {
            writer_.append_order(result.code, result.row);
            order_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else if (mid == mdl_szl2_msg::Transaction300191_v2::MessageID) {
        // SZ deal (MID=36)
        auto result = parse_sz_deal(body, body_size);
        if (result.valid) {
            writer_.append_deal(result.code, result.row);
            deal_count_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    msg_count_.fetch_add(1, std::memory_order_relaxed);
}

} // namespace quant::native_mdl
