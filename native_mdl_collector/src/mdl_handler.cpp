#include "mdl_handler.h"
#include "schema.h"

#include <mdl_api.h>
#include <mdl_api_msg.h>
#include <mdl_shl2_msg.h>
#include <mdl_szl2_msg.h>

#include <iostream>
#include <cstring>
#include <cstdio>
#include <ctime>

using namespace datayes::mdl;

namespace quant::native_mdl {

using namespace datayes::mdl;
using namespace schema;

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

    // Recv wall time (seconds since midnight)
    struct timespec rts;
    clock_gettime(CLOCK_REALTIME, &rts);
    struct tm rtm;
    localtime_r(&rts.tv_sec, &rtm);
    double recv_sec = rtm.tm_hour * 3600.0 + rtm.tm_min * 60.0 + rtm.tm_sec + rts.tv_nsec / 1e9;

    check_seq(head->ServiceID, mid, seq);

    if (mid == mdl_shl2_msg::SHL2MarketData::MessageID) {
        // SH tick (MID=4)
        auto result = parse_sh_tick(body, body_size, static_cast<std::int64_t>(seq), recv_sec);
        if (result.valid) {
            writer_.append_tick(result.code, result.row);
            _sample_internal_latency("tick", recv_sec);
            tick_count_.fetch_add(1, std::memory_order_relaxed);
            _sample_push_delay("tick", result.code, result.row[tick::Time], recv_sec);
        }
    } else if (mid == mdl_shl2_msg::NGTSTick::MessageID) {
        // SH NGTS (MID=24) → order +/or deal
        auto result = parse_sh_ngts(body, body_size, recv_sec);
        if (!result.code.empty()) {
            if (result.has_order) {
                writer_.append_order(result.code, result.order.row);
                _sample_internal_latency("order", recv_sec);
                order_count_.fetch_add(1, std::memory_order_relaxed);
                _sample_push_delay("order", result.code, result.order.row[order::Time], recv_sec);
            }
            if (result.has_deal) {
                writer_.append_deal(result.code, result.deal.row);
                _sample_internal_latency("deal", recv_sec);
                deal_count_.fetch_add(1, std::memory_order_relaxed);
                _sample_push_delay("deal", result.code, result.deal.row[deal::Time], recv_sec);
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

    // Recv wall time (seconds since midnight)
    struct timespec rts;
    clock_gettime(CLOCK_REALTIME, &rts);
    struct tm rtm;
    localtime_r(&rts.tv_sec, &rtm);
    double recv_sec = rtm.tm_hour * 3600.0 + rtm.tm_min * 60.0 + rtm.tm_sec + rts.tv_nsec / 1e9;

    check_seq(head->ServiceID, mid, seq);

    if (mid == mdl_szl2_msg::Snapshot300111_v2::MessageID) {
        // SZ tick (MID=28)
        auto result = parse_sz_tick(body, body_size, static_cast<std::int64_t>(seq), recv_sec);
        if (result.valid) {
            writer_.append_tick(result.code, result.row);
            _sample_internal_latency("tick", recv_sec);
            tick_count_.fetch_add(1, std::memory_order_relaxed);
            _sample_push_delay("tick", result.code, result.row[tick::Time], recv_sec);
        }
    } else if (mid == mdl_szl2_msg::Order300192_v2::MessageID) {
        // SZ order (MID=33)
        auto result = parse_sz_order(body, body_size, recv_sec);
        if (result.valid) {
            writer_.append_order(result.code, result.row);
            _sample_internal_latency("order", recv_sec);
            order_count_.fetch_add(1, std::memory_order_relaxed);
            _sample_push_delay("order", result.code, result.row[order::Time], recv_sec);
        }
    } else if (mid == mdl_szl2_msg::Transaction300191_v2::MessageID) {
        // SZ deal (MID=36)
        auto result = parse_sz_deal(body, body_size, recv_sec);
        if (result.valid) {
            writer_.append_deal(result.code, result.row);
            _sample_internal_latency("deal", recv_sec);
            deal_count_.fetch_add(1, std::memory_order_relaxed);
            _sample_push_delay("deal", result.code, result.row[deal::Time], recv_sec);
        }
    }

    msg_count_.fetch_add(1, std::memory_order_relaxed);
}

int MdlHandler::_kind_index(const char* kind) {
    // Bucket layout: 0=tick, 1=order, 2=deal. Unknown kinds → -1 (ignored).
    if (kind[0] == 't' && kind[1] == 'i') return 0;  // tick
    if (kind[0] == 'o' && kind[1] == 'r') return 1;  // order
    if (kind[0] == 'd' && kind[1] == 'e') return 2;  // deal
    return -1;
}

static const char* const _kind_names_[3] = {"tick", "order", "deal"};

void MdlHandler::_emit_bucket_(DelayBucket& b, const char* kind_name, const char* prefix) {
    // Emit a completed bucket and reset it. Caller holds delay_mutex_.
    if (b.count <= 0 || b.minute < 0) return;
    long hh = b.minute / 60;
    long mm = b.minute % 60;
    double avg_ms = b.sum_ms / static_cast<double>(b.count);
    fprintf(stderr,
            "[%s] %02ld:%02ld %s n=%lld min=%.0fms avg=%.0fms max=%.0fms\n",
            prefix, hh, mm, kind_name, b.count, b.min_ms, avg_ms, b.max_ms);
    b.reset(-1);
}

void MdlHandler::_sample_push_delay(const char* kind, const std::string& code, double exch_sec, double recv_sec) {
    // Filter implausible samples (clock skew, pre-open timestamps) so they
    // don't pollute the per-minute stats. Negative or huge delays indicate
    // recv/exch clocks are not comparable for this row.
    double delay_ms = (recv_sec - exch_sec) * 1000.0;
    if (delay_ms < 0.0 || delay_ms > 600000.0) {
        return;
    }

    int ki = _kind_index(kind);
    if (ki < 0) return;

    long recv_minute = static_cast<long>(recv_sec) / 60;  // integer minute-of-day

    std::lock_guard<std::mutex> lock(delay_mutex_);
    DelayBucket& b = delay_buckets_[ki];
    if (b.minute != recv_minute) {
        // Minute rolled over. Emit the previous minute now so its stats are not
        // lost — the metrics thread polls every 10s and is not aligned to the
        // minute boundary, so relying on it alone would drop the last minute.
        _emit_bucket_(b, _kind_names_[ki], "push-latency");
        b.reset(recv_minute);
    }
    if (b.count == 0) {
        b.min_ms = b.max_ms = delay_ms;
    } else {
        if (delay_ms < b.min_ms) b.min_ms = delay_ms;
        if (delay_ms > b.max_ms) b.max_ms = delay_ms;
    }
    b.count++;
    b.sum_ms += delay_ms;
}

void MdlHandler::_sample_internal_latency(const char* kind, double recv_sec) {
    // C++ internal processing time = parse + SHM write, measured from callback
    // entry (recv_sec) to just after writer_.append_* (now). This is
    // independent of Tonglian source delay and shows our own overhead.
    struct timespec rts;
    clock_gettime(CLOCK_REALTIME, &rts);
    struct tm rtm;
    localtime_r(&rts.tv_sec, &rtm);
    double post_shm_sec = rtm.tm_hour * 3600.0 + rtm.tm_min * 60.0 + rtm.tm_sec + rts.tv_nsec / 1e9;

    double internal_ms = (post_shm_sec - recv_sec) * 1000.0;
    // Negative means clock skew (shouldn't happen, same clock); huge values are
    // impossible for a single callback — filter them out defensively.
    if (internal_ms < 0.0 || internal_ms > 60000.0) {
        return;
    }

    int ki = _kind_index(kind);
    if (ki < 0) return;

    long recv_minute = static_cast<long>(recv_sec) / 60;

    std::lock_guard<std::mutex> lock(delay_mutex_);
    DelayBucket& b = internal_buckets_[ki];
    if (b.minute != recv_minute) {
        _emit_bucket_(b, _kind_names_[ki], "internal-proc");
        b.reset(recv_minute);
    }
    if (b.count == 0) {
        b.min_ms = b.max_ms = internal_ms;
    } else {
        if (internal_ms < b.min_ms) b.min_ms = internal_ms;
        if (internal_ms > b.max_ms) b.max_ms = internal_ms;
    }
    b.count++;
    b.sum_ms += internal_ms;
}

void MdlHandler::flush_push_delay(bool force) {
    // Emit any bucket whose minute has fully elapsed (current minute > bucket
    // minute). Buckets for the still-accumulating current minute are kept,
    // unless force=true (shutdown) which emits everything.
    long now_minute = -1;
    {
        struct timespec rts;
        clock_gettime(CLOCK_REALTIME, &rts);
        struct tm rtm;
        localtime_r(&rts.tv_sec, &rtm);
        long now_sec = rtm.tm_hour * 3600L + rtm.tm_min * 60L + rtm.tm_sec;
        now_minute = now_sec / 60;
    }

    std::lock_guard<std::mutex> lock(delay_mutex_);
    for (int ki = 0; ki < 3; ++ki) {
        DelayBucket& b = delay_buckets_[ki];
        if (b.count == 0 || b.minute < 0) continue;
        if (!force && b.minute >= now_minute) continue;  // minute still in progress
        _emit_bucket_(b, _kind_names_[ki], "push-latency");
    }
    for (int ki = 0; ki < 3; ++ki) {
        DelayBucket& b = internal_buckets_[ki];
        if (b.count == 0 || b.minute < 0) continue;
        if (!force && b.minute >= now_minute) continue;
        _emit_bucket_(b, _kind_names_[ki], "internal-proc");
    }
}

} // namespace quant::native_mdl
