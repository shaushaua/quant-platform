#pragma once

#include "shm_writer.h"
#include "parsers.h"

#include <mdl_api.h>
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>

namespace quant::native_mdl {

class MdlHandler : public datayes::mdl::MessageHandler {
public:
    MdlHandler(ShmWriter& writer);

    void OnMDLAPIMessage(const datayes::mdl::MDLMessage* msg) override;
    void OnMDLSHL2Message(const datayes::mdl::MDLMessage* msg) override;
    void OnMDLSZL2Message(const datayes::mdl::MDLMessage* msg) override;

    // Metrics accessors
    std::uint64_t msg_count()   const { return msg_count_.load(std::memory_order_relaxed); }
    std::uint64_t tick_count()  const { return tick_count_.load(std::memory_order_relaxed); }
    std::uint64_t order_count() const { return order_count_.load(std::memory_order_relaxed); }
    std::uint64_t deal_count()  const { return deal_count_.load(std::memory_order_relaxed); }
    std::uint64_t error_count() const { return error_count_.load(std::memory_order_relaxed); }
    std::uint64_t seq_gaps()    const { return seq_gaps_.load(std::memory_order_relaxed); }
    std::uint64_t dropped()     const { return writer_.total_dropped(); }

    // Flush completed per-minute push-delay buckets to stderr. Called by the
    // metrics thread; emits any minute that has fully elapsed and returns.
    // When force=true (e.g. at shutdown), also emits the in-progress minute.
    void flush_push_delay(bool force = false);

private:
    ShmWriter& writer_;

    // Counters
    std::atomic<std::uint64_t> msg_count_{0};
    std::atomic<std::uint64_t> tick_count_{0};
    std::atomic<std::uint64_t> order_count_{0};
    std::atomic<std::uint64_t> deal_count_{0};
    std::atomic<std::uint64_t> error_count_{0};
    std::atomic<std::uint64_t> seq_gaps_{0};
    std::atomic<std::uint64_t> push_sample_count_{0};

    void _sample_push_delay(const char* kind, const std::string& code, double exch_sec, double recv_sec);
    // Captures wall time NOW (just after writer_.append_*) and aggregates the
    // delta from recv_sec into internal_buckets_. Called immediately after the
    // SHM write so the measurement reflects parse+write cost for this row.
    void _sample_internal_latency(const char* kind, double recv_sec);

    // ── Per-minute push-delay aggregation ──────────────────────────────
    // Accumulates delay samples bucketed by the integer minute of recv_sec.
    // SH/SZ callbacks run concurrently (multithreaded subscriber), so all
    // access is guarded by delay_mutex_. flush_push_delay() emits and resets
    // a minute once it has fully elapsed.
    struct DelayBucket {
        long minute = -1;        // integer minute-of-day (HH*60+MM) of this bucket
        long long count = 0;
        double sum_ms = 0.0;
        double min_ms = 0.0;
        double max_ms = 0.0;
        void reset(long m) {
            minute = m; count = 0; sum_ms = 0.0; min_ms = 0.0; max_ms = 0.0;
        }
    };
    // One bucket per kind: index 0=tick, 1=order, 2=deal (see _kind_index).
    DelayBucket delay_buckets_[3];
    // Parallel buckets for internal C++ parse+SHM-write latency (post_shm-recv).
    DelayBucket internal_buckets_[3];
    std::mutex delay_mutex_;
    static int _kind_index(const char* kind);
    // Emit one bucket to stderr and reset it. NOT thread-safe — caller must
    // hold delay_mutex_. kind_name is the label printed in the log line;
    // prefix selects [push-latency] vs [internal-proc].
    void _emit_bucket_(DelayBucket& b, const char* kind_name, const char* prefix);

    // Per (serviceID, messageID) expected sequence tracking
    struct SeqKey {
        std::uint8_t sid;
        std::uint16_t mid;
        bool operator==(const SeqKey& o) const { return sid == o.sid && mid == o.mid; }
    };
    struct SeqKeyHash {
        std::size_t operator()(const SeqKey& k) const {
            return (static_cast<std::size_t>(k.sid) << 16) | k.mid;
        }
    };
    std::unordered_map<SeqKey, std::uint64_t, SeqKeyHash> last_seq_;
    std::mutex seq_mutex_;  // protects last_seq_ from concurrent callbacks

    void check_seq(std::uint8_t sid, std::uint16_t mid, std::uint64_t seq);
};

} // namespace quant::native_mdl
