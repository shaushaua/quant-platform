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

private:
    ShmWriter& writer_;

    // Counters
    std::atomic<std::uint64_t> msg_count_{0};
    std::atomic<std::uint64_t> tick_count_{0};
    std::atomic<std::uint64_t> order_count_{0};
    std::atomic<std::uint64_t> deal_count_{0};
    std::atomic<std::uint64_t> error_count_{0};
    std::atomic<std::uint64_t> seq_gaps_{0};

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
