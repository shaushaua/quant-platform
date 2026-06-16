#include "mdl_handler.h"
#include "shm_writer.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <thread>
#include <csignal>

namespace quant::native_mdl {

// Global flag for graceful shutdown
static std::atomic<bool> g_running{true};

static void signal_handler(int sig) {
    std::cerr << "\n[metrics] received signal " << sig << ", shutting down...\n";
    g_running.store(false);
}

void install_signal_handlers() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
}

bool is_running() {
    return g_running.load(std::memory_order_relaxed);
}

void metrics_loop(MdlHandler& handler, int interval_secs) {
    auto last_time = std::chrono::steady_clock::now();
    std::uint64_t last_msgs   = handler.msg_count();
    std::uint64_t last_ticks  = handler.tick_count();
    std::uint64_t last_orders = handler.order_count();
    std::uint64_t last_deals  = handler.deal_count();

    while (is_running()) {
        std::this_thread::sleep_for(std::chrono::seconds(interval_secs));
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - last_time).count();

        std::uint64_t cur_msgs   = handler.msg_count();
        std::uint64_t cur_ticks  = handler.tick_count();
        std::uint64_t cur_orders = handler.order_count();
        std::uint64_t cur_deals  = handler.deal_count();
        std::uint64_t cur_errors = handler.error_count();
        std::uint64_t cur_gaps   = handler.seq_gaps();
        std::uint64_t cur_dropped = handler.dropped();

        // Flush any fully-elapsed per-minute push-delay buckets.
        handler.flush_push_delay();

        double msg_rate   = (cur_msgs - last_msgs) / elapsed;
        double tick_rate  = (cur_ticks - last_ticks) / elapsed;
        double order_rate = (cur_orders - last_orders) / elapsed;
        double deal_rate  = (cur_deals - last_deals) / elapsed;

        // Timestamp
        auto t = std::time(nullptr);
        char ts[32];
        std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", std::localtime(&t));

        std::cout << "[" << ts << "] "
                  << "msg=" << cur_msgs << " (" << std::fixed << std::setprecision(0)
                  << msg_rate << "/s) "
                  << "tick=" << cur_ticks << " (" << tick_rate << "/s) "
                  << "order=" << cur_orders << " (" << order_rate << "/s) "
                  << "deal=" << cur_deals << " (" << deal_rate << "/s)"
                  << " gaps=" << cur_gaps
                  << " errors=" << cur_errors
                  << " dropped=" << cur_dropped
                  << "\n";

        last_time = now;
        last_msgs   = cur_msgs;
        last_ticks  = cur_ticks;
        last_orders = cur_orders;
        last_deals  = cur_deals;
    }
}

} // namespace quant::native_mdl
