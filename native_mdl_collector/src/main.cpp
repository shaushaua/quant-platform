// native-mdl-collector: C++ MDL SDK subscriber → parse → write mmap
// Replaces Python pymdl hot path for zero-GIL, zero-copy data collection.
//
// Environment variables:
//   MDL_SERVER           MDL server address (default: 127.0.0.1:9012)
//   MDL_TOKEN            MDL authentication token
//   NATIVE_SHM_DIR       mmap output directory (default: /data/quant/shm)
//   NATIVE_ROWS_PER_STOCK       default capacity per stock buffer (default: 200000)
//   NATIVE_TICK_ROWS_PER_STOCK  tick capacity per stock buffer
//   NATIVE_ORDER_ROWS_PER_STOCK order capacity per stock buffer
//   NATIVE_DEAL_ROWS_PER_STOCK  deal capacity per stock buffer
//   NATIVE_MDL_IO_THREADS  IO threads for MDL SDK (default: 4)
//   MDL_LOG_PATH         MDL SDK log directory (default: /data/quant/mdl_logs/mdl)

#include "shm_writer.h"
#include "mdl_handler.h"
#include "schema.h"

#include <mdl_api.h>
#include <mdl_shl2_msg.h>
#include <mdl_szl2_msg.h>

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <ctime>

using namespace datayes::mdl;

namespace quant::native_mdl {

// Forward declarations from metrics.cpp
extern void install_signal_handlers();
extern bool is_running();
extern void metrics_loop(MdlHandler& handler, int interval_secs);

} // namespace quant::native_mdl

using namespace quant::native_mdl;
using namespace quant::native_mdl::schema;

static std::string get_env(const char* name, const char* default_val) {
    const char* val = std::getenv(name);
    return val ? std::string(val) : std::string(default_val);
}

int main(int argc, char* argv[]) {
    // Parse config from environment
    std::string server     = get_env("MDL_SERVER", "127.0.0.1:9012");
    std::string token      = get_env("MDL_TOKEN", "");
    std::string shm_dir    = get_env("NATIVE_SHM_DIR", "/data/quant/shm");
    std::string mdl_log    = get_env("MDL_LOG_PATH", "/data/quant/mdl_logs/mdl");
    int rows_per_stock     = std::atoi(get_env("NATIVE_ROWS_PER_STOCK", "200000").c_str());
    std::string default_rows = std::to_string(rows_per_stock);
    int tick_rows          = std::atoi(get_env("NATIVE_TICK_ROWS_PER_STOCK", default_rows.c_str()).c_str());
    int order_rows         = std::atoi(get_env("NATIVE_ORDER_ROWS_PER_STOCK", default_rows.c_str()).c_str());
    int deal_rows          = std::atoi(get_env("NATIVE_DEAL_ROWS_PER_STOCK", default_rows.c_str()).c_str());
    int io_threads         = std::atoi(get_env("NATIVE_MDL_IO_THREADS", "4").c_str());

    if (token.empty()) {
        std::cerr << "[main] ERROR: MDL_TOKEN not set\n";
        return 1;
    }
    if (rows_per_stock <= 0) rows_per_stock = 200000;
    if (tick_rows <= 0) tick_rows = rows_per_stock;
    if (order_rows <= 0) order_rows = rows_per_stock;
    if (deal_rows <= 0) deal_rows = rows_per_stock;
    if (io_threads <= 0) io_threads = 4;

    std::cerr << "[main] config: server=" << server
              << " shm_dir=" << shm_dir
              << " tick_rows=" << tick_rows
              << " order_rows=" << order_rows
              << " deal_rows=" << deal_rows
              << " io_threads=" << io_threads << "\n";

    // Install signal handlers for graceful shutdown
    install_signal_handlers();

    // Determine trading day (YYYYMMDD) for SHM header
    std::time_t now = std::time(nullptr);
    std::tm* lt = std::localtime(&now);
    std::uint64_t trading_day = static_cast<std::uint64_t>(
        (lt->tm_year + 1900) * 10000 + (lt->tm_mon + 1) * 100 + lt->tm_mday);

    // Create ShmWriter
    ShmWriter writer(shm_dir, tick_rows, order_rows, deal_rows, trading_day);

    // Create handler
    MdlHandler handler(writer);

    // Create IOManager
    IOManagerPtr io_mgr = CreateIOManager(io_threads, io_threads);
    if (io_mgr.IsNull()) {
        std::cerr << "[main] ERROR: failed to create IOManager\n";
        return 1;
    }

    // Enable MDL SDK logging
    io_mgr->EnableLog(mdl_log.c_str(), false);

    // Create subscriber with multithreaded callbacks
    SubscriberPtr sub = io_mgr->CreateSubscriber(&handler, true);
    if (sub.IsNull()) {
        std::cerr << "[main] ERROR: failed to create Subscriber\n";
        return 1;
    }

    // Configure subscriber
    sub->SetServerAddress(server.c_str());
    sub->SetUserName(token.c_str());
    sub->SetMessageEncoding(MDLEID_MKTPRO);

    // Subscribe to the 5 message types
    sub->SubcribeMessage<mdl_shl2_msg::SHL2MarketData>();     // MID=4, SH tick
    sub->SubcribeMessage<mdl_shl2_msg::NGTSTick>();           // MID=24, SH NGTS (order+deal)
    sub->SubcribeMessage<mdl_szl2_msg::Snapshot300111_v2>();  // MID=28, SZ tick
    sub->SubcribeMessage<mdl_szl2_msg::Order300192_v2>();     // MID=33, SZ order
    sub->SubcribeMessage<mdl_szl2_msg::Transaction300191_v2>();// MID=36, SZ deal

    std::cerr << "[main] connecting to " << server << "...\n";

    // Connect
    const char* err = sub->Connect();
    if (err && std::strlen(err) > 0) {
        std::cerr << "[main] ERROR: connect failed: " << err << "\n";
        return 1;
    }

    std::cerr << "[main] connected. receiving data...\n";

    // Start metrics thread
    std::thread metrics_thread([&handler]() {
        metrics_loop(handler, 10);
    });

    // Wait for shutdown signal
    while (is_running()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cerr << "[main] shutting down...\n";

    // Cleanup
    io_mgr->Shutdown();
    metrics_thread.join();

    // Flush the last (in-progress) push-delay minute before reporting stats.
    handler.flush_push_delay(true);

    // Print final stats
    std::cerr << "[main] final stats: "
              << "msg=" << handler.msg_count()
              << " tick=" << handler.tick_count()
              << " order=" << handler.order_count()
              << " deal=" << handler.deal_count()
              << " gaps=" << handler.seq_gaps()
              << " dropped=" << handler.dropped()
              << "\n";

    return 0;
}
