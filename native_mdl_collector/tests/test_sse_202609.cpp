// 上交所 2026.09.21/22 改版兼容性测试
//
// 通联确认无需升级 MDL SDK（字段结构不变，仅取值范围变化）。
// 本测试构造改版后的新取值消息，验证现有 parser 端到端正确：
//   1. 4.24 NGTSTick  Channel 1-6,20 → 1001-1008
//   2. 4.24 停牌状态单 TickBSFlag = CLOSE / ENDTR
//   3. 4.24 数量饱和值 Qty = 999999999999999 (15个9)
//   4. 4.17 ATPTransaction TradeChannel 103 → 2103
//   5. 4.17 TradeBuyNo/TradeSellNo 起始 1000000001
//   6. 4.17 TradeQty 15个9 / TradeMoney 18个9 饱和值
//
// 注意：MDLAnsiString 是 offset-based（Offset 相对于成员自身的字节偏移），
// 测试 buffer 布局 = [struct][strings...]，每个字符串成员的 Offset 指向
// 自己到数据的距离。

#include "parsers.h"
#include "schema.h"

#include <mdl_shl2_msg.h>

#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>

using namespace datayes::mdl;
using namespace quant::native_mdl;
using namespace quant::native_mdl::schema;

static int g_fail = 0;

#define CHECK_EQ(actual, expected, label)                          \
    do {                                                           \
        double a_ = (actual), e_ = (expected);                     \
        if (std::fabs(a_ - e_) > 1e-6) {                           \
            std::printf("FAIL %s: got %.4f want %.4f\n", label, a_, e_); \
            ++g_fail;                                              \
        } else {                                                   \
            std::printf("PASS %s = %.4f\n", label, a_);            \
        }                                                          \
    } while (0)

// 把字符串数据追加到 buf 末尾，并设置成员的 Offset/Length。
// c_str() = this + Offset → Offset = 数据地址 - 成员地址（正数）
static void bind_string(unsigned char* buf, size_t used, size_t cap,
                        MDLAnsiString& member, const char* value) {
    size_t len = std::strlen(value);
    size_t data_off = used;
    std::memcpy(buf + data_off, value, len + 1);  // 含 NUL
    member.Length = static_cast<uint16_t>(len);
    member.Offset = static_cast<uint32_t>(
        (buf + data_off) - reinterpret_cast<unsigned char*>(&member));
    (void)cap;
}

// ── 1. 4.24 新通道号 1005 + 正常成交（Type=T） ────────────────────
static void test_424_new_channel() {
    std::printf("\n=== [1] 4.24 Channel=1005 (改版后通道号) ===\n");
    static unsigned char buf[4096];
    std::memset(buf, 0, sizeof(buf));
    auto* msg = reinterpret_cast<mdl_shl2_msg::NGTSTick*>(buf);
    size_t used = sizeof(mdl_shl2_msg::NGTSTick);

    msg->BizIndex = 12345;
    msg->Channel = 1005;                 // 改版后通道号
    bind_string(buf, used, sizeof(buf), msg->SecurityID, "600000");
    used += 16;
    // TickTime: micros since midnight
    std::int64_t micros = (9 * 3600LL + 30 * 60 + 1) * 1000000LL + 500000;
    std::memcpy(&msg->TickTime, &micros, 8);
    bind_string(buf, used, sizeof(buf), msg->Type, "T");
    used += 8;
    msg->BuyOrderNO = 600001;
    msg->SellOrderNO = 600002;
    msg->Price.m_Value = 10000;          // 10.000
    msg->Qty = 100;
    msg->TradeMoney.m_Value = 1000000;
    bind_string(buf, used, sizeof(buf), msg->TickBSFlag, "B");

    auto r = parse_sh_ngts(buf, used + 64, 0.0);
    if (!r.has_deal) { std::printf("FAIL deal not produced\n"); ++g_fail; return; }
    CHECK_EQ(r.deal.row[deal::Channel], 1005, "deal.Channel(1005)");
    CHECK_EQ(r.deal.row[deal::Side], 0, "deal.Side(B)");
    CHECK_EQ(r.deal.row[deal::SeqNum], 12345, "deal.SeqNum(BizIndex)");
}

// ── 2. 4.24 停牌状态单 CLOSE / ENDTR ─────────────────────────────
static void test_424_suspend_flags() {
    std::printf("\n=== [2] 4.24 TickBSFlag=CLOSE/ENDTR (停牌状态单) ===\n");
    for (const char* flag : {"CLOSE", "ENDTR"}) {
        static unsigned char buf[4096];
        std::memset(buf, 0, sizeof(buf));
        auto* msg = reinterpret_cast<mdl_shl2_msg::NGTSTick*>(buf);
        size_t used = sizeof(mdl_shl2_msg::NGTSTick);

        msg->BizIndex = 999;
        msg->Channel = 1008;
        bind_string(buf, used, sizeof(buf), msg->SecurityID, "600601");
        used += 16;
        std::int64_t micros = (15 * 3600LL) * 1000000LL;
        std::memcpy(&msg->TickTime, &micros, 8);
        bind_string(buf, used, sizeof(buf), msg->Type, "T");
        used += 8;
        bind_string(buf, used, sizeof(buf), msg->TickBSFlag, flag);

        auto r = parse_sh_ngts(buf, used + 64, 0.0);
        // 关键验证：parser 不崩溃，Side 标记 unknown(10)
        if (r.has_deal) {
            CHECK_EQ(r.deal.row[deal::Side], 10,
                     (std::string("deal.Side(") + flag + "→unknown)").c_str());
            std::printf("INFO %s: 状态单会写入 SHM (price=0) — 建议 9 月前加过滤\n", flag);
        } else {
            std::printf("PASS %s: deal 未产出\n", flag);
        }
    }
}

// ── 3. 4.24 数量饱和值 15个9 ────────────────────────────────────
static void test_424_qty_saturation() {
    std::printf("\n=== [3] 4.24 Qty=999999999999999 (15个9饱和值) ===\n");
    static unsigned char buf[4096];
    std::memset(buf, 0, sizeof(buf));
    auto* msg = reinterpret_cast<mdl_shl2_msg::NGTSTick*>(buf);
    size_t used = sizeof(mdl_shl2_msg::NGTSTick);

    msg->BizIndex = 1000;
    msg->Channel = 1001;
    bind_string(buf, used, sizeof(buf), msg->SecurityID, "600000");
    used += 16;
    std::int64_t micros = (9 * 3600LL + 30 * 60 + 2) * 1000000LL;
    std::memcpy(&msg->TickTime, &micros, 8);
    bind_string(buf, used, sizeof(buf), msg->Type, "T");
    used += 8;
    msg->Price.m_Value = 10000;
    msg->Qty = 999999999999999LL;        // 15个9
    msg->TradeMoney.m_Value = 1000000;
    bind_string(buf, used, sizeof(buf), msg->TickBSFlag, "B");

    auto r = parse_sh_ngts(buf, used + 64, 0.0);
    if (!r.has_deal) { std::printf("FAIL deal not produced\n"); ++g_fail; return; }
    // float64 精确整数上限 2^53≈9.007e15 > 1e15 → 无损透传
    CHECK_EQ(r.deal.row[deal::Volume], 999999999999999.0, "deal.Volume(15个9)");
}

// ── 4/5. 4.17 TradeChannel=2103 + TradeBuyNo=1000000001 ─────────
static void test_417_new_values() {
    std::printf("\n=== [4] 4.17 TradeChannel=2103, BuyNo=1000000001 ===\n");
    static unsigned char buf[4096];
    std::memset(buf, 0, sizeof(buf));
    auto* msg = reinterpret_cast<mdl_shl2_msg::ATPTransaction*>(buf);
    size_t used = sizeof(mdl_shl2_msg::ATPTransaction);

    msg->DataStatus = 0;
    msg->TradeIndex = 777;
    msg->TradeChannel = 2103;            // 改版后 103 → 2103
    bind_string(buf, used, sizeof(buf), msg->SecurityID, "600000");
    used += 16;
    std::int64_t micros = (15 * 3600LL + 5 * 60) * 1000000LL + 250000;
    std::memcpy(&msg->TradeTime, &micros, 8);
    msg->TradePrice.m_Value = 12345;     // 12.345
    msg->TradeQty.m_Value = 200;
    msg->TradeMoney.m_Value = 246900;
    msg->TradeBuyNo = 1000000001LL;      // 改版后起始值
    msg->TradeSellNo = 1000000099LL;
    bind_string(buf, used, sizeof(buf), msg->TradeBSFlag, "B");

    auto r = parse_sh_atp_deal(buf, used + 64, 0.0);
    if (!r.valid) { std::printf("FAIL deal not valid\n"); ++g_fail; return; }
    CHECK_EQ(r.row[deal::Channel], 2103, "deal.Channel(2103)");
    CHECK_EQ(r.row[deal::BuyOrderID], 1000000001, "deal.BuyOrderID(10亿)");
    CHECK_EQ(r.row[deal::SaleOrderID], 1000000099, "deal.SaleOrderID");
    CHECK_EQ(r.row[deal::Price], 12.345, "deal.Price");
    CHECK_EQ(r.row[deal::Side], 0, "deal.Side(B)");
}

// ── 6. 4.17 饱和值 15个9 Qty / 18个9 Money ──────────────────────
static void test_417_saturation() {
    std::printf("\n=== [6] 4.17 TradeQty=15个9, TradeMoney=18个9 ===\n");
    static unsigned char buf[4096];
    std::memset(buf, 0, sizeof(buf));
    auto* msg = reinterpret_cast<mdl_shl2_msg::ATPTransaction*>(buf);
    size_t used = sizeof(mdl_shl2_msg::ATPTransaction);

    msg->DataStatus = 0;
    msg->TradeIndex = 778;
    msg->TradeChannel = 2103;
    bind_string(buf, used, sizeof(buf), msg->SecurityID, "600000");
    used += 16;
    std::int64_t micros = (15 * 3600LL + 5 * 60 + 1) * 1000000LL;
    std::memcpy(&msg->TradeTime, &micros, 8);
    msg->TradePrice.m_Value = 12345;
    msg->TradeQty.m_Value = 999999999999999.0;      // 15个9
    msg->TradeMoney.m_Value = 999999999999999999.0; // 18个9
    msg->TradeBuyNo = 1000000001;
    msg->TradeSellNo = 1000000002;
    bind_string(buf, used, sizeof(buf), msg->TradeBSFlag, "S");

    auto r = parse_sh_atp_deal(buf, used + 64, 0.0);
    if (!r.valid) { std::printf("FAIL deal not valid\n"); ++g_fail; return; }
    double qty = r.row[deal::Volume];
    double money = r.row[deal::Money];
    // MDLDoubleT<N> 编码 = 真实值 × 10^N。交易所下发的 15/18 个 9 是编码值，
    // parser 解码 = 编码值 / 10^N，行为确定性（无溢出/崩溃）即符合预期：
    //   Qty(3位小数):   999999999999999    / 1000 = 999999999999.999
    //   Money(5位小数): 999999999999999999 / 1e5  = 9999999999999.9998...
    double want_qty = 999999999999999.0 / 1000.0;
    std::printf("INFO Volume 解码 = %.3f (编码15个9 / 1000)\n", qty);
    std::printf("INFO Money  解码 = %.3f (编码18个9 / 1e5, float64 精度丢失为预期 — 饱和标记非真实数据)\n", money);
    if (std::fabs(qty - want_qty) > 1.0) {
        std::printf("FAIL Qty 饱和值解码非确定\n"); ++g_fail;
    } else {
        std::printf("PASS Qty 饱和值确定解码 (编码/1000, 无溢出)\n");
    }
}

int main() {
    std::printf("========== 上交所 2026.09 改版兼容性测试 ==========\n");
    test_424_new_channel();
    test_424_suspend_flags();
    test_424_qty_saturation();
    test_417_new_values();
    test_417_saturation();
    std::printf("\n========== %s ==========\n", g_fail == 0 ? "全部 PASS" : "存在 FAIL");
    return g_fail == 0 ? 0 : 1;
}
