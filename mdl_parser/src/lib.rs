use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::types::PyString;
use pyo3::Bound;

// ================================================================== //
// MDL binary parsing helpers                                          //
// ================================================================== //

#[inline]
fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
fn read_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
fn read_i64(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// MDLFloatT<N>: i32 / 10^N → f64
#[inline]
fn mdl_float(buf: &[u8], off: usize, dec: i32) -> f64 {
    let v = read_i32(buf, off);
    if v == i32::MIN { return 0.0; }
    v as f64 / 10i32.pow(dec as u32) as f64
}

/// MDLDoubleT<N>: i64 / 10^N → f64
#[inline]
fn mdl_double(buf: &[u8], off: usize, dec: i32) -> f64 {
    let v = read_i64(buf, off);
    if v == i64::MIN { return 0.0; }
    v as f64 / 10i64.pow(dec as u32) as f64
}

/// Read MDLAnsiString: {u16 Length, u32 Offset} at `meta_off`, string at base + Offset
/// Safe against garbage offsets: saturating arithmetic prevents panic.
#[inline]
fn read_string<'a>(buf: &'a [u8], meta_off: usize, base: usize) -> &'a str {
    let len = read_u16(buf, meta_off) as usize;
    if len == 0 { return ""; }
    let str_off = read_u32(buf, meta_off + 2) as usize;
    let start = base.saturating_add(str_off);
    let end = start.saturating_add(len);
    if end > buf.len() || start >= buf.len() || start > end {
        return "";
    }
    std::str::from_utf8(&buf[start..end]).unwrap_or("")
}

/// Parse MDLTime (u32 hhmmssmmm format) → "YYYYMMDD HH:MM:SS.mmm"
fn format_mdl_time(time_val: u32, trading_day: &str) -> String {
    let hour = (time_val / 10000000) % 100;
    let minute = (time_val / 100000) % 100;
    let second = (time_val / 1000) % 100;
    let millis = time_val % 1000;
    format!("{} {:02}:{:02}:{:02}.{:03}", trading_day, hour, minute, second, millis)
}

fn is_stock(code: &str, market: &str) -> bool {
    let c = code.trim();
    if market == "SH" { c.starts_with('6') || c.starts_with('9') }
    else { c.starts_with('0') || c.starts_with('3') }
}

fn format_code(raw: &str, market: &str) -> String {
    let code = format!("{:>06}", raw.trim());
    let suffix = if market == "SH" { ".XSHG" } else { ".XSHE" };
    format!("{}{}", code, suffix)
}

/// Helper: push a string element
fn push_str<'a>(elems: &mut Vec<Bound<'a, PyAny>>, py: Python<'a>, s: &str) {
    elems.push(PyString::new(py, s).into_any());
}

/// Helper: push a float element
fn push_f64<'a>(elems: &mut Vec<Bound<'a, PyAny>>, py: Python<'a>, v: f64) {
    elems.push(v.into_pyobject(py).unwrap().into_any());
}

/// Helper: push an int element
fn push_i64<'a>(elems: &mut Vec<Bound<'a, PyAny>>, py: Python<'a>, v: i64) {
    elems.push(v.into_pyobject(py).unwrap().into_any());
}

// ================================================================== //
// Parsed data structs (pure Rust, no GIL needed)                     //
// ================================================================== //

struct ParsedTick {
    code: String,
    time: String,
    current_price: f64,
    total_volume: f64,
    total_money: f64,
    pre_close: f64,
    open: f64,
    high: f64,
    low: f64,
    high_limit: f64,
    low_limit: f64,
    iopv: f64,
    trade_num: f64,
    total_bid_vol: f64,
    total_ask_vol: f64,
    avg_bid: f64,
    avg_ask: f64,
    ask_price: [f64; 10],
    ask_vol: [f64; 10],
    ask_num: [f64; 10],
    bid_price: [f64; 10],
    bid_vol: [f64; 10],
    bid_num: [f64; 10],
    channel: i64,
    seq_id: i64,
}

struct ParsedNgtsOrder {
    code: String,
    time: String,
    order_id: i64,
    side: i64,
    price: f64,
    qty: f64,
    order_type: i64,
    channel: i64,
    seq_id: i64,
}

struct ParsedNgtsDeal {
    code: String,
    time: String,
    sell_no: i64,
    buy_no: i64,
    side: i64,
    price: f64,
    qty: f64,
    money: f64,
    channel: i64,
    seq_id: i64,
}

struct ParsedNgts {
    code: String,
    order: Option<ParsedNgtsOrder>,
    deal: Option<ParsedNgtsDeal>,
}

struct ParsedSzOrder {
    code: String,
    time: String,
    appl_seq: i64,
    side: i64,
    price: f64,
    qty: f64,
    ord_type: i64,
    channel: i64,
}

struct ParsedSzDeal {
    code: String,
    time: String,
    sell_id: i64,
    buy_id: i64,
    side: i64,
    price: f64,
    qty: f64,
    money: f64,
    channel: i64,
    appl_seq: i64,
}

// ================================================================== //
// Pure Rust parsing (no GIL, runs in parallel)                       //
// ================================================================== //

fn parse_sh_tick_raw(buf: &[u8], trading_day: &str, seq_id: i64) -> Option<ParsedTick> {
    if buf.len() < 248 { return None; }

    let security_id = read_string(buf, 4, 4);
    if !is_stock(security_id, "SH") { return None; }
    let code = format_code(security_id, "SH");

    let update_time_raw = read_u32(buf, 0);
    let time = format_mdl_time(update_time_raw, trading_day);

    let pre_clo   = mdl_float(buf, 14, 3);
    let open      = mdl_float(buf, 18, 3);
    let high      = mdl_float(buf, 22, 3);
    let low       = mdl_float(buf, 26, 3);
    let last      = mdl_float(buf, 30, 3);
    let trad_num  = read_u32(buf, 44) as f64;
    let trad_vol  = mdl_double(buf, 48, 3);
    let turnover  = mdl_double(buf, 56, 5);
    let tot_bid   = mdl_double(buf, 64, 3);
    let avg_bid   = mdl_float(buf, 72, 3);
    let avg_ask   = mdl_float(buf, 88, 3);
    let iopv      = mdl_float(buf, 244, 3);
    let tot_ask   = read_i64(buf, 80) as f64;

    let bid_len  = read_u32(buf, 228) as usize;
    let bid_base = 228 + read_u32(buf, 232) as usize;
    let ask_len  = read_u32(buf, 236) as usize;
    let ask_base = 236 + read_u32(buf, 240) as usize;
    let item_sz: usize = 28;

    let mut ap = [0.0f64; 10]; let mut av = [0.0f64; 10]; let mut an = [0.0f64; 10];
    let mut bp = [0.0f64; 10]; let mut bv = [0.0f64; 10]; let mut bn = [0.0f64; 10];

    for i in 0..10 {
        if i < ask_len {
            let o = ask_base + i * item_sz;
            if o + item_sz <= buf.len() {
                ap[i] = mdl_float(buf, o + 4, 3);
                av[i] = mdl_double(buf, o + 8, 3);
                an[i] = read_u32(buf, o + 16) as f64;
            }
        }
        if i < bid_len {
            let o = bid_base + i * item_sz;
            if o + item_sz <= buf.len() {
                bp[i] = mdl_float(buf, o + 4, 3);
                bv[i] = mdl_double(buf, o + 8, 3);
                bn[i] = read_u32(buf, o + 16) as f64;
            }
        }
    }

    Some(ParsedTick {
        code, time, current_price: last, total_volume: trad_vol, total_money: turnover,
        pre_close: pre_clo, open, high, low, high_limit: 0.0, low_limit: 0.0,
        iopv, trade_num: trad_num, total_bid_vol: tot_bid, total_ask_vol: tot_ask,
        avg_bid, avg_ask, ask_price: ap, ask_vol: av, ask_num: an,
        bid_price: bp, bid_vol: bv, bid_num: bn, channel: 0, seq_id,
    })
}

fn parse_sz_tick_raw(buf: &[u8], trading_day: &str, seq_id: i64) -> Option<ParsedTick> {
    if buf.len() < 224 { return None; }

    let security_id = read_string(buf, 14, 14);
    if !is_stock(security_id, "SZ") { return None; }
    let code = format_code(security_id, "SZ");

    let time_raw = read_u32(buf, 0);
    let time = format_mdl_time(time_raw, trading_day);
    let channel = read_u32(buf, 4) as i64;

    let pre_clo       = mdl_double(buf, 32, 4);
    let turn_num      = read_i64(buf, 40) as f64;
    let volume        = read_i64(buf, 48) as f64;
    let turnover      = mdl_double(buf, 56, 4);
    let last          = mdl_double(buf, 64, 6);
    let open          = mdl_double(buf, 72, 6);
    let high          = mdl_double(buf, 80, 6);
    let low           = mdl_double(buf, 88, 6);
    let high_limit    = mdl_double(buf, 176, 6);
    let low_limit     = mdl_double(buf, 184, 6);
    let iopv          = mdl_double(buf, 136, 6);
    let tot_offer     = read_i64(buf, 144) as f64;
    let wavg_offer    = mdl_double(buf, 152, 6);
    let tot_bid       = read_i64(buf, 160) as f64;
    let wavg_bid      = mdl_double(buf, 168, 6);

    let bid_len  = read_u32(buf, 208) as usize;
    let bid_base = 208 + read_u32(buf, 212) as usize;
    let ask_len  = read_u32(buf, 216) as usize;
    let ask_base = 216 + read_u32(buf, 220) as usize;
    let item_sz: usize = 32;

    let mut ap = [0.0f64; 10]; let mut av = [0.0f64; 10]; let mut an = [0.0f64; 10];
    let mut bp = [0.0f64; 10]; let mut bv = [0.0f64; 10]; let mut bn = [0.0f64; 10];

    for i in 0..10 {
        if i < ask_len {
            let o = ask_base + i * item_sz;
            if o + item_sz <= buf.len() {
                av[i] = read_i64(buf, o) as f64;
                ap[i] = mdl_double(buf, o + 8, 6);
                an[i] = read_u32(buf, o + 16) as f64;
            }
        }
        if i < bid_len {
            let o = bid_base + i * item_sz;
            if o + item_sz <= buf.len() {
                bv[i] = read_i64(buf, o) as f64;
                bp[i] = mdl_double(buf, o + 8, 6);
                bn[i] = read_u32(buf, o + 16) as f64;
            }
        }
    }

    Some(ParsedTick {
        code, time, current_price: last, total_volume: volume, total_money: turnover,
        pre_close: pre_clo, open, high, low, high_limit, low_limit,
        iopv, trade_num: turn_num, total_bid_vol: tot_bid, total_ask_vol: tot_offer,
        avg_bid: wavg_bid, avg_ask: wavg_offer,
        ask_price: ap, ask_vol: av, ask_num: an,
        bid_price: bp, bid_vol: bv, bid_num: bn, channel, seq_id,
    })
}

fn parse_sh_ngts_raw(buf: &[u8], trading_day: &str) -> Option<ParsedNgts> {
    if buf.len() < 70 { return None; }

    let security_id = read_string(buf, 12, 12);
    if !is_stock(security_id, "SH") { return None; }
    let code = format_code(security_id, "SH");

    let tick_time = format_mdl_time(read_u32(buf, 18), trading_day);
    let typ = read_string(buf, 22, 22).trim();
    let buy_no   = read_i64(buf, 28);
    let sell_no  = read_i64(buf, 36);
    let price    = mdl_float(buf, 44, 3);
    let qty      = read_i64(buf, 48) as f64;
    let money    = mdl_double(buf, 56, 3);
    let channel  = read_i32(buf, 8) as i64;
    let biz_idx  = read_i64(buf, 0);

    let flag = read_string(buf, 64, 64).trim();
    let side = match flag {
        "B" => 0i64, "S" => 1i64, _ => 10i64,
    };

    let order = if typ == "A" || typ == "D" {
        Some(ParsedNgtsOrder {
            code: code.clone(),
            time: tick_time.clone(),
            order_id: buy_no + sell_no,
            side,
            price,
            qty,
            order_type: if typ == "A" { 2 } else { 5 },
            channel,
            seq_id: biz_idx,
        })
    } else {
        None
    };

    let deal = if typ == "T" {
        let m = if money != 0.0 { money } else { price * qty };
        Some(ParsedNgtsDeal {
            code: code.clone(),
            time: tick_time.clone(),
            sell_no,
            buy_no,
            side,
            price,
            qty,
            money: m,
            channel,
            seq_id: biz_idx,
        })
    } else {
        None
    };

    Some(ParsedNgts { code, order, deal })
}

fn parse_sz_order_raw(buf: &[u8], trading_day: &str, _seq_id: i64) -> Option<ParsedSzOrder> {
    if buf.len() < 58 { return None; }

    let security_id = read_string(buf, 18, 18);
    if !is_stock(security_id, "SZ") { return None; }
    let code = format_code(security_id, "SZ");

    let channel  = read_u32(buf, 0) as i64;
    let appl_seq = read_i64(buf, 4);
    let price    = mdl_double(buf, 30, 4);
    let qty      = read_i64(buf, 38) as f64;
    let side = match read_i32(buf, 46) {
        49 => 0i64, 50 => 1i64, _ => 10i64,
    };
    let event_time = format_mdl_time(read_u32(buf, 50), trading_day);
    let ord_type = match read_i32(buf, 54) {
        49 => 1i64, 50 => 2i64, 85 => 3i64, _ => 0i64,
    };

    Some(ParsedSzOrder { code, time: event_time, appl_seq, side, price, qty, ord_type, channel })
}

fn parse_sz_deal_raw(buf: &[u8], trading_day: &str, _seq_id: i64) -> Option<ParsedSzDeal> {
    if buf.len() < 70 { return None; }

    let security_id = read_string(buf, 34, 34);
    if !is_stock(security_id, "SZ") { return None; }
    let code = format_code(security_id, "SZ");

    let channel  = read_u32(buf, 0) as i64;
    let appl_seq = read_i64(buf, 4);
    let buy_id   = read_i64(buf, 18);
    let sell_id  = read_i64(buf, 26);
    let last_px  = mdl_double(buf, 46, 4);
    let last_qty = read_i64(buf, 54) as f64;
    let exec_type = read_i32(buf, 62);
    let event_time = format_mdl_time(read_u32(buf, 66), trading_day);

    let mut side = if buy_id > sell_id { 0i64 } else { 1i64 };
    if exec_type == 52 { side = 4i64; }

    Some(ParsedSzDeal {
        code, time: event_time, sell_id, buy_id, side,
        price: last_px, qty: last_qty, money: last_px * last_qty,
        channel, appl_seq,
    })
}

// ================================================================== //
// Python object construction (GIL held, very fast)                   //
// ================================================================== //

fn tick_to_pytuple<'a>(py: Python<'a>, p: &ParsedTick, trading_day: &str) -> PyObject {
    let mut e = Vec::with_capacity(81);
    push_str(&mut e, py, trading_day);      // 0  TradingDay
    push_str(&mut e, py, &p.code);          // 1  Code
    push_str(&mut e, py, &p.time);          // 2  Time
    push_str(&mut e, py, &p.time);          // 3  UpdateTime
    push_f64(&mut e, py, p.current_price);  // 4  CurrentPrice
    push_f64(&mut e, py, p.total_volume);   // 5  TotalVolume
    push_f64(&mut e, py, p.total_money);    // 6  TotalMoney
    push_f64(&mut e, py, p.pre_close);      // 7  PreClosePrice
    push_f64(&mut e, py, p.open);           // 8  OpenPrice
    push_f64(&mut e, py, p.high);           // 9  HighestPrice
    push_f64(&mut e, py, p.low);            // 10 LowestPrice
    push_f64(&mut e, py, p.high_limit);     // 11 HighLimitPrice
    push_f64(&mut e, py, p.low_limit);      // 12 LowLimitPrice
    push_f64(&mut e, py, p.iopv);           // 13 IOPV
    push_f64(&mut e, py, p.trade_num);      // 14 TradeNum
    push_f64(&mut e, py, p.total_bid_vol);  // 15 TotalBidVolume
    push_f64(&mut e, py, p.total_ask_vol);  // 16 TotalAskVolume
    push_f64(&mut e, py, p.avg_bid);        // 17 AvgBidPrice
    push_f64(&mut e, py, p.avg_ask);        // 18 AvgAskPrice
    for i in 0..10 { push_f64(&mut e, py, p.ask_price[i]); } // 19-28
    for i in 0..10 { push_f64(&mut e, py, p.ask_vol[i]); }   // 29-38
    for i in 0..10 { push_f64(&mut e, py, p.ask_num[i]); }   // 39-48
    for i in 0..10 { push_f64(&mut e, py, p.bid_price[i]); } // 49-58
    for i in 0..10 { push_f64(&mut e, py, p.bid_vol[i]); }   // 59-68
    for i in 0..10 { push_f64(&mut e, py, p.bid_num[i]); }   // 69-78
    push_i64(&mut e, py, p.channel);        // 79 Channel
    push_i64(&mut e, py, p.seq_id);         // 80 SeqNum

    PyTuple::new(py, e).unwrap().into()
}

fn ngts_order_to_pytuple<'a>(py: Python<'a>, p: &ParsedNgtsOrder, trading_day: &str) -> PyObject {
    let mut e = Vec::with_capacity(11);
    push_str(&mut e, py, trading_day);
    push_str(&mut e, py, &p.code);
    push_str(&mut e, py, &p.time);
    push_str(&mut e, py, &p.time);
    push_i64(&mut e, py, p.order_id);
    push_i64(&mut e, py, p.side);
    push_f64(&mut e, py, p.price);
    push_f64(&mut e, py, p.qty);
    push_i64(&mut e, py, p.order_type);
    push_i64(&mut e, py, p.channel);
    push_i64(&mut e, py, p.seq_id);
    PyTuple::new(py, e).unwrap().into()
}

fn ngts_deal_to_pytuple<'a>(py: Python<'a>, p: &ParsedNgtsDeal, trading_day: &str) -> PyObject {
    let mut e = Vec::with_capacity(12);
    push_str(&mut e, py, trading_day);
    push_str(&mut e, py, &p.code);
    push_str(&mut e, py, &p.time);
    push_str(&mut e, py, &p.time);
    push_i64(&mut e, py, p.sell_no);
    push_i64(&mut e, py, p.buy_no);
    push_i64(&mut e, py, p.side);
    push_f64(&mut e, py, p.price);
    push_f64(&mut e, py, p.qty);
    push_f64(&mut e, py, p.money);
    push_i64(&mut e, py, p.channel);
    push_i64(&mut e, py, p.seq_id);
    PyTuple::new(py, e).unwrap().into()
}

fn sz_order_to_pytuple<'a>(py: Python<'a>, p: &ParsedSzOrder, trading_day: &str) -> PyObject {
    let mut e = Vec::with_capacity(11);
    push_str(&mut e, py, trading_day);
    push_str(&mut e, py, &p.code);
    push_str(&mut e, py, &p.time);
    push_str(&mut e, py, &p.time);
    push_i64(&mut e, py, p.appl_seq);
    push_i64(&mut e, py, p.side);
    push_f64(&mut e, py, p.price);
    push_f64(&mut e, py, p.qty);
    push_i64(&mut e, py, p.ord_type);
    push_i64(&mut e, py, p.channel);
    push_i64(&mut e, py, p.appl_seq);
    PyTuple::new(py, e).unwrap().into()
}

fn sz_deal_to_pytuple<'a>(py: Python<'a>, p: &ParsedSzDeal, trading_day: &str) -> PyObject {
    let mut e = Vec::with_capacity(12);
    push_str(&mut e, py, trading_day);
    push_str(&mut e, py, &p.code);
    push_str(&mut e, py, &p.time);
    push_str(&mut e, py, &p.time);
    push_i64(&mut e, py, p.sell_id);
    push_i64(&mut e, py, p.buy_id);
    push_i64(&mut e, py, p.side);
    push_f64(&mut e, py, p.price);
    push_f64(&mut e, py, p.qty);
    push_f64(&mut e, py, p.money);
    push_i64(&mut e, py, p.channel);
    push_i64(&mut e, py, p.appl_seq);
    PyTuple::new(py, e).unwrap().into()
}

// ================================================================== //
// PyO3 entry points: parse without GIL, build objects with GIL      //
// ================================================================== //

#[pyfunction]
fn parse_sh_tick(py: Python, buf: &[u8], trading_day: &str, seq_id: i64) -> Option<(String, PyObject)> {
    let parsed = py.allow_threads(|| parse_sh_tick_raw(buf, trading_day, seq_id))?;
    let code = parsed.code.clone();
    let tuple = tick_to_pytuple(py, &parsed, trading_day);
    Some((code, tuple))
}

#[pyfunction]
fn parse_sz_tick(py: Python, buf: &[u8], trading_day: &str, seq_id: i64) -> Option<(String, PyObject)> {
    let parsed = py.allow_threads(|| parse_sz_tick_raw(buf, trading_day, seq_id))?;
    let code = parsed.code.clone();
    let tuple = tick_to_pytuple(py, &parsed, trading_day);
    Some((code, tuple))
}

#[pyfunction]
fn parse_sh_ngts(py: Python, buf: &[u8], trading_day: &str) -> Option<(String, Option<PyObject>, Option<PyObject>)> {
    let parsed = py.allow_threads(|| parse_sh_ngts_raw(buf, trading_day))?;
    let code = parsed.code.clone();
    let order_py = parsed.order.as_ref().map(|o| ngts_order_to_pytuple(py, o, trading_day));
    let deal_py = parsed.deal.as_ref().map(|d| ngts_deal_to_pytuple(py, d, trading_day));
    Some((code, order_py, deal_py))
}

#[pyfunction]
fn parse_sz_order(py: Python, buf: &[u8], trading_day: &str, seq_id: i64) -> Option<(String, PyObject)> {
    let parsed = py.allow_threads(|| parse_sz_order_raw(buf, trading_day, seq_id))?;
    let code = parsed.code.clone();
    let tuple = sz_order_to_pytuple(py, &parsed, trading_day);
    Some((code, tuple))
}

#[pyfunction]
fn parse_sz_deal(py: Python, buf: &[u8], trading_day: &str, seq_id: i64) -> Option<(String, PyObject)> {
    let parsed = py.allow_threads(|| parse_sz_deal_raw(buf, trading_day, seq_id))?;
    let code = parsed.code.clone();
    let tuple = sz_deal_to_pytuple(py, &parsed, trading_day);
    Some((code, tuple))
}

// ================================================================== //
// PyO3 module                                                         //
// ================================================================== //

#[pymodule]
fn mdl_parser(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_sh_tick, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sz_tick, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sh_ngts, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sz_order, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sz_deal, m)?)?;
    Ok(())
}
