import os
import time
import json
import math
import logging
import requests
from datetime import datetime, timezone

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

MIN_LIQUIDITY  = 100
PAGE_SIZE      = 100
MAX_PAGES      = 50
REQUEST_DELAY  = 0.3
STD_MULTIPLIER = 2.0
EXPIRY_WINDOW  = 168

# ----------------------------------------------------------------
# Logging
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("PolyArb")


# ----------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] not configured, skipping")
        return
    api_url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    try:
        resp = requests.post(api_url, json={
            "chat_id": TELEGRAM_CHAT,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=10)
        if resp.status_code == 200:
            log.info("[Telegram] sent ok")
        else:
            log.warning("[Telegram] failed: " + resp.text)
    except Exception as ex:
        log.warning("[Telegram] error: " + str(ex))


# ----------------------------------------------------------------
# HTTP session
# ----------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


# ----------------------------------------------------------------
# API helpers
# ----------------------------------------------------------------
def gamma_get(path, params=None):
    try:
        r = SESSION.get(GAMMA_API + path, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as ex:
        log.warning("Gamma API error " + path + ": " + str(ex))
        return None


def clob_midprice(token_id):
    try:
        rb = SESSION.get(CLOB_API + "/price",
                         params={"token_id": token_id, "side": "BUY"}, timeout=8)
        rs = SESSION.get(CLOB_API + "/price",
                         params={"token_id": token_id, "side": "SELL"}, timeout=8)
        bp = float(rb.json().get("price", 0))
        sp = float(rs.json().get("price", 0))
        if bp > 0 and sp > 0:
            return (bp + sp) / 2
        return bp or sp or None
    except Exception:
        return None


_cache = {}


def clob_midprice_cached(token_id):
    if token_id in _cache:
        return _cache[token_id]
    p = clob_midprice(token_id)
    if p:
        _cache[token_id] = p
    return p


# ----------------------------------------------------------------
# Fetch all markets
# ----------------------------------------------------------------
def fetch_all_markets():
    markets = []
    offset = 0
    for _ in range(MAX_PAGES):
        log.info("fetching offset=" + str(offset))
        data = gamma_get("/markets", params={
            "limit": PAGE_SIZE,
            "offset": offset,
            "active": "true",
            "closed": "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        })
        if not data:
            break
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("data", data.get("markets", []))
        else:
            break
        if not batch:
            break
        markets.extend(batch)
        log.info("  total so far: " + str(len(markets)))
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)
    log.info("total markets fetched: " + str(len(markets)))
    return markets


# ----------------------------------------------------------------
# Price parsing
# ----------------------------------------------------------------
def parse_prices(market):
    prices = []
    raw = market.get("outcomePrices")
    if raw:
        try:
            if isinstance(raw, str):
                raw = json.loads(raw)
            prices = [float(p) for p in raw if p is not None]
        except Exception:
            pass
    if not prices:
        tokens = market.get("tokens", [])
        prices = [float(t.get("price", 0)) for t in tokens if t.get("price") is not None]
    return [p for p in prices if 0 < p < 1]


def get_url(market):
    slug = market.get("slug", "")
    if slug:
        return "https://polymarket.com/market/" + slug
    return "N/A"


def get_urgency(hours_left):
    if hours_left < 6:
        return "URGENT", "🔴 URGENT"
    if hours_left < 72:
        return "WATCH", "🟡 WATCH"
    return "EARLY", "🟢 EARLY"


# ----------------------------------------------------------------
# Dynamic threshold computation
# ----------------------------------------------------------------
def compute_stats(values):
    if len(values) < 2:
        return None, None
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance)
    return mean, std


def analyze_markets(markets):
    bundle_sums = []
    multi_sums  = []
    expiry_gaps = []
    now = datetime.now(timezone.utc)

    for market in markets:
        prices = parse_prices(market)
        if len(prices) == 2:
            bundle_sums.append(sum(prices))
        if len(prices) >= 3:
            multi_sums.append(sum(prices))
        end_date = market.get("endDate") or market.get("end_date_iso")
        if end_date and prices:
            try:
                ed = end_date.replace("Z", "+00:00")
                expiry = datetime.fromisoformat(ed)
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                hours_left = (expiry - now).total_seconds() / 3600
                if 0 < hours_left < EXPIRY_WINDOW:
                    for p in prices:
                        deviation = min(p, 1 - p)
                        expiry_gaps.append((hours_left, deviation))
            except Exception:
                pass

    b_mean, b_std = compute_stats(bundle_sums)
    if b_mean is not None and b_std is not None:
        bundle_threshold = b_mean - STD_MULTIPLIER * b_std
    else:
        bundle_threshold = 0.97

    m_mean, m_std = compute_stats(multi_sums)
    if m_mean is not None and m_std is not None:
        multi_lower = m_mean - STD_MULTIPLIER * m_std
        multi_upper = m_mean + STD_MULTIPLIER * m_std
    else:
        multi_lower = 0.97
        multi_upper = 1.03

    if expiry_gaps:
        deviations = [d for (h, d) in expiry_gaps]
        dev_mean, dev_std = compute_stats(deviations)
        if dev_mean is not None and dev_std is not None:
            near_expiry_dev_threshold = dev_mean + STD_MULTIPLIER * dev_std
        else:
            near_expiry_dev_threshold = 0.15
    else:
        near_expiry_dev_threshold = 0.15
        dev_mean, dev_std = 0.15, 0.0

    log.info("=== Dynamic Thresholds ===")
    if b_mean is not None:
        log.info("Bundle     -> mean=" + str(round(b_mean,4)) + " std=" + str(round(b_std,4)) + " lower=" + str(round(bundle_threshold,4)))
    if m_mean is not None:
        log.info("Multi  LOW -> mean=" + str(round(m_mean,4)) + " std=" + str(round(m_std,4)) + " lower=" + str(round(multi_lower,4)))
        log.info("Multi HIGH -> upper=" + str(round(multi_upper,4)) + "  (overround threshold)")
    log.info("Expiry     -> mean=" + str(round(dev_mean,4)) + " std=" + str(round(dev_std,4)) + " threshold=" + str(round(near_expiry_dev_threshold,4)))
    log.info("=========================")

    return bundle_threshold, multi_lower, multi_upper, near_expiry_dev_threshold


# ----------------------------------------------------------------
# ARB detectors
# ----------------------------------------------------------------
def detect_bundle(market, threshold):
    prices = parse_prices(market)
    if len(prices) != 2:
        return None
    total = sum(prices)
    if total < threshold:
        edge = round(1.0 - total, 4)
        return {
            "type": "Bundle ARB (YES+NO < 1)",
            "market": market.get("question", market.get("slug", "N/A")),
            "url": get_url(market),
            "prices": prices,
            "sum": round(total, 4),
            "edge": edge,
            "edge_pct": "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
            "threshold_used": round(threshold, 4),
            "urgency": "WATCH",
            "action": "BUY YES + BUY NO",
        }
    return None


def detect_multi_under(market, threshold):
    prices = parse_prices(market)
    if len(prices) < 3:
        return None
    total = sum(prices)
    if total < threshold:
        edge = round(1.0 - total, 4)
        return {
            "type": "Multi-Outcome UNDER ARB (" + str(len(prices)) + " outcomes)",
            "market": market.get("question", market.get("slug", "N/A")),
            "url": get_url(market),
            "prices": prices,
            "sum": round(total, 4),
            "edge": edge,
            "edge_pct": "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
            "threshold_used": round(threshold, 4),
            "urgency": "WATCH",
            "action": "BUY YES on all outcomes",
        }
    return None


def detect_multi_over(market, threshold):
    prices = parse_prices(market)
    if len(prices) < 3:
        return None
    total = sum(prices)
    if total > threshold:
        overround = round(total - 1.0, 4)
        return {
            "type": "Multi-Outcome OVER ARB (" + str(len(prices)) + " outcomes)",
            "market": market.get("question", market.get("slug", "N/A")),
            "url": get_url(market),
            "prices": prices,
            "sum": round(total, 4),
            "overround": overround,
            "overround_pct": "{:.2f}%".format(overround * 100),
            "edge": overround,
            "liquidity": market.get("liquidity", "N/A"),
            "threshold_used": round(threshold, 4),
            "urgency": "WATCH",
            "action": "SELL YES on all outcomes",
        }
    return None


def detect_near_expiry(market, dev_threshold):
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        ed = end_date.replace("Z", "+00:00")
        expiry = datetime.fromisoformat(ed)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        hours_left = (expiry - now).total_seconds() / 3600
        if 0 < hours_left < EXPIRY_WINDOW:
            prices = parse_prices(market)
            suspicious = []
            for p in prices:
                deviation = min(p, 1 - p)
                if deviation > dev_threshold:
                    suspicious.append(round(p, 4))
            if suspicious:
                urgency_key, urgency_label = get_urgency(hours_left)
                return {
                    "type": urgency_label + " Near-Expiry Mispricing",
                    "market": market.get("question", market.get("slug", "N/A")),
                    "url": get_url(market),
                    "prices": prices,
                    "hours_left": round(hours_left, 2),
                    "suspicious": suspicious,
                    "liquidity": market.get("liquidity", "N/A"),
                    "threshold_used": round(dev_threshold, 4),
                    "urgency": urgency_key,
                    "action": "Check price direction vs reality",
                }
    except Exception:
        pass
    return None


def detect_clob_confirmed(market, threshold):
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        return None
    clob_prices = []
    for token in tokens:
        tid = token.get("token_id") or token.get("tokenId")
        if not tid:
            continue
        mid = clob_midprice_cached(tid)
        if mid:
            clob_prices.append(mid)
        time.sleep(0.15)
    if len(clob_prices) == 2:
        total = sum(clob_prices)
        if total < threshold:
            edge = round(1.0 - total, 4)
            return {
                "type": "CLOB-Confirmed Bundle ARB",
                "market": market.get("question", market.get("slug", "N/A")),
                "url": get_url(market),
                "prices_clob": clob_prices,
                "sum": round(total, 4),
                "edge": edge,
                "edge_pct": "{:.2f}%".format(edge * 100),
                "liquidity": market.get("liquidity", "N/A"),
                "threshold_used": round(threshold, 4),
                "urgency": "URGENT",
                "action": "BUY YES + BUY NO immediately",
            }
    return None


# ----------------------------------------------------------------
# Output formatting
# ----------------------------------------------------------------
URGENCY_ORDER = {"URGENT": 0, "WATCH": 1, "EARLY": 2}


def sort_opps(opps):
    return sorted(opps, key=lambda o: (URGENCY_ORDER.get(o.get("urgency", "EARLY"), 2), -o.get("edge", 0)))


def fmt_opp(opp, idx):
    price_list = opp.get("prices") or opp.get("prices_clob", [])
    out = "=" * 60 + "\n"
    out += "#{:02d} {}\n".format(idx, opp["type"])
    out += "Market    : " + opp["market"][:80] + "\n"
    out += "URL       : " + opp.get("url", "N/A") + "\n"
    out += "Liquidity : $" + str(opp.get("liquidity", "N/A")) + "\n"
    out += "Action    : " + opp.get("action", "N/A") + "\n"
    out += "Threshold : " + str(opp.get("threshold_used", "N/A")) + "\n"
    if "overround" in opp:
        out += "Sum       : " + str(opp["sum"]) + "  Overround: " + opp.get("overround_pct", "N/A") + "\n"
        out += "Prices    : " + str(price_list) + "\n"
    elif "sum" in opp:
        out += "Sum       : " + str(opp["sum"]) + "  Edge: " + opp.get("edge_pct", "N/A") + "\n"
        out += "Prices    : " + str(price_list) + "\n"
    if "hours_left" in opp:
        out += "Expires in: " + str(opp["hours_left"]) + "h\n"
        out += "Suspicious: " + str(opp["suspicious"]) + "\n"
    return out


def build_tg_msg(opps, total, b_thr, m_low, m_high, e_thr):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    urgent_count = sum(1 for o in opps if o.get("urgency") == "URGENT")
    watch_count  = sum(1 for o in opps if o.get("urgency") == "WATCH")
    early_count  = sum(1 for o in opps if o.get("urgency") == "EARLY")
    sorted_opps  = sort_opps(opps)
    msg = "<b>Polymarket ARB Scanner - " + str(len(opps)) + " opportunities</b>\n"
    msg += "Time: " + now_str + "\n"
    msg += "Markets scanned: " + str(total) + "\n"
    msg += "🔴 Urgent: " + str(urgent_count) + "  🟡 Watch: " + str(watch_count) + "  🟢 Early: " + str(early_count) + "\n"
    msg += "Thresholds: Bundle=" + str(round(b_thr,4))
    msg += " Multi_low=" + str(round(m_low,4)) + " Multi_high=" + str(round(m_high,4))
    msg += " Expiry=" + str(round(e_thr,4)) + "\n\n"
    for idx, opp in enumerate(sorted_opps[:5], 1):
        url = opp.get("url", "#")
        msg += "<b>#" + str(idx) + " " + opp["type"] + "</b>\n"
        msg += opp.get("market", "N/A")[:60] + "\n"
        msg += "Action: " + opp.get("action", "N/A") + "\n"
        if "overround" in opp:
            msg += "Overround: " + opp["overround_pct"] + "  Sum: " + str(opp["sum"]) + "\n"
            msg += "Prices: " + str(opp.get("prices", [])) + "\n"
        elif "edge_pct" in opp:
            msg += "Edge: " + opp["edge_pct"] + "\n"
            if "sum" in opp:
                pl = opp.get("prices") or opp.get("prices_clob", [])
                msg += "Prices: " + str(pl) + " sum=" + str(opp["sum"]) + "\n"
        if "hours_left" in opp:
            msg += "Expires in: " + str(opp["hours_left"]) + "h\n"
        msg += '<a href="' + url + '">View Market</a>\n\n'
    return msg


# ----------------------------------------------------------------
# Main scan
# ----------------------------------------------------------------
def scan():
    log.info("=" * 60)
    log.info("Polymarket ARB Scanner (dynamic thresholds, 7-day window)")
    log.info("Time: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("Expiry window: " + str(EXPIRY_WINDOW) + "h | STD multiplier: " + str(STD_MULTIPLIER))
    log.info("=" * 60)

    markets = fetch_all_markets()
    if not markets:
        log.error("No markets fetched")
        send_telegram("ERROR: Could not fetch Polymarket markets")
        return

    log.info("Computing dynamic thresholds from " + str(len(markets)) + " markets...")
    b_thr, m_low, m_high, e_thr = analyze_markets(markets)

    opps = []
    bundle_candidates = []

    for i, market in enumerate(markets):
        if i % 500 == 0 and i > 0:
            log.info("progress: {}/{}".format(i, len(markets)))

        r = detect_bundle(market, b_thr)
        if r:
            bundle_candidates.append((market, r))

        r = detect_multi_under(market, m_low)
        if r:
            opps.append(r)

        r = detect_multi_over(market, m_high)
        if r:
            opps.append(r)

        r = detect_near_expiry(market, e_thr)
        if r:
            opps.append(r)

    log.info("bundle candidates : " + str(len(bundle_candidates)))
    log.info("other opps so far : " + str(len(opps)))

    log.info("CLOB confirming " + str(len(bundle_candidates)) + " bundle candidates...")
    for market, prelim in bundle_candidates:
        confirmed = detect_clob_confirmed(market, b_thr)
        opps.append(confirmed if confirmed else prelim)
        time.sleep(REQUEST_DELAY)

    opps = sort_opps(opps)

    urgent_n = sum(1 for o in opps if o.get("urgency") == "URGENT")
    watch_n  = sum(1 for o in opps if o.get("urgency") == "WATCH")
    early_n  = sum(1 for o in opps if o.get("urgency") == "EARLY")
    under_n  = sum(1 for o in opps if "UNDER" in o.get("type", ""))
    over_n   = sum(1 for o in opps if "OVER" in o.get("type", ""))

    log.info("=" * 60)
    log.info("DONE - " + str(len(opps)) + " opportunities total")
    log.info("  🔴 URGENT      : " + str(urgent_n))
    log.info("  🟡 WATCH       : " + str(watch_n))
    log.info("  🟢 EARLY       : " + str(early_n))
    log.info("  📉 Multi UNDER : " + str(under_n))
    log.info("  📈 Multi OVER  : " + str(over_n))
    log.info("=" * 60)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not opps:
        log.info("No opportunities this round")
        send_telegram(
            "<b>Polymarket ARB Scanner</b>\n"
            + "Time: " + now_str + "\n"
            + "Markets scanned: " + str(len(markets)) + "\n"
            + "Bundle threshold  : " + str(round(b_thr,4)) + "\n"
            + "Multi lower       : " + str(round(m_low,4)) + "\n"
            + "Multi upper       : " + str(round(m_high,4)) + "\n"
            + "Expiry threshold  : " + str(round(e_thr,4)) + "\n"
            + "Result: No opportunities found"
        )
    else:
        for idx, opp in enumerate(opps, 1):
            print(fmt_opp(opp, idx))
        send_telegram(build_tg_msg(opps, len(markets), b_thr, m_low, m_high, e_thr))

    report = {
        "scan_time": datetime.now(timezone.utc).isoformat(),
        "total_markets_scanned": len(markets),
        "dynamic_thresholds": {
            "bundle":      round(b_thr, 4),
            "multi_lower": round(m_low, 4),
            "multi_upper": round(m_high, 4),
            "near_expiry": round(e_thr, 4),
        },
        "opportunities_summary": {
            "total":       len(opps),
            "urgent":      urgent_n,
            "watch":       watch_n,
            "early":       early_n,
            "multi_under": under_n,
            "multi_over":  over_n,
        },
        "opportunities": opps,
    }
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("report saved to arb_report.json")


if __name__ == "__main__":
    scan()
