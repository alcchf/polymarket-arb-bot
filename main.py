import os
import time
import json
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

BUNDLE_ARB_THRESHOLD   = 0.97
MULTI_ARB_THRESHOLD    = 0.97
NEAR_EXPIRY_HOURS      = 6
NEAR_EXPIRY_PROB_RANGE = (0.15, 0.85)
MIN_LIQUIDITY = 500
PAGE_SIZE     = 100
MAX_PAGES     = 50
REQUEST_DELAY = 0.3

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


# ----------------------------------------------------------------
# ARB detectors
# ----------------------------------------------------------------
def detect_bundle(market):
    prices = parse_prices(market)
    if len(prices) != 2:
        return None
    total = sum(prices)
    if total < BUNDLE_ARB_THRESHOLD:
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
        }
    return None


def detect_multi(market):
    prices = parse_prices(market)
    if len(prices) < 3:
        return None
    total = sum(prices)
    if total < MULTI_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type": "Multi-Outcome ARB (" + str(len(prices)) + " outcomes)",
            "market": market.get("question", market.get("slug", "N/A")),
            "url": get_url(market),
            "prices": prices,
            "sum": round(total, 4),
            "edge": edge,
            "edge_pct": "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_near_expiry(market):
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        end_date = end_date.replace("Z", "+00:00")
        expiry = datetime.fromisoformat(end_date)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        hours_left = (expiry - now).total_seconds() / 3600
        if 0 < hours_left < NEAR_EXPIRY_HOURS:
            prices = parse_prices(market)
            suspicious = [p for p in prices
                          if NEAR_EXPIRY_PROB_RANGE[0] < p < NEAR_EXPIRY_PROB_RANGE[1]]
            if suspicious:
                return {
                    "type": "Near-Expiry ({:.1f}h left)".format(hours_left),
                    "market": market.get("question", market.get("slug", "N/A")),
                    "url": get_url(market),
                    "prices": prices,
                    "hours_left": round(hours_left, 2),
                    "suspicious": suspicious,
                    "liquidity": market.get("liquidity", "N/A"),
                }
    except Exception:
        pass
    return None


def detect_clob_confirmed(market):
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
        if total < BUNDLE_ARB_THRESHOLD:
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
            }
    return None


# ----------------------------------------------------------------
# Output formatting
# ----------------------------------------------------------------
def fmt_opp(opp, idx):
    price_list = opp.get("prices") or opp.get("prices_clob", [])
    out = "=" * 60 + "\n"
    out += "#{:02d} {}\n".format(idx, opp["type"])
    out += "Market : " + opp["market"][:80] + "\n"
    out += "URL    : " + opp.get("url", "N/A") + "\n"
    out += "Liq    : $" + str(opp.get("liquidity", "N/A")) + "\n"
    if "sum" in opp:
        out += "Sum    : " + str(opp["sum"]) + "  Edge: " + opp.get("edge_pct", "N/A") + "\n"
        out += "Prices : " + str(price_list) + "\n"
    if "hours_left" in opp:
        out += "Expiry : " + str(opp["hours_left"]) + "h left\n"
        out += "Suspicious: " + str(opp["suspicious"]) + "\n"
    return out


def build_tg_msg(opps, total):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = "<b>Polymarket ARB Scanner - " + str(len(opps)) + " opportunities</b>\n"
    msg += "Time: " + now_str + "\n"
    msg += "Markets scanned: " + str(total) + "\n\n"
    for idx, opp in enumerate(opps[:5], 1):
        url = opp.get("url", "#")
        msg += "<b>#" + str(idx) + " " + opp["type"] + "</b>\n"
        msg += opp.get("market", "N/A")[:60] + "\n"
        if "edge_pct" in opp:
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
    log.info("Polymarket ARB Scanner starting...")
    log.info("Time: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("=" * 60)

    markets = fetch_all_markets()
    if not markets:
        log.error("No markets fetched")
        send_telegram("ERROR: Could not fetch Polymarket markets")
        return

    opps = []
    bundle_candidates = []

    for i, market in enumerate(markets):
        if i % 500 == 0 and i > 0:
            log.info("progress: {}/{}".format(i, len(markets)))
        r = detect_bundle(market)
        if r:
            bundle_candidates.append((market, r))
        r = detect_multi(market)
        if r:
            opps.append(r)
        r = detect_near_expiry(market)
        if r:
            opps.append(r)

    log.info("bundle candidates: " + str(len(bundle_candidates)))
    log.info("other opps: " + str(len(opps)))

    for market, prelim in bundle_candidates:
        confirmed = detect_clob_confirmed(market)
        opps.append(confirmed if confirmed else prelim)
        time.sleep(REQUEST_DELAY)

    opps.sort(key=lambda o: o.get("edge", 0), reverse=True)

    log.info("=" * 60)
    log.info("DONE - found " + str(len(opps)) + " opportunities")
    log.info("=" * 60)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not opps:
        log.info("No ARB opportunities this round")
        send_telegram(
            "<b>Polymarket ARB Scanner</b>\n"
            + "Time: " + now_str + "\n"
            + "Markets scanned: " + str(len(markets)) + "\n"
            + "Result: No opportunities found"
        )
    else:
        for idx, opp in enumerate(opps, 1):
            print(fmt_opp(opp, idx))
        send_telegram(build_tg_msg(opps, len(markets)))

    report = {
        "scan_time": datetime.now(timezone.utc).isoformat(),
        "total_markets_scanned": len(markets),
        "opportunities_found": len(opps),
        "opportunities": opps,
    }
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("report saved to arb_report.json")


if __name__ == "__main__":
    scan()
