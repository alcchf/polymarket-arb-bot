import os
import time
import json
import math
import logging
import requests
import re
from datetime import datetime, timezone
from collections import defaultdict

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
GAMMA_API         = "https://gamma-api.polymarket.com"
CLOB_API          = "https://clob.polymarket.com"
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")
NOAA_API_KEY      = os.getenv("NOAA_API_KEY", "")
ODDS_API_KEY      = os.getenv("ODDS_API_KEY", "")
ODDS_API_BASE     = "https://api.the-odds-api.com/v4"

MIN_LIQUIDITY     = 100
PAGE_SIZE         = 100
MAX_PAGES         = 50
REQUEST_DELAY     = 0.3
STD_MULTIPLIER    = 2.0
EXPIRY_WINDOW     = 168
MIN_EDGE          = 0.01
WATCH_PUSH_MIN    = 0.02
SPREAD_SAMPLE     = 50
CROSS_MIN_KEYS    = 5
CROSS_MIN_SUM     = 0.05
SCAN_WINDOW_HOURS = 72
MAX_CLOB_CONFIRM  = 20
WEATHER_EDGE_MIN  = 0.12
SPORTS_EDGE_MIN   = 0.05

COUNTRIES = {
    "afghanistan","albania","algeria","argentina","australia","austria","azerbaijan",
    "bahrain","bangladesh","belgium","bolivia","brazil","bulgaria","cambodia",
    "canada","chile","china","colombia","croatia","cuba","czechia","denmark",
    "ecuador","egypt","england","ethiopia","finland","france","germany","ghana",
    "greece","guatemala","honduras","hungary","india","indonesia","iran","iraq",
    "ireland","israel","italy","japan","jordan","kazakhstan","kenya","kuwait",
    "malaysia","mexico","morocco","netherlands","nigeria","norway","pakistan",
    "panama","peru","philippines","poland","portugal","qatar","romania","russia",
    "saudi","scotland","senegal","serbia","singapore","slovakia","slovenia",
    "somalia","spain","sweden","switzerland","taiwan","thailand","turkey",
    "ukraine","uruguay","usa","venezuela","vietnam","wales",
}

EXCLUSIVE_EVENTS = {
    "world cup","championship","election","president","oscar","nobel",
    "champion","gold medal","title","award","winner","wins the","win the",
    "super bowl","world series","stanley cup","nba finals","premier league",
    "fa cup","champions league","ballon","mvp","best actor","best picture",
}

SUBJECT_STOPWORDS = {
    "will","won","win","the","his","her","their","this","that","who","what",
    "when","how","does","did","can","could","would","should","may","might",
    "shall","been","was","were","are","has","have","had","its","our","your",
    "above","below","over","under","than","more","less","new","old","first",
    "last","next","per","day","one","two","three","four","five",
}

WEATHER_KEYWORDS = [
    "temperature","rain","snow","hurricane","storm","wind","flood",
    "celsius","fahrenheit","precipitation","weather","degrees","hot",
    "cold","warm","freeze","blizzard","tornado","typhoon","cyclone",
    "landfall","tropical","drought","heatwave","heat wave","exceed",
    "reaches","high temp","low temp","record high","record low",
    "snowfall","rainfall","mph","category","wildfire","heat",
]

SPORTS_KEYWORDS = [
    "nfl","nba","mlb","nhl","soccer","football","basketball","baseball",
    "hockey","tennis","ufc","mma","boxing","golf","rugby","cricket",
    "f1","formula","premier league","champions league","la liga","serie a",
    "bundesliga","ligue 1","world cup","euros","copa","superbowl","super bowl",
    "playoffs","championship","match","game","vs","versus","beat","defeat",
]

ODDS_SPORT_MAP = {
    "nfl":              "americanfootball_nfl",
    "nba":              "basketball_nba",
    "mlb":              "baseball_mlb",
    "nhl":              "icehockey_nhl",
    "premier league":   "soccer_epl",
    "champions league": "soccer_uefa_champs_league",
    "la liga":          "soccer_spain_la_liga",
    "serie a":          "soccer_italy_serie_a",
    "bundesliga":       "soccer_germany_bundesliga",
    "ligue 1":          "soccer_france_ligue_one",
    "soccer":           "soccer_epl",
    "football":         "americanfootball_nfl",
    "basketball":       "basketball_nba",
    "baseball":         "baseball_mlb",
    "hockey":           "icehockey_nhl",
    "ufc":              "mma_mixed_martial_arts",
    "mma":              "mma_mixed_martial_arts",
    "tennis":           "tennis_atp_french_open",
    "golf":             "golf_masters_tournament_winner",
    "super bowl":       "americanfootball_nfl",
    "superbowl":        "americanfootball_nfl",
    "world cup":        "soccer_fifa_world_cup",
}

CITY_COORDS = {
    "new york":      (40.71, -74.01),
    "los angeles":   (34.05, -118.24),
    "chicago":       (41.88, -87.63),
    "houston":       (29.76, -95.37),
    "miami":         (25.77, -80.19),
    "london":        (51.51, -0.13),
    "paris":         (48.85, 2.35),
    "tokyo":         (35.69, 139.69),
    "beijing":       (39.91, 116.39),
    "sydney":        (-33.87, 151.21),
    "dubai":         (25.20, 55.27),
    "singapore":     (1.35, 103.82),
    "toronto":       (43.65, -79.38),
    "berlin":        (52.52, 13.41),
    "moscow":        (55.75, 37.62),
    "rome":          (41.90, 12.49),
    "madrid":        (40.42, -3.70),
    "amsterdam":     (52.37, 4.90),
    "seoul":         (37.57, 126.98),
    "mumbai":        (19.08, 72.88),
    "cairo":         (30.04, 31.24),
    "lagos":         (6.45, 3.39),
    "sao paulo":     (-23.55, -46.63),
    "mexico city":   (19.43, -99.13),
    "buenos aires":  (-34.60, -58.38),
    "istanbul":      (41.01, 28.95),
    "bangkok":       (13.75, 100.52),
    "jakarta":       (-6.21, 106.85),
    "manila":        (14.60, 120.98),
    "karachi":       (24.86, 67.01),
    "dallas":        (32.78, -96.80),
    "seattle":       (47.61, -122.33),
    "boston":        (42.36, -71.06),
    "denver":        (39.74, -104.98),
    "atlanta":       (33.75, -84.39),
    "nyc":           (40.71, -74.01),
    "la":            (34.05, -118.24),
    "sf":            (37.77, -122.42),
    "san francisco": (37.77, -122.42),
    "new orleans":   (29.95, -90.07),
    "las vegas":     (36.17, -115.14),
    "phoenix":       (33.45, -112.07),
    "florida":       (27.99, -81.76),
    "texas":         (31.00, -100.00),
    "california":    (36.78, -119.42),
    "midwest":       (41.88, -87.63),
    "gulf coast":    (29.76, -95.37),
    "northeast":     (42.36, -71.06),
}

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
# Telegram  (v8.1: auto-split messages > 4000 chars)
# ----------------------------------------------------------------
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] not configured, skipping")
        return
    api_url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    max_len = 4000
    chunks = []
    while len(msg) > max_len:
        split_pos = msg.rfind("\n", 0, max_len)
        if split_pos == -1:
            split_pos = max_len
        chunks.append(msg[:split_pos])
        msg = msg[split_pos:].lstrip("\n")
    chunks.append(msg)
    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            resp = requests.post(api_url, json={
                "chat_id": TELEGRAM_CHAT,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }, timeout=10)
            if resp.status_code == 200:
                log.info("[Telegram] sent ok (" + str(len(chunk)) + " chars)")
            else:
                log.warning("[Telegram] failed: " + resp.text)
        except Exception as ex:
            log.warning("[Telegram] error: " + str(ex))
        time.sleep(0.5)

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

def clob_get_spread(token_id):
    try:
        rb = SESSION.get(CLOB_API + "/price",
                         params={"token_id": token_id, "side": "BUY"}, timeout=8)
        rs = SESSION.get(CLOB_API + "/price",
                         params={"token_id": token_id, "side": "SELL"}, timeout=8)
        ask = float(rb.json().get("price", 0))
        bid = float(rs.json().get("price", 0))
        if ask > 0 and bid > 0 and ask > bid:
            return round(ask - bid, 4), round((ask + bid) / 2, 4)
        return None, None
    except Exception:
        return None, None

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
    total_raw = len(markets)
    log.info("total markets fetched (raw): " + str(total_raw))
    filtered = []
    for m in markets:
        h = hours_until_expiry(m)
        if h is not None and h <= SCAN_WINDOW_HOURS:
            filtered.append(m)
    log.info("after " + str(SCAN_WINDOW_HOURS) + "h filter: " + str(len(filtered)) + " markets remain (from " + str(total_raw) + " total)")
    return filtered

# ----------------------------------------------------------------
# Price parsing & helpers
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

def hours_until_expiry(market):
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        ed = end_date.replace("Z", "+00:00")
        expiry = datetime.fromisoformat(ed)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        h = (expiry - datetime.now(timezone.utc)).total_seconds() / 3600
        return h if h > 0 else None
    except Exception:
        return None

def tokenize(question):
    words = re.findall(r"[a-zA-Z0-9]+", question.lower())
    stop = {"will","the","a","an","in","of","to","at","is","be","for","on","by","or",
            "and","not","no","vs","who","what","when","does","would","have","has",
            "did","this","that","their","2025","2026","2027","2028"}
    return set(w for w in words if w not in stop and len(w) > 2)

def extract_subjects(question):
    words = question.split()
    caps = set()
    for w in words:
        cleaned = re.sub(r"[^a-zA-Z]", "", w)
        if len(cleaned) > 2 and cleaned[0].isupper() and cleaned.lower() not in SUBJECT_STOPWORDS:
            caps.add(cleaned.lower())
    q_low = question.lower()
    for c in COUNTRIES:
        if c in q_low:
            caps.add(c)
    return caps

def has_exclusive_event(q1, q2):
    q1l = q1.lower()
    q2l = q2.lower()
    for event in EXCLUSIVE_EVENTS:
        if event in q1l and event in q2l:
            return True
    return False

def is_mutually_exclusive(q1, q2):
    if not has_exclusive_event(q1, q2):
        return False
    s1 = extract_subjects(q1)
    s2 = extract_subjects(q2)
    common = s1 & s2
    unique1 = s1 - common
    unique2 = s2 - common
    if len(unique1) >= 1 and len(unique2) >= 1:
        return True
    return False

def has_numeric_threshold(q):
    pattern = r'\$\d+[\.,]?\d*\s*[MBKmb]|\d+\s*(?:million|billion|thousand)'
    return bool(re.search(pattern, q, re.IGNORECASE))

def is_push_worthy(opp):
    urgency = opp.get("urgency", "EARLY")
    edge    = opp.get("edge", 0)
    if urgency == "URGENT":
        return True
    if urgency == "WATCH" and edge >= WATCH_PUSH_MIN:
        return True
    return False

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
    spreads     = []
    now = datetime.now(timezone.utc)
    sampled = 0
    for market in markets:
        if sampled >= SPREAD_SAMPLE:
            break
        tokens = market.get("tokens", [])
        if len(tokens) == 2:
            tid = tokens[0].get("token_id") or tokens[0].get("tokenId")
            if tid:
                sp, mid = clob_get_spread(tid)
                if sp is not None:
                    spreads.append(sp)
                    sampled += 1
                time.sleep(0.1)
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
                h = (expiry - now).total_seconds() / 3600
                if 0 < h < EXPIRY_WINDOW:
                    for p in prices:
                        expiry_gaps.append((h, min(p, 1 - p)))
            except Exception:
                pass
    b_mean, b_std = compute_stats(bundle_sums)
    bundle_threshold = (b_mean - STD_MULTIPLIER * b_std) if b_mean is not None else 0.97
    m_mean, m_std = compute_stats(multi_sums)
    if m_mean is not None:
        multi_lower = m_mean - STD_MULTIPLIER * m_std
        multi_upper = m_mean + STD_MULTIPLIER * m_std
    else:
        multi_lower, multi_upper = 0.97, 1.03
    if expiry_gaps:
        devs = [d for (h, d) in expiry_gaps]
        dev_mean, dev_std = compute_stats(devs)
        expiry_threshold = (dev_mean + STD_MULTIPLIER * dev_std) if dev_mean is not None else 0.15
    else:
        expiry_threshold = 0.15
    spread_mean, spread_std = compute_stats(spreads)
    spread_threshold = (spread_mean + STD_MULTIPLIER * spread_std) if spread_mean is not None else 0.05
    log.info("=== Dynamic Thresholds ===")
    if b_mean is not None:
        log.info("Bundle     -> mean=" + str(round(b_mean,4)) + " std=" + str(round(b_std,4)) + " lower=" + str(round(bundle_threshold,4)))
    if m_mean is not None:
        log.info("Multi  LOW -> lower=" + str(round(multi_lower,4)) + "  HIGH -> upper=" + str(round(multi_upper,4)))
    log.info("Expiry     -> threshold=" + str(round(expiry_threshold,4)))
    if spread_mean is not None:
        log.info("Spread     -> mean=" + str(round(spread_mean,4)) + " threshold=" + str(round(spread_threshold,4)))
    log.info("=========================")
    return bundle_threshold, multi_lower, multi_upper, expiry_threshold, spread_threshold

# ----------------------------------------------------------------
# ARB detectors
# ----------------------------------------------------------------
def detect_bundle(market, threshold):
    prices = parse_prices(market)
    if len(prices) != 2:
        return None
    total = sum(prices)
    edge = round(1.0 - total, 4)
    if total < threshold and edge >= MIN_EDGE:
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
    edge = round(1.0 - total, 4)
    if total < threshold and edge >= MIN_EDGE:
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
    overround = round(total - 1.0, 4)
    if total > threshold and overround >= MIN_EDGE:
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
    h = hours_until_expiry(market)
    if h is None or h >= EXPIRY_WINDOW:
        return None
    prices = parse_prices(market)
    suspicious = [round(p, 4) for p in prices if min(p, 1 - p) > dev_threshold]
    if suspicious:
        urgency_key, urgency_label = get_urgency(h)
        edge = round(max(min(p, 1 - p) for p in suspicious), 4)
        if edge < MIN_EDGE:
            return None
        return {
            "type": urgency_label + " Near-Expiry Mispricing",
            "market": market.get("question", market.get("slug", "N/A")),
            "url": get_url(market),
            "prices": prices,
            "hours_left": round(h, 2),
            "suspicious": suspicious,
            "liquidity": market.get("liquidity", "N/A"),
            "threshold_used": round(dev_threshold, 4),
            "urgency": urgency_key,
            "edge": edge,
            "action": "Check price direction vs reality",
        }
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
        edge = round(1.0 - total, 4)
        if total < threshold and edge >= MIN_EDGE:
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
# Cross-market detector
# ----------------------------------------------------------------
def detect_cross_market(markets, b_lower, b_upper):
    opps = []
    skip_mutex  = 0
    skip_lowsum = 0
    skip_nested = 0
    candidates = []
    for m in markets:
        prices = parse_prices(m)
        if len(prices) == 2:
            candidates.append((m, prices[0]))
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            m1, p1 = candidates[i]
            m2, p2 = candidates[j]
            q1 = m1.get("question", "")
            q2 = m2.get("question", "")
            if not q1 or not q2:
                continue
            t1 = tokenize(q1)
            t2 = tokenize(q2)
            common = t1 & t2
            if len(common) < CROSS_MIN_KEYS:
                continue
            total = round(p1 + p2, 4)
            if total > b_upper:
                overround = round(total - 1.0, 4)
                if overround >= MIN_EDGE:
                    opps.append({
                        "type": "Cross-Market OVER ARB",
                        "market": q1[:60] + " | " + q2[:60],
                        "url": get_url(m1),
                        "url2": get_url(m2),
                        "prices": [p1, p2],
                        "sum": total,
                        "overround": overround,
                        "overround_pct": "{:.2f}%".format(overround * 100),
                        "edge": overround,
                        "liquidity": str(m1.get("liquidity","N/A")) + " / " + str(m2.get("liquidity","N/A")),
                        "threshold_used": round(b_upper, 4),
                        "urgency": "WATCH",
                        "action": "SELL YES on both markets",
                        "common_keywords": list(common)[:5],
                    })
            elif total < b_lower:
                if total < CROSS_MIN_SUM:
                    skip_lowsum += 1
                    continue
                if is_mutually_exclusive(q1, q2):
                    skip_mutex += 1
                    continue
                if has_numeric_threshold(q1) and has_numeric_threshold(q2):
                    skip_nested += 1
                    continue
                edge = round(1.0 - total, 4)
                if edge >= MIN_EDGE:
                    opps.append({
                        "type": "Cross-Market UNDER ARB",
                        "market": q1[:60] + " | " + q2[:60],
                        "url": get_url(m1),
                        "url2": get_url(m2),
                        "prices": [p1, p2],
                        "sum": total,
                        "edge": edge,
                        "edge_pct": "{:.2f}%".format(edge * 100),
                        "liquidity": str(m1.get("liquidity","N/A")) + " / " + str(m2.get("liquidity","N/A")),
                        "threshold_used": round(b_lower, 4),
                        "urgency": "WATCH",
                        "action": "BUY YES on both markets",
                        "common_keywords": list(common)[:5],
                    })
    log.info("Cross-market: skipped " + str(skip_mutex) + " mutex + " + str(skip_lowsum) + " low-sum + " + str(skip_nested) + " nested-threshold pairs")
    return opps

def detect_parent_child(markets):
    opps = []
    BROAD  = {"reach","advance","qualify","enter","make","final","semifinal","playoff","round"}
    NARROW = {"win","champion","winner","title","trophy","gold","first"}
    candidates = []
    for m in markets:
        prices = parse_prices(m)
        if len(prices) == 2:
            q = m.get("question", "").lower()
            words = set(re.findall(r"[a-zA-Z0-9]+", q))
            is_broad  = bool(words & BROAD)
            is_narrow = bool(words & NARROW)
            candidates.append((m, prices[0], is_broad, is_narrow))
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            m1, p1, broad1, narrow1 = candidates[i]
            m2, p2, broad2, narrow2 = candidates[j]
            q1 = m1.get("question", "")
            q2 = m2.get("question", "")
            t1 = tokenize(q1)
            t2 = tokenize(q2)
            if len(t1 & t2) < CROSS_MIN_KEYS:
                continue
            if broad1 and narrow2 and p2 > p1:
                edge = round(p2 - p1, 4)
                if edge >= MIN_EDGE:
                    opps.append({
                        "type": "Parent-Child Contradiction",
                        "market": "PARENT: " + q1[:50] + " | CHILD: " + q2[:50],
                        "url": get_url(m1),
                        "url2": get_url(m2),
                        "prices": [round(p1,4), round(p2,4)],
                        "edge": edge,
                        "edge_pct": "{:.2f}%".format(edge * 100),
                        "liquidity": str(m1.get("liquidity","N/A")) + " / " + str(m2.get("liquidity","N/A")),
                        "threshold_used": MIN_EDGE,
                        "urgency": "WATCH",
                        "action": "BUY parent YES + SELL child YES",
                    })
            elif broad2 and narrow1 and p1 > p2:
                edge = round(p1 - p2, 4)
                if edge >= MIN_EDGE:
                    opps.append({
                        "type": "Parent-Child Contradiction",
                        "market": "PARENT: " + q2[:50] + " | CHILD: " + q1[:50],
                        "url": get_url(m2),
                        "url2": get_url(m1),
                        "prices": [round(p2,4), round(p1,4)],
                        "edge": edge,
                        "edge_pct": "{:.2f}%".format(edge * 100),
                        "liquidity": str(m2.get("liquidity","N/A")) + " / " + str(m1.get("liquidity","N/A")),
                        "threshold_used": MIN_EDGE,
                        "urgency": "WATCH",
                        "action": "BUY parent YES + SELL child YES",
                    })
    return opps

# ----------------------------------------------------------------
# Time-series detector
# ----------------------------------------------------------------
def detect_time_series(markets):
    opps = []
    skip_no_subject = 0
    MONTHS = ["january","february","march","april","may","june",
              "july","august","september","october","november","december",
              "jan","feb","mar","apr","jun","jul","aug","sep","oct","nov","dec",
              "q1","q2","q3","q4","2025","2026","2027","2028"]
    candidates = []
    for m in markets:
        prices = parse_prices(m)
        if len(prices) == 2:
            q = m.get("question", "").lower()
            found_time = [t for t in MONTHS if t in q]
            if found_time:
                stem = re.sub(r"[,\.\?!]", "", q)
                for t in MONTHS:
                    stem = stem.replace(t, "").strip()
                stem = " ".join(stem.split())
                candidates.append((m, prices[0], found_time[0], stem))
    grouped = defaultdict(list)
    for m, p, time_token, stem in candidates:
        sig_words = set(stem.split())
        matched = False
        for key in list(grouped.keys()):
            key_words = set(key.split())
            if len(sig_words & key_words) >= 4:
                grouped[key].append((m, p, time_token))
                matched = True
                break
        if not matched:
            grouped[stem].append((m, p, time_token))
    for stem, group in grouped.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                m1, p1, t1 = group[i]
                m2, p2, t2 = group[j]
                q1 = m1.get("question", "")
                q2 = m2.get("question", "")
                s1 = extract_subjects(q1)
                s2 = extract_subjects(q2)
                if not (s1 & s2):
                    skip_no_subject += 1
                    continue
                idx1 = MONTHS.index(t1) if t1 in MONTHS else 99
                idx2 = MONTHS.index(t2) if t2 in MONTHS else 99
                short_p = p1 if idx1 > idx2 else p2
                long_p  = p2 if idx1 > idx2 else p1
                short_m = m1 if idx1 > idx2 else m2
                long_m  = m2 if idx1 > idx2 else m1
                if short_p > long_p:
                    edge = round(short_p - long_p, 4)
                    if edge >= MIN_EDGE:
                        opps.append({
                            "type": "Time-Series Inversion",
                            "market": short_m.get("question","")[:60] + " | " + long_m.get("question","")[:60],
                            "url": get_url(short_m),
                            "url2": get_url(long_m),
                            "prices": [round(short_p,4), round(long_p,4)],
                            "edge": edge,
                            "edge_pct": "{:.2f}%".format(edge * 100),
                            "liquidity": str(short_m.get("liquidity","N/A")) + " / " + str(long_m.get("liquidity","N/A")),
                            "threshold_used": MIN_EDGE,
                            "urgency": "WATCH",
                            "action": "SELL short-horizon YES + BUY long-horizon YES",
                        })
    log.info("Time-series: skipped " + str(skip_no_subject) + " pairs with no common subject")
    return opps

def detect_price_anchor(markets):
    opps = []
    for market in markets:
        h = hours_until_expiry(market)
        if h is None or h >= 2:
            continue
        prices = parse_prices(market)
        for p in prices:
            if 0.2 <= p <= 0.8:
                edge = round(min(p, 1 - p), 4)
                opps.append({
                    "type": "🔴 URGENT Price Anchor ARB",
                    "market": market.get("question", market.get("slug", "N/A")),
                    "url": get_url(market),
                    "prices": prices,
                    "hours_left": round(h, 2),
                    "edge": edge,
                    "edge_pct": "{:.2f}%".format(edge * 100),
                    "liquidity": market.get("liquidity", "N/A"),
                    "threshold_used": 0.2,
                    "urgency": "URGENT",
                    "action": "CHECK RESULT NOW - price should be near 0 or 1",
                })
                break
    return opps

def detect_liquidity_spread(markets, spread_threshold):
    opps = []
    sampled = 0
    for market in markets:
        if sampled >= SPREAD_SAMPLE:
            break
        tokens = market.get("tokens", [])
        if len(tokens) != 2:
            continue
        tid = tokens[0].get("token_id") or tokens[0].get("tokenId")
        if not tid:
            continue
        sp, mid = clob_get_spread(tid)
        sampled += 1
        if sp is not None and sp > spread_threshold:
            edge = round(sp / 2, 4)
            if edge < MIN_EDGE:
                continue
            opps.append({
                "type": "Liquidity Spread Opportunity",
                "market": market.get("question", market.get("slug", "N/A")),
                "url": get_url(market),
                "prices": [round(mid - sp/2, 4), round(mid + sp/2, 4)],
                "spread": round(sp, 4),
                "midpoint": round(mid, 4),
                "edge": edge,
                "edge_pct": "{:.2f}%".format(edge * 100),
                "liquidity": market.get("liquidity", "N/A"),
                "threshold_used": round(spread_threshold, 4),
                "urgency": "WATCH",
                "action": "PLACE LIMIT ORDERS AT MIDPOINT " + str(round(mid, 4)),
            })
        time.sleep(0.1)
    return opps

def detect_info_arbitrage(markets):
    opps = []
    for market in markets:
        h = hours_until_expiry(market)
        if h is None or h >= 6:
            continue
        prices = parse_prices(market)
        for p in prices:
            if 0.3 <= p <= 0.7:
                edge = round(min(p, 1 - p), 4)
                if edge < MIN_EDGE:
                    continue
                urgency_key, urgency_label = get_urgency(h)
                opps.append({
                    "type": urgency_label + " Info ARB Candidate",
                    "market": market.get("question", market.get("slug", "N/A")),
                    "url": get_url(market),
                    "prices": prices,
                    "hours_left": round(h, 2),
                    "edge": edge,
                    "edge_pct": "{:.2f}%".format(edge * 100),
                    "liquidity": market.get("liquidity", "N/A"),
                    "threshold_used": 0.3,
                    "urgency": urgency_key,
                    "action": "CHECK REAL WORLD STATUS - result may be known",
                })
                break
    return opps

# ----------------------------------------------------------------
# Weather predictor (v8.1: longest-match city scan)
# ----------------------------------------------------------------
def get_open_meteo_prob(lat, lon, condition, target_value):
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,precipitation_probability,windspeed_10m",
                "forecast_days": 3,
                "timezone": "UTC",
            },
            timeout=10
        )
        data = r.json()
        hourly = data.get("hourly", {})
        if condition == "temp_above":
            vals = hourly.get("temperature_2m", [])
            if not vals:
                return None
            count = sum(1 for v in vals if v is not None and v > target_value)
            return round(count / len(vals), 4)
        if condition == "temp_below":
            vals = hourly.get("temperature_2m", [])
            if not vals:
                return None
            count = sum(1 for v in vals if v is not None and v < target_value)
            return round(count / len(vals), 4)
        if condition == "precip_above":
            vals = hourly.get("precipitation_probability", [])
            if not vals:
                return None
            count = sum(1 for v in vals if v is not None and v > target_value)
            return round(count / len(vals), 4)
        if condition == "wind_above":
            vals = hourly.get("windspeed_10m", [])
            if not vals:
                return None
            count = sum(1 for v in vals if v is not None and v > target_value)
            return round(count / len(vals), 4)
        return None
    except Exception as ex:
        log.warning("Open-Meteo error: " + str(ex))
        return None

def parse_weather_question(question):
    q_low = question.lower()
    has_weather = any(kw in q_low for kw in WEATHER_KEYWORDS)
    if not has_weather:
        return None
    city = None
    best_len = 0
    for c in CITY_COORDS:
        if c in q_low and len(c) > best_len:
            city = c
            best_len = len(c)
    if city is None:
        return None
    num_match = re.search(r"(-?\d+\.?\d*)", question)
    threshold = float(num_match.group(1)) if num_match else 0.0
    condition = "temp_above"
    if any(w in q_low for w in ["rain","precipitation","flood","snow","blizzard","snowfall","rainfall"]):
        condition = "precip_above"
    elif any(w in q_low for w in ["wind","storm","hurricane","typhoon","cyclone","tornado","mph","landfall"]):
        condition = "wind_above"
    elif any(w in q_low for w in ["below","cold","freeze","freezing","low temp","record low"]):
        condition = "temp_below"
    elif any(w in q_low for w in ["above","exceed","hot","warm","high","heat","reaches","record high","heatwave"]):
        condition = "temp_above"
    return {"city": city, "condition": condition, "threshold": threshold}

def detect_weather_markets(markets):
    opps = []
    scanned = 0
    for market in markets:
        q = market.get("question", "")
        parsed = parse_weather_question(q)
        if parsed is None:
            continue
        scanned += 1
        city = parsed["city"]
        coords = CITY_COORDS.get(city)
        if coords is None:
            continue
        lat, lon = coords
        model_prob = get_open_meteo_prob(lat, lon, parsed["condition"], parsed["threshold"])
        if model_prob is None:
            continue
        prices = parse_prices(market)
        if len(prices) != 2:
            continue
        market_price = prices[0]
        edge = round(abs(model_prob - market_price), 4)
        if edge < WEATHER_EDGE_MIN:
            continue
        h = hours_until_expiry(market)
        urgency_key, urgency_label = get_urgency(h) if h else ("WATCH", "🟡 WATCH")
        direction = "BUY YES" if model_prob > market_price else "BUY NO"
        confidence = "HIGH" if edge > 0.20 else "MEDIUM"
        opps.append({
            "type": urgency_label + " Weather Prediction",
            "market": q,
            "url": get_url(market),
            "prices": prices,
            "model_prob": model_prob,
            "market_price": market_price,
            "edge": edge,
            "edge_pct": "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
            "threshold_used": WEATHER_EDGE_MIN,
            "urgency": urgency_key,
            "action": direction,
            "confidence": confidence,
            "city": city,
            "condition": parsed["condition"],
            "hours_left": round(h, 2) if h else None,
        })
    log.info("Weather: found " + str(len(opps)) + " opportunities from " + str(scanned) + " weather markets scanned")
    return opps

# ----------------------------------------------------------------
# Sports predictor (v8.1: The Odds API)
# ----------------------------------------------------------------
_odds_cache = {}

def get_odds_for_sport(sport_key):
    if sport_key in _odds_cache:
        return _odds_cache[sport_key]
    if not ODDS_API_KEY:
        return []
    try:
        r = SESSION.get(
            ODDS_API_BASE + "/sports/" + sport_key + "/odds/",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us,uk",
                "markets": "h2h",
                "oddsFormat": "decimal",
                "dateFormat": "iso",
            },
            timeout=10
        )
        remaining = r.headers.get("x-requests-remaining", "?")
        log.info("Odds API [" + sport_key + "] remaining requests: " + str(remaining))
        if r.status_code == 200:
            games = r.json()
            _odds_cache[sport_key] = games
            return games
        else:
            log.warning("Odds API error " + sport_key + ": " + r.text[:100])
            return []
    except Exception as ex:
        log.warning("Odds API exception: " + str(ex))
        return []

def decimal_to_prob(decimal_odds):
    if decimal_odds and decimal_odds > 1.0:
        return round(1.0 / decimal_odds, 4)
    return 0.0

def normalize_team_name(name):
    return re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()

def find_team_in_game(game, team_hint):
    hint = normalize_team_name(team_hint)
    home = normalize_team_name(game.get("home_team", ""))
    away = normalize_team_name(game.get("away_team", ""))
    if hint in home or home in hint:
        return game.get("home_team")
    if hint in away or away in hint:
        return game.get("away_team")
    hint_words = set(hint.split())
    home_words = set(home.split())
    away_words = set(away.split())
    if len(hint_words & home_words) >= 1:
        return game.get("home_team")
    if len(hint_words & away_words) >= 1:
        return game.get("away_team")
    return None

def get_best_market_prob(game, team_name):
    team_norm = normalize_team_name(team_name)
    probs = []
    for bm in game.get("bookmakers", []):
        for market in bm.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                if normalize_team_name(outcome.get("name", "")) == team_norm:
                    p = decimal_to_prob(outcome.get("price", 0))
                    if p > 0:
                        probs.append(p)
    if not probs:
        return None
    return round(sum(probs) / len(probs), 4)

def parse_sports_question(question):
    q_low = question.lower()
    sport_key = None
    for kw in ODDS_SPORT_MAP:
        if kw in q_low:
            sport_key = ODDS_SPORT_MAP[kw]
            break
    if sport_key is None:
        for kw in SPORTS_KEYWORDS:
            if kw in q_low:
                sport_key = "americanfootball_nfl"
                break
    if sport_key is None:
        return None, []
    words = question.split()
    teams = []
    for w in words:
        cleaned = re.sub(r"[^a-zA-Z]", "", w)
        if len(cleaned) > 2 and cleaned[0].isupper() and cleaned.lower() not in SUBJECT_STOPWORDS:
            teams.append(cleaned)
    return sport_key, teams

def detect_sports_markets(markets):
    if not ODDS_API_KEY:
        log.warning("Sports: ODDS_API_KEY not set, skipping")
        return []
    opps = []
    scanned = 0
    for market in markets:
        q = market.get("question", "")
        sport_key, teams = parse_sports_question(q)
        if sport_key is None:
            continue
        scanned += 1
        games = get_odds_for_sport(sport_key)
        if not games:
            continue
        prices = parse_prices(market)
        if len(prices) != 2:
            continue
        market_price = prices[0]
        best_edge = 0
        best_opp = None
        for game in games:
            for team_hint in teams:
                matched = find_team_in_game(game, team_hint)
                if matched is None:
                    continue
                implied_prob = get_best_market_prob(game, matched)
                if implied_prob is None:
                    continue
                edge = round(abs(implied_prob - market_price), 4)
                if edge < SPORTS_EDGE_MIN:
                    continue
                if edge > best_edge:
                    best_edge = edge
                    h = hours_until_expiry(market)
                    urgency_key, urgency_label = get_urgency(h) if h else ("WATCH", "🟡 WATCH")
                    direction = "BUY YES" if implied_prob > market_price else "BUY NO"
                    confidence = "HIGH" if edge > 0.10 else "MEDIUM"
                    home = game.get("home_team", "")
                    away = game.get("away_team", "")
                    best_opp = {
                        "type": urgency_label + " Sports Prediction",
                        "market": q,
                        "url": get_url(market),
                        "prices": prices,
                        "bookmaker_prob": implied_prob,
                        "market_price": market_price,
                        "edge": best_edge,
                        "edge_pct": "{:.2f}%".format(best_edge * 100),
                        "liquidity": market.get("liquidity", "N/A"),
                        "threshold_used": SPORTS_EDGE_MIN,
                        "urgency": urgency_key,
                        "action": direction,
                        "confidence": confidence,
                        "matchup": home + " vs " + away,
                        "hours_left": round(h, 2) if h else None,
                    }
        if best_opp:
            opps.append(best_opp)
    log.info("Sports: found " + str(len(opps)) + " opportunities from " + str(scanned) + " sports markets scanned")
    return opps

# ----------------------------------------------------------------
# Output formatting
# ----------------------------------------------------------------
URGENCY_ORDER = {"URGENT": 0, "WATCH": 1, "EARLY": 2}

def sort_opps(opps):
    return sorted(opps, key=lambda o: (URGENCY_ORDER.get(o.get("urgency","EARLY"),2), -o.get("edge",0)))

def fmt_opp(opp, idx):
    price_list = opp.get("prices") or opp.get("prices_clob", [])
    out = "=" * 60 + "\n"
    out += "#{:02d} {}\n".format(idx, opp["type"])
    out += "Market    : " + opp["market"][:80] + "\n"
    if opp.get("url2"):
        out += "URL 1     : " + opp.get("url","N/A") + "\n"
        out += "URL 2     : " + opp.get("url2","N/A") + "\n"
    else:
        out += "URL       : " + opp.get("url","N/A") + "\n"
    out += "Liquidity : $" + str(opp.get("liquidity","N/A")) + "\n"
    out += "Action    : " + opp.get("action","N/A") + "\n"
    if "model_prob" in opp:
        out += "Model Prob: " + str(opp["model_prob"]) + "  Market: " + str(opp["market_price"]) + "\n"
    if "bookmaker_prob" in opp:
        out += "Bookmaker : " + str(opp["bookmaker_prob"]) + "  Market: " + str(opp["market_price"]) + "\n"
    if "matchup" in opp:
        out += "Matchup   : " + opp["matchup"] + "\n"
    if "confidence" in opp:
        out += "Confidence: " + opp["confidence"] + "\n"
    if "overround" in opp:
        out += "Sum       : " + str(opp["sum"]) + "  Overround: " + opp.get("overround_pct","N/A") + "\n"
    elif "spread" in opp:
        out += "Spread    : " + str(opp["spread"]) + "  Midpoint: " + str(opp.get("midpoint","N/A")) + "\n"
    else:
        out += "Edge      : " + opp.get("edge_pct","N/A") + "\n"
    out += "Prices    : " + str(price_list) + "\n"
    if "hours_left" in opp and opp["hours_left"] is not None:
        out += "Expires in: " + str(opp["hours_left"]) + "h\n"
    if "common_keywords" in opp:
        out += "Keywords  : " + str(opp["common_keywords"]) + "\n"
    return out

def build_tg_msg(push_opps, total_opps, total_markets, thresholds, filtered_n):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    urgent_n = sum(1 for o in push_opps if o.get("urgency") == "URGENT")
    watch_n  = sum(1 for o in push_opps if o.get("urgency") == "WATCH")
    msg = "<b>Polymarket ARB Scanner v8.1 - " + str(len(push_opps)) + " alerts</b>\n"
    msg += "Time: " + now_str + "\n"
    msg += "Markets scanned (&lt;=72h): " + str(total_markets) + "\n"
    msg += "🔴 Urgent: " + str(urgent_n) + "  🟡 Watch(≥2%): " + str(watch_n) + "\n"
    msg += "Filtered out: " + str(filtered_n) + " (EARLY / edge&lt;2% / mutex / nested)\n"
    msg += "Thresholds: " + thresholds + "\n\n"
    for idx, opp in enumerate(push_opps[:10], 1):
        url  = opp.get("url", "#")
        url2 = opp.get("url2", "")
        msg += "<b>#" + str(idx) + " " + opp["type"] + "</b>\n"
        msg += opp.get("market","N/A")[:60] + "\n"
        msg += "Action: " + opp.get("action","N/A") + "\n"
        if "model_prob" in opp:
            msg += "Model: " + str(opp["model_prob"]) + " vs Market: " + str(opp["market_price"]) + "\n"
        elif "bookmaker_prob" in opp:
            msg += "Bookmaker: " + str(opp["bookmaker_prob"]) + " vs Market: " + str(opp["market_price"]) + "\n"
        elif "overround" in opp:
            msg += "Overround: " + opp["overround_pct"] + "  Sum: " + str(opp["sum"]) + "\n"
        elif "spread" in opp:
            msg += "Spread: " + str(opp["spread"]) + "  Mid: " + str(opp.get("midpoint","")) + "\n"
        else:
            msg += "Edge: " + opp.get("edge_pct","N/A") + "\n"
        if "confidence" in opp:
            msg += "Confidence: " + opp["confidence"] + "\n"
        msg += "Prices: " + str(opp.get("prices") or opp.get("prices_clob",[])) + "\n"
        if opp.get("hours_left") is not None:
            msg += "Expires: " + str(opp["hours_left"]) + "h\n"
        msg += '<a href="' + url + '">URL1</a>'
        if url2:
            msg += '  <a href="' + url2 + '">URL2</a>'
        msg += "\n\n"
    if len(push_opps) > 10:
        msg += "<i>... and " + str(len(push_opps) - 10) + " more in arb_report.json</i>\n"
    return msg

# ----------------------------------------------------------------
# Main scan
# ----------------------------------------------------------------
def scan():
    log.info("=" * 60)
    log.info("Polymarket ARB Scanner v8.1 - weather longest-match fix")
    log.info("MIN_EDGE=" + str(MIN_EDGE) + " WATCH_PUSH_MIN=" + str(WATCH_PUSH_MIN) + " SCAN_WINDOW=" + str(SCAN_WINDOW_HOURS) + "h")
    log.info("NOAA_KEY=" + ("SET" if NOAA_API_KEY else "NOT SET") + "  ODDS_KEY=" + ("SET" if ODDS_API_KEY else "NOT SET"))
    log.info("WEATHER_EDGE=" + str(WEATHER_EDGE_MIN) + "  SPORTS_EDGE=" + str(SPORTS_EDGE_MIN))
    log.info("Time: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("=" * 60)

    markets = fetch_all_markets()
    if not markets:
        log.error("No markets fetched or none within 72h window")
        send_telegram("No markets within 72h window - nothing to scan")
        return

    log.info("Computing dynamic thresholds...")
    b_thr, m_low, m_high, e_thr, sp_thr = analyze_markets(markets)

    opps = []
    bundle_candidates = []

    log.info("Scanning " + str(len(markets)) + " markets...")
    for i, market in enumerate(markets):
        if i % 200 == 0 and i > 0:
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

    log.info("CLOB confirming " + str(len(bundle_candidates)) + " bundle candidates...")
    for market, prelim in bundle_candidates[:MAX_CLOB_CONFIRM]:
        confirmed = detect_clob_confirmed(market, b_thr)
        opps.append(confirmed if confirmed else prelim)
        time.sleep(REQUEST_DELAY)

    log.info("Running cross-market detectors...")
    opps += detect_cross_market(markets, b_thr, m_high)
    opps += detect_parent_child(markets)
    opps += detect_time_series(markets)
    opps += detect_price_anchor(markets)
    opps += detect_liquidity_spread(markets, sp_thr)
    opps += detect_info_arbitrage(markets)

    log.info("Running weather predictor...")
    opps += detect_weather_markets(markets)
    log.info("Running sports predictor...")
    opps += detect_sports_markets(markets)

    seen = set()
    unique_opps = []
    for o in opps:
        key = o.get("type","") + "|" + o.get("url","")
        if key not in seen:
            seen.add(key)
            unique_opps.append(o)
    opps = sort_opps(unique_opps)

    push_opps     = [o for o in opps if is_push_worthy(o)]
    filtered_opps = [o for o in opps if not is_push_worthy(o)]
    filtered_n    = len(filtered_opps)

    urgent_n = sum(1 for o in opps if o.get("urgency") == "URGENT")
    watch_n  = sum(1 for o in opps if o.get("urgency") == "WATCH")
    early_n  = sum(1 for o in opps if o.get("urgency") == "EARLY")
    pushed_n = len(push_opps)

    log.info("=" * 60)
    log.info("DONE - " + str(len(opps)) + " total opportunities")
    log.info("  🔴 URGENT   : " + str(urgent_n) + "  (all pushed)")
    log.info("  🟡 WATCH    : " + str(watch_n) + "  (pushed if edge>=2%)")
    log.info("  🟢 EARLY    : " + str(early_n) + "  (file only)")
    log.info("  📤 Pushed   : " + str(pushed_n))
    log.info("  📁 Filtered : " + str(filtered_n))
    log.info("=" * 60)

    thr_str = "B=" + str(round(b_thr,4)) + " ML=" + str(round(m_low,4)) + " MH=" + str(round(m_high,4)) + " E=" + str(round(e_thr,4))
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    for idx, opp in enumerate(opps, 1):
        print(fmt_opp(opp, idx))

    if not push_opps:
        log.info("Nothing push-worthy this round")
        send_telegram(
            "<b>Polymarket ARB Scanner v8.1</b>\n"
            + "Time: " + now_str + "\n"
            + "Markets scanned (&lt;=72h): " + str(len(markets)) + "\n"
            + "Thresholds: " + thr_str + "\n"
            + "🔴 Urgent: 0  🟡 Watch(≥2%): 0\n"
            + "Filtered (EARLY/low-edge/mutex/nested): " + str(filtered_n) + "\n"
            + "Result: No high-priority opportunities"
        )
    else:
        send_telegram(build_tg_msg(push_opps, len(opps), len(markets), thr_str, filtered_n))

    report = {
        "scan_time": datetime.now(timezone.utc).isoformat(),
        "scan_window_hours": SCAN_WINDOW_HOURS,
        "total_markets_scanned": len(markets),
        "min_edge_filter": MIN_EDGE,
        "watch_push_min": WATCH_PUSH_MIN,
        "cross_min_sum": CROSS_MIN_SUM,
        "dynamic_thresholds": {
            "bundle":      round(b_thr,4),
            "multi_lower": round(m_low,4),
            "multi_upper": round(m_high,4),
            "near_expiry": round(e_thr,4),
            "spread":      round(sp_thr,4),
        },
        "opportunities_summary": {
            "total":    len(opps),
            "pushed":   pushed_n,
            "filtered": filtered_n,
            "urgent":   urgent_n,
            "watch":    watch_n,
            "early":    early_n,
        },
        "opportunities": opps,
    }
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("report saved -> " + str(len(opps)) + " total, " + str(pushed_n) + " pushed")


if __name__ == "__main__":
    scan()
