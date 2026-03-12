import os
import time
import json
import re
import requests
from collections import defaultdict

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

GAMMA_BASE = "https://gamma-api.polymarket.com"
USER_AGENT = "Mozilla/5.0 (compatible; PolymarketReadOnlyVerifier/1.0)"
PAGE_SIZE = 300
MAX_OFFSET = 2400
TIMEOUT = 10

# =========================
# 严格天气市场识别规则（最小改动替换原 TEMP_KEYWORDS 逻辑）
# =========================
WEATHER_INCLUDE_PATTERNS = [
    r"\bhighest temperature\b",
    r"\bhigh temperature\b",
    r"\btemperature in\b",
    r"\bhighest temp\b",
    r"\bhigh temp\b",
]

WEATHER_UNIT_PATTERNS = [
    r"°c",
    r"°f",
    r"\bcelsius\b",
    r"\bfahrenheit\b",
]

WEATHER_EXCLUDE_KEYWORDS = [
    "btc", "eth", "sol", "xrp", "doge",
    "coinbase", "defi", "finance", "market cap",
    "dominance", "nasdaq", "dow", "s&p", "stock",
    "price target", "earnings", "revenue", "valuation",
    "global temperature", "average global temperature",
    "coingecko", "token", "crypto", "coin"
]

# =========================
# Telegram
# =========================
def send(msg: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[WARN] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        print(msg)
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg[:4000]},
            timeout=5
        )
        print(f"[INFO] Telegram status={r.status_code}")
        print(f"[INFO] Telegram response={r.text[:500]}")
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")
        print(msg)

# =========================
# Safe GET
# =========================
def safe_get(url, params=None):
    try:
        r = requests.get(
            url,
            params=params or {},
            headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT
        )
        return r.status_code, r.text
    except Exception as e:
        return None, str(e)

# =========================
# Deep pagination fetch
# =========================
def fetch_markets():
    raw = []
    page_reports = []

    for offset in range(0, MAX_OFFSET, PAGE_SIZE):
        status, text = safe_get(
            f"{GAMMA_BASE}/markets",
            params={
                "active": "true",
                "limit": PAGE_SIZE,
                "offset": offset
            }
        )

        if status != 200:
            page_reports.append({
                "offset": offset,
                "status": status,
                "count": 0,
                "ok": False,
                "error": str(text)[:200]
            })
            continue

        try:
            data = json.loads(text)
        except Exception as e:
            page_reports.append({
                "offset": offset,
                "status": status,
                "count": 0,
                "ok": False,
                "error": f"JSON parse error: {e}"
            })
            continue

        if not isinstance(data, list):
            page_reports.append({
                "offset": offset,
                "status": status,
                "count": 0,
                "ok": False,
                "error": f"Unexpected payload type: {type(data).__name__}"
            })
            continue

        if len(data) == 0:
            page_reports.append({
                "offset": offset,
                "status": status,
                "count": 0,
                "ok": True,
                "error": None
            })
            break

        raw.extend(data)
        page_reports.append({
            "offset": offset,
            "status": status,
            "count": len(data),
            "ok": True,
            "error": None
        })

    return raw, page_reports

# =========================
# Parse YES price robustly
#
# Supports common shapes:
# 1) outcomePrices = '["0.27","0.73"]'
# 2) outcomes = [{"name":"Yes","price":"0.27"}, ...]
# 3) outcomes = "Yes,No" + outcomePrices = "0.27,0.73"
# =========================
def parse_yes_price(m):
    # Case 1: outcomePrices as JSON string
    try:
        op = m.get("outcomePrices")
        if isinstance(op, str) and op.strip():
            parsed = json.loads(op)
            if isinstance(parsed, list) and len(parsed) >= 1:
                return float(parsed[0]), "outcomePrices_json"
    except Exception:
        pass

    # Case 2: outcomes list of dicts
    try:
        outcomes = m.get("outcomes")
        if isinstance(outcomes, list):
            for o in outcomes:
                if isinstance(o, dict):
                    if str(o.get("name", "")).strip().lower() == "yes":
                        return float(o.get("price", 0)), "outcomes_list_dict"
    except Exception:
        pass

    # Case 3: outcomes string + outcomePrices string
    try:
        outcomes = m.get("outcomes")
        op = m.get("outcomePrices")
        if isinstance(outcomes, str) and isinstance(op, str):
            names = [x.strip() for x in outcomes.split(",")]
            prices = [x.strip() for x in op.split(",")]
            if len(names) == len(prices):
                for i, name in enumerate(names):
                    if name.lower() == "yes":
                        return float(prices[i]), "outcomes_string"
    except Exception:
        pass

    return None, None

# =========================
# 严格天气检测（替换原 is_temperature_like）
# =========================
def is_strict_weather_market(m):
    q = str(m.get("question", "")).lower()
    g = str(m.get("groupItemTitle", "")).lower()
    text = f"{q} || {g}"

    # 排除明显不是天气的
    for bad in WEATHER_EXCLUDE_KEYWORDS:
        if bad in text:
            return False

    matched_weather = any(re.search(p, text) for p in WEATHER_INCLUDE_PATTERNS)
    matched_unit = any(re.search(p, text) for p in WEATHER_UNIT_PATTERNS)

    if matched_weather:
        return True

    if ("temperature" in text or "temp" in text) and matched_unit:
        return True

    return False

# =========================
# Group key for read-only snapshots
# 只做天气市场的人工核验，不做套利判断
# =========================
def snapshot_group_key(m):
    group_title = str(m.get("groupItemTitle") or "").strip()
    end_date = str(m.get("endDate") or m.get("end") or "").strip()

    if group_title:
        return f"{group_title} || {end_date}"

    q = str(m.get("question") or "").strip()
    if q:
        return f"{q[:80]} || {end_date}"

    return None

# =========================
# Build diagnostics
# =========================
def build_diagnostics(markets):
    diag = {
        "total": len(markets),
        "unique_ids": 0,
        "with_slug": 0,
        "with_groupItemTitle": 0,
        "with_conditionId": 0,
        "with_events": 0,
        "yes_price_ok": 0,
        "yes_price_fail": 0,
        "yes_price_modes": defaultdict(int),
        "strict_weather_count": 0,
        "top_groups": [],
    }

    ids = set()
    groups_counter = defaultdict(int)

    for m in markets:
        mid = m.get("id")
        if mid:
            ids.add(str(mid))

        if m.get("slug"):
            diag["with_slug"] += 1

        if m.get("groupItemTitle"):
            diag["with_groupItemTitle"] += 1
            groups_counter[str(m["groupItemTitle"])] += 1

        if m.get("conditionId"):
            diag["with_conditionId"] += 1

        if m.get("events"):
            diag["with_events"] += 1

        yes_price, mode = parse_yes_price(m)
        if yes_price is not None:
            diag["yes_price_ok"] += 1
            diag["yes_price_modes"][mode] += 1
        else:
            diag["yes_price_fail"] += 1

        if is_strict_weather_market(m):
            diag["strict_weather_count"] += 1

    diag["unique_ids"] = len(ids)
    diag["yes_price_modes"] = dict(diag["yes_price_modes"])
    diag["top_groups"] = sorted(groups_counter.items(), key=lambda x: x[1], reverse=True)[:10]

    return diag

# =========================
# Collect strict weather market examples
# =========================
def collect_strict_weather_examples(markets, limit=12):
    out = []

    for m in markets:
        if not is_strict_weather_market(m):
            continue

        yes_price, mode = parse_yes_price(m)

        out.append({
            "question": m.get("question", ""),
            "groupItemTitle": m.get("groupItemTitle"),
            "slug": m.get("slug"),
            "conditionId": m.get("conditionId"),
            "endDate": m.get("endDate") or m.get("end"),
            "yes_price": yes_price,
            "yes_mode": mode,
            "liquidity": m.get("liquidity"),
        })

        if len(out) >= limit:
            break

    return out

# =========================
# Build grouped snapshots (strict weather only)
# 只做“每组有几个 bucket、每个 bucket 的 YES 值”
# 不做任何套利阈值判断
# =========================
def build_group_snapshots(markets, min_group_size=2, limit_groups=10):
    groups = defaultdict(list)

    for m in markets:
        # ✅ 关键：只对严格天气市场做分组
        if not is_strict_weather_market(m):
            continue

        key = snapshot_group_key(m)
        if not key:
            continue

        yes_price, mode = parse_yes_price(m)
        if yes_price is None:
            continue

        groups[key].append({
            "question": m.get("question", ""),
            "yes": yes_price,
            "slug": m.get("slug", ""),
            "liquidity": m.get("liquidity"),
            "mode": mode,
        })

    filtered = []
    for key, items in groups.items():
        if len(items) >= min_group_size:
            filtered.append((key, items))

    filtered.sort(key=lambda x: len(x[1]), reverse=True)

    snapshots = []
    for key, items in filtered[:limit_groups]:
        snapshots.append({
            "group_key": key,
            "bucket_count": len(items),
            "sum_yes": round(sum(x["yes"] for x in items), 6),
            "items": [
                {
                    "yes": round(x["yes"], 6),
                    "question": x["question"][:100],
                    "slug": x["slug"],
                }
                for x in items[:12]
            ]
        })

    return snapshots

# =========================
# Main
# =========================
def main():
    ts = int(time.time())

    raw, page_reports = fetch_markets()

    # de-dup by id
    uniq = {}
    for m in raw:
        mid = m.get("id")
        if mid:
            uniq[str(mid)] = m
    markets = list(uniq.values())

    diag = build_diagnostics(markets)
    weather_examples = collect_strict_weather_examples(markets, limit=10)
    group_snapshots = build_group_snapshots(markets, min_group_size=2, limit_groups=8)

    # Console logs (GitHub Actions)
    print("=== PAGE REPORTS ===")
    for p in page_reports:
        print(p)

    print("=== DIAGNOSTICS ===")
    print(json.dumps(diag, ensure_ascii=False, indent=2))

    print("=== STRICT WEATHER EXAMPLES ===")
    print(json.dumps(weather_examples, ensure_ascii=False, indent=2))

    print("=== WEATHER GROUP SNAPSHOTS ===")
    print(json.dumps(group_snapshots, ensure_ascii=False, indent=2))

    # Telegram summary
    lines = [
        f"✅ Strict weather discovery @ {ts}",
        f"📊 Raw fetched: {len(raw)}",
        f"✅ Unique markets: {diag['unique_ids']}",
        f"🔗 with slug: {diag['with_slug']}",
        f"🧩 with groupItemTitle: {diag['with_groupItemTitle']}",
        f"🧠 with conditionId: {diag['with_conditionId']}",
        f"📚 with events: {diag['with_events']}",
        f"💲 YES parsed OK: {diag['yes_price_ok']}",
        f"⚠️ YES parse failed: {diag['yes_price_fail']}",
        f"🌡️ strict weather markets: {diag['strict_weather_count']}",
        f"🛠️ YES parse modes: {diag['yes_price_modes']}",
    ]

    if diag["top_groups"]:
        lines.append("📌 Top groups:")
        for g, c in diag["top_groups"][:5]:
            short = g if len(g) <= 50 else g[:47] + "..."
            lines.append(f"- {short} ({c})")

    if weather_examples:
        lines.append("🌡️ Weather samples:")
        for t in weather_examples[:3]:
            q = t["question"]
            short_q = q if len(q) <= 48 else q[:45] + "..."
            lines.append(f"- yes={t['yes_price']} | {short_q}")
    else:
        lines.append("🌡️ Weather samples: NONE")

    if group_snapshots:
        lines.append("🧪 Weather snapshots:")
        for s in group_snapshots[:3]:
            g = s["group_key"]
            short_g = g if len(g) <= 45 else g[:42] + "..."
            lines.append(f"- n={s['bucket_count']} sum={s['sum_yes']} | {short_g}")
    else:
        lines.append("🧪 Weather snapshots: NONE")

    send("\n".join(lines))


if __name__ == "__main__":
    main()
