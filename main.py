import os
import time
import json
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
# Telegram
# =========================
def send(msg: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[WARN] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        print(msg)
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg[:4000]},
            timeout=5
        )
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")
        print(msg)


# =========================
# Robust GET
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
# Fetch markets with deep pagination
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
                "error": text[:200]
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
                "error": f"Unexpected type: {type(data).__name__}"
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
# Parse YES price safely
# Handles:
# 1) outcomePrices = '["0.27","0.73"]'
# 2) outcomes list of dicts
# 3) outcomes string + outcomePrices string
# =========================
def parse_yes_price(m):
    # Case A: outcomePrices JSON string
    try:
        op = m.get("outcomePrices")
        if isinstance(op, str) and op.strip():
            parsed = json.loads(op)
            if isinstance(parsed, list) and len(parsed) >= 1:
                return float(parsed[0]), "outcomePrices_json"
    except Exception:
        pass

    # Case B: outcomes = list[dict]
    try:
        outcomes = m.get("outcomes")
        if isinstance(outcomes, list):
            for o in outcomes:
                if isinstance(o, dict):
                    name = str(o.get("name", "")).strip().lower()
                    if name == "yes":
                        return float(o.get("price", 0)), "outcomes_list_dict"
    except Exception:
        pass

    # Case C: outcomes = "Yes,No" + outcomePrices = "0.27,0.73"
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
# Diagnostics
# =========================
def build_diagnostics(markets):
    diag = {
        "total": len(markets),
        "unique_ids": 0,
        "with_slug": 0,
        "with_group": 0,
        "with_condition": 0,
        "with_events": 0,
        "yes_price_ok": 0,
        "yes_price_fail": 0,
        "yes_price_by_mode": defaultdict(int),
        "temperature_like": 0,
        "top_groups": [],
        "sample_markets": [],
    }

    ids = set()
    groups = defaultdict(int)

    for m in markets:
        mid = m.get("id")
        if mid:
            ids.add(str(mid))

        if m.get("slug"):
            diag["with_slug"] += 1

        if m.get("groupItemTitle"):
            diag["with_group"] += 1
            groups[str(m["groupItemTitle"])] += 1

        if m.get("conditionId"):
            diag["with_condition"] += 1

        if m.get("events"):
            diag["with_events"] += 1

        yes_price, mode = parse_yes_price(m)
        if yes_price is not None:
            diag["yes_price_ok"] += 1
            diag["yes_price_by_mode"][mode] += 1
        else:
            diag["yes_price_fail"] += 1

        q = str(m.get("question", "")).lower()
        g = str(m.get("groupItemTitle", "")).lower()

        if any(k in q or k in g for k in [
            "temperature", "temp", "highest temperature", "°c", "°f", "fahrenheit", "celsius"
        ]):
            diag["temperature_like"] += 1

        if len(diag["sample_markets"]) < 10:
            diag["sample_markets"].append({
                "question": m.get("question", ""),
                "groupItemTitle": m.get("groupItemTitle"),
                "slug": m.get("slug"),
                "conditionId": m.get("conditionId"),
                "events_type": type(m.get("events")).__name__,
                "outcomes_type": type(m.get("outcomes")).__name__,
                "outcomePrices_type": type(m.get("outcomePrices")).__name__,
            })

    diag["unique_ids"] = len(ids)
    diag["top_groups"] = sorted(groups.items(), key=lambda x: x[1], reverse=True)[:10]
    diag["yes_price_by_mode"] = dict(diag["yes_price_by_mode"])

    return diag


# =========================
# Temperature-like market dump
# =========================
def collect_temperature_examples(markets, limit=10):
    out = []

    for m in markets:
        q = str(m.get("question", "")).lower()
        g = str(m.get("groupItemTitle", "")).lower()

        if any(k in q or k in g for k in [
            "temperature", "temp", "highest temperature", "°c", "°f", "fahrenheit", "celsius"
        ]):
            yes_price, mode = parse_yes_price(m)

            out.append({
                "question": m.get("question", ""),
                "groupItemTitle": m.get("groupItemTitle"),
                "slug": m.get("slug"),
                "conditionId": m.get("conditionId"),
                "yes_price": yes_price,
                "yes_mode": mode,
                "outcomes_raw_type": type(m.get("outcomes")).__name__,
                "outcomePrices_raw_type": type(m.get("outcomePrices")).__name__,
            })

            if len(out) >= limit:
                break

    return out


# =========================
# Main
# =========================
def main():
    ts = int(time.time())

    raw, pages = fetch_markets()

    # de-dup by id
    uniq = {}
    for m in raw:
        mid = m.get("id")
        if mid:
            uniq[str(mid)] = m
    markets = list(uniq.values())

    diag = build_diagnostics(markets)
    temp_examples = collect_temperature_examples(markets, limit=8)

    # Console logs for GitHub Actions
    print("=== PAGE REPORTS ===")
    for p in pages:
        print(p)

    print("=== DIAGNOSTICS ===")
    print(json.dumps(diag, ensure_ascii=False, indent=2))

    print("=== TEMPERATURE EXAMPLES ===")
    print(json.dumps(temp_examples, ensure_ascii=False, indent=2))

    # Telegram summary
    lines = [
        f"✅ Read-only check @ {ts}",
        f"📊 Raw fetched: {len(raw)}",
        f"✅ Unique markets: {diag['unique_ids']}",
        f"🔗 with slug: {diag['with_slug']}",
        f"🧩 with groupItemTitle: {diag['with_group']}",
        f"🧠 with conditionId: {diag['with_condition']}",
        f"📚 with events: {diag['with_events']}",
        f"💲 YES parsed OK: {diag['yes_price_ok']}",
        f"⚠️ YES parse failed: {diag['yes_price_fail']}",
        f"🌡️ temperature-like markets: {diag['temperature_like']}",
        f"🛠️ YES parse modes: {diag['yes_price_by_mode']}",
    ]

    if diag["top_groups"]:
        lines.append("📌 Top groups:")
        for g, c in diag["top_groups"][:5]:
            short = g if len(g) <= 50 else g[:47] + "..."
            lines.append(f"- {short} ({c})")

    if temp_examples:
        lines.append("🌡️ Sample temp-like:")
        for t in temp_examples[:3]:
            q = t["question"]
            short_q = q if len(q) <= 50 else q[:47] + "..."
            lines.append(f"- yes={t['yes_price']} | {short_q}")

    send("\n".join(lines))


if __name__ == "__main__":
    main()
