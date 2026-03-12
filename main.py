import os
import time
import requests
from collections import defaultdict

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# =========================
# Telegram
# =========================
def send(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg},
            timeout=5
        )
    except Exception as e:
        print(f"[WARN] Telegram failed: {e}")

# =========================
# YES Price Parser (Dual Format)
# =========================
def get_yes_price(m):
    try:
        outcomes = m.get("outcomes")

        # Format A: list of dicts
        if isinstance(outcomes, list):
            for o in outcomes:
                if isinstance(o, dict):
                    name = str(o.get("name", "")).strip().lower()
                    if name == "yes":
                        return float(o.get("price", 0))

        # Format B: "Yes,No" + outcomePrices
        if isinstance(outcomes, str):
            names = [x.strip() for x in outcomes.split(",")]
            prices_raw = str(m.get("outcomePrices", ""))
            prices = [x.strip() for x in prices_raw.split(",")]
            if len(names) == len(prices):
                for i, name in enumerate(names):
                    if name.lower() == "yes":
                        return float(prices[i])

    except Exception as e:
        print(f"[WARN] get_yes_price failed: {e}")

    return None

# =========================
# Group Key
# Polymarket Partition 必须用 conditionId 分组
# fallback 到 groupItemTitle
# =========================
def get_group_key(m):
    cid = m.get("conditionId")
    if cid:
        return f"cid:{cid}"

    g = m.get("groupItemTitle")
    if g:
        return f"group:{g}"

    return None

# =========================
# Deep Pagination
# =========================
def fetch_markets():
    all_markets = []

    for offset in range(0, 2400, 300):
        try:
            r = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "active": "true",
                    "limit": 300,
                    "offset": offset
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )

            if r.status_code != 200:
                print(f"[WARN] offset={offset} status={r.status_code}")
                continue

            data = r.json()

            if not isinstance(data, list):
                print(f"[WARN] offset={offset} unexpected type: {type(data)}")
                continue

            if len(data) == 0:
                print(f"[INFO] offset={offset} empty page, stopping")
                break

            all_markets.extend(data)
            print(f"[INFO] offset={offset} fetched={len(data)} total={len(all_markets)}")

        except Exception as e:
            print(f"[WARN] offset={offset} error: {e}")
            continue

    return all_markets

# =========================
# ⭐ Partition Arb
# 关键：用 conditionId 分组
# =========================
def partition_arb(markets):
    found = False
    groups = defaultdict(list)

    for m in markets:
        key = get_group_key(m)
        if not key:
            continue

        try:
            liq = float(m.get("liquidity", 0))
        except Exception:
            continue

        if liq < 500:
            continue

        yes_price = get_yes_price(m)
        if yes_price is None:
            continue

        groups[key].append({
            "question": m.get("question", ""),
            "yes": yes_price,
            "slug": m.get("slug", ""),
            "conditionId": m.get("conditionId", ""),
        })

    print(f"[INFO] partition groups: {len(groups)}")

    for key, buckets in groups.items():

        if len(buckets) < 4:
            continue

        sum_yes = sum(b["yes"] for b in buckets)

        print(f"[INFO] key={key} buckets={len(buckets)} sum_yes={round(sum_yes,4)}")

        slug = buckets[0].get("slug", "")
        url = f"https://polymarket.com/event/{slug}" if slug else ""

        # ⭐ Overround: Σ YES > 1
        if sum_yes > 1.03:
            found = True
            profit = round(sum_yes - 1, 4)

            lines = [
                "🚨🚨🚨 EXECUTE NOW 🚨🚨🚨",
                "",
                "Partition Arb → BUY ALL NO",
                "",
                f"Σ YES = {round(sum_yes, 4)}",
                f"Profit ≈ {profit}",
                "",
                "Buckets:"
            ]

            for b in buckets:
                lines.append(f"  YES={round(b['yes'],3)}  {b['question'][:50]}")

            if url:
                lines.append(f"\n🔗 {url}")

            send("\n".join(lines))

        # ⭐ Underround: Σ YES < 1
        elif sum_yes < 0.97:
            found = True
            profit = round(1 - sum_yes, 4)

            lines = [
                "🚨🚨🚨 EXECUTE NOW 🚨🚨🚨",
                "",
                "Partition Arb → BUY ALL YES",
                "",
                f"Σ YES = {round(sum_yes, 4)}",
                f"Profit ≈ {profit}",
                "",
                "Buckets:"
            ]

            for b in buckets:
                lines.append(f"  YES={round(b['yes'],3)}  {b['question'][:50]}")

            if url:
                lines.append(f"\n🔗 {url}")

            send("\n".join(lines))

    return found

# =========================
# Mutual Outcome Arb
# =========================
def mutual_arb(markets):
    found = False
    groups = defaultdict(list)

    for m in markets:
        key = get_group_key(m)
        if not key:
            continue

        try:
            liq = float(m.get("liquidity", 0))
        except Exception:
            continue

        if liq < 5000:
            continue

        yes_price = get_yes_price(m)
        if yes_price is None:
            continue

        groups[key].append({
            "question": m.get("question", ""),
            "yes": yes_price,
            "slug": m.get("slug", ""),
        })

    for key, items in groups.items():

        if len(items) < 3:
            continue

        s = sum(i["yes"] for i in items)

        if s > 1.05:
            found = True
            slug = items[0].get("slug", "")
            url = f"https://polymarket.com/event/{slug}" if slug else ""

            lines = [
                "⚠️ Mutual Outcome Arb",
                "",
                f"Σ YES = {round(s, 4)}",
                f"Gap = {round(s-1, 4)}",
                "",
                "SELL all YES",
                ""
            ]

            for i in items:
                lines.append(f"  YES={round(i['yes'],3)}  {i['question'][:50]}")

            if url:
                lines.append(f"\n🔗 {url}")

            send("\n".join(lines))

    return found

# =========================
# Nomination Arb
# =========================
def nomination_arb(markets):
    found = False

    pres = []
    nom = []

    for m in markets:
        q = m.get("question", "").lower()

        try:
            liq = float(m.get("liquidity", 0))
        except Exception:
            continue

        if liq < 5000:
            continue

        yes_price = get_yes_price(m)
        if yes_price is None:
            continue

        if "president" in q:
            pres.append((m, yes_price))

        if "nomination" in q or "primary" in q:
            nom.append((m, yes_price))

    for p_market, p_price in pres:
        for n_market, n_price in nom:

            pq = p_market.get("question", "").lower()
            nq = n_market.get("question", "").lower()

            # 必须包含至少 2 个相同词（排除 the/a/in/of 等）
            stop = {"the","a","in","of","will","who","win","be","is","to","for","at","on","by"}
            p_words = {w for w in pq.split() if len(w) > 2 and w not in stop}
            n_words = {w for w in nq.split() if len(w) > 2 and w not in stop}
            common = p_words & n_words

            if len(common) < 2:
                continue

            gap = p_price - n_price

            if gap > 0.05:
                found = True
                slug = n_market.get("slug", "")
                url = f"https://polymarket.com/event/{slug}" if slug else ""

                send(
                    f"🚨 EXECUTE NOW\n\n"
                    f"Nomination Arb\n"
                    f"Gap = {round(gap, 3)}\n\n"
                    f"Presidency: {p_market['question'][:60]}\n"
                    f"YES = {round(p_price, 3)}\n\n"
                    f"Nomination: {n_market['question'][:60]}\n"
                    f"YES = {round(n_price, 3)}\n\n"
                    f"BUY Nomination YES\n"
                    f"SELL Presidency YES\n\n"
                    f"🔗 {url}"
                )

    return found

# =========================
# Run
# =========================
def main():
    ts = int(time.time())
    print(f"[START] ts={ts}")

    markets = fetch_markets()
    print(f"[INFO] total markets fetched: {len(markets)}")

    p = partition_arb(markets)
    m1 = mutual_arb(markets)
    m2 = nomination_arb(markets)

    if not p and not m1 and not m2:
        send(f"✅ No Arb Found @ {ts}")
    else:
        send(f"✅ Scan complete @ {ts}")

main()
