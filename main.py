import os
import time
import json
import requests
from collections import defaultdict

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GAMMA_BASE = "https://gamma-api.polymarket.com"

def send(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg[:4000]},
            timeout=5
        )
    except Exception as e:
        print(f"[WARN] Telegram: {e}")

def parse_yes_price(market):
    try:
        op = market.get("outcomePrices")
        if op and isinstance(op, str):
            prices = json.loads(op)
            if isinstance(prices, list) and len(prices) >= 1:
                return float(prices[0])
    except Exception as e:
        print(f"[WARN] parse_yes_price: {e}")
    return None

# ✅ 加 markets=true 参数
def fetch_events():
    all_events = []
    for offset in range(0, 1200, 100):
        try:
            r = requests.get(
                f"{GAMMA_BASE}/events",
                params={
                    "active": "true",
                    "limit": 100,
                    "offset": offset,
                    "markets": "true"    # ← 关键
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15
            )
            if r.status_code != 200:
                print(f"[WARN] events offset={offset} status={r.status_code}")
                continue
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                print(f"[INFO] events empty at offset={offset}")
                break
            all_events.extend(data)
            print(f"[INFO] events offset={offset} page={len(data)} total={len(all_events)}")
        except Exception as e:
            print(f"[WARN] events offset={offset}: {e}")
    return all_events

def fetch_markets():
    all_markets = []
    for offset in range(0, 2400, 300):
        try:
            r = requests.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "active": "true",
                    "limit": 300,
                    "offset": offset
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if r.status_code != 200:
                continue
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            all_markets.extend(data)
            print(f"[INFO] markets offset={offset} page={len(data)} total={len(all_markets)}")
        except Exception as e:
            print(f"[WARN] markets offset={offset}: {e}")
    return all_markets

def partition_arb(events):
    found = False
    arb_count = 0

    for event in events:
        title = event.get("title","") or event.get("slug","")
        markets = event.get("markets",[])

        if not isinstance(markets, list) or len(markets) < 3:
            continue

        buckets = []
        for m in markets:
            yes = parse_yes_price(m)
            if yes is None:
                continue
            try:
                liq = float(m.get("liquidity", 0))
            except Exception:
                liq = 0
            buckets.append({
                "question": m.get("question",""),
                "yes": yes,
                "slug": m.get("slug",""),
                "liq": liq,
            })

        if len(buckets) < 3:
            continue

        sum_yes = sum(b["yes"] for b in buckets)
        print(f"[DEBUG] '{title[:50]}' buckets={len(buckets)} sum={round(sum_yes,4)}")

        slug = buckets[0].get("slug","")
        url = f"https://polymarket.com/event/{slug}" if slug else ""

        if sum_yes > 1.03:
            found = True
            arb_count += 1
            lines = [
                "🚨🚨🚨 EXECUTE NOW 🚨🚨🚨",
                "Partition Arb → BUY ALL NO",
                f"Event: {title[:60]}",
                f"Σ YES = {round(sum_yes,4)}",
                f"Profit ≈ {round(sum_yes-1,4)}",
                "Buckets:"
            ]
            for b in sorted(buckets, key=lambda x: x["yes"], reverse=True)[:8]:
                lines.append(f"  {round(b['yes'],3)}  {b['question'][:50]}")
            if url:
                lines.append(f"\n🔗 {url}")
            send("\n".join(lines))

        elif sum_yes < 0.97:
            found = True
            arb_count += 1
            lines = [
                "🚨🚨🚨 EXECUTE NOW 🚨🚨🚨",
                "Partition Arb → BUY ALL YES",
                f"Event: {title[:60]}",
                f"Σ YES = {round(sum_yes,4)}",
                f"Profit ≈ {round(1-sum_yes,4)}",
                "Buckets:"
            ]
            for b in sorted(buckets, key=lambda x: x["yes"], reverse=True)[:8]:
                lines.append(f"  {round(b['yes'],3)}  {b['question'][:50]}")
            if url:
                lines.append(f"\n🔗 {url}")
            send("\n".join(lines))

    print(f"[INFO] partition arb found: {arb_count}")
    return found

def mutual_arb(markets):
    found = False
    groups = defaultdict(list)

    for m in markets:
        g = m.get("groupItemTitle")
        if not g:
            continue
        try:
            liq = float(m.get("liquidity",0))
        except Exception:
            continue
        if liq < 5000:
            continue
        yes = parse_yes_price(m)
        if yes is None:
            continue
        groups[g].append({
            "question": m.get("question",""),
            "yes": yes,
            "slug": m.get("slug",""),
        })

    for g, items in groups.items():
        if len(items) < 3:
            continue
        s = sum(i["yes"] for i in items)
        if s > 1.05:
            found = True
            slug = items[0].get("slug","")
            url = f"https://polymarket.com/event/{slug}" if slug else ""
            lines = [
                "⚠️ Mutual Outcome Arb",
                f"Σ YES = {round(s,4)}",
                f"Gap = {round(s-1,4)}",
                "SELL all YES",""
            ]
            for i in items[:6]:
                lines.append(f"  {round(i['yes'],3)}  {i['question'][:50]}")
            if url:
                lines.append(f"\n🔗 {url}")
            send("\n".join(lines))

    return found

def nomination_arb(markets):
    found = False
    pres = []
    nom = []

    stop = {
        "the","a","in","of","will","who","win","be",
        "is","to","for","at","on","by","can","get",
        "has","have","had","was","were","and","or"
    }

    for m in markets:
        q = m.get("question","").lower()
        try:
            liq = float(m.get("liquidity",0))
        except Exception:
            continue
        if liq < 5000:
            continue
        yes = parse_yes_price(m)
        if yes is None:
            continue
        if "president" in q:
            pres.append((m, yes))
        if "nomination" in q or "primary" in q:
            nom.append((m, yes))

    for p_m, p_price in pres:
        for n_m, n_price in nom:
            pq = p_m.get("question","").lower()
            nq = n_m.get("question","").lower()
            p_words = {w for w in pq.split() if len(w)>2 and w not in stop}
            n_words = {w for w in nq.split() if len(w)>2 and w not in stop}
            if len(p_words & n_words) < 2:
                continue
            gap = p_price - n_price
            if gap > 0.05:
                found = True
                slug = n_m.get("slug","")
                url = f"https://polymarket.com/event/{slug}" if slug else ""
                send(
                    f"🚨 EXECUTE NOW\n\n"
                    f"Nomination Arb\nGap={round(gap,3)}\n\n"
                    f"Presidency: {p_m.get('question','')[:60]}\n"
                    f"YES={round(p_price,3)}\n\n"
                    f"Nomination: {n_m.get('question','')[:60]}\n"
                    f"YES={round(n_price,3)}\n\n"
                    f"BUY Nomination YES\nSELL Presidency YES\n\n"
                    f"🔗 {url}"
                )

    return found

def main():
    ts = int(time.time())
    print(f"[START] ts={ts}")

    events = fetch_events()
    print(f"[INFO] total events: {len(events)}")

    markets = fetch_markets()
    print(f"[INFO] total markets: {len(markets)}")

    p = partition_arb(events)
    m1 = mutual_arb(markets)
    m2 = nomination_arb(markets)

    if not p and not m1 and not m2:
        send(f"✅ No Arb Found @ {ts}")
    else:
        send(f"✅ Scan complete @ {ts}")

main()
