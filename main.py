import requests
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# =========================
# Telegram 推送
# =========================
def send_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, json=payload)


# =========================
# 获取 Polymarket 市场
# =========================
def fetch_markets():
    url = "https://gamma-api.polymarket.com/markets"
    params = {"active": "true", "closed": "false", "limit": 300}
    response = requests.get(url, params=params)
    return response.json()


# =========================
# Conditional Arb
# =========================
def detect_conditional(markets):

    base = []
    cond = []

    for m in markets:
        if not m.get("question"):
            continue

        q = m["question"].lower()

        try:
            price = float(m["outcomes"][0]["price"])
            liq = float(m["liquidity"])
        except:
            continue

        if liq < 30000:
            continue

        if "if" in q:
            cond.append({"q": q, "p": price})
        elif any(k in q for k in ["approved","pass","release","launch"]):
            base.append({"q": q, "p": price})

    ops = []

    for c in cond:
        for b in base:
            if any(word in c["q"] for word in b["q"].split()):
                if c["p"] > b["p"]:
                    gap = round(c["p"]-b["p"],3)
                    if gap > 0.05:
                        ops.append(("CONDITIONAL",b,c,gap))

    return ops


# =========================
# Release vs Announce Arb
# =========================
def detect_release(markets):

    release = []
    announce = []

    for m in markets:
        if not m.get("question"):
            continue

        q = m["question"].lower()

        try:
            price = float(m["outcomes"][0]["price"])
            liq = float(m["liquidity"])
        except:
            continue

        if liq < 30000:
            continue

        if "release" in q:
            release.append({"q": q, "p": price})

        if "announce" in q:
            announce.append({"q": q, "p": price})

    ops = []

    for r in release:
        for a in announce:

            if any(word in r["q"] for word in a["q"].split()):

                if r["p"] > a["p"]:
                    gap = round(r["p"]-a["p"],3)

                    if gap > 0.05:
                        ops.append(("RELEASE",a,r,gap))

    return ops


# =========================
# Nomination Arb
# =========================
def detect_nomination(markets):

    pres = []
    nom = []

    for m in markets:
        if not m.get("question"):
            continue

        q = m["question"].lower()

        try:
            price = float(m["outcomes"][0]["price"])
            liq = float(m["liquidity"])
        except:
            continue

        if liq < 30000:
            continue

        if "president" in q:
            pres.append({"q": q, "p": price})

        if "nomination" in q or "primary" in q:
            nom.append({"q": q, "p": price})

    ops = []

    for p in pres:
        for n in nom:

            if any(word in p["q"] for word in n["q"].split()):

                if p["p"] > n["p"]:
                    gap = round(p["p"]-n["p"],3)

                    if gap > 0.05:
                        ops.append(("NOMINATION",n,p,gap))

    return ops


# =========================
# CPI Bucket Arb
# =========================
def detect_bucket(markets):

    buckets = []

    for m in markets:

        if not m.get("question"):
            continue

        q = m["question"].lower()

        try:
            price = float(m["outcomes"][0]["price"])
            liq = float(m["liquidity"])
        except:
            continue

        if liq < 30000:
            continue

        if any(k in q for k in ["cpi","inflation","rate","unemployment"]):
            if "%" in q or "-" in q:
                buckets.append({"q": q, "p": price})

    ops = []

    for i in range(len(buckets)):
        for j in range(i+1,len(buckets)):
            for k in range(j+1,len(buckets)):

                total = round(
                    buckets[i]["p"]
                    +buckets[j]["p"]
                    +buckets[k]["p"],3
                )

                if total > 1.05:

                    ops.append(("BUCKET",
                                buckets[i],
                                buckets[j],
                                buckets[k],
                                total))

    return ops


# =========================
# 主执行逻辑
# =========================
markets = fetch_markets()

ops1 = detect_conditional(markets)
ops2 = detect_release(markets)
ops3 = detect_nomination(markets)
ops4 = detect_bucket(markets)

ops = ops1 + ops2 + ops3 + ops4


if len(ops)==0:
    send_alert("✅ Scan complete. No Logical Arb found.")
else:
    for op in ops:

        if op[0]=="CONDITIONAL":
            msg=f"""
⚠️ Conditional Arb

Base:
{op[1]['q']}
YES={op[1]['p']}

Conditional:
{op[2]['q']}
YES={op[2]['p']}

Gap={op[3]}

BUY Base YES
SELL Conditional YES
"""

        elif op[0]=="RELEASE":
            msg=f"""
⚠️ Release vs Announce Arb

Announce:
{op[1]['q']}
YES={op[1]['p']}

Release:
{op[2]['q']}
YES={op[2]['p']}

Gap={op[3]}

BUY Announce YES
SELL Release YES
"""

        elif op[0]=="NOMINATION":
            msg=f"""
⚠️ Nomination Arb

Nomination:
{op[1]['q']}
YES={op[1]['p']}

Presidency:
{op[2]['q']}
YES={op[2]['p']}

Gap={op[3]}

BUY Nomination YES
SELL Presidency YES
"""

        elif op[0]=="BUCKET":
            msg=f"""
⚠️ CPI Bucket Arb

Bucket 1:
{op[1]['q']}
YES={op[1]['p']}

Bucket 2:
{op[2]['q']}
YES={op[2]['p']}

Bucket 3:
{op[3]['q']}
YES={op[3]['p']}

Sum={op[4]}

SELL all 3 YES
"""

        send_alert(msg)
