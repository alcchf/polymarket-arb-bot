import requests
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    requests.post(url, json=payload)

def fetch_markets():
    url = "https://gamma-api.polymarket.com/markets"
    params = {"active": "true", "closed": "false", "limit": 300}
    response = requests.get(url, params=params)
    return response.json()

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


markets = fetch_markets()

ops1 = detect_conditional(markets)
ops2 = detect_release(markets)

ops = ops1 + ops2

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
        else:
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

        send_alert(msg)
