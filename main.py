import requests
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 获取所有活跃市场
def fetch_markets():
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "active": "true",
        "closed": "false",
        "limit": 200
    }
    response = requests.get(url, params=params)
    return response.json()

# Telegram推送
def send_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message
    }
    requests.post(url, json=payload)

# 判断是否为Conditional市场
def is_conditional(question):
    keywords = [
        "if",
        "assuming",
        "provided that"
    ]
    q = question.lower()
    return any(k in q for k in keywords)

# 判断Base Market
def is_base(question):
    keywords = [
        "approved",
        "pass",
        "win",
        "launch",
        "release"
    ]
    q = question.lower()
    return any(k in q for k in keywords)

# 主套利逻辑
def detect_arbitrage(markets):

    base_markets = []
    conditional_markets = []

    for m in markets:
        if not m.get("question"):
            continue

        q = m["question"]

        try:
            yes_price = float(m["outcomes"][0]["price"])
            liquidity = float(m["liquidity"])
        except:
            continue

        if liquidity < 30000:
            continue

        if is_conditional(q):
            conditional_markets.append({
                "question": q,
                "price": yes_price
            })
        elif is_base(q):
            base_markets.append({
                "question": q,
                "price": yes_price
            })

    opportunities = []

    for c in conditional_markets:
        for b in base_markets:

            # 条件市场必须包含Base关键词
            if any(word in c["question"].lower() for word in b["question"].lower().split()):

                if c["price"] > b["price"]:

                    gap = round(c["price"] - b["price"], 3)

                    if gap > 0.05:

                        opportunities.append({
                            "base": b,
                            "conditional": c,
                            "gap": gap
                        })

    return opportunities

# 运行
markets = fetch_markets()
ops = detect_arbitrage(markets)

if len(ops) == 0:
    send_alert("✅ Bot ran successfully. No Logical Arb found today.")
else:
    for op in ops:

        msg = f"""
⚠️ Logical Arb Detected

Base Market:
{op['base']['question']}
YES = {op['base']['price']}

Conditional Market:
{op['conditional']['question']}
YES = {op['conditional']['price']}

Violation:
Conditional > Base by {op['gap']}

Suggested Trade:
BUY Base YES
SELL Conditional YES
"""
        send_alert(msg)
