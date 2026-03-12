import requests,os
from collections import defaultdict

TELEGRAM_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID=os.getenv("TELEGRAM_CHAT_ID")

# =========================
# Telegram
# =========================
def send(msg):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url,json={"chat_id":CHAT_ID,"text":msg},timeout=5)
    except: pass

# =========================
# ✅ Extract YES Price (Dual Format)
# =========================
def get_yes_price(m):

    yes_price=None

    # Format A
    if isinstance(m.get("outcomes"),list):

        for o in m["outcomes"]:
            if isinstance(o,dict) and o.get("name","").lower()=="yes":
                yes_price=float(o.get("price",0))
                return yes_price

    # Format B
    elif isinstance(m.get("outcomes"),str):

        names=m["outcomes"].split(",")
        prices=m.get("outcomePrices","").split(",")

        for i in range(len(names)):
            if names[i].strip().lower()=="yes":
                yes_price=float(prices[i])
                return yes_price

    return None

# =========================
# Deep Pagination
# =========================
def markets():

    all=[]

    for i in range(0,2400,300):

        try:
            r=requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active":"true","limit":300,"offset":i},
                headers={"User-Agent":"Mozilla/5.0"},
                timeout=10
            )

            if r.status_code==200:
                data=r.json()
                if len(data)==0:
                    break
                all+=data
        except:
            continue

    return all

# =========================
# ⭐ Partition Arb
# =========================
def partition(ms):

    groups=defaultdict(list)
    found=False

    for m in ms:

        g=m.get("groupItemTitle")
        if not g:continue

        try:
            liq=float(m["liquidity"])
        except:
            continue

        if liq<500:continue

        yes_price=get_yes_price(m)

        if yes_price is None:
            continue

        groups[g].append((m,yes_price))

    for g in groups:

        if len(groups[g])<4:
            continue

        sum_yes=sum([p for _,p in groups[g]])

        if sum_yes>1.03:

            send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Partition Arb (BUY ALL NO)

Group:
{g}

Σ YES={round(sum_yes,3)}

Profit≈{round(sum_yes-1,3)}
""")
            found=True

        elif sum_yes<0.97:

            send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Partition Arb (BUY ALL YES)

Group:
{g}

Σ YES={round(sum_yes,3)}

Profit≈{round(1-sum_yes,3)}
""")
            found=True

    return found

# =========================
# Mutual Arb
# =========================
def mutual(ms):

    groups=defaultdict(list)
