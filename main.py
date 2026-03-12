import requests,os
from collections import defaultdict

TELEGRAM_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID=os.getenv("TELEGRAM_CHAT_ID")

def send(msg):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url,json={"chat_id":CHAT_ID,"text":msg},timeout=5)
    except: pass

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
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<500:continue

        groups[g].append((m,p))

    for g in groups:

        if len(groups[g])<4:
            continue

        sum_yes=0

        for m,p in groups[g]:
            sum_yes+=p

        if sum_yes>1.03:

            send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Partition Arb (BUY ALL NO)

Group:
{g}

Σ YES={round(sum_yes,3)}

BUY NO on ALL outcomes
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

BUY YES on ALL outcomes
Profit≈{round(1-sum_yes,3)}
""")
            found=True

    return found

# =========================
# Mutual Arb
# =========================
def mutual(ms):

    groups=defaultdict(list)
    found=False

    for m in ms:

        g=m.get("groupItemTitle")
        if not g:continue

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<20000:continue

        groups[g].append((m,p))

    for g in groups:
        if len(groups[g])<3:continue

        s=sum([p for _,p in groups[g]])

        if s>1.02:

            slug=groups[g][0][0].get("slug")
            if not slug:continue

            send(f"""
🚨 EXECUTE NOW

Mutual Arb
Σ YES={round(s,3)}

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")
            found=True

    return found

# =========================
# Nomination Arb
# =========================
def nomination(ms):

    pres=[];nom=[]
    found=False

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<20000:continue

        if "president" in q: pres.append((m,p))
        if "nomination" in q or "primary" in q: nom.append((m,p))

    for p in pres:
        for n in nom:

            if any(w in p[0]["question"].lower() for w in n[0]["question"].lower().split()):

                gap=p[1]-n[1]

                if gap>0.05:

                    slug=n[0].get("slug")
                    if not slug:continue

                    send(f"""
🚨 EXECUTE NOW

Nomination Arb
Gap={round(gap,3)}

BUY Nomination YES
SELL Presidency YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

    return found

# =========================
# Run
# =========================
ms=markets()

p=partition(ms)
m1=mutual(ms)
m2=nomination(ms)

if not p and not m1 and not m2:
    send("✅ Hybrid scan complete - No Arb Found")
