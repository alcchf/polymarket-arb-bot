import requests,os,math,re
from datetime import datetime

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
# Markets
# =========================
def markets():
    try:
        return requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"active":"true","limit":300},
            timeout=5
        ).json()
    except: return []

# =========================
# Mutual Outcome Arb
# =========================
def mutual(ms):

    groups={}

    for m in ms:

        g=m.get("groupItemTitle")
        if not g: continue

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except: continue

        if liq<20000: continue

        if g not in groups: groups[g]=[]
        groups[g].append((m,p))

    for g in groups:
        if len(groups[g])<3: continue

        for i in range(len(groups[g])):
            for j in range(i+1,len(groups[g])):
                for k in range(j+1,len(groups[g])):

                    s=groups[g][i][1]+groups[g][j][1]+groups[g][k][1]

                    if s>1.02:

                        slug=groups[g][i][0].get("slug")
                        if not slug: continue

                        send(f"""
⚠️ Mutual Outcome Arb

Sum={round(s,2)}

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")

# =========================
# Nomination Arb
# =========================
def nomination(ms):

    pres=[];nom=[]

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except: continue

        if liq<20000: continue

        if "president" in q:
            pres.append((m,p))

        if "nomination" in q or "primary" in q:
            nom.append((m,p))

    for p in pres:
        for n in nom:

            if any(w in p[0]["question"].lower() for w in n[0]["question"].lower().split()):

                if p[1]>n[1]+0.03:

                    slug=n[0].get("slug")
                    if not slug: continue

                    send(f"""
⚠️ Nomination Arb

BUY Nomination YES
SELL Presidency YES

🔗 https://polymarket.com/event/{slug}
""")

# =========================
# Release Arb
# =========================
def release(ms):

    rel=[];ann=[]

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except: continue

        if liq<20000: continue

        if "release" in q: rel.append((m,p))
        if "announce" in q: ann.append((m,p))

    for r in rel:
        for a in ann:

            if any(w in r[0]["question"].lower() for w in a[0]["question"].lower().split()):

                if r[1]>a[1]+0.03:

                    slug=a[0].get("slug")
                    if not slug: continue

                    send(f"""
⚠️ Release vs Announce Arb

BUY Announce YES
SELL Release YES

🔗 https://polymarket.com/event/{slug}
""")

# =========================
# Bucket Arb
# =========================
def bucket(ms):

    b=[]

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except: continue

        if liq<20000: continue

        if any(k in q for k in ["cpi","rate","inflation","unemployment"]):
            if "%" in q or "-" in q:
                b.append((m,p))

    for i in range(len(b)):
        for j in range(i+1,len(b)):
            for k in range(j+1,len(b)):

                s=b[i][1]+b[j][1]+b[k][1]

                if s>1.05:

                    slug=b[i][0].get("slug")
                    if not slug: continue

                    send(f"""
⚠️ Bucket Arb

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")

# =========================
# Weather (辅助)
# =========================
def weather(ms):

    try:
        url="https://api.weather.gov/stations/KNYC/observations/latest"
        d=requests.get(url,timeout=5).json()
        mean=d["properties"]["temperature"]["value"]*9/5+32
    except: return

    std=2

    for m in ms:

        q=m.get("question","").lower()

        if not any(k in q for k in ["reach","above","below"]): continue

        try:
            price=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except: continue

        if liq<20000: continue

        slug=m.get("slug")
        if not slug: continue

        single=re.search(r'(\d+)',q)
        if not single: continue

        t=float(single.group(1))

        if "above" in q or "reach" in q:
            p=1-0.5*(1+math.erf((t-mean)/(std*math.sqrt(2))))
        elif "below" in q:
            p=0.5*(1+math.erf((t-mean)/(std*math.sqrt(2))))
        else: continue

        if p>price+0.01:

            send(f"""
🌦️ Weather Arb

BUY YES

🔗 https://polymarket.com/event/{slug}
""")

# =========================
# Run
# =========================
ms=markets()

mutual(ms)
nomination(ms)
release(ms)
bucket(ms)
weather(ms)

send("✅ Hybrid scan complete")
``
