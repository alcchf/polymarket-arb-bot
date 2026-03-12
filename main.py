import requests,os,math,re
from datetime import datetime

TELEGRAM_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID=os.getenv("TELEGRAM_CHAT_ID")

NAV=1000
peak_NAV=1000
daily_start_NAV=1000

# =========================
# Telegram
# =========================
def send(msg):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url,json={"chat_id":CHAT_ID,"text":msg},timeout=5)
    except:
        pass

# =========================
# Kelly
# =========================
def kelly(p,price):
    if price<=0 or price>=1:return 0
    b=(1-price)/price
    q=1-p
    f=(b*p-q)/b
    return max(0,0.5*f)

# =========================
# Risk Controls
# =========================
def dd():
    global NAV,peak_NAV
    if NAV>peak_NAV: peak_NAV=NAV
    return (peak_NAV-NAV)/peak_NAV

def dd_adj(f):
    d=dd()
    if d<0.05:return f
    elif d<0.1:return .7*f
    elif d<0.15:return .4*f
    elif d<0.2:return .2*f
    else:return 0

def dloss():
    global NAV,daily_start_NAV
    return (daily_start_NAV-NAV)/daily_start_NAV

def dloss_adj(f):
    l=dloss()
    if l<.02:return f
    elif l<.04:return .5*f
    elif l<.06:return .2*f
    else:return 0

def seasonal(f):
    m=datetime.utcnow().month
    if m in [6,7,8]:mult=1
    elif m in [9,10,11]:mult=.7
    elif m in [3,4,5]:mult=.5
    else:mult=.3
    return f*mult

# =========================
# ✅ Official Settlement Station
# =========================
def knyc():
    try:
        url="https://api.weather.gov/stations/KNYC/observations/latest"
        d=requests.get(url,timeout=5).json()
        t=d["properties"]["temperature"]["value"]

        if t is None:
            return None

        t=t*9/5+32
        return t
    except:
        return None

# =========================
# Normal CDF
# =========================
def cdf(x,m,s):
    return .5*(1+math.erf((x-m)/(s*math.sqrt(2))))

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
    except:
        return []

# =========================
# Binary Weather Arb
# =========================
def weather(ms):

    mean=knyc()
    if mean is None:return False

    std=2  # station short‑term variance

    found=False

    for m in ms:

        q=m.get("question","").lower()

        if not any(k in q for k in [
            "reach","above","below",
            "at least","high"
        ]):
            continue

        try:
            price=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<30000:continue

        slug=m.get("slug")
        if not slug:continue

        single=re.search(r'(\d+)',q)
        if not single:continue

        t=float(single.group(1))

        if "above" in q or "reach" in q or "at least" in q:
            p=1-cdf(t,mean,std)
        elif "below" in q:
            p=cdf(t,mean,std)
        else:
            continue

        # ⭐ 1% Edge
        if p>price+0.01:

            f=kelly(p,price)
            f=dd_adj(f)
            f=dloss_adj(f)
            f=seasonal(f)

            url=f"https://polymarket.com/event/{slug}"

            send(f"""
🌦️ Binary Weather Arb (KNYC)

{m['question']}

Market={round(price,2)}
Model ={round(p,2)}

Kelly ={round(f*100,2)}% NAV

🔗 {url}
""")
            found=True

    return found

# =========================
# Mutual Arb
# =========================
def mutual(ms):

    groups={}
    found=False

    for m in ms:

        g=m.get("groupItemTitle")
        if not g:continue

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<30000:continue

        if g not in groups:groups[g]=[]
        groups[g].append((m,p))

    for g in groups:
        if len(groups[g])<3:continue

        for i in range(len(groups[g])):
            for j in range(i+1,len(groups[g])):
                for k in range(j+1,len(groups[g])):

                    s=groups[g][i][1]+groups[g][j][1]+groups[g][k][1]

                    if s>1.05:

                        slug=groups[g][i][0].get("slug")
                        if not slug:continue

                        url=f"https://polymarket.com/event/{slug}"

                        send(f"""
⚠️ Mutual Outcome Arb

Sum={round(s,2)}

SELL all YES

🔗 {url}
""")
                        found=True

    return found

# =========================
# Run
# =========================
ms=markets()

w=weather(ms)
m=mutual(ms)

if not w and not m:
    send("✅ Bot ran successfully - No Arb Found")
