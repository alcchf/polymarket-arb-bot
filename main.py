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
    url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url,json={"chat_id":CHAT_ID,"text":msg})

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
# Drawdown
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

# =========================
# Daily Loss
# =========================
def dloss():
    global NAV,daily_start_NAV
    return (daily_start_NAV-NAV)/daily_start_NAV

def dloss_adj(f):
    l=dloss()
    if l<.02:return f
    elif l<.04:return .5*f
    elif l<.06:return .2*f
    else:return 0

# =========================
# Vol Kelly
# =========================
def vol_adj(f,std):
    if std<=0:return f
    return max(0,f*(1/std))

# =========================
# Seasonal
# =========================
def seasonal(f):
    m=datetime.utcnow().month
    if m in [6,7,8]:mult=1
    elif m in [9,10,11]:mult=.7
    elif m in [3,4,5]:mult=.5
    else:mult=.3
    return f*mult,mult

# =========================
# NOAA
# =========================
def noaa():
    try:
        url="https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly"
        d=requests.get(url,timeout=5).json()["properties"]["periods"][:12]
        t=[p["temperature"]*9/5+32 for p in d]
        return sum(t)/len(t),2
    except:
        return None,None

# =========================
# ECMWF
# =========================
def ecmwf():
    try:
        url="https://api.open-meteo.com/v1/forecast?latitude=40.7&longitude=-74&hourly=temperature_2m"
        t=requests.get(url,timeout=5).json()["hourly"]["temperature_2m"][:12]
        t=[x*9/5+32 for x in t]
        return sum(t)/len(t),1.5
    except:
        return None,None

# =========================
# NAM
# =========================
def nam():
    try:
        url="https://api.open-meteo.com/v1/forecast?latitude=40.7&longitude=-74&hourly=temperature_2m&models=nam"
        t=requests.get(url,timeout=5).json()["hourly"]["temperature_2m"][:12]
        t=[x*9/5+32 for x in t]
        return sum(t)/len(t),1.2
    except:
        return None,None

# =========================
# Normal CDF
# =========================
def cdf(x,m,s):
    return .5*(1+math.erf((x-m)/(s*math.sqrt(2))))

# =========================
# Fetch Markets
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
# Weather Arb
# =========================
def weather(ms):

    m1,s1=noaa()
    m2,s2=ecmwf()
    m3,s3=nam()

    means=[]
    stds=[]

    if m1:
        means.append(m1)
        stds.append(s1)

    if m2:
        means.append(m2)
        stds.append(s2)

    if m3:
        means.append(m3)
        stds.append(s3)

    if len(means)==0:return

    mean=sum(means)/len(means)
    std=sum(stds)/len(stds)

    for m in ms:

        q=m.get("question","").lower()
        if "temp" not in q:continue

        try:
            price=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<30000:continue

        slug=m.get("slug")
        if not slug:continue

        match=re.search(r'(\d+)\s*-\s*(\d+)',q)
        if not match:continue

        a=float(match.group(1))
        b=float(match.group(2))

        p=cdf(b,mean,std)-cdf(a,mean,std)

        if p>price+.05:

            f=kelly(p,price)
            f=dd_adj(f)
            f=dloss_adj(f)
            f=vol_adj(f,std)
            f,season=seasonal(f)

            url=f"https://polymarket.com/event/{slug}"

            send(f"""
🌦️ Stable Ensemble Weather Arb

Market:
{m['question']}

Market={round(price,2)}
Model ={round(p,2)}

Kelly ={round(f*100,2)}% NAV
SeasonAdj={season}
Drawdown ={round(dd()*100,2)}%
DailyLoss={round(dloss()*100,2)}%

BUY YES

🔗 Trade:
{url}
""")

# =========================
# Mutual Arb
# =========================
def mutual(ms):

    groups={}

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

🔗 Trade:
{url}
""")

# =========================
# Run
# =========================
ms=markets()
weather(ms)
mutual(ms)
