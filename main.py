import requests,os

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
# ✅ Deep Pagination
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

    ids=set()
    for m in all:
        if m.get("id"):
            ids.add(m["id"])

    send(f"""
📊 Raw fetched: {len(all)}
✅ Unique markets: {len(ids)}
""")

    return all

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

        if liq<20000:continue

        if g not in groups:groups[g]=[]
        groups[g].append((m,p))

    for g in groups:
        if len(groups[g])<3:continue

        for i in range(len(groups[g])):
            for j in range(i+1,len(groups[g])):
                for k in range(j+1,len(groups[g])):

                    s=groups[g][i][1]+groups[g][j][1]+groups[g][k][1]
                    gap=s-1

                    slug=groups[g][i][0].get("slug")
                    if not slug:continue

                    if gap>0.05:
                        send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Strong Mutual Arb
Gap={round(gap,3)}

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")
                        found=True

                    elif gap>0.02:
                        send(f"""
⚠️ Mutual Arb
Gap={round(gap,3)}

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
                slug=n[0].get("slug")
                if not slug:continue

                if gap>0.05:
                    send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Strong Nomination Arb
Gap={round(gap,3)}

BUY Nomination YES
SELL Presidency YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

                elif gap>0.03:
                    send(f"""
⚠️ Nomination Arb
Gap={round(gap,3)}

BUY Nomination YES
SELL Presidency YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

    return found

# =========================
# Release Arb
# =========================
def release(ms):

    rel=[];ann=[]
    found=False

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<20000:continue

        if "release" in q: rel.append((m,p))
        if "announce" in q: ann.append((m,p))

    for r in rel:
        for a in ann:

            if any(w in r[0]["question"].lower() for w in a[0]["question"].lower().split()):

                gap=r[1]-a[1]
                slug=a[0].get("slug")
                if not slug:continue

                if gap>0.05:
                    send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Strong Release Arb
Gap={round(gap,3)}

BUY Announce YES
SELL Release YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

    return found

# =========================
# Bucket Arb
# =========================
def bucket(ms):

    b=[]
    found=False

    for m in ms:

        q=m.get("question","").lower()

        try:
            p=float(m["outcomes"][0]["price"])
            liq=float(m["liquidity"])
        except:continue

        if liq<20000:continue

        if any(k in q for k in ["cpi","rate","inflation","unemployment"]):
            if "%" in q or "-" in q:
                b.append((m,p))

    for i in range(len(b)):
        for j in range(i+1,len(b)):
            for k in range(j+1,len(b)):

                s=b[i][1]+b[j][1]+b[k][1]
                gap=s-1

                slug=b[i][0].get("slug")
                if not slug:continue

                if gap>0.07:
                    send(f"""
🚨🚨🚨 EXECUTE NOW 🚨🚨🚨

Strong Bucket Arb
Gap={round(gap,3)}

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

                elif gap>0.05:
                    send(f"""
⚠️ Bucket Arb
Gap={round(gap,3)}

SELL all YES

🔗 https://polymarket.com/event/{slug}
""")
                    found=True

    return found

# =========================
# Run
# =========================
ms=markets()

m1=mutual(ms)
m2=nomination(ms)
m3=release(ms)
m4=bucket(ms)

if not m1 and not m2 and not m3 and not m4:
    send("✅ Hybrid scan complete - No Arb Found")
