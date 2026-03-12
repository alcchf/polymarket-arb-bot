"""
Polymarket 全市场套利扫描器
套利类型：
  1. Bundle ARB     - YES + NO 价格之和 < 1.00
  2. Multi-Outcome  - 多结果市场所有结果价格之和 < 1.00
  3. Near-Expiry    - 临近结算且价格仍偏离 0/1
  4. CLOB-Confirmed - 通过订单簿中间价二次验证 Bundle ARB
作者: Aidi Zeng | 运行环境: Python 3.10+ / GitHub Actions
"""

import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

# ----------------------------------------------------------------
# 配置区
# ----------------------------------------------------------------
GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")  # GitHub Secret: TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")    # GitHub Secret: TELEGRAM_CHAT_ID

BUNDLE_ARB_THRESHOLD   = 0.97
MULTI_ARB_THRESHOLD    = 0.97
NEAR_EXPIRY_HOURS      = 6
NEAR_EXPIRY_PROB_RANGE = (0.15, 0.85)
MIN_LIQUIDITY = 500
PAGE_SIZE     = 100
MAX_PAGES     = 50
REQUEST_DELAY = 0.3

# ----------------------------------------------------------------
# 日志
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("PolyArb")


# ----------------------------------------------------------------
# Telegram 通知
# ----------------------------------------------------------------
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] 未配置，跳过推送")
        return
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id":    TELEGRAM_CHAT,
            "text":       msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=10)
        if resp.status_code == 200:
            log.info("[Telegram] 消息发送成功")
        else:
            log.warning("[Telegram] 发送失败: " + resp.text)
    except Exception as e:
        log.warning("[Telegram] 异常: " + str(e))


# ----------------------------------------------------------------
# HTTP Session
# ----------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


# ----------------------------------------------------------------
# API 工具
# ----------------------------------------------------------------
def gamma_get(path, params=None):
    url = GAMMA_API + path
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Gamma API 请求失败 " + path + ": " + str(e))
        return None


def clob_get_midprice(token_id):
    try:
        r_buy  = SESSION.get(CLOB_API + "/price",
                             params={"token_id": token_id, "side": "BUY"},  timeout=8)
        r_sell = SESSION.get(CLOB_API + "/price",
                             params={"token_id": token_id, "side": "SELL"}, timeout=8)
        buy_price  = float(r_buy.json().get("price",  0))
        sell_price = float(r_sell.json().get("price", 0))
        if buy_price > 0 and sell_price > 0:
            return (buy_price + sell_price) / 2
        return buy_price or sell_price or None
    except Exception:
        return None


_clob_cache = {}


def clob_midprice_cached(token_id):
    if token_id in _clob_cache:
        return _clob_cache[token_id]
    price = clob_get_midprice(token_id)
    if price:
        _clob_cache[token_id] = price
    return price


# ----------------------------------------------------------------
# 数据抓取：全量市场
# ----------------------------------------------------------------
def fetch_all_markets():
    markets = []
    offset  = 0
    for _ in range(MAX_PAGES):
        log.info("抓取市场 offset=" + str(offset) + " ...")
        data = gamma_get("/markets", params={
            "limit":             PAGE_SIZE,
            "offset":            offset,
            "active":            "true",
            "closed":            "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        })
        if not data:
            break
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("data", data.get("markets", []))
        else:
            break
        if not batch:
            log.info("已抓取全部市场")
            break
        markets.extend(batch)
        log.info("  已累计 " + str(len(markets)) + " 个市场")
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)
    log.info("✅ 共抓取 " + str(len(markets)) + " 个活跃市场")
    return markets


# ----------------------------------------------------------------
# 套利检测模块
# ----------------------------------------------------------------
def parse_prices(market):
    prices = []
    raw = market.get("outcomePrices")
    if raw:
        try:
            if isinstance(raw, str):
                raw = json.loads(raw)
            prices = [float(p) for p in raw if p is not None]
        except Exception:
            pass
    if not prices:
        tokens = market.get("tokens", [])
        prices = [float(t.get("price", 0)) for t in tokens if t.get("price") is not None]
    return [p for p in prices if 0 < p < 1]


def market_url(market):
    slug = market.get("slug", "")
    if slug:
        return "https://polymarket.com/market/" + slug
    return "N/A"


def detect_bundle_arb(market):
    prices = parse_prices(market)
    if len(prices) != 2:
        return None
    total = sum(prices)
    if total < BUNDLE_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type":      "Bundle ARB (YES+NO < 1)",
            "market":    market.get("question", market.get("slug", "N/A")),
            "url":       market_url(market),
            "prices":    prices,
            "sum":       round(total, 4),
            "edge":      edge,
            "edge_pct":  "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_multi_outcome_arb(market):
    prices = parse_prices(market)
    if len(prices) < 3:
        return None
    total = sum(prices)
    if total < MULTI_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type":      "Multi-Outcome ARB (" + str(len(prices)) + " outcomes)",
            "market":    market.get("question", market.get("slug", "N/A")),
            "url":       market_url(market),
            "prices":    prices,
            "sum":       round(total, 4),
            "edge":      edge,
            "edge_pct":  "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_near_expiry_arb(market):
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        end_date = end_date.replace("Z", "+00:00")
        expiry   = datetime.fromisoformat(end_date)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now        = datetime.now(timezone.utc)
        hours_left = (expiry - now).total_seconds() / 3600
        if 0 < hours_left < NEAR_EXPIRY_HOURS:
            prices     = parse_prices(market)
            suspicious = [p for p in prices
                          if NEAR_EXPIRY_PROB_RANGE[0] < p < NEAR_EXPIRY_PROB_RANGE[1]]
            if suspicious:
                return {
                    "type":       "Near-Expiry Mispricing ({:.1f}h left)".format(hours_left),
                    "market":     market.get("question", market.get("slug", "N/A")),
                    "url":        market_url(market),
                    "prices":     prices,
                    "hours_left": round(hours_left, 2),
                    "suspicious": suspicious,
                    "liquidity":  market.get("liquidity", "N/A"),
                }
    except Exception:
        pass
    return None


def detect_clob_spread_arb(market):
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        return None
    prices_clob = []
    for token in tokens:
        tid = token.get("token_id") or token.get("tokenId")
        if not tid:
            continue
        mid = clob_midprice_cached(tid)
        if mid:
            prices_clob.append(mid)
        time.sleep(0.15)
    if len(prices_clob) == 2:
        total = sum(prices_clob)
        if total < BUNDLE_ARB_THRESHOLD:
            edge = round(1.0 - total, 4)
            return {
                "type":        "CLOB-Confirmed Bundle ARB",
                "market":      market.get("question", market.get("slug", "N/A")),
                "url":         market_url(market),
                "prices_clob": prices_clob,
                "sum":         round(total, 4),
                "edge":        edge,
                "edge_pct":    "{:.2f}%".format(edge * 100),
                "liquidity":   market.get("liquidity", "N/A"),
            }
    return None


# ----------------------------------------------------------------
# 格式化输出
# ----------------------------------------------------------------
def format_opportunity(opp, idx):
    price_list = opp.get("prices") or opp.get("prices_clob", [])
    sep = "=" * 60
    out = sep + "\n"
    out += "🔥 #{:02d}  {}\n".format(idx, opp["type"])
    out += "📋 市场: {}\n".format(opp["market"][:80])
    out += "🔗 链接: {}\n".format(opp.get("url", "N/A"))
    out += "💰 流动性: ${}\n".format(opp.get("liquidity", "N/A"))
    if "sum" in opp:
        out += "📊 价格之和: {}  (套利空间: {})\n".format(opp["sum"], opp.get("edge_pct", "N/A"))
        out += "   各结果价格: {}\n".format(price_list)
    if "hours_left" in opp:
        out += "⏰ 距结算: {} 小时\n".format(opp["hours_left"])
        out += "⚠️  异常价格: {}\n".format(opp["suspicious"])
    return out


def build_telegram_message(opportunities, total_markets):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg  = "<b>🔥 Polymarket 套利扫描 — 发现 " + str(len(opportunities)) + " 个机会</b>\n"
    msg += "⏰ " + now_str + "\n"
    msg += "📦 共扫描市场: " + str(total_markets) + " 个\n\n"
    for idx, opp in enumerate(opportunities[:5], 1):
        url      = opp.get("url", "#")
        mkt_name = opp.get("market", "N/A")[:60]
        msg += "<b>#" + str(idx) + " " + opp["type"] + "</b>\n"
        msg += "📋 " + mkt_name + "\n"
        if "edge_pct" in opp:
            msg += "💹 套利空间: " + opp["edge_pct"] + "\n"
        if "sum" in opp:
            price_list = opp.get("prices") or opp.get("prices_clob", [])
            msg += "📊 价格: " + str(price_list) + "  合计: " + str(opp["sum"]) + "\n"
        if "hours_left" in opp:
            msg += "⏰ 距结算: " + str(opp["hours_left"]) + "h\n"
        msg += '🔗 <a href="' + url + '">查看市场</a>\n\n'
    return msg


# ----------------------------------------------------------------
# 主扫描流程
# ----------------------------------------------------------------
def scan():
    log.info("=" * 60)
    log.info("🚀 Polymarket 套利扫描器启动")
    log.info("⏰ 时间: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("📌 阈值: Bundle={}, Multi={}".format(BUNDLE_ARB_THRESHOLD, MULTI_ARB_THRESHOLD))
    log.info("=" * 60)

    # Step 1: 抓取全量市场
    markets = fetch_all_markets()
    if not markets:
        log.error("❌ 未能获取任何市场数据")
        send_telegram("❌ Polymarket 扫描失败：无法获取市场数据")
        return

    # Step 2: 逐市场初步扫描
    opportunities     = []
    bundle_candidates = []
    for i, market in enumerate(markets):
        if i % 500 == 0 and i > 0:
            log.info("  扫描进度: {}/{} ...".format(i, len(markets)))
        result = detect_bundle_arb(market)
        if result:
            bundle_candidates.append((market, result))
        result = detect_multi_outcome_arb(market)
        if result:
            opportunities.append(result)
        result = detect_near_expiry_arb(market)
