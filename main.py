"""
Polymarket 全市场套利扫描器
套利类型：
  1. Bundle ARB     — YES + NO 价格之和 < 1.00
  2. Multi-Outcome  — 多结果市场所有结果价格之和 < 1.00
  3. Near-Expiry    — 临近结算且价格仍偏离 0/1
  4. CLOB-Confirmed — 通过订单簿中间价二次验证 Bundle ARB
作者: Aidi Zeng | 运行环境: Python 3.10+ / GitHub Actions
"""

import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

# ──────────────────────────────────────────────────────────────
# 配置区
# ──────────────────────────────────────────────────────────────
GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")   # ✅ 对应 GitHub Secret: TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")     # ✅ 对应 GitHub Secret: TELEGRAM_CHAT_ID

# 套利阈值
BUNDLE_ARB_THRESHOLD   = 0.97          # YES+NO 之和低于此值触发
MULTI_ARB_THRESHOLD    = 0.97          # 多结果市场价格之和低于此值触发
NEAR_EXPIRY_HOURS      = 6             # 距结算 N 小时以内
NEAR_EXPIRY_PROB_RANGE = (0.15, 0.85)  # 概率在此区间内视为定价异常

MIN_LIQUIDITY = 500    # 最小流动性（USDC），过滤垃圾市场
PAGE_SIZE     = 100    # Gamma API 每页条数
MAX_PAGES     = 50     # 最多抓取页数（50×100 = 5000 个市场）
REQUEST_DELAY = 0.3    # 请求间隔（秒），避免触发限速

# ──────────────────────────────────────────────────────────────
# 日志
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("PolyArb")


# ──────────────────────────────────────────────────────────────
# Telegram 通知
# ──────────────────────────────────────────────────────────────
def send_telegram(msg: str):
    """向 Telegram Bot 发送消息；未配置时静默跳过。"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] 未配置，跳过推送")
        return
    url = "https://api.telegram.org/bot{}/sendMessage".format(TELEGRAM_TOKEN)
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
            log.warning("[Telegram] 发送失败: {}".format(resp.text))
    except Exception as e:
        log.warning("[Telegram] 异常: {}".format(e))


# ──────────────────────────────────────────────────────────────
# HTTP Session
# ──────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


# ──────────────────────────────────────────────────────────────
# API 工具
# ──────────────────────────────────────────────────────────────
def gamma_get(path: str, params: dict = None) -> Optional[dict]:
    """请求 Gamma REST API，失败时返回 None。"""
    url = "{}{}".format(GAMMA_API, path)
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("Gamma API 请求失败 {}: {}".format(path, e))
        return None


def clob_get_midprice(token_id: str) -> Optional[float]:
    """从 CLOB 订单簿获取某 token 的中间价 (best_buy + best_sell) / 2。"""
    try:
        r_buy  = SESSION.get("{}/price".format(CLOB_API),
                             params={"token_id": token_id, "side": "BUY"},  timeout=8)
        r_sell = SESSION.get("{}/price".format(CLOB_API),
                             params={"token_id": token_id, "side": "SELL"}, timeout=8)
        buy_price  = float(r_buy.json().get("price",  0))
        sell_price = float(r_sell.json().get("price", 0))
        if buy_price > 0 and sell_price > 0:
            return (buy_price + sell_price) / 2
        return buy_price or sell_price or None
    except Exception:
        return None


# CLOB 中间价缓存，避免重复请求
_clob_cache = {}


def clob_midprice_cached(token_id: str) -> Optional[float]:
    """带缓存的 CLOB 中间价查询。"""
    if token_id in _clob_cache:
        return _clob_cache[token_id]
    price = clob_get_midprice(token_id)
    if price:
        _clob_cache[token_id] = price
    return price


# ──────────────────────────────────────────────────────────────
# 数据抓取：全量市场
# ──────────────────────────────────────────────────────────────
def fetch_all_markets() -> list:
    """分页抓取所有活跃市场，过滤流动性不足的市场。"""
    markets = []
    offset  = 0

    for _ in range(MAX_PAGES):
        log.info("抓取市场 offset={} ...".format(offset))
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
        log.info("  已累计 {} 个市场".format(len(markets)))

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    log.info("✅ 共抓取 {} 个活跃市场".format(len(markets)))
    return markets


# ──────────────────────────────────────────────────────────────
# 套利检测模块
# ──────────────────────────────────────────────────────────────
def parse_prices(market: dict) -> list:
    """解析市场中各 outcome 的价格，过滤掉 0 和 1 边界值。"""
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


def market_url(market: dict) -> str:
    """拼接市场链接。"""
    slug = market.get("slug", "")
    return "https://polymarket.com/market/{}".format(slug) if slug else "N/A"


def detect_bundle_arb(market: dict) -> Optional[dict]:
    """
    Bundle 套利：YES + NO 价格之和 < 阈值
    原理：YES + NO 应等于 $1.00，若 < 0.97 则可同时买入双边锁定利润。
    """
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


def detect_multi_outcome_arb(market: dict) -> Optional[dict]:
    """
    多结果市场套利：所有结果价格之和 < 阈值
    原理：A + B + C + ... 应等于 $1.00，若 < 0.97 则可全部买入。
    """
    prices = parse_prices(market)
    if len(prices) < 3:
        return None
    total = sum(prices)
    if total < MULTI_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type":      "Multi-Outcome ARB ({} outcomes)".format(len(prices)),
            "market":    market.get("question", market.get("slug", "N/A")),
            "url":       market_url(market),
            "prices":    prices,
            "sum":       round(total, 4),
            "edge":      edge,
            "edge_pct":  "{:.2f}%".format(edge * 100),
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_near_expiry_arb(market: dict) -> Optional[dict]:
    """
    临近结算异常：距结算 < N 小时，但价格仍在 [0.15, 0.85] 区间。
    原理：结算前几小时结果基本确定，价格偏离 0/1 说明市场定价异常。
    """
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


def detect_clob_spread_arb(market: dict) -> Optional[dict]:
    """
    CLOB 订单簿确认：用实际中间价二次验证 Bundle ARB。
    仅在 Bundle ARB 候选命中时调用，避免过多 API 请求。
    """
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


# ──────────────────────────────────────────────────────────────
# 格式化输出
# ──────────────────────────────────────────────────────────────
def format_opportunity(opp: dict, idx: int) -> str:
    """将套利机会格式化为终端可读字符串。"""
    price_list = opp.get("prices") or opp.get("prices_clob", [])
    lines = [
        "=" * 60,
        "🔥 #{:02d}  {}".format(idx, opp["type"]),
        "📋 市场: {}".format(opp["market"][:80]),
        "🔗 链接: {}".format(opp.get("url", "N/A")),
        "💰 流动性: ${}".format(opp.get("liquidity", "N/A")),
    ]
    if "sum" in opp:
        lines.append("📊 价格之和: {}  (套利空间: {})".format(
            opp["sum"], opp.get("edge_pct", "N/A")))
        lines.append("   各结果价格: {}".format(price_list))
    if "hours_left" in opp:
        lines.append("⏰ 距结算: {} 小时".format(opp["hours_left"]))
        lines.append("⚠️  异常价格: {}".format(opp["suspicious"]))
    return "
".join(lines)


def build_telegram_message(opportunities: list, total_markets: int) -> str:
    """构建发往 Telegram 的 HTML 消息，展示 TOP 5 机会。"""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "<b>🔥 Polymarket 套利扫描 — 发现 {} 个机会</b>".format(len(opportunities)),
        "⏰ {}".format(now_str),
        "📦 共扫描市场: {} 个".format(total_markets),
        "",
    ]
    for idx, opp in enumerate(opportunities[:5], 1):
        url      = opp.get("url", "#")
        mkt_name = opp.get("market", "N/A")[:60]
        lines.append("<b>#{} {}</b>".format(idx, opp["type"]))
        lines.append("📋 {}".format(mkt_name))
        if "edge_pct" in opp:
            lines.append("💹 套利空间: {}".format(opp["edge_pct"]))
        if "sum" in opp:
            price_list = opp.get("prices") or opp.get("prices_clob", [])
            lines.append("📊 价格: {}  合计: {}".format(price_list, opp["sum"]))
        if "hours_left" in opp:
            lines.append("⏰ 距结算: {}h".format(opp["hours_left"]))
        lines.append('🔗 <a href="{}">查看市场</a>'.format(url))
        lines.append("")
    return "
".join(lines)


# ──────────────────────────────────────────────────────────────
# 主扫描流程
# ──────────────────────────────────────────────────────────────
def scan():
    log.info("=" * 60)
    log.info("🚀 Polymarket 套利扫描器启动")
    log.info("⏰ 时间: {}".format(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
    log.info("📌 阈值: Bundle={}, Multi={}".format(BUNDLE_ARB_THRESHOLD, MULTI_ARB_THRESHOLD))
    log.info("=" * 60)

    # ── Step 1: 抓取全量市场 ──────────────────────────────────
    markets = fetch_all_markets()
    if not markets:
        log.error("❌ 未能获取任何市场数据，请检查网络或 API 状态")
        send_telegram("❌ Polymarket 扫描失败：无法获取市场数据")
        return

    # ── Step 2: 逐市场初步扫描 ────────────────────────────────
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
        if result:
            opportunities.append(result)

    log.info("📊 初步扫描完成: Bundle候选={}, 其他机会={}".format(
        len(bundle_candidates), len(opportunities)))

    # ── Step 3: CLOB 二次验证 Bundle 候选 ────────────────────
    log.info("🔍 CLOB 二次验证 {} 个 Bundle 候选...".format(len(bundle_candidates)))
    for market, prelim in bundle_candidates:
        confirmed = detect_clob_spread_arb(market)
        opportunities.append(confirmed if confirmed else prelim)
        time.sleep(REQUEST_DELAY)

    # ── Step 4: 按套利空间降序排列 ───────────────────────────
    opportunities.sort(key=lambda o: o.get("edge", 0), reverse=True)

    # ── Step 5: 输出结果 ─────────────────────────────────────
    log.info("=" * 60)
    log.info("✅ 扫描完成！发现 {} 个套利机会".format(len(opportunities)))
    log.info("=" * 60)

    if not opportunities:
        log.info("😴 本轮未发现套利机会")
        send_telegram(
            "😴 <b>Polymarket 套利扫描完成</b>\n"
            "⏰ {}\n"
            "📦 共扫描: {} 个市场\n"
            "📭 本轮无套利机会".format(
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                len(markets)
            )
        )
    else:
        for idx, opp in enumerate(opportunities, 1):
            print(format_opportunity(opp, idx))
        send_telegram(build_telegram_message(opportunities, len(markets)))

    # ── Step 6: 保存 JSON 报告 ────────────────────────────────
    report = {
        "scan_time":             datetime.now(timezone.utc).isoformat(),
        "total_markets_scanned": len(markets),
        "opportunities_found":   len(opportunities),
        "opportunities":         opportunities,
    }
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    log.info("📁 报告已保存至 arb_report.json")


if __name__ == "__main__":
    scan()
