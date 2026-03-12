"""
Polymarket 全市场套利扫描器
套利类型：
  1. Bundle Arb    — YES + NO 价格之和 < 1.00（即同一市场的两个结果加起来不等于$1）
  2. Multi-Outcome — 多结果市场所有结果价格之和 < 1.00（如 A/B/C 三选一）
  3. Cross-Market  — 同一事件的不同切割方式存在价格矛盾
  4. Near-Expiry   — 临近结算且价格严重偏离 0/1 的市场（高确定性机会）
作者: Aidi Zeng | 运行环境: Python 3.10+ / GitHub Actions
"""

import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

# ──────────────────────────────────────────────
# 配置区
# ──────────────────────────────────────────────
GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")      # GitHub Secret
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")    # GitHub Secret

# 套利阈值
BUNDLE_ARB_THRESHOLD    = 0.97   # YES+NO 之和低于此值触发
MULTI_ARB_THRESHOLD     = 0.97   # 多结果市场价格之和低于此值触发
NEAR_EXPIRY_HOURS       = 6      # 距离结算 N 小时以内
NEAR_EXPIRY_PROB_RANGE  = (0.15, 0.85)  # 概率仍在此范围内视为异常

MIN_LIQUIDITY   = 500    # 最小流动性（USDC），过滤垃圾市场
PAGE_SIZE       = 100    # Gamma API 每页条数
MAX_PAGES       = 50     # 最多抓取页数（50*100=5000 个市场）
REQUEST_DELAY   = 0.3    # 请求间隔（秒），避免触发限速

# ──────────────────────────────────────────────
# 日志
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("PolyArb")


# ──────────────────────────────────────────────
# Telegram 通知
# ──────────────────────────────────────────────
def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] 未配置，跳过推送")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT,
            "text": msg,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            log.info("[Telegram] 消息发送成功")
        else:
            log.warning(f"[Telegram] 发送失败: {resp.text}")
    except Exception as e:
        log.warning(f"[Telegram] 异常: {e}")


# ──────────────────────────────────────────────
# API 工具
# ──────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def gamma_get(path: str, params: dict = None) -> Optional[dict]:
    url = f"{GAMMA_API}{path}"
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Gamma API 请求失败 {path}: {e}")
        return None


def clob_get_midprice(token_id: str) -> Optional[float]:
    """从 CLOB 获取某 token 的中间价（(best_bid + best_ask) / 2）"""
    try:
        r = SESSION.get(f"{CLOB_API}/price", params={"token_id": token_id, "side": "BUY"}, timeout=8)
        buy_price = float(r.json().get("price", 0))
        r2 = SESSION.get(f"{CLOB_API}/price", params={"token_id": token_id, "side": "SELL"}, timeout=8)
        sell_price = float(r2.json().get("price", 0))
        if buy_price > 0 and sell_price > 0:
            return (buy_price + sell_price) / 2
        return buy_price or sell_price or None
    except Exception:
        return None


# ──────────────────────────────────────────────
# 数据抓取：全量市场
# ──────────────────────────────────────────────
def fetch_all_markets() -> list[dict]:
    """分页抓取所有活跃市场，过滤流动性不足的市场"""
    markets = []
    offset  = 0

    for page in range(MAX_PAGES):
        log.info(f"抓取市场 offset={offset} ...")
        data = gamma_get("/markets", params={
            "limit":           PAGE_SIZE,
            "offset":          offset,
            "active":          "true",
            "closed":          "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        })

        if not data:
            break

        # Gamma API 可能返回 list 或 {"data": list}
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
        log.info(f"  已累计 {len(markets)} 个市场")

        if len(batch) < PAGE_SIZE:
            break  # 最后一页

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    log.info(f"✅ 共抓取 {len(markets)} 个活跃市场")
    return markets


# ──────────────────────────────────────────────
# 套利检测模块
# ──────────────────────────────────────────────

def parse_prices(market: dict) -> list[float]:
    """解析市场中各 outcome 的价格"""
    prices = []

    # 方式1: outcomePrices 字段（JSON字符串列表）
    raw = market.get("outcomePrices")
    if raw:
        try:
            if isinstance(raw, str):
                raw = json.loads(raw)
            prices = [float(p) for p in raw if p is not None]
        except Exception:
            pass

    # 方式2: tokens 列表
    if not prices:
        tokens = market.get("tokens", [])
        prices = [float(t.get("price", 0)) for t in tokens if t.get("price") is not None]

    return [p for p in prices if 0 < p < 1]


def detect_bundle_arb(market: dict) -> Optional[dict]:
    """
    Bundle 套利：YES + NO 价格之和 < 阈值
    原理：YES+NO 应等于 $1.00，若 < 0.97 则可同时买入双边，锁定利润
    """
    prices = parse_prices(market)
    if len(prices) != 2:
        return None

    total = sum(prices)
    if total < BUNDLE_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type":     "Bundle ARB (YES+NO < 1)",
            "market":   market.get("question", market.get("slug", "N/A")),
            "url":      f"https://polymarket.com/market/{market.get('slug', '')}",
            "prices":   prices,
            "sum":      round(total, 4),
            "edge":     edge,
            "edge_pct": f"{edge*100:.2f}%",
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_multi_outcome_arb(market: dict) -> Optional[dict]:
    """
    多结果市场套利：所有结果价格之和 < 阈值
    原理：A+B+C+... 应等于 $1.00，若 < 0.97 则可全部买入
    """
    prices = parse_prices(market)
    if len(prices) < 3:
        return None

    total = sum(prices)
    if total < MULTI_ARB_THRESHOLD:
        edge = round(1.0 - total, 4)
        return {
            "type":     f"Multi-Outcome ARB ({len(prices)} outcomes)",
            "market":   market.get("question", market.get("slug", "N/A")),
            "url":      f"https://polymarket.com/market/{market.get('slug', '')}",
            "prices":   prices,
            "sum":      round(total, 4),
            "edge":     edge,
            "edge_pct": f"{edge*100:.2f}%",
            "liquidity": market.get("liquidity", "N/A"),
        }
    return None


def detect_near_expiry_arb(market: dict) -> Optional[dict]:
    """
    临近结算异常：距结算 < N 小时，但价格仍在 [0.15, 0.85] 区间
    原理：结算前几小时结果基本确定，价格偏离 0/1 说明市场定价异常
    """
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None

    try:
        # 兼容多种时间格式
        end_date = end_date.replace("Z", "+00:00")
        expiry   = datetime.fromisoformat(end_date)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now      = datetime.now(timezone.utc)
        hours_left = (expiry - now).total_seconds() / 3600

        if 0 < hours_left < NEAR_EXPIRY_HOURS:
            prices = parse_prices(market)
            suspicious = [p for p in prices if NEAR_EXPIRY_PROB_RANGE[0] < p < NEAR_EXPIRY_PROB_RANGE[1]]
            if suspicious:
                return {
                    "type":       f"Near-Expiry Mispricing (≤{hours_left:.1f}h left)",
                    "market":     market.get("question", market.get("slug", "N/A")),
                    "url":        f"https://polymarket.com/market/{market.get('slug', '')}",
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
    CLOB 买卖价差套利：从订单簿层面验证 Bundle ARB
    仅在 Bundle ARB 疑似命中时调用，避免过多 API 请求
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
                "type":     "CLOB-Confirmed Bundle ARB",
                "market":   market.get("question", market.get("slug", "N/A")),
                "url":      f"https://polymarket.com/market/{market.get('slug', '')}",
                "prices_clob": prices_clob,
                "sum":      round(total, 4),
                "edge":     edge,
                "edge_pct": f"{edge*100:.2f}%",
                "liquidity": market.get("liquidity", "N/A"),
            }
    return None


# CLOB 中间价缓存，避免重复请求
_clob_cache: dict[str, float] = {}

def clob_midprice_cached(token_id: str) -> Optional[float]:
    if token_id in _clob_cache:
        return _clob_cache[token_id]
    price = clob_get_midprice(token_id)
    if price:
        _clob_cache[token_id] = price
    return price


# ──────────────────────────────────────────────
# 主扫描流程
# ──────────────────────────────────────────────
def format_opportunity(opp: dict, idx: int) -> str:
    """格式化套利机会为可读字符串"""
    lines = [
        f"{'='*60}",
        f"🔥 #{idx} {opp['type']}",
        f"📋 市场: {opp['market'][:80]}",
        f"🔗 链接: {opp.get('url', 'N/A')}",
        f"💰 流动性: ${opp.get('liquidity', 'N/A')}",
    ]

    if "sum" in opp:
        lines.append(f"📊 价格之和: {opp['sum']} (套利空间: {opp.get('edge_pct', 'N/A')})")
        lines.append(f"   各结果价格: {opp.get('prices') or opp.get('prices_clob', [])}")

    if "hours_left" in opp:
        lines.append(f"⏰ 距结算: {opp['hours_left']} 小时")
        lines.append(f"⚠️  异常价格: {opp['suspicious']}")

    return "\n".join(lines)


def scan():
    log.info("=" * 60)
    log.info("🚀 Polymarket 套利扫描器启动")
    log.info(f"⏰ 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"📌 阈值: Bundle={BUNDLE_ARB_THRESHOLD}, Multi={MULTI_ARB_THRESHOLD}")
    log.info("=" * 60)

    # Step 1: 抓取全量市场
    markets = fetch_all_markets()
    if not markets:
        log.error("❌ 未能获取任何市场数据，请检查网络或 API 状态")
        return

    # Step 2: 逐市场扫描
    opportunities = []
    bundle_candidates = []  # 待 CLOB 二次验证的候选

    for i, market in enumerate(markets):
        if i % 500 == 0 and i > 0:
            log.info(f"  扫描进度: {i}/{len(markets)} ...")

        # 检测1: Bundle ARB
        result = detect_bundle_arb(market)
        if result:
            bundle_candidates.append((market, result))

        # 检测2: Multi-Outcome ARB
        result = detect_multi_outcome_arb(market)
        if result:
            opportunities.append(result)

        # 检测3: Near-Expiry
        result = detect_near_expiry_arb(market)
        if result:
            opportunities.append(result)

    log.info(f"📊 初步扫描完成: Bundle候选={len(bundle_candidates)}, 其他机会={len(opportunities)}")

    # Step 3: CLOB 二次验证 Bundle 候选
    log.info(f"🔍 CLOB 二次验证 {len(bundle_candidates)} 个 Bundle 候选...")
    for market, prelim in bundle_candidates:
        confirmed = detect_clob_spread_arb(market)
        if confirmed:
            opportunities.append(confirmed)
        else:
            # 即使 CLOB 未确认，gamma 层面的 prelim 也值得记录
            opportunities.append(prelim)
        time.sleep(REQUEST_DELAY)

    # Step 4: 按套利空间排序
    def sort_key(o):
        return o.get("edge", 0) if "edge" in o else 0.05

    opportunities.sort(key=sort_key, reverse=True)

    # Step 5: 输出结果
    log.info(f"\n{'='*60}")
    log.info(f"✅ 扫描完成！发现 {len(opportunities)} 个套利机会")
    log.info(f"{'='*60}\n")

    if not opportunities:
        log.info("😴 本轮未发现套利机会")
        send_telegram("😴 Polymarket 套利扫描完成，本轮无机会")
        return

    # 打印全部
    for idx, opp in enumerate(opportunities, 1):
        print(format_opportunity(opp, idx))

    # Telegram 只推送 TOP 5
    top5 = opportunities[:5]
    tg_msg = f"<b>🔥 Polymarket 套利扫描 — 发现 {len(opportunities)} 个机会</b>\n"
    tg_msg += f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"

    for idx, opp in enumerate(top5, 1):
        tg_msg += f"<b>#{idx} {opp['type']}</b>\n"
        tg_msg += f"📋 {opp['market'][:60]}\n"
        if "edge_pct" in opp:
            tg_msg += f"💹 套利空间: {opp['edge_pct']}\n"
        tg_msg += f"🔗 <a href='{opp.get('url', '')}'>查看市场</a>\n\n"

    send_telegram(tg_msg)

    # 保存 JSON 报告
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump({
            "scan_time": datetime.now(timezone.utc).isoformat(),
            "total_markets_scanned": len(markets),
            "opportunities_found":   len(opportunities),
            "opportunities":         opportunities,
        }, f, ensure_ascii=False, indent=2)
    log.info("📁 报告已保存至 arb_report.json")


if __name__ == "__main__":
    scan()
