#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================
  main.py — WeatherArb Bot (单次运行模式)
========================================================

功能:
  1. 扫描 Polymarket 天气相关市场
  2. 用 Open-Meteo 双模型集成预报 (best_match + ECMWF) 计算真实概率
  3. 贝叶斯正态CDF 对比市场隐含概率，识别 Edge >= 12% 的套利机会
  4. Quarter-Kelly 仓位建议，通过 Telegram 推送信号
  5. 单次运行模式：跑完一轮立即退出，由 GitHub Actions cron 控制触发频率

环境变量:
  TELEGRAM_BOT_TOKEN   — Telegram Bot Token
  TELEGRAM_CHAT_ID     — Telegram Chat ID (数字)

依赖:
  pip install requests scipy python-dateutil
========================================================
"""

import os
import time
import json
import math
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple, Any

import requests
from scipy.stats import norm
from dateutil import parser as dateutil_parser

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
GAMMA_API           = "https://gamma-api.polymarket.com"
CLOB_API            = "https://clob.polymarket.com"
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT       = os.getenv("TELEGRAM_CHAT_ID", "")

MIN_LIQUIDITY       = 500
PAGE_SIZE           = 100
MAX_PAGES           = 50
REQUEST_DELAY       = 0.3
SCAN_WINDOW_HOURS   = 72
WEATHER_EDGE_MIN    = 0.12
MIN_EDGE            = 0.08
WATCH_PUSH_MIN      = 0.10
MODEL_WEIGHT_BEST   = 0.60
MODEL_WEIGHT_ECMWF  = 0.40
DEFAULT_UNCERTAINTY = 1.5
MIN_UNCERTAINTY     = 1.0

CITY_COORDS: Dict[str, Tuple[float, float]] = {
    "new york":      (40.71, -74.01),
    "nyc":           (40.71, -74.01),
    "los angeles":   (34.05, -118.24),
    "la":            (34.05, -118.24),
    "chicago":       (41.88, -87.63),
    "houston":       (29.76, -95.37),
    "miami":         (25.77, -80.19),
    "london":        (51.51, -0.13),
    "paris":         (48.85,   2.35),
    "tokyo":         (35.69, 139.69),
    "beijing":       (39.91, 116.39),
    "sydney":        (-33.87, 151.21),
    "dubai":         (25.20,  55.27),
    "singapore":     ( 1.35, 103.82),
    "toronto":       (43.65, -79.38),
    "berlin":        (52.52,  13.41),
    "moscow":        (55.75,  37.62),
    "rome":          (41.90,  12.49),
    "madrid":        (40.42,  -3.70),
    "amsterdam":     (52.37,   4.90),
    "seoul":         (37.57, 126.98),
    "mumbai":        (19.08,  72.88),
    "cairo":         (30.04,  31.24),
    "lagos":         ( 6.45,   3.39),
    "sao paulo":     (-23.55, -46.63),
    "mexico city":   (19.43, -99.13),
    "buenos aires":  (-34.60, -58.38),
    "istanbul":      (41.01,  28.95),
    "bangkok":       (13.75, 100.52),
    "jakarta":       (-6.21, 106.85),
    "manila":        (14.60, 120.98),
    "karachi":       (24.86,  67.01),
    "dallas":        (32.78, -96.80),
    "seattle":       (47.61, -122.33),
    "boston":        (42.36, -71.06),
    "denver":        (39.74, -104.98),
    "atlanta":       (33.75, -84.39),
    "sf":            (37.77, -122.42),
    "san francisco": (37.77, -122.42),
    "new orleans":   (29.95, -90.07),
    "las vegas":     (36.17, -115.14),
    "phoenix":       (33.45, -112.07),
    "florida":       (27.99, -81.76),
    "texas":         (31.00, -100.00),
    "california":    (36.78, -119.42),
    "midwest":       (41.88, -87.63),
    "gulf coast":    (29.76, -95.37),
    "northeast":     (42.36, -71.06),
    "hong kong":     (22.32, 114.17),
    "taipei":        (25.03, 121.56),
    "kuala lumpur":  ( 3.14, 101.69),
    "johannesburg":  (-26.20,  28.04),
    "nairobi":       (-1.29,  36.82),
    "lima":          (-12.05, -77.04),
    "santiago":      (-33.45, -70.67),
    "vancouver":     (49.25, -123.12),
    "montreal":      (45.50, -73.57),
    "stockholm":     (59.33,  18.07),
    "oslo":          (59.91,  10.75),
    "copenhagen":    (55.68,  12.57),
    "helsinki":      (60.17,  24.94),
    "vienna":        (48.21,  16.37),
    "zurich":        (47.38,   8.54),
    "brussels":      (50.85,   4.35),
    "lisbon":        (38.72,  -9.14),
    "athens":        (37.98,  23.73),
    "warsaw":        (52.23,  21.01),
    "prague":        (50.08,  14.44),
    "budapest":      (47.50,  19.04),
    "bucharest":     (44.43,  26.10),
    "kiev":          (50.45,  30.52),
    "riyadh":        (24.69,  46.72),
    "tehran":        (35.69,  51.39),
    "baghdad":       (33.34,  44.40),
    "lahore":        (31.55,  74.34),
    "dhaka":         (23.81,  90.41),
    "colombo":       ( 6.93,  79.85),
    "kathmandu":     (27.72,  85.32),
    "yangon":        (16.87,  96.19),
    "ho chi minh":   (10.82, 106.63),
    "hanoi":         (21.03, 105.85),
    "phnom penh":    (11.57, 104.92),
    "vientiane":     (17.97, 102.63),
}

WEATHER_KEYWORDS = [
    "temperature", "rain", "snow", "hurricane", "storm", "wind", "flood",
    "celsius", "fahrenheit", "precipitation", "weather", "degrees", "hot",
    "cold", "warm", "freeze", "blizzard", "tornado", "typhoon", "cyclone",
    "landfall", "tropical", "drought", "heatwave", "heat wave", "exceed",
    "reaches", "high temp", "low temp", "record high", "record low",
    "snowfall", "rainfall", "mph", "category", "wildfire", "heat",
]

# ----------------------------------------------------------------
# Logging
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("WeatherArb")

# ----------------------------------------------------------------
# Telegram (分段发送，超4000字自动切割)
# ----------------------------------------------------------------
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.info("[Telegram] not configured, skipping")
        return
    api_url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    max_len = 4000
    chunks = []
    while len(msg) > max_len:
        split_pos = msg.rfind("\n", 0, max_len)
        if split_pos == -1:
            split_pos = max_len
        chunks.append(msg[:split_pos])
        msg = msg[split_pos:].lstrip("\n")
    chunks.append(msg)
    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            resp = requests.post(api_url, json={
                "chat_id":                  TELEGRAM_CHAT,
                "text":                     chunk,
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            }, timeout=10)
            if resp.status_code == 200:
                log.info("[Telegram] sent ok (%d chars)", len(chunk))
            else:
                log.warning("[Telegram] failed: %s", resp.text)
        except Exception as ex:
            log.warning("[Telegram] error: %s", str(ex))
        time.sleep(0.5)

# ----------------------------------------------------------------
# HTTP Session
# ----------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

# ----------------------------------------------------------------
# Retry GET
# ----------------------------------------------------------------
def retry_get(url, params=None, timeout=10, retries=3):
    """带指数退避的 GET 请求，最多重试 retries 次，返回 Response 或 None。"""
    for attempt in range(1, retries + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as ex:
            wait = 2 ** attempt
            log.warning("retry_get attempt %d/%d failed (%s) -> wait %ds",
                        attempt, retries, str(ex), wait)
            time.sleep(wait)
    log.error("retry_get gave up: %s", url)
    return None

# ----------------------------------------------------------------
# Helpers
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

def get_url(market):
    slug = market.get("slug", "")
    if slug:
        return "https://polymarket.com/market/" + slug
    return "N/A"

def hours_until_expiry(market):
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        ed = end_date.replace("Z", "+00:00")
        expiry = datetime.fromisoformat(ed)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        h = (expiry - datetime.now(timezone.utc)).total_seconds() / 3600
        return h if h > 0 else None
    except Exception:
        return None

def get_urgency(hours_left):
    if hours_left is None:
        return "WATCH", "\U0001f7e1 WATCH"
    if hours_left < 6:
        return "URGENT", "\U0001f534 URGENT"
    if hours_left < 72:
        return "WATCH", "\U0001f7e1 WATCH"
    return "EARLY", "\U0001f7e2 EARLY"

def sanitize_for_json(obj):
    """递归清理 nan/inf，替换为 None。"""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj

# ----------------------------------------------------------------
# Fetch weather markets
# ----------------------------------------------------------------
def fetch_weather_markets():
    """翻页拉取 Polymarket 市场，过滤出天气相关且在72h内到期的市场。"""
    all_markets = []
    offset = 0
    for page in range(MAX_PAGES):
        log.info("fetching page %d (offset=%d)...", page + 1, offset)
        resp = retry_get(GAMMA_API + "/markets", params={
            "limit":             PAGE_SIZE,
            "offset":            offset,
            "active":            "true",
            "closed":            "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        })
        if resp is None:
            break
        try:
            data = resp.json()
        except Exception:
            break
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("data", data.get("markets", []))
        else:
            break
        if not batch:
            break
        all_markets.extend(batch)
        log.info("  total fetched so far: %d", len(all_markets))
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    log.info("raw markets fetched: %d", len(all_markets))

    filtered = []
    for m in all_markets:
        q = (m.get("question") or m.get("title") or "").lower()
        if not any(kw in q for kw in WEATHER_KEYWORDS):
            continue
        h = hours_until_expiry(m)
        if h is None or h > SCAN_WINDOW_HOURS:
            continue
        filtered.append(m)

    log.info("weather markets within %dh: %d", SCAN_WINDOW_HOURS, len(filtered))
    return filtered

# ----------------------------------------------------------------
# Weather Fetcher (Open-Meteo 双模型集成)
# ----------------------------------------------------------------
class WeatherFetcher:
    """从 Open-Meteo 获取 best_match + ECMWF 双模型集成气象预报。"""

    OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
    MODEL_WEIGHTS  = {
        "best_match":   MODEL_WEIGHT_BEST,
        "ecmwf_ifs025": MODEL_WEIGHT_ECMWF,
    }

    def __init__(self):
        self._cache: Dict[str, Any] = {}

    @staticmethod
    def celsius_to_fahrenheit(c):
        return c * 9.0 / 5.0 + 32.0

    @staticmethod
    def fahrenheit_to_celsius(f):
        return (f - 32.0) * 5.0 / 9.0

    def _fetch_model(self, lat, lon, model):
        """获取单一模型的 daily temperature_2m_max 预报，返回 {date: temp_c} 或 None。"""
        resp = retry_get(self.OPEN_METEO_URL, params={
            "latitude":      lat,
            "longitude":     lon,
            "daily":         "temperature_2m_max",
            "models":        model,
            "timezone":      "UTC",
            "forecast_days": 7,
        })
        if resp is None:
            return None
        try:
            data  = resp.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            temps = daily.get("temperature_2m_max", [])
            return {d: t for d, t in zip(dates, temps) if t is not None}
        except Exception as ex:
            log.warning("_fetch_model parse error (%s): %s", model, str(ex))
            return None

    def get_ensemble_forecast(self, city_name, target_date):
        """
        获取指定城市 + 日期的双模型集成最高温预报。

        Returns:
            dict(ensemble_temp, std, model_temps, city, date) 或 None
        """
        cache_key = "%s_%s" % (city_name, target_date)
        if cache_key in self._cache:
            return self._cache[cache_key]

        coords = CITY_COORDS.get(city_name)
        if coords is None:
            log.warning("get_ensemble_forecast: unknown city '%s'", city_name)
            return None

        lat, lon = coords
        model_results: Dict[str, float] = {}
        for model in self.MODEL_WEIGHTS:
            result = self._fetch_model(lat, lon, model)
            if result and target_date in result:
                model_results[model] = result[target_date]
            else:
                log.warning("no data for %s/%s from model %s", city_name, target_date, model)

        if not model_results:
            log.error("all models failed for %s/%s", city_name, target_date)
            return None

        total_w = sum(self.MODEL_WEIGHTS[m] for m in model_results)
        ensemble_temp = sum(
            self.MODEL_WEIGHTS[m] * t / total_w
            for m, t in model_results.items()
        )

        temps_list = list(model_results.values())
        if len(temps_list) > 1:
            mean_t = sum(temps_list) / len(temps_list)
            variance = sum((t - mean_t) ** 2 for t in temps_list) / len(temps_list)
            std = max(math.sqrt(variance), MIN_UNCERTAINTY)
        else:
            std = DEFAULT_UNCERTAINTY

        result = {
            "ensemble_temp": ensemble_temp,
            "std":           std,
            "model_temps":   model_results,
            "city":          city_name,
            "date":          target_date,
        }
        self._cache[cache_key] = result
        log.info("[%s] %s => ensemble=%.1fC +/-%.1fC  models=%s",
                 city_name, target_date, ensemble_temp, std, str(model_results))
        return result

# ----------------------------------------------------------------
# Probability Engine (贝叶斯正态CDF)
# ----------------------------------------------------------------
class ProbabilityEngine:
    """使用正态分布 CDF 计算温度超/低于阈值的概率。"""

    @staticmethod
    def calc_probability(forecast_temp, uncertainty_std, threshold, operator="above"):
        if operator == "above":
            p = 1.0 - norm.cdf(threshold, loc=forecast_temp, scale=uncertainty_std)
        elif operator == "below":
            p = norm.cdf(threshold, loc=forecast_temp, scale=uncertainty_std)
        elif operator == "between" and isinstance(threshold, (tuple, list)):
            low, high = threshold
            p = (norm.cdf(high, loc=forecast_temp, scale=uncertainty_std) -
                 norm.cdf(low,  loc=forecast_temp, scale=uncertainty_std))
        else:
            log.warning("calc_probability: unknown operator '%s', returning 0.5", operator)
            return 0.5
        return float(max(0.005, min(0.995, p)))

    @staticmethod
    def parse_market_title(title):
        q_low = title.lower()

        # 1. 城市识别（最长匹配优先）
        matched_city = None
        best_len = 0
        for city_key in CITY_COORDS:
            if city_key in q_low and len(city_key) > best_len:
                matched_city = city_key
                best_len = len(city_key)
        if matched_city is None:
            return None

        # 2. 日期识别
        parsed_date = None
        date_patterns = [
            r"on\s+(january|february|march|april|may|june|july|august"
            r"|september|october|november|december)\s+(\d{1,2})",
            r"(\d{4}-\d{2}-\d{2})",
        ]
        for pattern in date_patterns:
            m = re.search(pattern, q_low)
            if m:
                try:
                    raw_str = m.group(0).replace("on ", "")
                    now_year = datetime.now(timezone.utc).year
                    dt = dateutil_parser.parse(raw_str,
                         default=datetime(now_year, 1, 1, tzinfo=timezone.utc))
                    parsed_date = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                break
        if not parsed_date:
            parsed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # 3. 温度阈值 & 方向
        threshold_match = re.search(
            r"(above|exceed|over|below|under)\s*(-?\d+(?:\.\d+)?)\s*(degrees|deg|f|c)?",
            q_low
        )
        if not threshold_match:
            return None

        direction_word = threshold_match.group(1)
        raw_temp       = float(threshold_match.group(2))
        unit_raw       = (threshold_match.group(3) or "").strip().lower()

        # 4. 单位识别
        if "f" in unit_raw and "c" not in unit_raw:
            unit      = "F"
            threshold = WeatherFetcher.fahrenheit_to_celsius(raw_temp)
        else:
            unit      = "C"
            threshold = raw_temp

        # 5. 方向映射
        if direction_word in ("above", "exceed", "over"):
            operator = "above"
        else:
            operator = "below"

        return {
            "city":      matched_city,
            "date":      parsed_date,
            "threshold": threshold,
            "operator":  operator,
            "unit":      unit,
            "raw_temp":  raw_temp,
        }

# ----------------------------------------------------------------
# Kelly 仓位计算
# ----------------------------------------------------------------
def calc_kelly(model_prob, yes_price, direction):
    market_price = yes_price if direction == "BUY_YES" else (1.0 - yes_price)
    if market_price <= 0.01 or market_price >= 0.99:
        return 0.01
    b = 1.0 / market_price - 1.0
    p = model_prob
    kelly_full = (p * b - (1.0 - p)) / b
    return float(max(0.0, min(kelly_full * 0.25, 0.15)))

# ----------------------------------------------------------------
# Weather ARB Detector
# ----------------------------------------------------------------
class WeatherArbDetector:
    """扫描市场，识别天气套利机会。"""

    def __init__(self):
        self.fetcher  = WeatherFetcher()
        self.prob_eng = ProbabilityEngine()

    def find_opportunities(self, markets):
        opps = []
        for market in markets:
            title = market.get("question") or market.get("title") or ""
            if not title:
                continue

            parsed = self.prob_eng.parse_market_title(title)
            if not parsed:
                continue

            city      = parsed["city"]
            date      = parsed["date"]
            threshold = parsed["threshold"]
            operator  = parsed["operator"]

            forecast = self.fetcher.get_ensemble_forecast(city, date)
            if not forecast:
                continue

            ensemble_temp = forecast["ensemble_temp"]
            uncertainty   = forecast["std"]
            model_temps   = forecast["model_temps"]

            model_prob = self.prob_eng.calc_probability(
                ensemble_temp, uncertainty, threshold, operator
            )

            prices = parse_prices(market)
            if not prices:
                continue
            yes_price = prices[0]

            edge = round(abs(model_prob - yes_price), 4)
            if edge < WEATHER_EDGE_MIN:
                continue

            direction  = "BUY_YES" if model_prob > yes_price else "BUY_NO"
            confidence = "\u5f3a\u503e\u5411" if edge >= 0.15 else "\u4e2d\u503e\u5411"
            kelly_size = calc_kelly(model_prob, yes_price, direction)

            h = hours_until_expiry(market)
            urgency_key, urgency_label = get_urgency(h)

            forecast_f = WeatherFetcher.celsius_to_fahrenheit(ensemble_temp)
            thresh_display = (
                "%.1f\u00b0%s (%.1f\u00b0C)" % (parsed["raw_temp"], parsed["unit"], threshold)
                if parsed["unit"] == "F"
                else "%.1f\u00b0C" % threshold
            )

            opps.append({
                "type":          urgency_label + " Weather ARB",
                "market":        title,
                "url":           get_url(market),
                "city":          city,
                "date":          date,
                "threshold_c":   round(threshold, 2),
                "threshold_raw": parsed["raw_temp"],
                "threshold_disp":thresh_display,
                "unit":          parsed["unit"],
                "operator":      operator,
                "ensemble_temp": round(ensemble_temp, 2),
                "ensemble_f":    round(forecast_f, 1),
                "uncertainty":   round(uncertainty, 2),
                "model_temps":   model_temps,
                "model_prob":    round(model_prob, 4),
                "market_price":  round(yes_price, 4),
                "edge":          edge,
                "edge_pct":      "%.2f%%" % (edge * 100),
                "direction":     direction,
                "confidence":    confidence,
                "kelly_size":    round(kelly_size, 4),
                "liquidity":     market.get("liquidity", "N/A"),
                "hours_left":    round(h, 2) if h is not None else None,
                "urgency":       urgency_key,
                "action":        direction,
            })

        log.info("find_opportunities: %d markets scanned, %d opps found",
                 len(markets), len(opps))
        return opps

# ----------------------------------------------------------------
# Output formatting
# ----------------------------------------------------------------
def fmt_opp(opp, idx):
    try:
        models_str = "  ".join(
            "%s: %.1fC" % (k, v) for k, v in opp.get("model_temps", {}).items()
        )
        dir_emoji = "\U0001f7e2 BUY YES" if opp["direction"] == "BUY_YES" else "\U0001f534 BUY NO"
        out  = "=" * 60 + "\n"
        out += "#%02d %s [%s]\n" % (idx, opp["type"], opp["confidence"])
        out += "Market    : %s\n" % opp["market"][:80]
        out += "URL       : %s\n" % opp.get("url", "N/A")
        out += "City      : %s  |  Date: %s\n" % (opp["city"], opp["date"])
        out += "Forecast  : %.1f\u00b0C (%.1f\u00b0F) +/-%.1f\u00b0C\n" % (
            opp["ensemble_temp"], opp["ensemble_f"], opp["uncertainty"])
        out += "Condition : %s %s\n" % (opp["operator"].upper(), opp["threshold_disp"])
        out += "Model Prob: %.1f%%  |  Market: %.1f%%\n" % (
            opp["model_prob"] * 100, opp["market_price"] * 100)
        out += "Edge      : %s\n" % opp["edge_pct"]
        out += "Direction : %s  [%s]\n" % (dir_emoji, opp["confidence"])
        out += "Kelly Size: %.1f%% of bankroll\n" % (opp["kelly_size"] * 100)
        out += "Liquidity : $%s\n" % str(opp.get("liquidity", "N/A"))
        if opp.get("hours_left") is not None:
            out += "Expires in: %.1fh\n" % opp["hours_left"]
        out += "Models    : %s\n" % models_str
        return out
    except Exception as ex:
        return "#%02d [fmt error: %s]\n" % (idx, str(ex))


def build_tg_msg(opps, total_markets, scan_time):
    strong_n = sum(1 for o in opps if o.get("confidence") == "\u5f3a\u503e\u5411")
    medium_n = sum(1 for o in opps if o.get("confidence") == "\u4e2d\u503e\u5411")

    msg  = "<b>WeatherArb Bot - %d \u4e2a\u4fe1\u53f7</b>\n" % len(opps)
    msg += "\u65f6\u95f4: %s\n" % scan_time
    msg += "\u626b\u63cf\u5e02\u573a\u6570: %d\n" % total_markets
    msg += "\U0001f4aa \u5f3a\u503e\u5411: %d  |  \U0001f4ca \u4e2d\u503e\u5411: %d\n\n" % (strong_n, medium_n)

    for idx, opp in enumerate(opps[:8], 1):
        dir_emoji = "\U0001f7e2" if opp["direction"] == "BUY_YES" else "\U0001f534"
        msg += "<b>#%d %s [%s]</b>\n" % (idx, opp["type"], opp["confidence"])
        msg += "%s\n" % opp["market"][:60]
        msg += "\u6a21\u578b: %.1f%%  \u5e02\u573a: %.1f%%  Edge: %s\n" % (
            opp["model_prob"] * 100, opp["market_price"] * 100, opp["edge_pct"])
        msg += "%s %s  Kelly: %.1f%%\n" % (
            dir_emoji, opp["direction"], opp["kelly_size"] * 100)
        msg += '<a href="%s">\u67e5\u770b\u5e02\u573a</a>\n\n' % opp.get("url", "#")

    if len(opps) > 8:
        msg += "<i>... \u8fd8\u6709 %d \u4e2a\u4fe1\u53f7\uff0c\u8be6\u89c1 arb_report.json</i>\n" % (len(opps) - 8)
    return msg

# ----------------------------------------------------------------
# Main scan (单次运行)
# ----------------------------------------------------------------
def scan():
    log.info("=" * 60)
    log.info("WeatherArb Bot scan start")
    log.info("  WEATHER_EDGE_MIN=%.0f%%  WATCH_PUSH_MIN=%.0f%%  WINDOW=%dh",
             WEATHER_EDGE_MIN * 100, WATCH_PUSH_MIN * 100, SCAN_WINDOW_HOURS)
    log.info("  MODEL_WEIGHTS: best_match=%.0f%%  ecmwf=%.0f%%",
             MODEL_WEIGHT_BEST * 100, MODEL_WEIGHT_ECMWF * 100)
    log.info("  Time: %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("=" * 60)

    markets = fetch_weather_markets()
    if not markets:
        log.warning("No weather markets found in %dh window", SCAN_WINDOW_HOURS)
        send_telegram(
            "<b>WeatherArb Bot</b>\n"
            "\u5f53\u524d\u7a97\u53e3\u5185\u65e0\u5929\u6c14\u5e02\u573a\u3002\n"
            "%s" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        )
        return

    detector = WeatherArbDetector()
    opps     = detector.find_opportunities(markets)
    opps.sort(key=lambda o: -o.get("edge", 0))

    for idx, opp in enumerate(opps, 1):
        print(fmt_opp(opp, idx))

    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    push_opps = [o for o in opps if o["edge"] >= WATCH_PUSH_MIN]

    if push_opps:
        send_telegram(build_tg_msg(push_opps, len(markets), scan_time))
    else:
        send_telegram(
            "<b>WeatherArb Bot</b>\n"
            "\u65f6\u95f4: %s\n"
            "\u626b\u63cf\u5e02\u573a: %d  |  \u8bc6\u522b\u4fe1\u53f7: %d\n"
            "\u5f53\u524d\u65e0 Edge >= %.0f%% \u7684\u4fe1\u53f7" % (
                scan_time, len(markets), len(opps), WATCH_PUSH_MIN * 100)
        )

    log.info("=" * 60)
    log.info("DONE -- %d markets | %d opps | %d pushed",
             len(markets), len(opps), len(push_opps))
    log.info("=" * 60)

    report = {
        "scan_time":     scan_time,
        "window_hours":  SCAN_WINDOW_HOURS,
        "total_markets": len(markets),
        "pushed":        len(push_opps),
        "opportunities": opps,
    }
    with open("arb_report.json", "w", encoding="utf-8") as f:
        json.dump(sanitize_for_json(report), f, ensure_ascii=False, indent=2)
    log.info("arb_report.json saved (%d opps)", len(opps))


# ----------------------------------------------------------------
# Entry point (单次运行，由 GitHub Actions cron 控制频率)
# ----------------------------------------------------------------
if __name__ == "__main__":
    scan()
