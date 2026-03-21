"""Vercel Serverless - 기간별 외국인/기관 랭킹 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

import requests as _req

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kis_client import KISClient, log

REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = _req.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=5,
        )
        return r.json().get("result")
    except Exception:
        return None

KST = timezone(timedelta(hours=9))
TOP_N = 10

PERIOD_LABELS = ["당일", "전일", "최근 3일", "최근 1주일", "최근 2주일", "최근 1개월", "최근 3개월"]
PERIOD_DAYS = [1, 1, 3, 5, 10, 21, 63]


def fetch_all_histories(kis, stock_meta):
    """모든 종목의 투자자별 일별 이력 수집."""
    histories = {}
    total = len(stock_meta)
    for i, (code, meta) in enumerate(stock_meta.items()):
        log(f"  일별 이력: {meta['name']} ({i+1}/{total})")
        history = kis.fetch_investor_history(code)
        if history:
            histories[code] = {
                "name": meta["name"],
                "market": meta["market"],
                "history": history,
            }
    return histories


def merge_redis_histories(histories):
    """Redis 캐시 이력을 병합하여 3개월 커버리지 확보."""
    today = datetime.now(KST)
    merged_dates = 0
    for offset in range(1, 91):
        date_str = (today - timedelta(days=offset)).strftime("%Y%m%d")
        cached = redis_cmd("GET", f"history:daily:{date_str}")
        if not cached:
            continue
        try:
            daily = json.loads(cached)
        except (json.JSONDecodeError, TypeError):
            continue
        merged_dates += 1
        for code, data in daily.items():
            if code not in histories:
                histories[code] = {
                    "name": data["name"],
                    "market": data["market"],
                    "history": [],
                }
            existing_dates = {r["date"] for r in histories[code]["history"]}
            if date_str not in existing_dates:
                histories[code]["history"].append({
                    "date": date_str,
                    "close": data.get("close", 0),
                    "foreign_net": data.get("foreign_net", 0),
                    "foreign_amount": data.get("foreign_amount", 0),
                    "inst_net": data.get("inst_net", 0),
                    "inst_amount": data.get("inst_amount", 0),
                })
    log(f"  Redis 이력 병합: {merged_dates}일치")
    return histories


def calculate_periods(histories, ranking_data, price_data, investor_type="foreign"):
    """기간별 순매수 / 보유 변동 계산."""
    all_dates = sorted(set(
        r["date"] for data in histories.values() for r in data["history"]
    ))
    empty = {k: [] for k in PERIOD_LABELS}
    if not all_dates:
        return empty, empty

    amt_key = "foreign_amount" if investor_type == "foreign" else "inst_amount"
    vol_key = "foreign_net" if investor_type == "foreign" else "inst_net"

    net_result = {}
    sub_result = {}

    for label, days in zip(PERIOD_LABELS, PERIOD_DAYS):
        # ── 순매수 금액 ──
        if label == "당일" and ranking_data:
            net_result[label] = ranking_data
        else:
            if label == "당일":
                target = set(all_dates[-1:])
            elif label == "전일":
                target = set(all_dates[-2:-1]) if len(all_dates) >= 2 else set()
            else:
                target = set(all_dates[-days:]) if len(all_dates) >= days else set(all_dates)

            rows = []
            for code, data in histories.items():
                pd = [r for r in data["history"] if r["date"] in target]
                if not pd:
                    continue
                p = price_data.get(code, {})
                current_price = p.get("price", 0)

                if label in ("당일", "전일"):
                    change = p.get("change", 0)
                else:
                    oldest = min(pd, key=lambda x: x["date"])
                    oldest_close = oldest.get("close", 0)
                    change = round((current_price - oldest_close) / oldest_close * 100, 2) if oldest_close > 0 else 0

                rows.append({
                    "code": code,
                    "name": data["name"],
                    "market": data["market"],
                    "buy_amount": sum(r[amt_key] for r in pd),
                    "buy_volume": sum(r[vol_key] for r in pd),
                    "price": current_price,
                    "change": change,
                })
            rows.sort(key=lambda x: x["buy_amount"], reverse=True)
            net_result[label] = rows

        # ── 보조 지표 ──
        if label == "당일":
            tsub = set(all_dates[-1:])
        elif label == "전일":
            tsub = set(all_dates[-2:-1]) if len(all_dates) >= 2 else set()
        else:
            tsub = set(all_dates[-days:]) if len(all_dates) >= days else set(all_dates)

        sub_rows = []
        for code, data in histories.items():
            pd = [r for r in data["history"] if r["date"] in tsub]
            if not pd:
                continue
            net_vol = sum(r[vol_key] for r in pd)
            net_amt = sum(r[amt_key] for r in pd)
            p = price_data.get(code, {})

            if investor_type == "foreign":
                listed = p.get("listed_shares", 0)
                rate_end = p.get("foreign_rate", 0)
                rate_change = round(net_vol / listed * 100, 2) if listed > 0 else 0
                sub_rows.append({
                    "code": code, "name": data["name"], "market": data["market"],
                    "rate_end": rate_end, "rate_change": rate_change,
                    "hold_change": net_vol,
                })
            else:
                sub_rows.append({
                    "code": code, "name": data["name"], "market": data["market"],
                    "buy_volume": net_vol, "buy_amount": net_amt,
                })

        if investor_type == "foreign":
            sub_rows.sort(key=lambda x: x["rate_change"], reverse=True)
        else:
            sub_rows.sort(key=lambda x: x["buy_volume"], reverse=True)
        sub_result[label] = sub_rows

    return net_result, sub_result


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        now = datetime.now(KST)
        weekday = now.weekday()
        t = now.hour * 60 + now.minute
        market_open = weekday < 5 and 540 <= t < 930

        try:
            kis = KISClient()

            # 1) 당일 실시간 랭킹
            foreign_rank = kis.fetch_rankings("foreign")
            inst_rank = kis.fetch_rankings("institutional")

            # 2) 종목 메타 수집
            stock_meta = {}
            for r in foreign_rank + inst_rank:
                stock_meta[r["code"]] = {"name": r["name"], "market": r["market"]}

            # 3) 일별 이력 수집 + Redis 캐시 병합
            histories = fetch_all_histories(kis, stock_meta)
            histories = merge_redis_histories(histories)

            # 4) 현재가 + 보유비율 수집
            price_data = {}
            for code in stock_meta:
                p = kis.fetch_price(code)
                if p:
                    price_data[code] = p

            # 5) 기간별 계산
            f_net, f_sub = calculate_periods(histories, foreign_rank, price_data, "foreign")
            i_net, i_sub = calculate_periods(histories, inst_rank, price_data, "institutional")

            # 6) Top N 자르기
            result = {
                "timestamp": now.isoformat(),
                "market_open": market_open,
                "periods": PERIOD_LABELS,
                "foreign": {
                    "net_buying": {k: v[:TOP_N] for k, v in f_net.items()},
                    "holding_ratio": {k: v[:TOP_N] for k, v in f_sub.items()},
                },
                "institutional": {
                    "net_buying": {k: v[:TOP_N] for k, v in i_net.items()},
                    "net_volume": {k: v[:TOP_N] for k, v in i_sub.items()},
                },
            }

            body = json.dumps(result, ensure_ascii=False)
            status = 200

            # 데이터가 비었으면 Redis 캐시에서 로드
            all_empty = all(
                len(v) == 0
                for v in result.get("foreign", {}).get("net_buying", {}).values()
            )
            if all_empty:
                cached = redis_cmd("GET", "rankings_periods:latest")
                if cached:
                    body = cached
            else:
                # 데이터가 있을 때만 Redis에 캐싱
                redis_cmd("SET", "rankings_periods:latest", json.dumps(result, ensure_ascii=False))
                redis_cmd("EXPIRE", "rankings_periods:latest", 86400)

        except Exception as e:
            body = json.dumps({"error": "Internal server error"}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=300, stale-while-revalidate=120")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
