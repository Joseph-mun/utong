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
                rows.append({
                    "code": code,
                    "name": data["name"],
                    "market": data["market"],
                    "buy_amount": sum(r[amt_key] for r in pd),
                    "buy_volume": sum(r[vol_key] for r in pd),
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
        market_open = weekday < 5 and 540 <= t <= 930

        try:
            kis = KISClient()

            # 1) 당일 실시간 랭킹
            foreign_rank = kis.fetch_rankings("foreign")
            inst_rank = kis.fetch_rankings("institutional")

            # 2) 종목 메타 수집
            stock_meta = {}
            for r in foreign_rank + inst_rank:
                stock_meta[r["code"]] = {"name": r["name"], "market": r["market"]}

            # 3) 일별 이력 수집
            histories = fetch_all_histories(kis, stock_meta)

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

            # 데이터가 있으면 Redis에 캐싱
            has_data = any(v for v in f_net.values()) or any(v for v in i_net.values())
            if has_data:
                redis_cmd("SET", "rankings_periods:latest", json.dumps(result, ensure_ascii=False))
                redis_cmd("EXPIRE", "rankings_periods:latest", 86400)

            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": str(e)}, ensure_ascii=False)
            status = 500

        # 데이터가 비었으면 Redis 캐시에서 로드
        if status == 200:
            check = json.loads(body)
            all_empty = all(
                len(v) == 0
                for v in check.get("foreign", {}).get("net_buying", {}).values()
            )
            if all_empty:
                cached = redis_cmd("GET", "rankings_periods:latest")
                if cached:
                    body = cached

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=300, stale-while-revalidate=120")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
