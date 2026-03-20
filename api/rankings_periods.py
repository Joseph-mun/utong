"""Vercel Serverless - 기간별 외국인/기관 랭킹 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kis_client import KISClient
from generate_report import calculate_periods, fetch_all_histories, PERIOD_LABELS

KST = timezone(timedelta(hours=9))
TOP_N = 10


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

            # 2) 종목 메타 수집 (코드 → name, market)
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
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": str(e)}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=300, stale-while-revalidate=120")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
