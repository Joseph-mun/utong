"""Vercel Serverless Function - 외국인/기관 순매수 랭킹 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kis_client import KISClient

KST = timezone(timedelta(hours=9))


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        now = datetime.now(KST)

        # 장중 판별 (월~금 9:00~15:30)
        weekday = now.weekday()
        t = now.hour * 60 + now.minute
        market_open = weekday < 5 and 540 <= t <= 930

        try:
            kis = KISClient()

            foreign = kis.fetch_rankings("foreign")
            institutional = kis.fetch_rankings("institutional")

            result = {
                "timestamp": now.isoformat(),
                "market_open": market_open,
                "foreign": foreign,
                "institutional": institutional,
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": str(e)}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=300, stale-while-revalidate=60")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
