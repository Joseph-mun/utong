"""Vercel Serverless Function - 외국인/기관 순매수 랭킹 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

import requests as _req

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kis_client import KISClient

KST = timezone(timedelta(hours=9))
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


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        now = datetime.now(KST)

        # 장중 판별 (월~금 9:00~15:30)
        weekday = now.weekday()
        t = now.hour * 60 + now.minute
        market_open = weekday < 5 and 540 <= t < 930

        try:
            kis = KISClient()

            foreign = kis.fetch_rankings("foreign")
            institutional = kis.fetch_rankings("institutional")

            has_data = bool(foreign or institutional)

            if has_data:
                # 데이터가 있으면 Redis에 캐싱
                cache = json.dumps({
                    "foreign": foreign,
                    "institutional": institutional,
                    "cached_at": now.isoformat(),
                }, ensure_ascii=False)
                redis_cmd("SET", "rankings:latest", cache)
                redis_cmd("EXPIRE", "rankings:latest", 86400)  # 24시간
            else:
                # 데이터가 없으면 (장외) Redis 캐시에서 로드
                cached = redis_cmd("GET", "rankings:latest")
                if cached:
                    data = json.loads(cached)
                    foreign = data.get("foreign", [])
                    institutional = data.get("institutional", [])

            result = {
                "timestamp": now.isoformat(),
                "market_open": market_open,
                "foreign": foreign,
                "institutional": institutional,
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": "Internal server error"}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=300, stale-while-revalidate=60")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
