"""Vercel Cron Function - 5분마다 매크로 지표를 Redis에 축적"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from macro import fetch_macro_indicators

KST = timezone(timedelta(hours=9))
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


def redis_cmd(*args):
    """Upstash REST API로 Redis 명령 실행."""
    r = requests.post(
        REDIS_URL,
        headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
        json=list(args),
        timeout=5,
    )
    return r.json().get("result")


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        now = datetime.now(KST)
        today_key = f"macro:{now.strftime('%Y%m%d')}"
        ts = now.isoformat()

        try:
            indicators = fetch_macro_indicators()

            # Redis에 저장: ZADD macro:YYYYMMDD timestamp json_data
            entry = json.dumps({
                "t": ts,
                "d": {ind["name"]: ind["value"] for ind in indicators},
            }, ensure_ascii=False)

            score = now.timestamp()
            redis_cmd("ZADD", today_key, score, entry)

            # 키 만료: 48시간 (이틀 후 자동 삭제)
            redis_cmd("EXPIRE", today_key, 172800)

            result = {
                "status": "ok",
                "timestamp": ts,
                "count": len(indicators),
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
