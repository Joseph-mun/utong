"""Vercel Serverless Function - 매크로 지표 + Redis 이력 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

import requests as _req

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from macro import fetch_macro_indicators

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
        today_key = f"macro:{now.strftime('%Y%m%d')}"

        try:
            indicators = fetch_macro_indicators()

            # Redis에 현재값 저장 (호출 시마다 축적)
            if indicators and REDIS_URL:
                try:
                    today_key = f"macro:{now.strftime('%Y%m%d')}"
                    entry = json.dumps({
                        "t": now.isoformat(),
                        "d": {ind["name"]: ind["value"] for ind in indicators},
                    }, ensure_ascii=False)
                    redis_cmd("ZADD", today_key, now.timestamp(), entry)
                    redis_cmd("EXPIRE", today_key, 172800)
                except Exception:
                    pass

            # Redis에서 오늘 이력 조회
            history = {}
            raw = redis_cmd("ZRANGE", today_key, 0, -1)
            if raw and isinstance(raw, list):
                for entry_str in raw:
                    try:
                        entry = json.loads(entry_str)
                        t = entry.get("t", "")
                        for name, val in entry.get("d", {}).items():
                            if name not in history:
                                history[name] = []
                            history[name].append({"t": t, "v": val})
                    except (json.JSONDecodeError, TypeError):
                        continue

            result = {
                "timestamp": now.isoformat(),
                "indicators": indicators,
                "history": history,
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": "Internal server error"}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=120, stale-while-revalidate=60")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
