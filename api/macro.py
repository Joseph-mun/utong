"""Vercel Serverless Function - 매크로 지표 JSON API"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from macro import fetch_macro_indicators

KST = timezone(timedelta(hours=9))


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            import os, requests as _req
            errors = []

            # KIS 토큰 직접 테스트
            try:
                r = _req.post(
                    "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    json={"grant_type": "client_credentials",
                          "appkey": os.environ.get("KIS_APP_KEY", ""),
                          "appsecret": os.environ.get("KIS_APP_SECRET", "")},
                    timeout=10)
                errors.append(f"KIS token: {r.status_code} {r.text[:200]}")
            except Exception as e:
                errors.append(f"KIS token error: {e}")

            # Massive 직접 테스트
            try:
                mk = os.environ.get("MASSIVE_API_KEY", "")
                r = _req.get(f"https://api.massive.com/v2/aggs/ticker/C:USDKRW/range/1/day/2026-03-15/2026-03-20?apiKey={mk}", timeout=10)
                errors.append(f"Massive: {r.status_code} {r.text[:200]}")
            except Exception as e:
                errors.append(f"Massive error: {e}")

            indicators = fetch_macro_indicators()
            result = {
                "timestamp": datetime.now(KST).isoformat(),
                "indicators": indicators,
                "debug": errors,
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
        except Exception as e:
            body = json.dumps({"error": str(e)}, ensure_ascii=False)
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "s-maxage=120, stale-while-revalidate=60")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
