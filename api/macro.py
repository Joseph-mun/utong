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
            import os
            indicators = fetch_macro_indicators()
            result = {
                "timestamp": datetime.now(KST).isoformat(),
                "indicators": indicators,
                "debug": {
                    "kis_key_set": bool(os.environ.get("KIS_APP_KEY")),
                    "kis_secret_set": bool(os.environ.get("KIS_APP_SECRET")),
                    "massive_key_set": bool(os.environ.get("MASSIVE_API_KEY")),
                },
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
