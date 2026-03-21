"""Vercel Serverless - 일별 랭킹 + 투자자 이력 스냅샷 수집

매일 18:00 KST cron 호출. 당일 랭킹과 투자자 이력을 Redis에 저장.
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler

import requests as _req

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kis_client import KISClient, log

REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
KST = timezone(timedelta(hours=9))
TTL = 86400 * 100  # 100일


def redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        return None
    try:
        r = _req.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=10,
        )
        return r.json().get("result")
    except Exception:
        return None


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            now = datetime.now(KST)
            today = now.strftime("%Y%m%d")
            log(f"일별 스냅샷 수집 시작: {today}")

            kis = KISClient()

            # 1) 랭킹 조회
            foreign_rank = kis.fetch_rankings("foreign")
            inst_rank = kis.fetch_rankings("institutional")
            log(f"  외국인 {len(foreign_rank)}개, 기관 {len(inst_rank)}개")

            # 2) 종목 메타
            stock_meta = {}
            for r in foreign_rank + inst_rank:
                stock_meta[r["code"]] = {"name": r["name"], "market": r["market"]}

            # 3) 투자자 이력 조회 → 날짜별 그룹핑
            daily_data = defaultdict(dict)
            total = len(stock_meta)

            for i, (code, meta) in enumerate(stock_meta.items()):
                log(f"  이력: {meta['name']} ({i+1}/{total})")
                history = kis.fetch_investor_history(code)
                if not history:
                    continue
                for record in history:
                    daily_data[record["date"]][code] = {
                        "name": meta["name"],
                        "market": meta["market"],
                        "close": record["close"],
                        "foreign_net": record["foreign_net"],
                        "foreign_amount": record["foreign_amount"],
                        "inst_net": record["inst_net"],
                        "inst_amount": record["inst_amount"],
                    }

            # 4) Redis에 날짜별 저장
            saved = 0
            for date_str, stocks in daily_data.items():
                key = f"history:daily:{date_str}"
                value = json.dumps(stocks, ensure_ascii=False)
                redis_cmd("SET", key, value)
                redis_cmd("EXPIRE", key, TTL)
                saved += 1

            # 5) 랭킹 스냅샷 저장
            rankings_snapshot = json.dumps({
                "foreign": foreign_rank,
                "institutional": inst_rank,
            }, ensure_ascii=False)
            redis_cmd("SET", f"rankings:daily:{today}", rankings_snapshot)
            redis_cmd("EXPIRE", f"rankings:daily:{today}", TTL)

            result = {
                "status": "ok",
                "date": today,
                "stocks": len(stock_meta),
                "dates_saved": saved,
            }
            body = json.dumps(result, ensure_ascii=False)
            status = 200
            log(f"스냅샷 수집 완료: {saved}개 날짜, {len(stock_meta)}개 종목")
        except Exception as e:
            body = json.dumps({"error": "Internal server error"}, ensure_ascii=False)
            status = 500
            log(f"스냅샷 수집 오류: {e}")

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
