#!/usr/bin/env python3
"""과거 3개월 일별 투자자 이력을 Redis에 일괄 저장.

사용법: python bootstrap_history.py
환경변수: KIS_APP_KEY, KIS_APP_SECRET, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kis_client import KISClient, log

REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
TTL = 86400 * 100  # 100일


def redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        log("Redis 환경변수 미설정")
        return None
    try:
        r = requests.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=10,
        )
        return r.json().get("result")
    except Exception as e:
        log(f"Redis 오류: {e}")
        return None


def main():
    log("=== 부트스트랩 시작: 과거 3개월 이력 수집 ===")

    kis = KISClient()

    # 1) 랭킹 조회
    log("외국인 랭킹 조회...")
    foreign_rank = kis.fetch_rankings("foreign")
    log(f"  외국인 Top {len(foreign_rank)}개")

    log("기관 랭킹 조회...")
    inst_rank = kis.fetch_rankings("institutional")
    log(f"  기관 Top {len(inst_rank)}개")

    # 2) 종목 메타 수집 (중복 제거)
    stock_meta = {}
    for r in foreign_rank + inst_rank:
        stock_meta[r["code"]] = {"name": r["name"], "market": r["market"]}

    log(f"고유 종목 수: {len(stock_meta)}개")

    # 3) 각 종목 투자자 이력 조회 → 날짜별 그룹핑
    daily_data = defaultdict(dict)
    total = len(stock_meta)

    for i, (code, meta) in enumerate(stock_meta.items()):
        log(f"  이력 조회: {meta['name']} ({i+1}/{total})")
        history = kis.fetch_investor_history(code)
        if not history:
            continue

        for record in history:
            date_str = record["date"]
            daily_data[date_str][code] = {
                "name": meta["name"],
                "market": meta["market"],
                "close": record["close"],
                "foreign_net": record["foreign_net"],
                "foreign_amount": record["foreign_amount"],
                "inst_net": record["inst_net"],
                "inst_amount": record["inst_amount"],
            }

    # 4) Redis에 날짜별 저장
    dates = sorted(daily_data.keys())
    log(f"수집된 날짜 수: {len(dates)}개 ({dates[0] if dates else '?'} ~ {dates[-1] if dates else '?'})")

    saved = 0
    for date_str in dates:
        key = f"history:daily:{date_str}"
        value = json.dumps(daily_data[date_str], ensure_ascii=False)
        redis_cmd("SET", key, value)
        redis_cmd("EXPIRE", key, TTL)
        saved += 1

    log(f"Redis 저장 완료: {saved}개 날짜")

    # 5) 랭킹 스냅샷 저장 (당일)
    today = datetime.now().strftime("%Y%m%d")
    rankings_snapshot = {
        "foreign": foreign_rank,
        "institutional": inst_rank,
    }
    redis_cmd("SET", f"rankings:daily:{today}", json.dumps(rankings_snapshot, ensure_ascii=False))
    redis_cmd("EXPIRE", f"rankings:daily:{today}", TTL)
    log(f"랭킹 스냅샷 저장: rankings:daily:{today}")

    log("=== 부트스트랩 완료 ===")


if __name__ == "__main__":
    main()
