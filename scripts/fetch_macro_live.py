#!/usr/bin/env python3
"""GitHub Actions에서 실행 - Investing.com 실시간 데이터를 Redis에 저장.

GitHub Actions IP는 Cloudflare 차단을 받지 않으므로
Investing.com 크롤링이 정상 동작한다.
"""

import json
import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup

REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}
TTL = 7200  # 2시간 (주말/공휴일 cron 미실행 대비)


def redis_cmd(*args):
    if not REDIS_URL or not REDIS_TOKEN:
        print("ERROR: Redis 환경변수 미설정", flush=True)
        return None
    try:
        r = requests.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=5,
        )
        return r.json().get("result")
    except Exception as e:
        print(f"Redis 오류: {e}", flush=True)
        return None


def fetch_investing(url):
    """Investing.com 크롤링으로 가격/변동 추출."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        price_el = soup.find(attrs={"data-test": "instrument-price-last"})
        change_el = soup.find(attrs={"data-test": "instrument-price-change"})
        pct_el = soup.find(attrs={"data-test": "instrument-price-change-percent"})

        if not price_el:
            return None

        value = float(price_el.text.strip().replace(",", ""))
        change = float(change_el.text.strip().replace(",", "")) if change_el else 0
        pct_text = pct_el.text.strip().replace("%", "").replace("(", "").replace(")", "") if pct_el else "0"
        change_pct = float(pct_text)

        return {"value": value, "change": change, "change_pct": change_pct}
    except Exception as e:
        print(f"  크롤링 실패 ({url}): {e}", flush=True)
        return None


def main():
    now = datetime.now(tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] GitHub Actions 매크로 수집 시작", flush=True)

    # Brent유
    brent = fetch_investing("https://kr.investing.com/commodities/brent-oil-historical-data")
    if brent:
        data = json.dumps({**brent, "category": "commodity", "unit": "$", "updated": now})
        redis_cmd("SET", "macro:live:brent", data)
        redis_cmd("EXPIRE", "macro:live:brent", TTL)
        print(f"  Brent유: {brent['value']} ({brent['change']:+})", flush=True)
    else:
        print("  Brent유: 크롤링 실패", flush=True)

    # USD/KRW
    fx = fetch_investing("https://kr.investing.com/currencies/usd-krw-historical-data")
    if fx:
        data = json.dumps({**fx, "category": "fx", "unit": "원", "updated": now})
        redis_cmd("SET", "macro:live:usdkrw", data)
        redis_cmd("EXPIRE", "macro:live:usdkrw", TTL)
        print(f"  USD/KRW: {fx['value']} ({fx['change']:+})", flush=True)
    else:
        print("  USD/KRW: 크롤링 실패", flush=True)

    print("완료", flush=True)


if __name__ == "__main__":
    main()
