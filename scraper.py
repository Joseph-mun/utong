#!/usr/bin/env python3
"""UTONG - 공통 스크래핑 모듈

네이버 금융에서 외국인 수급 데이터를 수집하는 공통 함수들.
generate_report.py와 api/rankings.py에서 공유한다.
"""

import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
SLEEP = 0.2
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA}


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def naver_get(url, retries=1):
    for attempt in range(retries + 1):
        try:
            time.sleep(SLEEP)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                return resp
        except Exception as e:
            if attempt == retries:
                log(f"  요청 실패: {url[:80]} → {e}")
    return None


def parse_int(s):
    s = s.strip().replace(",", "")
    if not s or s == "-":
        return 0
    s = re.sub(r"[^0-9\-+]", "", s)
    try:
        return int(s)
    except ValueError:
        return 0


def parse_float(s):
    s = s.strip().replace(",", "").replace("%", "").replace("%p", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


# ──────────────────────────────────────────────
# 외국인 순매수 랭킹 (당일/전일)
# ──────────────────────────────────────────────
def fetch_rankings():
    """네이버 외국인 순매수 Top 20 (당일/전일, KOSPI+KOSDAQ)."""
    rankings = {"당일": [], "전일": []}
    dates = {"당일": None, "전일": None}

    for sosok, market in [("01", "KOSPI"), ("02", "KOSDAQ")]:
        url = f"https://finance.naver.com/sise/sise_deal_rank_iframe.naver?sosok={sosok}&investor_gubun=9000&type=buy"
        log(f"랭킹 수집: {market}")
        resp = naver_get(url)
        if resp is None:
            continue

        soup = BeautifulSoup(resp.content, "html.parser", from_encoding="euc-kr")
        tables = soup.find_all("table", class_="type_1")

        # 테이블 0/1 = 전일, 테이블 2/3 = 당일
        period_table_map = []
        for i, t in enumerate(tables):
            prev = t.find_previous_sibling()
            if prev:
                dt = prev.get_text(strip=True)
                if re.match(r"\d{2}\.\d{2}\.\d{2}", dt):
                    date_str = "20" + dt.replace(".", "")
                    period_table_map.append((i, date_str, t))

        # 날짜순 정렬 → 앞=전일, 뒤=당일
        period_table_map.sort(key=lambda x: x[1])
        if len(period_table_map) >= 2:
            for (_, ds, t), period_key in zip(period_table_map, ["전일", "당일"]):
                if dates[period_key] is None:
                    dates[period_key] = ds
                rows = _parse_ranking_table(t, market)
                rankings[period_key].extend(rows)
        elif len(period_table_map) == 1:
            _, ds, t = period_table_map[0]
            dates["당일"] = ds
            rankings["당일"].extend(_parse_ranking_table(t, market))

    # 금액 내림차순 정렬
    for k in rankings:
        rankings[k].sort(key=lambda x: x["buy_amount"], reverse=True)

    return rankings, dates


def _parse_ranking_table(table, market):
    rows = []
    for tr in table.find_all("tr"):
        a = tr.find("a", href=re.compile(r"code="))
        if not a:
            continue
        code = re.search(r"code=(\w+)", a["href"]).group(1)
        name = a.get_text(strip=True)
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 4:
            continue
        # cells: [종목명, 수량(천주), 금액(백만원), 당일거래량]
        volume_1k = parse_int(cells[1])
        amount_mil = parse_int(cells[2])
        rows.append({
            "code": code,
            "name": name,
            "market": market,
            "buy_amount": amount_mil * 1_000_000,  # 원
            "buy_volume": volume_1k * 1000,  # 주
        })
    return rows


# ──────────────────────────────────────────────
# 현재가 보충 (Naver Polling API)
# ──────────────────────────────────────────────
def fetch_prices(codes):
    """네이버 실시간 API에서 현재가를 조회한다."""
    log(f"현재가 조회: {len(codes)}개 종목")
    price_map = {}
    for code in codes:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        resp = naver_get(url)
        if resp is None:
            continue
        try:
            data = resp.json()
            item = data["datas"][0]
            price_map[code] = {
                "price": parse_int(item.get("closePrice", "0")),
                "change": parse_float(item.get("fluctuationsRatio", "0")),
                "volume": parse_int(item.get("accumulatedTradingVolume", "0")),
            }
        except (KeyError, IndexError, ValueError):
            continue
    return price_map
