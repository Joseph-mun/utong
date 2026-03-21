#!/usr/bin/env python3
"""UTONG - 매크로 지표 수집

데이터 소스:
  - KIS API: KOSPI, KOSDAQ (실시간), S&P500, NASDAQ (해외지수 일간)
  - Investing.com: Brent유, USD/KRW (실시간 크롤링)
  - EIA API: Brent유 (폴백)
  - Massive API: USD/KRW (폴백)
환경변수: KIS_APP_KEY, KIS_APP_SECRET, MASSIVE_API_KEY
"""

import os
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA}
TIMEOUT = 8


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_macro_indicators(kis_client=None):
    """매크로 지표 수집. kis_client를 전달하면 KIS 토큰을 재사용한다."""
    log("매크로 지표 수집 시작")
    indicators = []

    # ── KIS 토큰 준비 (공유 또는 신규) ──
    kis_token = None
    if kis_client and kis_client._token:
        kis_token = {
            "token": kis_client._token,
            "app_key": kis_client.app_key,
            "app_secret": kis_client.app_secret,
        }
    else:
        kis_token = _get_kis_token()

    # ── 1) KIS: KOSPI, KOSDAQ (실시간) ──
    if kis_token:
        for iscd, name in [("0001", "KOSPI"), ("1001", "KOSDAQ")]:
            data = _fetch_kis_index(kis_token, iscd)
            if data:
                indicators.append({"name": name, **data})

        # ── 2) KIS: S&P500, NASDAQ (해외지수 일간) ──
        for iscd, name in [("SPX", "S&P 500"), ("COMP", "NASDAQ")]:
            data = _fetch_kis_world_index(kis_token, iscd)
            if data:
                indicators.append({"name": name, **data})

    # ── 3) Brent유: Investing.com → Yahoo Finance → EIA ──
    brent = _fetch_investing_brent()
    brent_src = "investing"
    if not brent:
        brent = _fetch_yahoo_brent()
        brent_src = "yahoo"
    if not brent:
        brent = _fetch_eia_brent()
        brent_src = "eia"
    if brent:
        indicators.append({"name": "Brent유", "source": brent_src, **brent})

    # ── 4) USD/KRW: Investing.com → Yahoo Finance → Massive ──
    fx = _fetch_investing_usdkrw()
    fx_src = "investing"
    if not fx:
        fx = _fetch_yahoo_usdkrw()
        fx_src = "yahoo"
    if not fx:
        fx = _fetch_massive_fx()
        fx_src = "massive"
    if fx:
        indicators.append({"name": "USD/KRW", "source": fx_src, **fx})

    log(f"매크로 지표 수집 완료: {len(indicators)}개")
    return indicators


# ── KIS 국내 지수 (실시간) ────────────────────────

_kis_cached_token = None
_kis_token_expires = None


def _get_kis_token():
    global _kis_cached_token, _kis_token_expires
    if _kis_cached_token and _kis_token_expires and datetime.now() < _kis_token_expires:
        return _kis_cached_token

    app_key = os.environ.get("KIS_APP_KEY", "")
    app_secret = os.environ.get("KIS_APP_SECRET", "")
    if not app_key or not app_secret:
        return None

    try:
        resp = requests.post(
            "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
            headers={"Content-Type": "application/json; charset=utf-8"},
            json={"grant_type": "client_credentials",
                  "appkey": app_key, "appsecret": app_secret},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        _kis_cached_token = {
            "token": data["access_token"],
            "app_key": app_key,
            "app_secret": app_secret,
        }
        _kis_token_expires = datetime.now() + timedelta(
            seconds=int(data.get("expires_in", 86400))
        )
        return _kis_cached_token
    except Exception as e:
        log(f"  KIS 토큰 오류: {e}")
        return None


def _kis_headers(token_info, tr_id):
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token_info['token']}",
        "appkey": token_info["app_key"],
        "appsecret": token_info["app_secret"],
        "tr_id": tr_id,
    }


def _fetch_kis_index(token_info, iscd):
    """KIS 국내 지수 (FHPUP02100000)."""
    try:
        time.sleep(0.08)
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443"
            "/uapi/domestic-stock/v1/quotations/inquire-index-price",
            headers=_kis_headers(token_info, "FHPUP02100000"),
            params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": iscd},
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("rt_cd") != "0":
            return None
        o = data.get("output", {})
        return {
            "value": float(o.get("bstp_nmix_prpr", "0")),
            "change": float(o.get("bstp_nmix_prdy_vrss", "0")),
            "change_pct": float(o.get("bstp_nmix_prdy_ctrt", "0")),
            "category": "index", "unit": "",
        }
    except Exception as e:
        log(f"  KIS 국내지수 오류 ({iscd}): {e}")
        return None


def _fetch_kis_world_index(token_info, iscd):
    """KIS 해외지수 일간 (FHKST03030100)."""
    try:
        time.sleep(0.08)
        today = datetime.now().strftime("%Y%m%d")
        week_ago = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
        resp = requests.get(
            "https://openapi.koreainvestment.com:9443"
            "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
            headers=_kis_headers(token_info, "FHKST03030100"),
            params={
                "FID_COND_MRKT_DIV_CODE": "N",
                "FID_INPUT_ISCD": iscd,
                "FID_INPUT_DATE_1": week_ago,
                "FID_INPUT_DATE_2": today,
                "FID_PERIOD_DIV_CODE": "D",
            },
            timeout=TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("rt_cd") != "0":
            return None
        rows = data.get("output2", [])
        if len(rows) < 2:
            return None

        latest = float(rows[0].get("ovrs_nmix_prpr", "0"))
        prev = float(rows[1].get("ovrs_nmix_prpr", "0"))
        change = round(latest - prev, 2)
        change_pct = round(change / prev * 100, 2) if prev else 0

        return {
            "value": latest, "change": change, "change_pct": change_pct,
            "category": "index", "unit": "",
        }
    except Exception as e:
        log(f"  KIS 해외지수 오류 ({iscd}): {e}")
        return None


# ── Investing.com 크롤링 (실시간) ────────────────

def _fetch_investing_com(url, category, unit):
    """Investing.com 크롤링으로 실시간 시세 조회."""
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://kr.investing.com/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=TIMEOUT)
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

        return {
            "value": value, "change": change, "change_pct": change_pct,
            "category": category, "unit": unit,
        }
    except Exception as e:
        log(f"  Investing.com 크롤링 오류 ({url}): {e}")
        return None


def _fetch_investing_brent():
    return _fetch_investing_com(
        "https://kr.investing.com/commodities/brent-oil-historical-data", "commodity", "$"
    )


def _fetch_investing_usdkrw():
    return _fetch_investing_com(
        "https://kr.investing.com/currencies/usd-krw-historical-data", "fx", "원"
    )


# ── Yahoo Finance (서버 환경 폴백) ────────────────

def _fetch_yahoo_finance(ticker, category, unit):
    """Yahoo Finance API로 시세 조회 (서버 IP에서도 동작)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2d"
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None
        meta = resp.json()["chart"]["result"][0]["meta"]
        value = meta["regularMarketPrice"]
        prev = meta["chartPreviousClose"]
        change = round(value - prev, 2)
        change_pct = round(change / prev * 100, 2) if prev else 0
        return {
            "value": value, "change": change, "change_pct": change_pct,
            "category": category, "unit": unit,
        }
    except Exception as e:
        log(f"  Yahoo Finance 오류 ({ticker}): {e}")
        return None


def _fetch_yahoo_brent():
    return _fetch_yahoo_finance("BZ=F", "commodity", "$")


def _fetch_yahoo_usdkrw():
    return _fetch_yahoo_finance("USDKRW=X", "fx", "원")


# ── EIA API (Brent유, 무료) ──────────────────────

def _fetch_eia_brent():
    """EIA API로 Brent 원유 현물가 조회."""
    try:
        url = (
            "https://api.eia.gov/v2/petroleum/pri/spt/data/"
            "?api_key=DEMO_KEY"
            "&frequency=daily&data[0]=value"
            "&facets[series][]=RBRTE"
            "&sort[0][column]=period&sort[0][direction]=desc"
            "&length=3"
        )
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None

        rows = resp.json().get("response", {}).get("data", [])
        if len(rows) < 2:
            return None

        latest = float(rows[0]["value"])
        prev = float(rows[1]["value"])
        change = round(latest - prev, 2)
        change_pct = round(change / prev * 100, 2) if prev else 0

        return {
            "value": latest, "change": change, "change_pct": change_pct,
            "category": "commodity", "unit": "$",
        }
    except Exception as e:
        log(f"  EIA Brent 오류: {e}")
        return None


# ── Massive API (USD/KRW) ────────────────────────

def _fetch_massive_fx():
    """Massive API로 USD/KRW 환율 조회."""
    try:
        api_key = os.environ.get("MASSIVE_API_KEY", "")
        if not api_key:
            return None

        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        url = (
            f"https://api.massive.com/v2/aggs/ticker/C:USDKRW"
            f"/range/1/day/{week_ago}/{today}"
            f"?apiKey={api_key}"
        )
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None

        results = resp.json().get("results", [])
        if len(results) < 2:
            return None

        latest = results[-1]["c"]
        prev = results[-2]["c"]
        change = round(latest - prev, 2)
        change_pct = round(change / prev * 100, 2) if prev else 0

        return {
            "value": latest, "change": change, "change_pct": change_pct,
            "category": "fx", "unit": "원",
        }
    except Exception as e:
        log(f"  Massive FX 오류: {e}")
        return None
