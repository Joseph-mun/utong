#!/usr/bin/env python3
"""UTONG - 한국투자증권 API 클라이언트

KIS OpenAPI를 사용하여 외국인/기관 수급 데이터를 수집한다.
환경변수: KIS_APP_KEY, KIS_APP_SECRET
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

SLEEP = 0.08  # API 호출 간격 (초)
TIMEOUT = 10


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class KISClient:
    """한국투자증권 OpenAPI 클라이언트 (UTONG 경량 버전)"""

    BASE_URL = "https://openapi.koreainvestment.com:9443"
    TOKEN_PATH = "/oauth2/tokenP"

    # Quotation API paths
    FOREIGN_INST_TOTAL = "/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    INQUIRE_INVESTOR = "/uapi/domestic-stock/v1/quotations/inquire-investor"
    INQUIRE_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-price"

    def __init__(self):
        self.app_key = os.environ.get("KIS_APP_KEY", "")
        self.app_secret = os.environ.get("KIS_APP_SECRET", "")

        if not self.app_key or not self.app_secret:
            log("KIS_APP_KEY / KIS_APP_SECRET 환경변수를 설정하세요")

        self._token = None
        self._token_expires = None
        self._token_lock = threading.Lock()

        # 토큰 캐시 (/tmp 우선 → 로컬 .cache 폴백)
        import tempfile
        tmp_dir = Path(tempfile.gettempdir()) / "utong_cache"
        local_dir = Path(__file__).parent / ".cache"
        try:
            tmp_dir.mkdir(exist_ok=True)
            self._cache_dir = tmp_dir
        except OSError:
            local_dir.mkdir(exist_ok=True)
            self._cache_dir = local_dir
        self._token_file = self._cache_dir / "kis_token.json"
        self._load_cached_token()

    # ── 토큰 관리 ──────────────────────────────

    def _load_cached_token(self):
        if not self._token_file.exists():
            return
        try:
            data = json.loads(self._token_file.read_text())
            expires = datetime.fromisoformat(data["expires_at"])
            if datetime.now() < expires - timedelta(minutes=5):
                self._token = data["access_token"]
                self._token_expires = expires
                log("캐시된 KIS 토큰 로드 성공")
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    def _save_token(self):
        if not self._token or not self._token_expires:
            return
        try:
            self._token_file.write_text(json.dumps({
                "access_token": self._token,
                "expires_at": self._token_expires.isoformat(),
            }))
        except OSError:
            pass

    def _ensure_token(self):
        if (self._token and self._token_expires
                and datetime.now() < self._token_expires - timedelta(minutes=5)):
            return

        with self._token_lock:
            if (self._token and self._token_expires
                    and datetime.now() < self._token_expires - timedelta(minutes=5)):
                return

            log("KIS 토큰 발급 중...")
            resp = requests.post(
                f"{self.BASE_URL}{self.TOKEN_PATH}",
                headers={"Content-Type": "application/json; charset=utf-8"},
                json={
                    "grant_type": "client_credentials",
                    "appkey": self.app_key,
                    "appsecret": self.app_secret,
                },
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            self._token = data["access_token"]
            self._token_expires = datetime.now() + timedelta(
                seconds=int(data.get("expires_in", 86400))
            )
            self._save_token()
            log(f"KIS 토큰 발급 성공 (만료: {self._token_expires})")

    def _headers(self, tr_id):
        self._ensure_token()
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    def _get(self, path, tr_id, params, retries=2):
        for attempt in range(retries + 1):
            try:
                time.sleep(SLEEP)
                resp = requests.get(
                    f"{self.BASE_URL}{path}",
                    headers=self._headers(tr_id),
                    params=params,
                    timeout=TIMEOUT,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("rt_cd") == "0":
                        return data
                    log(f"  API 오류: {data.get('msg1', '')}")
                elif resp.status_code == 429:
                    time.sleep(1)
                    continue
                else:
                    log(f"  HTTP {resp.status_code}")
            except Exception as e:
                if attempt == retries:
                    log(f"  요청 실패: {e}")
        return None

    # ── 순매수 랭킹 ──────────────────────────────

    def fetch_rankings(self, investor_type="foreign"):
        """외국인/기관 순매수 상위 종목 (KOSPI + KOSDAQ).

        Args:
            investor_type: "foreign" 또는 "institutional"
        Returns:
            list[dict] - code, name, market, buy_amount, buy_volume, price, change
        """
        etc_cls = "1" if investor_type == "foreign" else "2"
        label = "외국인" if investor_type == "foreign" else "기관"

        results = []
        for input_iscd, market_name in [("0001", "KOSPI"), ("1001", "KOSDAQ")]:
            log(f"  {label} 랭킹 수집: {market_name}")
            params = {
                "FID_COND_MRKT_DIV_CODE": "V",
                "FID_COND_SCR_DIV_CODE": "16449",
                "FID_INPUT_ISCD": input_iscd,
                "FID_DIV_CLS_CODE": "1",
                "FID_RANK_SORT_CLS_CODE": "0",
                "FID_ETC_CLS_CODE": etc_cls,
            }
            data = self._get(self.FOREIGN_INST_TOTAL, "FHPTJ04400000", params)
            if not data:
                continue

            for item in data.get("output", []):
                name = item.get("hts_kor_isnm", "").strip()
                code = item.get("mksc_shrn_iscd", "").strip()
                if not name or not code:
                    continue

                if investor_type == "foreign":
                    buy_amount = int(item.get("frgn_ntby_tr_pbmn", "0") or "0") * 1_000_000
                    buy_volume = int(item.get("frgn_ntby_qty", "0") or "0")
                else:
                    buy_amount = int(item.get("orgn_ntby_tr_pbmn", "0") or "0") * 1_000_000
                    buy_volume = int(item.get("orgn_ntby_qty", "0") or "0")

                results.append({
                    "code": code,
                    "name": name,
                    "market": market_name,
                    "buy_amount": buy_amount,
                    "buy_volume": buy_volume,
                    "price": int(item.get("stck_prpr", "0") or "0"),
                    "change": float(item.get("prdy_ctrt", "0") or "0"),
                })

        results.sort(key=lambda x: x["buy_amount"], reverse=True)
        return results

    # ── 종목별 투자자 일별 매매동향 ─────────────

    def fetch_investor_history(self, code):
        """종목별 외국인+기관 일별 매매동향.

        Returns:
            list[dict] - date, close, foreign_net, foreign_amount, inst_net, inst_amount
        """
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }
        data = self._get(self.INQUIRE_INVESTOR, "FHKST01010900", params)
        if not data:
            return []

        results = []
        for item in data.get("output", []):
            date = item.get("stck_bsop_date", "")
            if not date:
                continue
            results.append({
                "date": date,
                "close": int(item.get("stck_clpr", "0") or "0"),
                "foreign_net": int(item.get("frgn_ntby_qty", "0") or "0"),
                "foreign_amount": int(item.get("frgn_ntby_tr_pbmn", "0") or "0") * 1_000_000,
                "inst_net": int(item.get("orgn_ntby_qty", "0") or "0"),
                "inst_amount": int(item.get("orgn_ntby_tr_pbmn", "0") or "0") * 1_000_000,
            })

        return sorted(results, key=lambda x: x["date"], reverse=True)

    # ── 현재가 + 외국인 지분율 ──────────────────

    def fetch_price(self, code):
        """현재가, 등락률, 외국인 지분율, 보유주수, 상장주식수."""
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }
        data = self._get(self.INQUIRE_PRICE, "FHKST01010100", params)
        if not data:
            return None

        o = data.get("output", {})
        return {
            "price": int(o.get("stck_prpr", "0") or "0"),
            "change": float(o.get("prdy_ctrt", "0") or "0"),
            "volume": int(o.get("acml_vol", "0") or "0"),
            "foreign_rate": float(o.get("hts_frgn_ehrt", "0") or "0"),
            "foreign_holdings": int(o.get("frgn_hldn_qty", "0") or "0"),
            "listed_shares": int(o.get("lstn_stcn", "0") or "0"),
        }

    def fetch_prices(self, codes):
        """복수 종목 현재가 일괄 조회."""
        log(f"현재가 조회: {len(codes)}개 종목")
        price_map = {}
        for code in codes:
            result = self.fetch_price(code)
            if result:
                price_map[code] = result
        return price_map
