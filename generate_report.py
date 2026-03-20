#!/usr/bin/env python3
"""UTONG - 시장 대시보드 생성기

한국투자증권 OpenAPI + 매크로 지표로 자체 완결형 HTML 대시보드를 생성한다.
- 상단: 매크로 지표 (KOSPI, KOSDAQ, S&P500, USD/KRW, WTI, Gold, 미국채10Y)
- 하단: 수급 데이터 (외국인 / 기관)
"""

import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from kis_client import KISClient, log
from macro import fetch_macro_indicators

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
KST = timezone(timedelta(hours=9))
TOP_N = 10
OUTPUT_DIR = Path(__file__).parent / "public"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT = OUTPUT_DIR / "index.html"

DONATE_URL = "https://qr.kakaopay.com/Ej759Ivs1"

PERIOD_LABELS = ["당일", "전일", "최근 3일", "최근 1주일", "최근 2주일", "최근 1개월", "최근 3개월"]
PERIOD_DAYS = [1, 1, 3, 5, 10, 21, 63]


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────
def fetch_all_histories(kis, stock_meta):
    """모든 종목의 투자자별 일별 이력 수집."""
    histories = {}
    total = len(stock_meta)
    for i, (code, meta) in enumerate(stock_meta.items()):
        log(f"  일별 이력: {meta['name']} ({i+1}/{total})")
        history = kis.fetch_investor_history(code)
        if history:
            histories[code] = {
                "name": meta["name"],
                "market": meta["market"],
                "history": history,
            }
    return histories


# ──────────────────────────────────────────────
# 기간별 계산
# ──────────────────────────────────────────────
def calculate_periods(histories, ranking_data, price_data, investor_type="foreign"):
    """기간별 순매수 / 보유 변동 계산."""
    all_dates = sorted(set(
        r["date"] for data in histories.values() for r in data["history"]
    ))
    empty = {k: [] for k in PERIOD_LABELS}
    if not all_dates:
        return empty, empty

    amt_key = "foreign_amount" if investor_type == "foreign" else "inst_amount"
    vol_key = "foreign_net" if investor_type == "foreign" else "inst_net"

    net_result = {}
    sub_result = {}

    for label, days in zip(PERIOD_LABELS, PERIOD_DAYS):
        # ── 순매수 금액 ──
        if label == "당일" and ranking_data:
            net_result[label] = ranking_data
        else:
            if label == "당일":
                target = set(all_dates[-1:])
            elif label == "전일":
                target = set(all_dates[-2:-1]) if len(all_dates) >= 2 else set()
            else:
                target = set(all_dates[-days:]) if len(all_dates) >= days else set(all_dates)

            rows = []
            for code, data in histories.items():
                pd = [r for r in data["history"] if r["date"] in target]
                if not pd:
                    continue
                rows.append({
                    "code": code,
                    "name": data["name"],
                    "market": data["market"],
                    "buy_amount": sum(r[amt_key] for r in pd),
                    "buy_volume": sum(r[vol_key] for r in pd),
                })
            rows.sort(key=lambda x: x["buy_amount"], reverse=True)
            net_result[label] = rows

        # ── 보조 지표 ──
        if label == "당일":
            tsub = set(all_dates[-1:])
        elif label == "전일":
            tsub = set(all_dates[-2:-1]) if len(all_dates) >= 2 else set()
        else:
            tsub = set(all_dates[-days:]) if len(all_dates) >= days else set(all_dates)

        sub_rows = []
        for code, data in histories.items():
            pd = [r for r in data["history"] if r["date"] in tsub]
            if not pd:
                continue
            net_vol = sum(r[vol_key] for r in pd)
            net_amt = sum(r[amt_key] for r in pd)
            p = price_data.get(code, {})

            if investor_type == "foreign":
                listed = p.get("listed_shares", 0)
                rate_end = p.get("foreign_rate", 0)
                rate_change = round(net_vol / listed * 100, 2) if listed > 0 else 0
                sub_rows.append({
                    "code": code, "name": data["name"], "market": data["market"],
                    "rate_end": rate_end, "rate_change": rate_change,
                    "hold_change": net_vol,
                })
            else:
                sub_rows.append({
                    "code": code, "name": data["name"], "market": data["market"],
                    "buy_volume": net_vol, "buy_amount": net_amt,
                })

        if investor_type == "foreign":
            sub_rows.sort(key=lambda x: x["rate_change"], reverse=True)
        else:
            sub_rows.sort(key=lambda x: x["buy_volume"], reverse=True)
        sub_result[label] = sub_rows

    return net_result, sub_result


# ──────────────────────────────────────────────
# 포매팅 헬퍼
# ──────────────────────────────────────────────
def fmt_amt(v):
    a = v / 1e8
    s = f"{abs(a):,.0f}" if abs(a) >= 1000 else f"{abs(a):,.1f}"
    return f"{'+'if a >= 0 else '-'}{s}억"

def fmt_vol(v):
    if abs(v) >= 1e6:
        return f"{'+' if v >= 0 else ''}{v/1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"{'+' if v >= 0 else ''}{v/1e3:.0f}K"
    return f"{'+' if v >= 0 else ''}{v:,.0f}"

def fmt_price(v):
    return f"{v:,.0f}원" if v else "-"

def fmt_pct(v):
    return f"{'+' if v >= 0 else ''}{v:.2f}%p"

def fmt_rate(v):
    return f"{v:.2f}%"

def val_cls(v):
    return "positive" if v > 0 else ("negative" if v < 0 else "")

def mkt_badge(m):
    return f'<span class="badge badge-{m.lower()}">{m}</span>'


# ──────────────────────────────────────────────
# HTML 빌더
# ──────────────────────────────────────────────
def _build_css():
    # 동적 패널 규칙 생성
    pr = []  # panel rules
    tr = []  # tab active rules
    for prefix in ["fn", "fo", "gn", "go"]:
        for i in range(len(PERIOD_LABELS)):
            pr.append(f"#{prefix}{i}:checked~#{prefix}{i}v{{display:block}}")
            tr.append(f"#{prefix}{i}:checked~.tabs label[for={prefix}{i}]{{background:#252836;color:#e1e4ea}}")

    return (
        # 기본
        "*{margin:0;padding:0;box-sizing:border-box}"
        "body{background:#0f1117;color:#e1e4ea;font-family:-apple-system,'Pretendard','Noto Sans KR',sans-serif;"
        "line-height:1.5;padding:16px;max-width:1200px;margin:0 auto}"
        "a{color:#818cf8;text-decoration:none}"
        # 헤더
        ".header{padding:24px 0 16px;border-bottom:1px solid #252836;margin-bottom:20px;"
        "display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px}"
        ".header-left{flex:1;min-width:0}"
        ".header h1{font-size:24px;font-weight:800;color:#fff}"
        ".header h1 span{color:#818cf8;font-weight:400;font-size:14px;margin-left:8px}"
        ".header .meta{color:#8b8fa3;font-size:13px;margin-top:4px}"
        # 후원
        ".donate-btn{display:inline-flex;align-items:center;gap:6px;padding:10px 18px;"
        "background:linear-gradient(135deg,#f59e0b,#d97706);color:#fff;border-radius:10px;"
        "font-size:13px;font-weight:600;text-decoration:none;white-space:nowrap;"
        "transition:transform .15s,box-shadow .15s;flex-shrink:0}"
        ".donate-btn:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(245,158,11,.3)}"
        # 실시간
        ".live-indicator{color:#34d399;font-weight:600}"
        ".live-indicator::before{content:'\\25CF ';animation:blink 2s infinite}"
        ".refresh-btn{background:#252836;color:#8b8fa3;border:1px solid #363a4e;border-radius:6px;"
        "padding:4px 12px;font-size:12px;cursor:pointer;transition:all .15s;margin-left:8px;vertical-align:middle}"
        ".refresh-btn:hover{background:#363a4e;color:#e1e4ea}"
        ".refresh-btn:disabled{opacity:.5;cursor:not-allowed}"
        "@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}"
        # 매크로 섹션
        ".macro-section{margin-bottom:24px}"
        ".macro-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}"
        ".macro-title{font-size:14px;font-weight:700;color:#8b8fa3}"
        ".macro-updated{font-size:11px;color:#636678}"
        ".macro-grid{display:flex;gap:10px;overflow-x:auto;padding-bottom:8px;"
        "-webkit-overflow-scrolling:touch;scrollbar-width:thin}"
        ".macro-card{flex:0 0 auto;min-width:150px;background:#1a1d27;border:1px solid #252836;"
        "border-radius:10px;padding:14px 16px;transition:border-color .15s;position:relative;overflow:hidden}"
        ".macro-card:hover{border-color:#363a4e}"
        ".macro-name{font-size:11px;color:#636678;font-weight:600;text-transform:uppercase;letter-spacing:.5px}"
        ".macro-value{font-size:18px;font-weight:700;margin-top:4px;color:#fff}"
        ".macro-change{font-size:12px;margin-top:2px;font-variant-numeric:tabular-nums}"
        ".macro-change.positive{color:#34d399}.macro-change.negative{color:#f87171}"
        ".spark{display:block;width:100%;height:32px;margin-top:6px}"
        ".spark polyline{fill:none;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}"
        ".spark .area{stroke:none;opacity:.1}"
        # 숨겨진 radio
        ".sr{position:absolute;opacity:0;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}"
        # 투자자 탭 (Level 1)
        ".inv-tab{display:inline-block;padding:14px 28px;color:#8b8fa3;cursor:pointer;"
        "font-size:16px;font-weight:700;border-bottom:3px solid transparent;margin-bottom:-3px;transition:all .15s}"
        ".inv-tab:hover{color:#e1e4ea}"
        ".inv-tab-line{border-bottom:2px solid #252836;margin-bottom:20px}"
        ".inv-sec{display:none;margin-bottom:40px}"
        "#inv-f:checked~#sec-f{display:block}"
        "#inv-i:checked~#sec-i{display:block}"
        "#inv-f:checked~label[for='inv-f'],"
        "#inv-i:checked~label[for='inv-i']"
        "{color:#818cf8;border-bottom-color:#818cf8}"
        # 데이터 탭 (Level 2)
        ".main-tab{display:inline-block;padding:12px 20px;color:#8b8fa3;cursor:pointer;"
        "font-size:14px;font-weight:600;border-bottom:2px solid transparent;margin-bottom:-2px}"
        ".main-tab:hover{color:#e1e4ea}"
        ".main-tab-line{border-bottom:2px solid #252836;margin-bottom:16px}"
        ".section{display:none}"
        # Level 2 규칙 (외국인)
        "#fm-n:checked~#fm-n-s{display:block}#fm-o:checked~#fm-o-s{display:block}"
        "#fm-n:checked~label[for='fm-n'],#fm-o:checked~label[for='fm-o']"
        "{color:#818cf8;border-bottom-color:#818cf8}"
        # Level 2 규칙 (기관)
        "#gm-n:checked~#gm-n-s{display:block}#gm-o:checked~#gm-o-s{display:block}"
        "#gm-n:checked~label[for='gm-n'],#gm-o:checked~label[for='gm-o']"
        "{color:#818cf8;border-bottom-color:#818cf8}"
        # 기간 탭 (Level 3)
        + ".tabs{display:flex;gap:4px;background:#1a1d27;border-radius:12px;padding:4px;margin-bottom:16px;overflow-x:auto}"
        ".tab-btn{display:block;padding:10px 14px;border-radius:10px;color:#8b8fa3;"
        "cursor:pointer;font-size:13px;font-weight:500;white-space:nowrap}"
        ".tab-btn:hover{color:#e1e4ea}"
        ".pp{display:none}"
        + ''.join(pr)
        + ''.join(tr)
        # 요약 카드
        + ".summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:16px}"
        ".stat-card{background:#1a1d27;border:1px solid #252836;border-radius:10px;padding:14px}"
        ".stat-card .label{font-size:11px;color:#636678;text-transform:uppercase;letter-spacing:.5px}"
        ".stat-card .value{font-size:18px;font-weight:700;margin-top:4px}"
        ".stat-card .value.green{color:#34d399}"
        ".stat-card .value.red{color:#f87171}"
        ".stat-card .value.accent{color:#818cf8}"
        ".stat-card .value.amber{color:#fbbf24}"
        # 테이블
        ".table-wrap{overflow-x:auto;border-radius:10px;border:1px solid #252836}"
        "table{width:100%;border-collapse:collapse;font-size:13px}"
        "thead th{background:#1a1d27;padding:10px 12px;text-align:left;font-weight:600;"
        "color:#8b8fa3;position:sticky;top:0;white-space:nowrap}"
        "tbody tr{border-top:1px solid #252836;transition:background .1s}"
        "tbody tr:hover{background:#1a1d27}"
        "tbody tr:nth-child(even){background:#14161e}"
        "td{padding:10px 12px;white-space:nowrap}"
        "td.right{text-align:right;font-variant-numeric:tabular-nums}"
        "td .sub{font-size:11px;color:#636678}"
        ".positive{color:#34d399}.negative{color:#f87171}"
        ".badge{display:inline-block;font-size:11px;padding:2px 8px;border-radius:6px}"
        ".badge-kospi{background:rgba(79,70,229,.15);color:#a5b4fc}"
        ".badge-kosdaq{background:rgba(251,191,36,.12);color:#fbbf24}"
        ".empty-msg{text-align:center;color:#636678;padding:40px}"
        # 반응형
        "@media(max-width:768px){"
        "body{padding:8px}.header h1{font-size:20px}.header h1 span{display:block;margin-left:0;margin-top:2px}"
        ".inv-tab{padding:12px 18px;font-size:14px}"
        ".main-tab{padding:10px 14px;font-size:13px}"
        ".tab-btn{padding:8px 10px;font-size:12px}"
        ".summary{grid-template-columns:repeat(2,1fr)}"
        ".donate-btn{padding:8px 14px;font-size:12px}"
        ".macro-card{min-width:110px;padding:10px 12px}"
        ".macro-value{font-size:15px}"
        "td,th{padding:8px 6px;font-size:12px}}"
    )


def _build_macro_html(macro_data):
    if not macro_data:
        return ""
    cards = []
    for ind in macro_data:
        v = ind["value"]
        c = ind["change"]
        cp = ind["change_pct"]
        cat = ind.get("category", "")
        unit = ind.get("unit", "")
        cls = "positive" if c >= 0 else "negative"
        s = "+" if c >= 0 else ""

        if cat == "rate":
            val_str = f"{v:.2f}%"
            chg_str = f"{s}{c:.2f}%p ({s}{cp:.2f}%)"
        elif unit == "$":
            val_str = f"${v:,.2f}"
            chg_str = f"{s}{c:,.2f} ({s}{cp:.2f}%)"
        elif unit == "원":
            val_str = f"{v:,.1f}"
            chg_str = f"{s}{c:,.1f} ({s}{cp:.2f}%)"
        else:
            val_str = f"{v:,.2f}"
            chg_str = f"{s}{c:,.2f} ({s}{cp:.2f}%)"

        cards.append(
            f'<div class="macro-card" data-macro="{ind["name"]}">'
            f'<div class="macro-name">{ind["name"]}</div>'
            f'<div class="macro-value">{val_str}</div>'
            f'<div class="macro-change {cls}">{chg_str}</div>'
            f'<svg class="spark" viewBox="0 0 120 32" preserveAspectRatio="none"></svg>'
            f'</div>'
        )
    return (
        '<div class="macro-section">\n'
        '<div class="macro-header">'
        '<span class="macro-title">매크로 지표</span>'
        '<span class="macro-updated" id="macro-updated"></span>'
        '</div>\n'
        '<div class="macro-grid">\n' + '\n'.join(cards) + '\n'
        '</div>\n</div>\n'
    )


def _build_net_panels(prefix, net_data, price_data):
    """순매수 금액 패널 (7개 기간)."""
    radios = []
    labels = []
    panels = []
    for i, period in enumerate(PERIOD_LABELS):
        rid = f"{prefix}{i}"
        pid = f"{prefix}{i}v"
        checked = " checked" if i == 0 else ""
        radios.append(f'<input type="radio" name="{prefix}" id="{rid}"{checked} class="sr">')
        labels.append(f'<label for="{rid}" class="tab-btn">{period}</label>')

        all_rows = net_data.get(period, [])
        rows = all_rows[:TOP_N]

        # 요약
        summary = ""
        if all_rows:
            top_name = all_rows[0]["name"]
            total = sum(r["buy_amount"] for r in all_rows)
            kospi_t = sum(r["buy_amount"] for r in all_rows if r["market"] == "KOSPI")
            kosdaq_t = sum(r["buy_amount"] for r in all_rows if r["market"] == "KOSDAQ")
            tc = "green" if total >= 0 else "red"
            kc = "green" if kospi_t >= 0 else "red"
            dc = "green" if kosdaq_t >= 0 else "red"
            summary = (
                '<div class="summary">'
                f'<div class="stat-card"><div class="label">1위 종목</div><div class="value accent">{top_name}</div></div>'
                f'<div class="stat-card"><div class="label">전체 합계</div><div class="value {tc}">{fmt_amt(total)}</div></div>'
                f'<div class="stat-card"><div class="label">KOSPI</div><div class="value {kc}">{fmt_amt(kospi_t)}</div></div>'
                f'<div class="stat-card"><div class="label">KOSDAQ</div><div class="value {dc}">{fmt_amt(kosdaq_t)}</div></div>'
                '</div>\n'
            )

        # 테이블
        if rows:
            trs = []
            for j, r in enumerate(rows):
                p = price_data.get(r["code"], {})
                chg = p.get("change", r.get("change", 0))
                price = p.get("price", r.get("price", 0))
                cs = "+" if chg >= 0 else ""
                trs.append(
                    f'<tr><td>{j+1}</td>'
                    f'<td>{r["name"]} <span class="sub">{r["code"]}</span></td>'
                    f'<td>{mkt_badge(r["market"])}</td>'
                    f'<td class="right">{fmt_price(price)}'
                    f'<br><span class="sub {val_cls(chg)}">{cs}{chg:.2f}%</span></td>'
                    f'<td class="right {val_cls(r["buy_amount"])}">{fmt_amt(r["buy_amount"])}</td>'
                    f'<td class="right {val_cls(r["buy_volume"])}">{fmt_vol(r["buy_volume"])}</td></tr>'
                )
            tbody = '\n'.join(trs)
        else:
            tbody = '<tr><td colspan="6" class="empty-msg">데이터 없음</td></tr>'

        panels.append(
            f'<div class="pp" id="{pid}">\n' + summary
            + '<div class="table-wrap"><table>\n'
            + '<thead><tr><th>#</th><th>종목명</th><th>시장</th><th>종가</th>'
            + '<th>순매수금액</th><th>순매수수량</th></tr></thead>\n'
            + f'<tbody>\n{tbody}\n</tbody></table></div>\n</div>\n'
        )

    return (
        '\n'.join(radios) + '\n'
        + '<div class="tabs">' + ''.join(labels) + '</div>\n'
        + ''.join(panels)
    )


def _build_own_panels(prefix, own_data, price_data):
    """외국인 보유비율 변동 패널 (7개 기간)."""
    radios = []
    labels = []
    panels = []
    for i, period in enumerate(PERIOD_LABELS):
        rid = f"{prefix}{i}"
        pid = f"{prefix}{i}v"
        checked = " checked" if i == 0 else ""
        radios.append(f'<input type="radio" name="{prefix}" id="{rid}"{checked} class="sr">')
        labels.append(f'<label for="{rid}" class="tab-btn">{period}</label>')

        all_rows = own_data.get(period, [])
        rows = all_rows[:TOP_N]

        summary = ""
        if all_rows:
            top_name = all_rows[0]["name"]
            max_chg = max((r["rate_change"] for r in all_rows), default=0)
            avg_chg = sum(r["rate_change"] for r in all_rows) / len(all_rows)
            summary = (
                '<div class="summary">'
                f'<div class="stat-card"><div class="label">1위 종목</div><div class="value accent">{top_name}</div></div>'
                f'<div class="stat-card"><div class="label">최대 증가</div><div class="value green">{fmt_pct(max_chg)}</div></div>'
                f'<div class="stat-card"><div class="label">평균 증가</div><div class="value green">{fmt_pct(avg_chg)}</div></div>'
                f'<div class="stat-card"><div class="label">종목 수</div><div class="value amber">{len(all_rows)}</div></div>'
                '</div>\n'
            )

        if rows:
            trs = []
            for j, r in enumerate(rows):
                p = price_data.get(r["code"], {})
                chg = p.get("change", 0)
                price = p.get("price", 0)
                cs = "+" if chg >= 0 else ""
                trs.append(
                    f'<tr><td>{j+1}</td>'
                    f'<td>{r["name"]} <span class="sub">{r["code"]}</span></td>'
                    f'<td>{mkt_badge(r["market"])}</td>'
                    f'<td class="right">{fmt_price(price)}'
                    f'<br><span class="sub {val_cls(chg)}">{cs}{chg:.2f}%</span></td>'
                    f'<td class="right">{fmt_rate(r["rate_end"])}</td>'
                    f'<td class="right {val_cls(r["rate_change"])}">{fmt_pct(r["rate_change"])}</td>'
                    f'<td class="right {val_cls(r["hold_change"])}">{fmt_vol(r["hold_change"])}</td></tr>'
                )
            tbody = '\n'.join(trs)
        else:
            tbody = '<tr><td colspan="7" class="empty-msg">데이터 없음</td></tr>'

        panels.append(
            f'<div class="pp" id="{pid}">\n' + summary
            + '<div class="table-wrap"><table>\n'
            + '<thead><tr><th>#</th><th>종목명</th><th>시장</th><th>종가</th>'
            + '<th>현재 지분율</th><th>지분율 변동</th><th>보유수량 변동</th></tr></thead>\n'
            + f'<tbody>\n{tbody}\n</tbody></table></div>\n</div>\n'
        )

    return (
        '\n'.join(radios) + '\n'
        + '<div class="tabs">' + ''.join(labels) + '</div>\n'
        + ''.join(panels)
    )


LIVE_JS = r'''<script>
(function(){
/* ── 포매팅 ── */
function fmtAmt(v){var a=v/1e8,ab=Math.abs(a);
var s=ab>=1000?ab.toLocaleString('ko-KR',{maximumFractionDigits:0}):ab.toLocaleString('ko-KR',{minimumFractionDigits:1,maximumFractionDigits:1});
return(a>=0?'+':'-')+s+'\uc5b5';}
function fmtVol(v){var ab=Math.abs(v),s=v>=0?'+':'';
if(ab>=1e6)return s+(v/1e6).toFixed(1)+'M';if(ab>=1e3)return s+Math.round(v/1e3)+'K';return s+v.toLocaleString('ko-KR');}
function fmtPrice(v){return v?v.toLocaleString('ko-KR')+'\uc6d0':'-';}
function vc(v){return v>0?'positive':(v<0?'negative':'');}
function mb(m){return'<span class="badge badge-'+m.toLowerCase()+'">'+m+'</span>';}

/* ── 수급 패널 업데이트 ── */
function updatePanel(pid,rows){
if(!rows||!rows.length)return;
var panel=document.getElementById(pid);if(!panel)return;
var top=rows.slice(0,10),all=rows;
var ta=0,ka=0,da=0;
for(var i=0;i<all.length;i++){ta+=all[i].buy_amount;if(all[i].market==='KOSPI')ka+=all[i].buy_amount;else da+=all[i].buy_amount;}
var tc=ta>=0?'green':'red',kc=ka>=0?'green':'red',dc=da>=0?'green':'red';
var h='<div class="summary">'
+'<div class="stat-card"><div class="label">1\uc704 \uc885\ubaa9</div><div class="value accent">'+all[0].name+'</div></div>'
+'<div class="stat-card"><div class="label">\uc804\uccb4 \ud569\uacc4</div><div class="value '+tc+'">'+fmtAmt(ta)+'</div></div>'
+'<div class="stat-card"><div class="label">KOSPI</div><div class="value '+kc+'">'+fmtAmt(ka)+'</div></div>'
+'<div class="stat-card"><div class="label">KOSDAQ</div><div class="value '+dc+'">'+fmtAmt(da)+'</div></div></div>';
h+='<div class="table-wrap"><table><thead><tr><th>#</th><th>\uc885\ubaa9\uba85</th><th>\uc2dc\uc7a5</th><th>\uc885\uac00</th><th>\uc21c\ub9e4\uc218\uae08\uc561</th><th>\uc21c\ub9e4\uc218\uc218\ub7c9</th></tr></thead><tbody>';
for(var j=0;j<top.length;j++){var r=top[j],cs=r.change>=0?'+':'';
h+='<tr><td>'+(j+1)+'</td><td>'+r.name+' <span class="sub">'+r.code+'</span></td><td>'+mb(r.market)+'</td>'
+'<td class="right">'+fmtPrice(r.price)+'<br><span class="sub '+vc(r.change)+'">'+cs+r.change.toFixed(2)+'%</span></td>'
+'<td class="right '+vc(r.buy_amount)+'">'+fmtAmt(r.buy_amount)+'</td>'
+'<td class="right '+vc(r.buy_volume)+'">'+fmtVol(r.buy_volume)+'</td></tr>';}
h+='</tbody></table></div>';panel.innerHTML=h;}

/* ── 매크로 스파크라인 ── */
var STORE_KEY='utong_spark_'+new Date().toISOString().slice(0,10);
function loadSparkData(){try{return JSON.parse(localStorage.getItem(STORE_KEY))||{};}catch(e){return {};}}
function saveSparkData(d){try{localStorage.setItem(STORE_KEY,JSON.stringify(d));}catch(e){}}

function fmtMacroVal(ind){
var v=ind.value,c=ind.category,u=ind.unit||'';
if(c==='rate')return v.toFixed(2)+'%';
if(u==='$')return'$'+v.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
if(u==='\uc6d0')return v.toLocaleString('ko-KR',{minimumFractionDigits:1,maximumFractionDigits:1});
return v.toLocaleString('ko-KR',{minimumFractionDigits:2,maximumFractionDigits:2});}

function fmtMacroChg(ind){
var c=ind.change,cp=ind.change_pct,u=ind.unit||'',cat=ind.category;
var s=c>=0?'+':'';
if(cat==='rate')return s+c.toFixed(2)+'%p ('+s+cp.toFixed(2)+'%)';
if(u==='$')return s+c.toFixed(2)+' ('+s+cp.toFixed(2)+'%)';
if(u==='\uc6d0')return s+c.toFixed(1)+' ('+s+cp.toFixed(2)+'%)';
return s+c.toFixed(2)+' ('+s+cp.toFixed(2)+'%)';}

function renderSpark(svg,pts,color){
if(!pts||pts.length<2){svg.innerHTML='';return;}
var vals=pts.map(function(p){return p.v;});
var mn=Math.min.apply(null,vals),mx=Math.max.apply(null,vals);
var range=mx-mn||1;var W=120,H=32,pad=2;
var coords=[];
for(var i=0;i<vals.length;i++){
var x=pad+(W-2*pad)*i/(vals.length-1);
var y=H-pad-(H-2*pad)*(vals[i]-mn)/range;
coords.push(x.toFixed(1)+','+y.toFixed(1));}
var lineStr=coords.join(' ');
var areaStr=coords.join(' ')+' '+W+','+H+' 0,'+H;
svg.innerHTML='<polyline class="area" points="'+areaStr+'" fill="'+color+'" />'
+'<polyline points="'+lineStr+'" stroke="'+color+'" />';}

function updateMacroCards(indicators){
var sparkData=loadSparkData();
var now=Date.now();
indicators.forEach(function(ind){
var name=ind.name;
if(!sparkData[name])sparkData[name]=[];
sparkData[name].push({t:now,v:ind.value});
if(sparkData[name].length>200)sparkData[name]=sparkData[name].slice(-200);
var card=document.querySelector('[data-macro="'+name+'"]');
if(!card)return;
var valEl=card.querySelector('.macro-value');
var chgEl=card.querySelector('.macro-change');
if(valEl)valEl.textContent=fmtMacroVal(ind);
if(chgEl){chgEl.textContent=fmtMacroChg(ind);chgEl.className='macro-change '+(ind.change>=0?'positive':'negative');}
var svg=card.querySelector('.spark');
var color=ind.change>=0?'#34d399':'#f87171';
renderSpark(svg,sparkData[name],color);});
saveSparkData(sparkData);}

function updateMacroTimestamp(ts){
var el=document.getElementById('macro-updated');
if(el&&ts){var d=new Date(ts);el.textContent='\u2022 '+d.getHours()+':'+(d.getMinutes()<10?'0':'')+d.getMinutes()+' \uac31\uc2e0';}}

function fetchMacro(){
fetch('/api/macro').then(function(r){return r.json();}).then(function(d){
if(d.indicators)updateMacroCards(d.indicators);
if(d.timestamp)updateMacroTimestamp(d.timestamp);
}).catch(function(e){console.log('macro fetch error:',e);});}

/* ── 수급 새로고침 ── */
var btn=document.getElementById('refresh-btn');
function doRefresh(){
if(btn){btn.disabled=true;btn.textContent='\uac31\uc2e0 \uc911...';}
Promise.all([
fetch('/api/rankings').then(function(r){return r.json();}),
fetch('/api/macro').then(function(r){return r.json();})
]).then(function(results){
var d=results[0],m=results[1];
if(d.foreign)updatePanel('fn0v',d.foreign);
if(d.institutional)updatePanel('gn0v',d.institutional);
if(m.indicators)updateMacroCards(m.indicators);
var md=document.getElementById('meta-date');
if(md&&d.timestamp){var ts=d.timestamp;md.textContent=ts.substring(0,4)+'.'+ts.substring(5,7)+'.'+ts.substring(8,10)+' '+ts.substring(11,16);}
if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}
}).catch(function(e){console.log('UTONG error:',e);if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}});}
if(btn){btn.addEventListener('click',doRefresh);}

/* ── 자동 폴링: 5분 간격 매크로 업데이트 ── */
fetchMacro();
setInterval(fetchMacro,5*60*1000);
})();
</script>'''


# ──────────────────────────────────────────────
# HTML 조립
# ──────────────────────────────────────────────
def generate_html(macro_data, f_net, f_sub, i_net, i_sub, price_data):
    date_display = datetime.now(KST).strftime("%Y.%m.%d %H:%M")
    css = _build_css()
    macro_html = _build_macro_html(macro_data)

    # 외국인 패널
    fn_panels = _build_net_panels("fn", f_net, price_data)
    fo_panels = _build_own_panels("fo", f_sub, price_data)
    # 기관 패널
    gn_panels = _build_net_panels("gn", i_net, price_data)
    go_panels = _build_net_panels("go", i_sub, price_data)

    html = (
        '<!DOCTYPE html>\n<html lang="ko">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        '<title>UTONG - 시장 대시보드</title>\n'
        f'<style>\n{css}\n</style>\n'
        '</head>\n<body>\n\n'

        # 헤더
        '<div class="header">\n'
        '  <div class="header-left">\n'
        '    <h1>UTONG <span>시장 대시보드</span></h1>\n'
        f'    <div class="meta">기준일: <span id="meta-date">{date_display}</span>'
        ' | KOSPI + KOSDAQ | Data: KIS OpenAPI'
        ' <button class="refresh-btn" id="refresh-btn">↻ 새로고침</button></div>\n'
        '  </div>\n'
        f'  <a href="{DONATE_URL}" target="_blank" rel="noopener" class="donate-btn">\n'
        '    ☕ 커피 한 잔 후원하기\n'
        '  </a>\n'
        '</div>\n\n'

        # 매크로
        + macro_html + '\n'

        # Level 1: 투자자 탭
        '<input type="radio" name="inv" id="inv-f" checked class="sr">\n'
        '<input type="radio" name="inv" id="inv-i" class="sr">\n'
        '<label for="inv-f" class="inv-tab">외국인 수급</label>'
        '<label for="inv-i" class="inv-tab">기관 수급</label>\n'
        '<div class="inv-tab-line"></div>\n\n'

        # 외국인 섹션
        '<div class="inv-sec" id="sec-f">\n'
        '<input type="radio" name="fm" id="fm-n" checked class="sr">\n'
        '<input type="radio" name="fm" id="fm-o" class="sr">\n'
        '<label for="fm-n" class="main-tab">순매수 금액 TOP 10</label>'
        '<label for="fm-o" class="main-tab">보유비율 증가 TOP 10</label>\n'
        '<div class="main-tab-line"></div>\n'
        f'<div class="section" id="fm-n-s">\n{fn_panels}</div>\n'
        f'<div class="section" id="fm-o-s">\n{fo_panels}</div>\n'
        '</div>\n\n'

        # 기관 섹션
        '<div class="inv-sec" id="sec-i">\n'
        '<input type="radio" name="gm" id="gm-n" checked class="sr">\n'
        '<input type="radio" name="gm" id="gm-o" class="sr">\n'
        '<label for="gm-n" class="main-tab">순매수 금액 TOP 10</label>'
        '<label for="gm-o" class="main-tab">순매수 수량 TOP 10</label>\n'
        '<div class="main-tab-line"></div>\n'
        f'<div class="section" id="gm-n-s">\n{gn_panels}</div>\n'
        f'<div class="section" id="gm-o-s">\n{go_panels}</div>\n'
        '</div>\n\n'

        # 푸터
        '<div style="text-align:center;padding:24px 0;color:#636678;font-size:12px">\n'
        '  Generated by UTONG | Data source: KIS OpenAPI\n'
        '</div>\n\n'
        + LIVE_JS + '\n\n'
        '</body>\n</html>'
    )
    return html


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    log("UTONG 데이터 수집 시작")
    start_time = time.time()

    kis = KISClient()
    if not kis.app_key or not kis.app_secret:
        log("KIS_APP_KEY / KIS_APP_SECRET 환경변수를 설정하세요.")
        sys.exit(1)

    # 1. 매크로 지표 (KIS 토큰 공유)
    macro_data = fetch_macro_indicators(kis_client=kis)

    # 2. 외국인/기관 순매수 랭킹 (당일)
    log("외국인 랭킹 수집...")
    foreign_rankings = kis.fetch_rankings("foreign")
    log(f"  외국인 랭킹: {len(foreign_rankings)}개 종목")

    log("기관 랭킹 수집...")
    inst_rankings = kis.fetch_rankings("institutional")
    log(f"  기관 랭킹: {len(inst_rankings)}개 종목")

    if not foreign_rankings and not inst_rankings:
        log("랭킹 데이터 없음. 종료.")
        sys.exit(1)

    # 3. 유니크 종목 목록
    stock_meta = {}
    for s in foreign_rankings + inst_rankings:
        if s["code"] not in stock_meta:
            stock_meta[s["code"]] = {"name": s["name"], "market": s["market"]}
    log(f"랭킹 종목 수: {len(stock_meta)}개")

    # 4. 일별 투자자 매매 이력
    log("일별 투자자 매매 이력 수집...")
    histories = fetch_all_histories(kis, stock_meta)
    log(f"이력 수집 완료: {len(histories)}개 종목")

    # 5. 현재가 + 외국인 지분율
    price_data = kis.fetch_prices(set(stock_meta.keys()))

    # 6. 기간별 계산
    log("기간별 데이터 계산 중...")
    f_net, f_sub = calculate_periods(histories, foreign_rankings, price_data, "foreign")
    i_net, i_sub = calculate_periods(histories, inst_rankings, price_data, "institutional")

    # 7. HTML 생성
    log("HTML 생성 중...")
    html = generate_html(macro_data, f_net, f_sub, i_net, i_sub, price_data)
    OUTPUT.write_text(html, encoding="utf-8")

    elapsed = time.time() - start_time
    log(f"완료! {OUTPUT} ({elapsed:.1f}초)")


if __name__ == "__main__":
    main()
