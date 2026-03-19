#!/usr/bin/env python3
"""UTONG - 외국인 수급 추적 대시보드 생성기

네이버 금융에서 데이터를 수집하여 자체 완결형 HTML 대시보드를 생성한다.
- 당일/전일: 네이버 외국인 순매수 랭킹 (정확한 거래대금)
- 3일~3개월: 개별 종목 일별 외국인 매매 이력에서 계산
- 지분율: 개별 종목 일별 보유율 변동에서 계산
"""

import re
import time
import sys
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
from pathlib import Path

from bs4 import BeautifulSoup

from scraper import (
    log, naver_get, parse_int, parse_float,
    fetch_rankings, fetch_prices,
)

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
TOP_N = 10
HISTORY_PAGES = 4  # 페이지당 ~20영업일, 4페이지 ≈ 3개월
OUTPUT_DIR = Path(__file__).parent / "public"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT = OUTPUT_DIR / "index.html"

DONATE_URL = "https://qr.kakaopay.com/Ej759Ivs1"

PERIOD_LABELS = ["당일", "전일", "최근 3일", "최근 1주일", "최근 2주일", "최근 1개월", "최근 3개월"]
PERIOD_DAYS = [1, 1, 3, 5, 10, 21, 63]  # 영업일 기준 슬라이싱 수


# ──────────────────────────────────────────────
# 2. 개별 종목 외국인 일별 매매 이력
# ──────────────────────────────────────────────
def fetch_stock_history(code, pages=HISTORY_PAGES):
    """종목별 일별 외국인 매매 데이터 (최대 ~80영업일)."""
    all_rows = []
    for page in range(1, pages + 1):
        url = f"https://finance.naver.com/item/frgn.naver?code={code}&page={page}"
        resp = naver_get(url)
        if resp is None:
            continue

        soup = BeautifulSoup(resp.content, "html.parser", from_encoding="euc-kr")
        # 외국인 매매 데이터 테이블 찾기
        target = None
        for t in soup.find_all("table"):
            txt = t.get_text()
            if "기관" in txt and "외국인" in txt and "보유주수" in txt:
                target = t
                break
        if target is None:
            continue

        for tr in target.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 9:
                continue
            if not re.match(r"\d{4}\.\d{2}\.\d{2}", cells[0]):
                continue

            date_str = cells[0].replace(".", "")
            close = parse_int(cells[1])
            if close == 0:
                continue
            change_rate = parse_float(cells[3])
            volume = parse_int(cells[4])
            inst_net = parse_int(cells[5])
            foreign_net = parse_int(cells[6])
            hold_shares = parse_int(cells[7])
            hold_rate = parse_float(cells[8])

            all_rows.append({
                "date": date_str,
                "close": close,
                "change_rate": change_rate,
                "volume": volume,
                "foreign_net": foreign_net,
                "hold_shares": hold_shares,
                "hold_rate": hold_rate,
            })

    # 날짜 내림차순 정렬, 중복 제거
    seen = set()
    unique = []
    for r in sorted(all_rows, key=lambda x: x["date"], reverse=True):
        if r["date"] not in seen:
            seen.add(r["date"])
            unique.append(r)
    return unique


def fetch_all_histories(stock_meta):
    """모든 종목의 일별 이력을 수집한다."""
    histories = {}
    total = len(stock_meta)
    for i, (code, meta) in enumerate(stock_meta.items()):
        log(f"  일별 이력 수집: {meta['name']} ({i+1}/{total})")
        history = fetch_stock_history(code)
        if history:
            histories[code] = {
                "name": meta["name"],
                "market": meta["market"],
                "history": history,
            }
    return histories


# ──────────────────────────────────────────────
# 3. 기간별 순매수/지분율 계산
# ──────────────────────────────────────────────
def calculate_periods(histories, ranking_data, dates):
    """일별 이력에서 기간별 순매수 금액 Top 10 / 지분율 변동 Top 10 계산."""
    # 모든 고유 영업일 수집 (오름차순)
    all_dates = sorted(set(
        r["date"] for data in histories.values() for r in data["history"]
    ))

    if not all_dates:
        return {k: [] for k in PERIOD_LABELS}, {k: [] for k in PERIOD_LABELS}

    net_result = {}
    own_result = {}

    for label, days in zip(PERIOD_LABELS, PERIOD_DAYS):
        # 당일/전일: 랭킹 데이터 사용 (순매수)
        if label in ("당일", "전일") and ranking_data.get(label):
            net_result[label] = ranking_data[label]  # 전체 보존
        else:
            # 기간의 영업일 범위 결정
            if label == "당일":
                target_dates = set(all_dates[-1:])
            elif label == "전일":
                target_dates = set(all_dates[-2:-1]) if len(all_dates) >= 2 else set()
            else:
                target_dates = set(all_dates[-days:]) if len(all_dates) >= days else set(all_dates)

            # 순매수 계산
            net_rows = []
            for code, data in histories.items():
                period_data = [r for r in data["history"] if r["date"] in target_dates]
                if not period_data:
                    continue
                total_amount = sum(r["foreign_net"] * r["close"] for r in period_data)
                total_volume = sum(r["foreign_net"] for r in period_data)
                net_rows.append({
                    "code": code,
                    "name": data["name"],
                    "market": data["market"],
                    "buy_amount": total_amount,
                    "buy_volume": total_volume,
                })
            net_rows.sort(key=lambda x: x["buy_amount"], reverse=True)
            net_result[label] = net_rows  # 전체 보존 (TOP_N은 HTML 생성 시 적용)

        # 지분율 변동: 항상 이력에서 계산
        if label == "당일":
            target_dates_own = all_dates[-1:]
            ref_dates_own = all_dates[-2:-1] if len(all_dates) >= 2 else []
        elif label == "전일":
            target_dates_own = all_dates[-2:-1] if len(all_dates) >= 2 else []
            ref_dates_own = all_dates[-3:-2] if len(all_dates) >= 3 else []
        else:
            target_dates_own = all_dates[-1:]
            start_idx = max(0, len(all_dates) - days)
            ref_dates_own = [all_dates[start_idx]]

        own_rows = []
        if target_dates_own and ref_dates_own:
            end_date = target_dates_own[0]
            start_date = ref_dates_own[0]

            for code, data in histories.items():
                history_map = {r["date"]: r for r in data["history"]}
                r_end = history_map.get(end_date)
                r_start = history_map.get(start_date)
                if r_end is None or r_start is None:
                    continue
                rate_change = round(r_end["hold_rate"] - r_start["hold_rate"], 2)
                hold_change = r_end["hold_shares"] - r_start["hold_shares"]
                own_rows.append({
                    "code": code,
                    "name": data["name"],
                    "market": data["market"],
                    "rate_end": r_end["hold_rate"],
                    "rate_change": rate_change,
                    "hold_change": hold_change,
                })

        own_rows.sort(key=lambda x: x["rate_change"], reverse=True)
        own_result[label] = own_rows  # 전체 보존

    return net_result, own_result


# ──────────────────────────────────────────────
# 5. HTML 생성
# ──────────────────────────────────────────────
LIVE_UPDATE_JS = r'''<script>
(function(){
function fmtAmt(v){
var a=v/1e8,ab=Math.abs(a);
var s=ab>=1000?ab.toLocaleString('ko-KR',{maximumFractionDigits:0}):ab.toLocaleString('ko-KR',{minimumFractionDigits:1,maximumFractionDigits:1});
return (a>=0?'+':'-')+s+'\uc5b5';
}
function fmtVol(v){
var ab=Math.abs(v),sign=v>=0?'+':'';
if(ab>=1e6)return sign+(v/1e6).toFixed(1)+'M';
if(ab>=1e3)return sign+Math.round(v/1e3)+'K';
return sign+v.toLocaleString('ko-KR');
}
function fmtPrice(v){return v?v.toLocaleString('ko-KR')+'\uc6d0':'-';}
function valCls(v){return v>0?'positive':(v<0?'negative':'');}
function mktBadge(m){return '<span class="badge badge-'+m.toLowerCase()+'">'+m+'</span>';}
var btn=document.getElementById('refresh-btn');
function updateRankings(){
if(btn){btn.disabled=true;btn.textContent='\uac31\uc2e0 \uc911...';}
fetch('/api/rankings').then(function(r){return r.json();}).then(function(data){
if(!data.rankings||!data.rankings.length){if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}return;}
var panel=document.getElementById('n0v');
if(!panel){if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}return;}
var rows=data.rankings.slice(0,10),all=data.rankings;
var totalAmt=0,kospiAmt=0,kosdaqAmt=0;
for(var i=0;i<all.length;i++){
totalAmt+=all[i].buy_amount;
if(all[i].market==='KOSPI')kospiAmt+=all[i].buy_amount;
else kosdaqAmt+=all[i].buy_amount;
}
var tc=totalAmt>=0?'green':'red',kc=kospiAmt>=0?'green':'red',dc=kosdaqAmt>=0?'green':'red';
var html='<div class="summary">'
+'<div class="stat-card"><div class="label">1\uc704 \uc885\ubaa9</div><div class="value accent">'+all[0].name+'</div></div>'
+'<div class="stat-card"><div class="label">\uc804\uccb4 \ud569\uacc4</div><div class="value '+tc+'">'+fmtAmt(totalAmt)+'</div></div>'
+'<div class="stat-card"><div class="label">KOSPI</div><div class="value '+kc+'">'+fmtAmt(kospiAmt)+'</div></div>'
+'<div class="stat-card"><div class="label">KOSDAQ</div><div class="value '+dc+'">'+fmtAmt(kosdaqAmt)+'</div></div>'
+'</div>';
html+='<div class="table-wrap"><table>'
+'<thead><tr><th>#</th><th>\uc885\ubaa9\uba85</th><th>\uc2dc\uc7a5</th><th>\uc885\uac00</th><th>\uc21c\ub9e4\uc218\uae08\uc561</th><th>\uc21c\ub9e4\uc218\uc218\ub7c9</th></tr></thead><tbody>';
for(var j=0;j<rows.length;j++){
var r=rows[j],cs=r.change>=0?'+':'';
html+='<tr><td>'+(j+1)+'</td>'
+'<td>'+r.name+' <span class="sub">'+r.code+'</span></td>'
+'<td>'+mktBadge(r.market)+'</td>'
+'<td class="right">'+fmtPrice(r.price)+'<br><span class="sub '+valCls(r.change)+'">'+cs+r.change.toFixed(2)+'%</span></td>'
+'<td class="right '+valCls(r.buy_amount)+'">'+fmtAmt(r.buy_amount)+'</td>'
+'<td class="right '+valCls(r.buy_volume)+'">'+fmtVol(r.buy_volume)+'</td></tr>';
}
html+='</tbody></table></div>';
panel.innerHTML=html;
var meta=document.querySelector('.meta');
if(meta){
var ts=data.timestamp||'';
var timeStr=ts.length>=16?ts.substring(11,16):new Date().toTimeString().substring(0,5);
var dateStr=ts.length>=10?ts.substring(0,4)+'.'+ts.substring(5,7)+'.'+ts.substring(8,10):'';
if(dateStr){meta.innerHTML='\uae30\uc900\uc77c: '+dateStr+' '+timeStr+' | KOSPI + KOSDAQ | Data: Naver Finance | <span class="live-indicator">\uc2e4\uc2dc\uac04</span>';}
else{var base=meta.textContent.replace(/\s*\|\s*\uc2e4\uc2dc\uac04.*$/,'');meta.innerHTML=base+' | <span class="live-indicator">\uc2e4\uc2dc\uac04 ('+timeStr+' \uac31\uc2e0)</span>';}
}
if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}
}).catch(function(e){console.log('UTONG live update error:',e);if(btn){btn.disabled=false;btn.textContent='\u21bb \uc0c8\ub85c\uace0\uce68';}});
}
if(btn){btn.addEventListener('click',updateRankings);}
})();
</script>'''


def generate_html(today_str, net_data, own_data, price_data):
    """자체 완결형 HTML 파일 생성. 테이블은 Python에서 미리 렌더링."""
    if today_str:
        date_display = f"{today_str[:4]}.{today_str[4:6]}.{today_str[6:]}"
    else:
        date_display = datetime.now(KST).strftime("%Y.%m.%d")

    # ── 포매팅 헬퍼 ──
    def fmt(v):
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

    # ── 순매수 패널 (7개 기간) ──
    def build_net_panels(prefix):
        radios = []
        labels = []
        panels = []
        for i, period in enumerate(PERIOD_LABELS):
            rid = f"{prefix}{i}"
            pid = f"{prefix}{i}v"
            checked = " checked" if i == 0 else ""
            radios.append(f'<input type="radio" name="{prefix}p" id="{rid}"{checked} class="sr">')
            labels.append(f'<label for="{rid}" class="tab-btn">{period}</label>')
            all_rows = net_data.get(period, [])
            rows = all_rows[:TOP_N]
            summary_html = ""
            if all_rows:
                top_name = all_rows[0]['name']
                total = sum(r['buy_amount'] for r in all_rows)
                kospi_t = sum(r['buy_amount'] for r in all_rows if r['market'] == 'KOSPI')
                kosdaq_t = sum(r['buy_amount'] for r in all_rows if r['market'] == 'KOSDAQ')
                tc = "green" if total >= 0 else "red"
                kc = "green" if kospi_t >= 0 else "red"
                dc = "green" if kosdaq_t >= 0 else "red"
                summary_html = (
                    '<div class="summary">'
                    f'<div class="stat-card"><div class="label">1위 종목</div><div class="value accent">{top_name}</div></div>'
                    f'<div class="stat-card"><div class="label">전체 합계</div><div class="value {tc}">{fmt(total)}</div></div>'
                    f'<div class="stat-card"><div class="label">KOSPI</div><div class="value {kc}">{fmt(kospi_t)}</div></div>'
                    f'<div class="stat-card"><div class="label">KOSDAQ</div><div class="value {dc}">{fmt(kosdaq_t)}</div></div>'
                    '</div>\n'
                )
            if rows:
                trs = []
                for j, r in enumerate(rows):
                    p = price_data.get(r['code'], {})
                    chg = p.get('change', 0)
                    chg_s = '+' if chg >= 0 else ''
                    trs.append(
                        f'<tr><td>{j+1}</td>'
                        f'<td>{r["name"]} <span class="sub">{r["code"]}</span></td>'
                        f'<td>{mkt_badge(r["market"])}</td>'
                        f'<td class="right">{fmt_price(p.get("price", 0))}'
                        f'<br><span class="sub {val_cls(chg)}">{chg_s}{chg:.2f}%</span></td>'
                        f'<td class="right {val_cls(r["buy_amount"])}">{fmt(r["buy_amount"])}</td>'
                        f'<td class="right {val_cls(r["buy_volume"])}">{fmt_vol(r["buy_volume"])}</td></tr>'
                    )
                tbody_html = '\n'.join(trs)
            else:
                tbody_html = '<tr><td colspan="6" class="empty-msg">데이터 없음</td></tr>'
            panels.append(
                f'<div class="pp" id="{pid}">\n'
                + summary_html
                + '<div class="table-wrap"><table>\n'
                + '<thead><tr><th>#</th><th>종목명</th><th>시장</th><th>종가</th>'
                + '<th>순매수금액</th><th>순매수수량</th></tr></thead>\n'
                + f'<tbody>\n{tbody_html}\n</tbody>\n'
                + '</table></div>\n</div>\n'
            )
        return (
            '\n'.join(radios) + '\n'
            + '<div class="tabs">' + ''.join(labels) + '</div>\n'
            + ''.join(panels)
        )

    # ── 지분율 패널 (7개 기간) ──
    def build_own_panels(prefix):
        radios = []
        labels = []
        panels = []
        for i, period in enumerate(PERIOD_LABELS):
            rid = f"{prefix}{i}"
            pid = f"{prefix}{i}v"
            checked = " checked" if i == 0 else ""
            radios.append(f'<input type="radio" name="{prefix}p" id="{rid}"{checked} class="sr">')
            labels.append(f'<label for="{rid}" class="tab-btn">{period}</label>')
            all_rows = own_data.get(period, [])
            rows = all_rows[:TOP_N]
            summary_html = ""
            if all_rows:
                top_name = all_rows[0]['name']
                max_chg = max(r['rate_change'] for r in all_rows)
                avg_chg = sum(r['rate_change'] for r in all_rows) / len(all_rows)
                summary_html = (
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
                    p = price_data.get(r['code'], {})
                    chg = p.get('change', 0)
                    chg_s = '+' if chg >= 0 else ''
                    trs.append(
                        f'<tr><td>{j+1}</td>'
                        f'<td>{r["name"]} <span class="sub">{r["code"]}</span></td>'
                        f'<td>{mkt_badge(r["market"])}</td>'
                        f'<td class="right">{fmt_price(p.get("price", 0))}'
                        f'<br><span class="sub {val_cls(chg)}">{chg_s}{chg:.2f}%</span></td>'
                        f'<td class="right">{fmt_rate(r["rate_end"])}</td>'
                        f'<td class="right {val_cls(r["rate_change"])}">{fmt_pct(r["rate_change"])}</td>'
                        f'<td class="right {val_cls(r["hold_change"])}">{fmt_vol(r["hold_change"])}</td></tr>'
                    )
                tbody_html = '\n'.join(trs)
            else:
                tbody_html = '<tr><td colspan="7" class="empty-msg">데이터 없음</td></tr>'
            panels.append(
                f'<div class="pp" id="{pid}">\n'
                + summary_html
                + '<div class="table-wrap"><table>\n'
                + '<thead><tr><th>#</th><th>종목명</th><th>시장</th><th>종가</th>'
                + '<th>현재 지분율</th><th>지분율 변동</th><th>보유수량 변동</th></tr></thead>\n'
                + f'<tbody>\n{tbody_html}\n</tbody>\n'
                + '</table></div>\n</div>\n'
            )
        return (
            '\n'.join(radios) + '\n'
            + '<div class="tabs">' + ''.join(labels) + '</div>\n'
            + ''.join(panels)
        )

    # ── 조립 ──
    net_content = build_net_panels('n')
    own_content = build_own_panels('o')

    # CSS: radio 기반 탭 전환
    panel_rules = []
    tab_active_rules = []
    for prefix in ['n', 'o']:
        for i in range(len(PERIOD_LABELS)):
            panel_rules.append(f"#{prefix}{i}:checked~#{prefix}{i}v{{display:block}}")
            tab_active_rules.append(f"#{prefix}{i}:checked~.tabs label[for={prefix}{i}]{{background:#252836;color:#e1e4ea}}")

    main_rules = "#tab-net:checked~#sec-net{display:block}#tab-own:checked~#sec-own{display:block}"
    main_label_rules = (
        "#tab-net:checked~label[for=tab-net],"
        "#tab-own:checked~label[for=tab-own]"
        "{color:#818cf8;border-bottom-color:#818cf8}"
    )

    css = (
        "*{margin:0;padding:0;box-sizing:border-box}"
        "body{background:#0f1117;color:#e1e4ea;font-family:-apple-system,'Pretendard','Noto Sans KR',sans-serif;"
        "line-height:1.5;padding:16px;max-width:1200px;margin:0 auto}"
        "a{color:#818cf8;text-decoration:none}"
        # header (flex layout)
        ".header{padding:24px 0 16px;border-bottom:1px solid #252836;margin-bottom:24px;"
        "display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px}"
        ".header-left{flex:1;min-width:0}"
        ".header h1{font-size:24px;font-weight:800;color:#fff}"
        ".header h1 span{color:#818cf8;font-weight:400;font-size:14px;margin-left:8px}"
        ".header .meta{color:#8b8fa3;font-size:13px;margin-top:4px}"
        # donate button
        ".donate-btn{display:inline-flex;align-items:center;gap:6px;padding:10px 18px;"
        "background:linear-gradient(135deg,#f59e0b,#d97706);color:#fff;border-radius:10px;"
        "font-size:13px;font-weight:600;text-decoration:none;white-space:nowrap;"
        "transition:transform .15s,box-shadow .15s;flex-shrink:0}"
        ".donate-btn:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(245,158,11,.3)}"
        # live indicator
        ".live-indicator{color:#34d399;font-weight:600}"
        ".live-indicator::before{content:'\\25CF ';animation:blink 2s infinite}"
        ".refresh-btn{background:#252836;color:#8b8fa3;border:1px solid #363a4e;border-radius:6px;"
        "padding:4px 12px;font-size:12px;cursor:pointer;transition:all .15s;margin-left:8px;vertical-align:middle}"
        ".refresh-btn:hover{background:#363a4e;color:#e1e4ea}"
        ".refresh-btn:disabled{opacity:.5;cursor:not-allowed}"
        "@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}"
        # 숨겨진 radio
        ".sr{position:absolute;opacity:0;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}"
        # 메인 탭 (label)
        ".main-tab{display:inline-block;padding:14px 24px;color:#8b8fa3;cursor:pointer;"
        "font-size:15px;font-weight:600;border-bottom:2px solid transparent;margin-bottom:-2px}"
        ".main-tab:hover{color:#e1e4ea}"
        ".main-tab-line{border-bottom:2px solid #252836;margin-bottom:20px}"
        # 메인 섹션 표시
        ".section{display:none;margin-bottom:40px}"
        + main_rules
        + main_label_rules
        # 기간 탭 (label)
        + ".tabs{display:flex;gap:4px;background:#1a1d27;border-radius:12px;padding:4px;margin-bottom:16px;overflow-x:auto}"
        ".tab-btn{display:block;padding:10px 14px;border-radius:10px;color:#8b8fa3;"
        "cursor:pointer;font-size:13px;font-weight:500;white-space:nowrap}"
        ".tab-btn:hover{color:#e1e4ea}"
        # 패널 기본 숨김 + 체크 시 표시
        ".pp{display:none}"
        + ''.join(panel_rules)
        + ''.join(tab_active_rules)
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
        ".main-tab{padding:12px 16px;font-size:14px}"
        ".tab-btn{padding:8px 10px;font-size:12px}"
        ".summary{grid-template-columns:repeat(2,1fr)}"
        ".donate-btn{padding:8px 14px;font-size:12px}"
        "td,th{padding:8px 6px;font-size:12px}}"
    )

    html = (
        '<!DOCTYPE html>\n<html lang="ko">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        '<title>UTONG - 외국인 수급 추적</title>\n'
        '<style>\n' + css + '\n</style>\n'
        '</head>\n<body>\n\n'
        # header with donate button
        '<div class="header">\n'
        '  <div class="header-left">\n'
        '    <h1>UTONG <span>외국인 수급 추적 대시보드</span></h1>\n'
        f'    <div class="meta">기준일: {date_display} {datetime.now(KST).strftime("%H:%M")} | KOSPI + KOSDAQ | Data: Naver Finance'
        f' <button class="refresh-btn" id="refresh-btn">↻ 새로고침</button></div>\n'
        '  </div>\n'
        f'  <a href="{DONATE_URL}" target="_blank" rel="noopener" class="donate-btn">\n'
        '    ☕ 커피 한 잔 후원하기\n'
        '  </a>\n'
        '</div>\n\n'
        # 메인 탭: radio + label
        '<input type="radio" name="main" id="tab-net" checked class="sr">\n'
        '<input type="radio" name="main" id="tab-own" class="sr">\n'
        '<label for="tab-net" class="main-tab">순매수 금액 TOP 10</label>'
        '<label for="tab-own" class="main-tab">지분율 증가 TOP 10</label>\n'
        '<div class="main-tab-line"></div>\n\n'
        # 섹션 1: 순매수
        '<div class="section" id="sec-net">\n'
        + net_content
        + '</div>\n\n'
        # 섹션 2: 지분율
        '<div class="section" id="sec-own">\n'
        + own_content
        + '</div>\n\n'
        '<div style="text-align:center;padding:24px 0;color:#636678;font-size:12px">\n'
        '  Generated by UTONG | Data source: Naver Finance\n'
        '</div>\n\n'
        + LIVE_UPDATE_JS + '\n\n'
        '</body>\n</html>'
    )
    return html


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    log("UTONG 데이터 수집 시작")
    start_time = time.time()

    # 1. 외국인 순매수 랭킹 (당일/전일)
    rankings, dates = fetch_rankings()
    today_str = dates.get("당일") or dates.get("전일")
    if not today_str:
        log("랭킹 데이터를 가져올 수 없습니다. 종료.")
        sys.exit(1)
    log(f"기준일: 당일={dates.get('당일')} 전일={dates.get('전일')}")

    # 2. 유니크 종목 목록 수집
    stock_meta = {}
    for period_key in rankings:
        for s in rankings[period_key]:
            if s["code"] not in stock_meta:
                stock_meta[s["code"]] = {"name": s["name"], "market": s["market"]}
    log(f"랭킹 종목 수: {len(stock_meta)}개")

    # 3. 개별 종목 일별 이력 수집
    log("일별 외국인 매매 이력 수집 시작...")
    histories = fetch_all_histories(stock_meta)
    log(f"이력 수집 완료: {len(histories)}개 종목")

    # 4. 기간별 순매수/지분율 계산
    log("기간별 데이터 계산 중...")
    net_data, own_data = calculate_periods(histories, rankings, dates)

    # 5. 현재가 보충
    all_codes = set()
    for rows in net_data.values():
        for r in rows:
            all_codes.add(r["code"])
    for rows in own_data.values():
        for r in rows:
            all_codes.add(r["code"])
    price_data = fetch_prices(all_codes)

    # 6. HTML 생성
    log("HTML 생성 중...")
    html = generate_html(today_str, net_data, own_data, price_data)
    OUTPUT.write_text(html, encoding="utf-8")

    elapsed = time.time() - start_time
    log(f"완료! {OUTPUT} ({elapsed:.1f}초)")


if __name__ == "__main__":
    main()
