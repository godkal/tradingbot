from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any
from urllib.parse import quote_plus

import FinanceDataReader as fdr
import pandas as pd
import requests
import streamlit as st

try:
    KST = timezone(timedelta(hours=9))
except Exception:
    KST = timezone(timedelta(hours=9))

PAGE_TITLE = "장전 단타 브리핑"
DEFAULT_MIN_SCORE = 75
DEFAULT_CANDIDATE_COUNT = 3


@dataclass
class Snapshot:
    ticker: str
    name: str
    close_price: float
    change_rate: float
    volume: float
    trading_value: float
    high_price: float
    low_price: float
    open_price: float


@dataclass
class ScoreRow:
    ticker: str
    name: str
    material_score: float
    money_flow_score: float
    setup_score: float
    risk_penalty: float
    total_score: float
    reason_text: str


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def trade_date_kst() -> str:
    current = now_kst()
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.strftime("%Y-%m-%d")


def display_time(value: datetime | str) -> str:
    if isinstance(value, str):
        value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    return value.strftime("%m-%d %H:%M")


def init_page() -> None:
    st.set_page_config(page_title=PAGE_TITLE, page_icon="📈", layout="wide")
    st.markdown(
        """
        <style>
        .main { background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%); }
        .block-container { max-width: 1180px; padding-top: 2rem; padding-bottom: 3rem; }
        .hero-card, .info-card, .sub-card, .log-card {
            background: white; border-radius: 20px; padding: 1.35rem 1.45rem;
            border: 1px solid rgba(15,23,42,0.08); box-shadow: 0 14px 36px rgba(15,23,42,0.06);
        }
        .hero-card { background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); color: white; }
        .hero-title { font-size: 1.9rem; font-weight: 800; margin-bottom: 0.3rem; }
        .hero-subtitle { color: rgba(255,255,255,0.78); margin-bottom: 0.8rem; }
        .status-badge { display:inline-block; padding:0.35rem 0.7rem; border-radius:999px; font-size:0.82rem; font-weight:700; margin-bottom:0.85rem; }
        .status-buy { background: rgba(22,163,74,0.15); color:#15803d; }
        .status-wait { background: rgba(59,130,246,0.14); color:#1d4ed8; }
        .status-no { background: rgba(239,68,68,0.14); color:#b91c1c; }
        .status-chase { background: rgba(245,158,11,0.16); color:#b45309; }
        .metric-label { font-size:0.82rem; color:#64748b; margin-bottom:0.2rem; }
        .metric-value { font-size:1.08rem; font-weight:700; color:#0f172a; }
        .hero-card .metric-label { color: rgba(255,255,255,0.70); }
        .hero-card .metric-value { color: white; }
        .price-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:0.8rem 1.2rem; margin-top:1rem; }
        .sub-card-title { font-size:1.15rem; font-weight:800; margin-bottom:0.45rem; color:#0f172a; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def status_class(status: str) -> str:
    return {
        "지금 매수 가능": "status-buy",
        "눌림 대기": "status-wait",
        "제외": "status-no",
        "추격 금지": "status-chase",
    }.get(status, "status-wait")


def price_range_text(plan: dict[str, Any]) -> str:
    if plan["buy_min"] == plan["buy_max"]:
        return f"{plan['buy_min']:,.0f}원"
    return f"{plan['buy_min']:,.0f}원 ~ {plan['buy_max']:,.0f}원"


def google_rss_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ko&gl=KR&ceid=KR:ko"


def collect_market_data(limit: int = 60) -> tuple[list[Snapshot], str]:
    listing = fdr.StockListing("KRX")
    listing = listing[listing["Market"].isin(["KOSPI", "KOSDAQ"])].copy()
    listing["Amount"] = listing["Amount"].fillna(0)
    listing["Volume"] = listing["Volume"].fillna(0)
    listing["ChagesRatio"] = listing["ChagesRatio"].fillna(0)
    target = listing[(listing["Amount"] > 10_000_000_000) & (listing["Volume"] > 100_000)].copy()
    target = target.sort_values(["Amount", "ChagesRatio"], ascending=[False, False]).head(limit)
    rows: list[Snapshot] = []
    for row in target.to_dict("records"):
        rows.append(
            Snapshot(
                ticker=str(row["Code"]).zfill(6),
                name=str(row["Name"]),
                close_price=float(row.get("Close") or 0),
                change_rate=float(row.get("ChagesRatio") or 0),
                volume=float(row.get("Volume") or 0),
                trading_value=float(row.get("Amount") or 0),
                high_price=float(row.get("High") or 0),
                low_price=float(row.get("Low") or 0),
                open_price=float(row.get("Open") or 0),
            )
        )
    return rows, "actual"


def collect_news(tickers: list[Snapshot], max_tickers: int = 15) -> tuple[dict[str, list[dict[str, Any]]], str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    news_map: dict[str, list[dict[str, Any]]] = {}
    for item in tickers[:max_tickers]:
        try:
            response = requests.get(google_rss_url(f"{item.ticker} when:1d"), headers=headers, timeout=15)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            rss_items = root.findall(".//item")[:2]
            if not rss_items:
                continue
            news_map[item.ticker] = []
            for news in rss_items:
                title = unescape(news.findtext("title", default="")).strip()
                link = news.findtext("link", default="").strip()
                desc = unescape(news.findtext("description", default="")).strip()
                pub_date = news.findtext("pubDate", default="")
                published_at = parsedate_to_datetime(pub_date) if pub_date else now_kst()
                news_map[item.ticker].append(
                    {
                        "title": title,
                        "url": link,
                        "source": "Google News RSS",
                        "summary": desc,
                        "published_at": published_at.astimezone(KST),
                    }
                )
        except Exception:
            continue
    return news_map, "actual"


def collect_disclosures(tickers: set[str], date_key: str) -> tuple[dict[str, float], str]:
    api_key = os.getenv("DART_API_KEY", "").strip()
    if not api_key:
        return {}, "disabled"
    try:
        response = requests.get(
            "https://opendart.fss.or.kr/api/list.json",
            params={
                "crtfc_key": api_key,
                "bgn_de": date_key.replace("-", ""),
                "end_de": date_key.replace("-", ""),
                "corp_cls": "Y",
                "sort": "date",
                "sort_mth": "desc",
                "page_count": "100",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "000":
            return {}, "actual_empty"
        scores: dict[str, float] = {}
        for row in payload.get("list", []):
            ticker = str(row.get("stock_code", "")).zfill(6)
            if not ticker or ticker not in tickers:
                continue
            title = str(row.get("report_nm", ""))
            strength = 6.5
            for keyword in ["계약", "수주", "취득", "양수", "투자", "승인", "합병", "영업이익"]:
                if keyword in title:
                    strength += 0.8
            scores[ticker] = max(scores.get(ticker, 0.0), min(strength, 9.5))
        return scores, "actual" if scores else "actual_empty"
    except Exception:
        return {}, "disabled"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def news_strength(news_items: list[dict[str, Any]]) -> tuple[float, float]:
    if not news_items:
        return 0.0, 0.0
    max_strength = 0.0
    for item in news_items:
        text = f"{item['title']} {item['summary']}".lower()
        score = 6.0
        for keyword in ["수주", "계약", "실적", "승인", "ai", "hbm", "투자", "신약", "공급", "수혜"]:
            if keyword in text:
                score += 0.7
        max_strength = max(max_strength, min(score, 9.5))
    return max_strength, float(len(news_items))


def build_score(snapshot: Snapshot, news_items: list[dict[str, Any]], disclosure_strength: float) -> ScoreRow:
    max_news_strength, spread_count = news_strength(news_items)
    material = 0.0
    tags: list[str] = []
    if disclosure_strength >= 8.5:
        material += 16
        tags.append("강한 공시")
    elif disclosure_strength >= 7:
        material += 11
        tags.append("유의미 공시")
    if max_news_strength >= 9:
        material += 14
        tags.append("직접 수혜 뉴스")
    elif max_news_strength >= 7.5:
        material += 10
        tags.append("재료 부각 뉴스")
    elif max_news_strength >= 6:
        material += 6
    if spread_count >= 2:
        material += 5
        tags.append("뉴스 확산")
    elif spread_count == 1:
        material += 3
    material = clamp(material, 0, 35)

    money = 0.0
    if snapshot.trading_value >= 50_000_000_000:
        money += 14
    elif snapshot.trading_value >= 20_000_000_000:
        money += 10
    elif snapshot.trading_value >= 10_000_000_000:
        money += 6
    if snapshot.volume >= 3_000_000:
        money += 8
    elif snapshot.volume >= 1_000_000:
        money += 5
    elif snapshot.volume >= 500_000:
        money += 3
    close_position = (snapshot.close_price - snapshot.low_price) / max(snapshot.high_price - snapshot.low_price, 1)
    if close_position >= 0.82:
        money += 8
    elif close_position >= 0.68:
        money += 5
    elif close_position >= 0.55:
        money += 3
    money = clamp(money, 0, 30)

    retrace_ratio = (snapshot.high_price - snapshot.close_price) / max(snapshot.high_price, 1)
    day_range_ratio = (snapshot.high_price - snapshot.low_price) / max(snapshot.close_price, 1)
    setup = 0.0
    setup += 9 if retrace_ratio <= 0.02 else 7 if retrace_ratio <= 0.04 else 4
    setup += 8 if snapshot.change_rate <= 15 else 5 if snapshot.change_rate <= 20 else 2
    setup += 8 if day_range_ratio <= 0.13 else 6 if day_range_ratio <= 0.18 else 3
    setup = clamp(setup, 0, 25)

    risk = 0.0
    if snapshot.change_rate >= 18:
        risk += 4
    if (snapshot.high_price - snapshot.low_price) / max(snapshot.close_price, 1) >= 0.18:
        risk += 3
    if "직접 수혜 뉴스" not in tags and snapshot.change_rate >= 10:
        risk += 2
    if (snapshot.close_price - snapshot.low_price) / max(snapshot.close_price, 1) >= 0.14:
        risk += 2
    risk = clamp(risk, 0, 10)

    total = round(material + money + setup - risk, 1)
    lead = ", ".join(tags[:2]) if tags else "재료 혼재"
    reason = f"{lead}, 거래대금 {snapshot.trading_value / 100_000_000:.0f}억, 총점 {total:.1f}점"
    return ScoreRow(snapshot.ticker, snapshot.name, round(material, 1), round(money, 1), round(setup, 1), round(risk, 1), total, reason)


def build_plan(score: ScoreRow, snapshot: Snapshot, related_news: list[dict[str, Any]]) -> dict[str, Any]:
    retrace_ratio = (snapshot.high_price - snapshot.close_price) / max(snapshot.high_price, 1)
    if score.total_score >= 82 and snapshot.change_rate >= 10 and retrace_ratio <= 0.025:
        plan_type = "눌림형"
        buy_min = round(snapshot.close_price * 0.985 / 10) * 10
        buy_max = round(snapshot.close_price * 1.005 / 10) * 10
        stop_loss = round((snapshot.low_price + (snapshot.close_price - snapshot.low_price) * 0.35) / 10) * 10
        action = "장초반 첫 눌림 확인 후 매수"
    elif snapshot.close_price >= snapshot.high_price * 0.985:
        plan_type = "돌파형"
        buy_min = round(snapshot.high_price / 10) * 10
        buy_max = round(snapshot.high_price * 1.01 / 10) * 10
        stop_loss = round(snapshot.close_price * 0.975 / 10) * 10
        action = "전일 고가 돌파 후 안착 시 매수"
    else:
        plan_type = "시초형"
        buy_min = round(snapshot.open_price * 0.995 / 10) * 10
        buy_max = round(snapshot.open_price * 1.01 / 10) * 10
        stop_loss = round(snapshot.low_price * 1.01 / 10) * 10
        action = "시초가 지지 확인 시 매수, 갭 과열 시 제외"
    rr = max(buy_max - stop_loss, snapshot.close_price * 0.02)
    tp1 = round((buy_max + rr * 1.5) / 10) * 10
    tp2 = round((buy_max + rr * (2.5 if plan_type == '눌림형' else 2.0)) / 10) * 10
    status = "제외"
    if score.total_score >= 80 and snapshot.change_rate < 18:
        status = "지금 매수 가능"
    elif score.total_score >= 65 and snapshot.change_rate < 18:
        status = "눌림 대기"
    elif snapshot.change_rate >= 18:
        status = "추격 금지"
    if status == "추격 금지":
        action = "갭 과열 또는 급등 지속 시 추격 금지"
    elif status == "제외":
        action = "손절 라인이 불명확해 제외"
    return {
        "ticker": snapshot.ticker,
        "ticker_name": snapshot.name,
        "plan_type": plan_type,
        "status": status,
        "buy_min": float(buy_min),
        "buy_max": float(buy_max),
        "stop_loss": float(stop_loss),
        "take_profit_1": float(tp1),
        "take_profit_2": float(tp2),
        "action_guide": action,
        "reason_text": score.reason_text,
        "total_score": score.total_score,
        "related_news": related_news,
    }


def generate_briefing(min_score: float, candidate_limit: int) -> dict[str, Any]:
    trade_date = trade_date_kst()
    snapshots, market_source = collect_market_data()
    news_map, news_source = collect_news(snapshots)
    disclosure_map, disclosure_source = collect_disclosures({s.ticker for s in snapshots}, trade_date)

    scores: list[ScoreRow] = []
    for snapshot in snapshots:
        scores.append(build_score(snapshot, news_map.get(snapshot.ticker, []), disclosure_map.get(snapshot.ticker, 0.0)))
    ordered_scores = sorted(scores, key=lambda x: x.total_score, reverse=True)

    snapshot_map = {s.ticker: s for s in snapshots}
    recommendations = []
    for row in ordered_scores:
        if row.total_score >= min_score and len(recommendations) < candidate_limit:
            recommendations.append(build_plan(row, snapshot_map[row.ticker], news_map.get(row.ticker, [])))

    review_rows = []
    for idx, row in enumerate(ordered_scores[:15], start=1):
        snap = snapshot_map[row.ticker]
        review_rows.append(
            {
                "rank": idx,
                "ticker": row.ticker,
                "ticker_name": row.name,
                "material_score": row.material_score,
                "money_flow_score": row.money_flow_score,
                "setup_score": row.setup_score,
                "risk_penalty": row.risk_penalty,
                "total_score": row.total_score,
                "reason_text": row.reason_text,
                "close_price": snap.close_price,
                "change_rate": snap.change_rate,
                "trading_value": snap.trading_value,
            }
        )

    return {
        "trade_date": trade_date,
        "created_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "min_score": min_score,
        "candidate_limit": candidate_limit,
        "recommendations": recommendations,
        "review_rows": review_rows,
        "excluded_notes": ["재료는 강하지만 과열", "거래대금 부족", "추격 위험 높음"],
        "logs": {
            "news_count": sum(len(v) for v in news_map.values()),
            "disclosure_count": len(disclosure_map),
            "analyzed_count": len(scores),
            "recommended_count": len(recommendations),
            "market_source": market_source,
            "news_source": news_source,
            "disclosure_source": disclosure_source,
        },
    }


def render_news_links(news_items: list[dict[str, Any]], dark: bool = False) -> None:
    if not news_items:
        return
    if dark:
        html = ['<div style="margin-top:1rem;padding-top:1rem;border-top:1px solid rgba(255,255,255,0.14);"><div style="font-size:0.82rem;color:rgba(255,255,255,0.70);margin-bottom:0.4rem;">확인용 뉴스 링크</div>']
        for item in news_items[:3]:
            html.append(f"<div style='margin-top:0.35rem;'><a href=\"{item['url']}\" target=\"_blank\" style='color:#bfdbfe;text-decoration:none;'>[{item['source']}] {item['title']}</a></div>")
        html.append("</div>")
        st.markdown("".join(html), unsafe_allow_html=True)
    else:
        st.caption("확인용 뉴스 링크")
        for item in news_items[:3]:
            st.markdown(f"- [[{item['source']}] {item['title']}]({item['url']})")


def render_briefing(briefing: dict[str, Any]) -> None:
    st.markdown(f"## {PAGE_TITLE}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("기준 일자", briefing["trade_date"])
    c2.metric("생성 시각", display_time(briefing["created_at"]))
    c3.metric("추천 종목 수", str(len(briefing["recommendations"])))
    c4.metric("최소 기준 점수", f"{briefing['min_score']:.0f}점")

    st.markdown("### 내일의 최고 후보 1위")
    if briefing["recommendations"]:
        top = briefing["recommendations"][0]
        st.markdown(
            f"""
            <div class="hero-card">
                <div class="hero-title">{top['ticker_name']} <span style="font-size:1rem; font-weight:600;">({top['ticker']})</span></div>
                <div class="hero-subtitle">{top['plan_type']} · 추천 점수 {top['total_score']:.1f}점</div>
                <div class="status-badge {status_class(top['status'])}">{top['status']}</div>
                <div style="font-size:1.08rem; font-weight:700; margin-bottom:0.45rem;">{top['action_guide']}</div>
                <div style="font-size:0.96rem; color:rgba(255,255,255,0.84); margin-bottom:1rem;">{top['reason_text']}</div>
                <div class="price-grid">
                    <div><div class="metric-label">매수 구간</div><div class="metric-value">{price_range_text(top)}</div></div>
                    <div><div class="metric-label">손절 가격</div><div class="metric-value">{top['stop_loss']:,.0f}원</div></div>
                    <div><div class="metric-label">1차 매도</div><div class="metric-value">{top['take_profit_1']:,.0f}원</div></div>
                    <div><div class="metric-label">최종 매도</div><div class="metric-value">{top['take_profit_2']:,.0f}원</div></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        render_news_links(top.get("related_news", []), dark=True)
    else:
        st.info("오늘은 최소 추천 점수를 넘은 종목이 없어 추천 후보를 비웠습니다. 아래 검토 순위를 확인해 주세요.")

    st.markdown("### 차순위 후보")
    subs = briefing["recommendations"][1:3]
    if subs:
        cols = st.columns(min(2, len(subs)))
        for i, plan in enumerate(subs):
            with cols[i % len(cols)]:
                st.markdown(
                    f"""
                    <div class="sub-card">
                        <div class="sub-card-title">{plan['ticker_name']} <span style="font-size:0.92rem; color:#64748b;">({plan['ticker']})</span></div>
                        <div class="status-badge {status_class(plan['status'])}">{plan['status']}</div>
                        <div style="font-size:0.95rem; color:#475569; margin-bottom:0.9rem;">{plan['reason_text']}</div>
                        <div class="metric-label">매수 구간</div>
                        <div class="metric-value" style="margin-bottom:0.7rem;">{price_range_text(plan)}</div>
                        <div class="metric-label">손절 가격</div>
                        <div class="metric-value" style="margin-bottom:0.7rem;">{plan['stop_loss']:,.0f}원</div>
                        <div class="metric-label">1차 매도 / 최종 매도</div>
                        <div class="metric-value">{plan['take_profit_1']:,.0f}원 / {plan['take_profit_2']:,.0f}원</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                render_news_links(plan.get("related_news", []), dark=False)
    else:
        st.write("차순위 후보는 없습니다.")

    left, right = st.columns([1.2, 1])
    with left:
        st.markdown("### 제외 메모")
        st.markdown('<div class="info-card">' + '<br>'.join(f"• {x}" for x in briefing["excluded_notes"]) + '</div>', unsafe_allow_html=True)
    with right:
        logs = briefing["logs"]
        st.markdown("### 추천 생성 로그")
        a, b, c, d = st.columns(4)
        a.markdown(f'<div class="log-card"><div class="metric-label">뉴스 수집 건수</div><div class="metric-value">{logs["news_count"]}</div></div>', unsafe_allow_html=True)
        b.markdown(f'<div class="log-card"><div class="metric-label">공시 수집 건수</div><div class="metric-value">{logs["disclosure_count"]}</div></div>', unsafe_allow_html=True)
        c.markdown(f'<div class="log-card"><div class="metric-label">최종 분석 대상</div><div class="metric-value">{logs["analyzed_count"]}</div></div>', unsafe_allow_html=True)
        d.markdown(f'<div class="log-card"><div class="metric-label">최종 추천 종목 수</div><div class="metric-value">{logs["recommended_count"]}</div></div>', unsafe_allow_html=True)
        st.caption(f"시장 데이터: {logs['market_source']} | 뉴스: {logs['news_source']} | 공시: {logs['disclosure_source']}")

    st.markdown("### 검토한 종목 보기")
    review_rows = briefing["review_rows"]
    if review_rows:
        st.dataframe(pd.DataFrame([
            {
                "순위": row["rank"],
                "종목": row["ticker_name"],
                "코드": row["ticker"],
                "총점": row["total_score"],
                "재료": row["material_score"],
                "수급": row["money_flow_score"],
                "자리": row["setup_score"],
                "감점": row["risk_penalty"],
                "등락률": row["change_rate"],
            }
            for row in review_rows
        ]), hide_index=True, use_container_width=True)
        with st.expander("점수 산정 근거 상세 보기", expanded=False):
            for row in review_rows:
                st.markdown(f"**{row['rank']}위 · {row['ticker_name']} ({row['ticker']}) · 총점 {row['total_score']:.1f}점**")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("재료 점수", f"{row['material_score']:.1f}")
                c2.metric("수급 점수", f"{row['money_flow_score']:.1f}")
                c3.metric("자리 점수", f"{row['setup_score']:.1f}")
                c4.metric("리스크 감점", f"{row['risk_penalty']:.1f}")
                st.write(row["reason_text"])
                st.caption(f"종가 {row['close_price']:,.0f}원 | 등락률 {row['change_rate']:.2f}% | 거래대금 {row['trading_value'] / 100_000_000:,.0f}억")
                st.divider()


def main() -> None:
    init_page()
    if "briefing" not in st.session_state:
        st.session_state["briefing"] = generate_briefing(DEFAULT_MIN_SCORE, DEFAULT_CANDIDATE_COUNT)

    st.sidebar.header("브리핑 설정")
    candidate_limit = st.sidebar.number_input("추천 종목 수", min_value=1, max_value=3, value=DEFAULT_CANDIDATE_COUNT, step=1)
    min_score = st.sidebar.slider("최소 추천 점수", min_value=60, max_value=90, value=DEFAULT_MIN_SCORE, step=1)
    if st.sidebar.button("오늘 브리핑 새로 생성", use_container_width=True):
        with st.spinner("실데이터로 브리핑 생성 중..."):
            st.session_state["briefing"] = generate_briefing(float(min_score), int(candidate_limit))

    render_briefing(st.session_state["briefing"])


if __name__ == "__main__":
    main()
