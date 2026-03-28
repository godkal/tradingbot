from __future__ import annotations

import json
import os
import sqlite3
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import FinanceDataReader as fdr
import pandas as pd
import requests
import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger


KST = timezone(timedelta(hours=9))
PAGE_TITLE = "장전 단타 브리핑"
DEFAULT_MIN_SCORE = 63
DEFAULT_CANDIDATE_COUNT = 3
DEFAULT_REFRESH_HOURS = 3
JOB_NAME = "briefing_refresh"
DB_PATH = Path(os.getenv("APP_DB_PATH", str(Path(__file__).resolve().parent / "data" / "app.db")))

_scheduler: BackgroundScheduler | None = None
_scheduler_lock = threading.Lock()


@dataclass(slots=True)
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


@dataclass(slots=True)
class NewsItem:
    title: str
    source: str
    summary: str
    published_at: datetime
    related_tickers: list[str]
    news_strength: float
    url: str


@dataclass(slots=True)
class ScoreRow:
    ticker: str
    ticker_name: str
    material_score: float
    money_flow_score: float
    setup_score: float
    risk_penalty: float
    total_score: float
    reason_text: str
    score_breakdown: dict[str, float]
    material_reasons: list[str]
    risk_notes: list[str]


SAMPLE_MARKET_ROWS = [
    ("005930", "SamsungElec", 84500, 3.1, 18_200_000, 1_520_000_000_000, 84800, 82100, 82600),
    ("042660", "HanwhaOcean", 31250, 11.6, 4_850_000, 149_000_000_000, 31700, 28350, 28650),
    ("028300", "HLB", 76800, 18.7, 3_920_000, 294_000_000_000, 78100, 64200, 65100),
    ("000660", "SKHynix", 208500, 1.8, 2_640_000, 548_000_000_000, 210000, 203500, 204000),
]

SAMPLE_NEWS_ROWS = [
    ("Memory demand outlook improved", "AI memory demand stays strong", ["005930"], 8.4),
    ("Defense ship order momentum", "Order book visibility improved", ["042660"], 8.9),
    ("Bio pipeline headline", "High volatility but event driven", ["028300"], 7.6),
]


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def current_trade_date() -> str:
    current = now_kst()
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current.strftime("%Y-%m-%d")


def to_storage_time(value: datetime) -> str:
    return value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def to_display_time(value: str | datetime | None) -> str:
    if not value:
        return "-"
    parsed = value if isinstance(value, datetime) else datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    return parsed.astimezone(KST).strftime("%m-%d %H:%M")


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS briefings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                candidate_limit INTEGER NOT NULL DEFAULT 3,
                min_score REAL NOT NULL DEFAULT 63,
                recommended_count INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scheduler_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                message TEXT,
                trade_date TEXT,
                briefing_created_at TEXT
            );
            """
        )
        conn.commit()


def save_briefing(payload: dict[str, Any]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO briefings (
                trade_date, created_at, candidate_limit, min_score, recommended_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["trade_date"],
                payload["created_at"],
                payload["candidate_limit"],
                payload["min_score"],
                len(payload["recommendations"]),
                json.dumps(payload, ensure_ascii=True),
            ),
        )
        conn.commit()


def load_latest_briefing() -> dict[str, Any] | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT payload_json FROM briefings ORDER BY created_at DESC LIMIT 1").fetchone()
    return json.loads(row[0]) if row else None


def load_latest_briefing_for_trade_date(trade_date: str) -> dict[str, Any] | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT payload_json FROM briefings WHERE trade_date = ? ORDER BY created_at DESC LIMIT 1",
            (trade_date,),
        ).fetchone()
    return json.loads(row[0]) if row else None


def save_scheduler_run(status: str, started_at: str, finished_at: str | None, message: str, trade_date: str, briefing_created_at: str | None = None) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO scheduler_runs (
                job_name, started_at, finished_at, status, message, trade_date, briefing_created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (JOB_NAME, started_at, finished_at, status, message, trade_date, briefing_created_at),
        )
        conn.commit()


def load_latest_scheduler_run() -> dict[str, Any] | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM scheduler_runs WHERE job_name = ? ORDER BY started_at DESC LIMIT 1",
            (JOB_NAME,),
        ).fetchone()
    return dict(row) if row else None


def google_rss_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ko&gl=KR&ceid=KR:ko"


def keyword_strength(title: str, summary: str) -> float:
    text = f"{title} {summary}".lower()
    score = 6.0
    for keyword in ["order", "contract", "earnings", "approval", "ai", "hbm", "investment", "drug", "supply"]:
        if keyword in text:
            score += 0.7
    return min(score, 9.5)


def collect_market_data(limit: int = 60) -> tuple[list[Snapshot], str]:
    try:
        listing = fdr.StockListing("KRX")
        listing = listing[listing["Market"].isin(["KOSPI", "KOSDAQ"])].copy()
        listing["Amount"] = listing["Amount"].fillna(0)
        listing["Volume"] = listing["Volume"].fillna(0)
        listing["ChagesRatio"] = listing["ChagesRatio"].fillna(0)
        rows = listing[
            (listing["Amount"] > 10_000_000_000)
            & (listing["Volume"] > 100_000)
            & (listing["ChagesRatio"] > 0)
        ].sort_values(["Amount", "ChagesRatio"], ascending=[False, False]).head(limit)
        snapshots = [
            Snapshot(
                ticker=str(row["Code"]).zfill(6),
                name=str(row["Name"]),
                close_price=safe_float(row.get("Close")),
                change_rate=safe_float(row.get("ChagesRatio")),
                volume=safe_float(row.get("Volume")),
                trading_value=safe_float(row.get("Amount")),
                high_price=safe_float(row.get("High")),
                low_price=safe_float(row.get("Low")),
                open_price=safe_float(row.get("Open")),
            )
            for row in rows.to_dict("records")
        ]
        if snapshots:
            return snapshots, "actual"
    except Exception:
        pass
    return [Snapshot(*row) for row in SAMPLE_MARKET_ROWS], "fallback_sample"


def fetch_news_for_ticker(snapshot: Snapshot, created_at: datetime) -> list[NewsItem]:
    response = requests.get(google_rss_url(f'"{snapshot.name}" when:1d'), headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    items: list[NewsItem] = []
    for row in root.findall(".//item")[:2]:
        title = unescape(row.findtext("title", default="")).strip()
        if not title:
            continue
        summary = unescape(row.findtext("description", default="")).strip()
        pub_date = row.findtext("pubDate", default="")
        published_at = parsedate_to_datetime(pub_date) if pub_date else created_at
        items.append(
            NewsItem(
                title=title,
                source="Google News RSS",
                summary=summary or f"{snapshot.name} related news",
                published_at=published_at.astimezone(KST),
                related_tickers=[snapshot.ticker],
                news_strength=keyword_strength(title, summary),
                url=row.findtext("link", default="").strip(),
            )
        )
    return items


def collect_news(snapshots: list[Snapshot], max_tickers: int = 15) -> tuple[list[NewsItem], str]:
    created_at = now_kst()
    candidates = snapshots[:max_tickers]
    try:
        rows: list[NewsItem] = []
        with ThreadPoolExecutor(max_workers=min(6, len(candidates)) or 1) as executor:
            futures = [executor.submit(fetch_news_for_ticker, item, created_at) for item in candidates]
            for future in as_completed(futures):
                try:
                    rows.extend(future.result())
                except Exception:
                    continue
        if rows:
            return rows, "actual"
    except Exception:
        pass
    return [
        NewsItem(
            title=title,
            source="Sample",
            summary=summary,
            published_at=created_at - timedelta(minutes=index * 15),
            related_tickers=tickers,
            news_strength=strength,
            url=f"https://news.google.com/search?q={quote_plus(title)}",
        )
        for index, (title, summary, tickers, strength) in enumerate(SAMPLE_NEWS_ROWS)
    ], "fallback_sample"


def score_snapshot(snapshot: Snapshot, news_strength: float, spread_count: int) -> ScoreRow:
    material = 0.0
    reasons: list[str] = []
    if news_strength >= 9:
        material += 14
        reasons.append(f"news bonus 14 from strength {news_strength:.1f}")
    elif news_strength >= 7.5:
        material += 10
        reasons.append(f"news bonus 10 from strength {news_strength:.1f}")
    elif news_strength >= 6:
        material += 6
        reasons.append(f"news bonus 6 from strength {news_strength:.1f}")
    if spread_count >= 2:
        material += 5
        reasons.append(f"spread bonus 5 from {spread_count} articles")
    elif spread_count == 1:
        material += 3
        reasons.append("spread bonus 3 from 1 article")
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

    retrace = (snapshot.high_price - snapshot.close_price) / max(snapshot.high_price, 1)
    day_range = (snapshot.high_price - snapshot.low_price) / max(snapshot.close_price, 1)
    setup = 0.0
    setup += 9 if retrace <= 0.02 else 7 if retrace <= 0.04 else 4
    setup += 8 if snapshot.change_rate <= 15 else 5 if snapshot.change_rate <= 20 else 2
    setup += 8 if day_range <= 0.13 else 6 if day_range <= 0.18 else 3
    setup = clamp(setup, 0, 25)

    penalty = 0.0
    notes: list[str] = []
    if snapshot.change_rate >= 18:
        penalty += 4
        notes.append("overheated move")
    if day_range >= 0.18:
        penalty += 3
        notes.append("wide intraday range")
    if news_strength < 7.5 and snapshot.change_rate >= 10:
        penalty += 2
        notes.append("price strength without enough news support")
    penalty = clamp(penalty, 0, 10)

    total = round(material + money + setup - penalty, 1)
    return ScoreRow(
        ticker=snapshot.ticker,
        ticker_name=snapshot.name,
        material_score=round(material, 1),
        money_flow_score=round(money, 1),
        setup_score=round(setup, 1),
        risk_penalty=round(penalty, 1),
        total_score=total,
        reason_text=f"trading value {snapshot.trading_value / 100_000_000:,.0f}b KRW, total {total:.1f}",
        score_breakdown={
            "material_score": round(material, 1),
            "money_flow_score": round(money, 1),
            "setup_score": round(setup, 1),
            "risk_penalty": round(penalty, 1),
            "news_bonus": 14.0 if news_strength >= 9 else 10.0 if news_strength >= 7.5 else 6.0 if news_strength >= 6 else 0.0,
            "spread_bonus": 5.0 if spread_count >= 2 else 3.0 if spread_count == 1 else 0.0,
        },
        material_reasons=reasons or ["no material bonus"],
        risk_notes=notes,
    )


def build_plan(score: ScoreRow, snapshot: Snapshot, related_news: list[dict[str, Any]]) -> dict[str, Any]:
    if score.total_score >= 80:
        status = "BUY"
    elif score.total_score >= DEFAULT_MIN_SCORE:
        status = "WAIT"
    elif snapshot.change_rate >= 18:
        status = "NO_CHASE"
    else:
        status = "SKIP"
    buy_min = round(snapshot.close_price * 0.985 / 10) * 10
    buy_max = round(snapshot.close_price * 1.005 / 10) * 10
    stop_loss = round(snapshot.low_price * 1.01 / 10) * 10
    rr = max(buy_max - stop_loss, snapshot.close_price * 0.02)
    take_profit_1 = round((buy_max + rr * 1.5) / 10) * 10
    take_profit_2 = round((buy_max + rr * 2.0) / 10) * 10
    return {
        "ticker": score.ticker,
        "ticker_name": score.ticker_name,
        "plan_type": "Pullback",
        "status": status,
        "buy_min": float(buy_min),
        "buy_max": float(buy_max),
        "stop_loss": float(stop_loss),
        "take_profit_1": float(take_profit_1),
        "take_profit_2": float(take_profit_2),
        "action_guide": "Wait for stabilization before entry",
        "reason_text": score.reason_text,
        "total_score": score.total_score,
        "score_breakdown": score.score_breakdown,
        "material_reasons": score.material_reasons,
        "risk_notes": score.risk_notes,
        "related_news": related_news[:3],
    }


def generate_briefing(candidate_limit: int, min_score: float) -> dict[str, Any]:
    trade_date = current_trade_date()
    snapshots, market_source = collect_market_data()
    news_items, news_source = collect_news(snapshots)
    news_by_ticker: dict[str, list[dict[str, Any]]] = {}
    strength_by_ticker: dict[str, tuple[float, int]] = {}
    for item in news_items:
        payload = {
            "title": item.title,
            "source": item.source,
            "summary": item.summary,
            "published_at": to_storage_time(item.published_at),
            "news_strength": item.news_strength,
            "url": item.url,
            "related_tickers": item.related_tickers,
        }
        for ticker in item.related_tickers:
            news_by_ticker.setdefault(ticker, []).append(payload)
            max_strength, count = strength_by_ticker.get(ticker, (0.0, 0))
            strength_by_ticker[ticker] = (max(max_strength, item.news_strength), count + 1)

    scores = []
    for snapshot in snapshots:
        max_strength, count = strength_by_ticker.get(snapshot.ticker, (0.0, 0))
        scores.append(score_snapshot(snapshot, max_strength, count))
    ordered_scores = sorted(scores, key=lambda item: item.total_score, reverse=True)

    selected_scores = [item for item in ordered_scores if item.total_score >= min_score][:candidate_limit]
    snapshot_map = {item.ticker: item for item in snapshots}
    recommendations = [build_plan(score, snapshot_map[score.ticker], news_by_ticker.get(score.ticker, [])) for score in selected_scores]

    review_rows = []
    for idx, score in enumerate(ordered_scores[:15], start=1):
        snap = snapshot_map[score.ticker]
        review_rows.append(
            {
                "rank": idx,
                "ticker": score.ticker,
                "ticker_name": score.ticker_name,
                "material_score": score.material_score,
                "money_flow_score": score.money_flow_score,
                "setup_score": score.setup_score,
                "risk_penalty": score.risk_penalty,
                "total_score": score.total_score,
                "reason_text": score.reason_text,
                "score_breakdown": score.score_breakdown,
                "material_reasons": score.material_reasons,
                "risk_notes": score.risk_notes,
                "close_price": snap.close_price,
                "change_rate": snap.change_rate,
                "trading_value": snap.trading_value,
                "matched_news": news_by_ticker.get(score.ticker, [])[:3],
            }
        )

    payload = {
        "trade_date": trade_date,
        "created_at": to_storage_time(now_kst()),
        "candidate_limit": candidate_limit,
        "min_score": min_score,
        "recommendations": recommendations,
        "score_review": review_rows,
        "excluded_notes": [note for row in ordered_scores for note in row.risk_notes][:5] or ["no exclusion note"],
        "collected_news": [
            {
                "title": item.title,
                "source": item.source,
                "summary": item.summary,
                "published_at": to_storage_time(item.published_at),
                "related_tickers": item.related_tickers,
                "news_strength": item.news_strength,
                "url": item.url,
            }
            for item in sorted(news_items, key=lambda row: row.published_at, reverse=True)
        ],
        "logs": {
            "news_count": len(news_items),
            "disclosure_count": 0,
            "analyzed_count": len(ordered_scores),
            "recommended_count": len(recommendations),
            "market_source": market_source,
            "news_source": news_source,
            "disclosure_source": "disabled",
        },
    }
    save_briefing(payload)
    return payload


def run_background_refresh() -> None:
    started_at = now_kst()
    trade_date = current_trade_date()
    try:
        payload = generate_briefing(DEFAULT_CANDIDATE_COUNT, DEFAULT_MIN_SCORE)
        save_scheduler_run("success", to_storage_time(started_at), to_storage_time(now_kst()), "auto refresh completed", trade_date, payload["created_at"])
    except Exception as exc:
        save_scheduler_run("failed", to_storage_time(started_at), to_storage_time(now_kst()), str(exc), trade_date)


def ensure_scheduler_started() -> BackgroundScheduler:
    global _scheduler
    with _scheduler_lock:
        if _scheduler and _scheduler.running:
            return _scheduler
        scheduler = BackgroundScheduler(timezone=KST)
        scheduler.add_job(
            run_background_refresh,
            trigger=IntervalTrigger(hours=DEFAULT_REFRESH_HOURS, timezone=KST),
            id=JOB_NAME,
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            next_run_time=now_kst(),
        )
        scheduler.start()
        _scheduler = scheduler
        return scheduler


def scheduler_status() -> dict[str, Any]:
    payload = load_latest_scheduler_run() or {"status": "idle", "finished_at": None, "message": ""}
    job = ensure_scheduler_started().get_job(JOB_NAME)
    payload["next_run_at"] = to_storage_time(job.next_run_time.astimezone(KST)) if job and job.next_run_time else None
    return payload


def ensure_initial_briefing() -> dict[str, Any]:
    today = current_trade_date()
    return load_latest_briefing_for_trade_date(today) or load_latest_briefing() or generate_briefing(DEFAULT_CANDIDATE_COUNT, DEFAULT_MIN_SCORE)


def render_ui(briefing: dict[str, Any], status: dict[str, Any]) -> None:
    st.title(PAGE_TITLE)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("거래일", briefing["trade_date"])
    c2.metric("생성 시각", to_display_time(briefing["created_at"]))
    c3.metric("추천 종목 수", len(briefing["recommendations"]))
    c4.metric("최소 점수", f"{briefing['min_score']:.0f}")

    s1, s2, s3 = st.columns(3)
    s1.metric("스케줄러", status.get("status", "idle"))
    s2.metric("최근 실행", to_display_time(status.get("finished_at")))
    s3.metric("다음 실행", to_display_time(status.get("next_run_at")))

    st.subheader("상위 추천 종목")
    if not briefing["recommendations"]:
        st.info("현재 점수 기준을 만족하는 추천 종목이 없습니다.")
    for item in briefing["recommendations"][:3]:
        st.markdown(f"**{item['ticker_name']} ({item['ticker']})** - 점수 {item['total_score']:.1f} - 상태 {item['status']}")
        st.write(f"매수 범위: {item['buy_min']:,.0f} - {item['buy_max']:,.0f}, 손절가: {item['stop_loss']:,.0f}")
        if item.get("material_reasons"):
            st.caption("점수 반영 사유: " + " | ".join(item["material_reasons"]))

    st.subheader("수집된 뉴스")
    news_rows = briefing.get("collected_news", [])[:30]
    if news_rows:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "time": to_display_time(item["published_at"]),
                        "strength": item["news_strength"],
                        "source": item["source"],
                        "ticker": ",".join(item.get("related_tickers", [])),
                        "title": item["title"],
                    }
                    for item in news_rows
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.write("표시할 뉴스가 없습니다.")

    st.subheader("점수 반영 내역")
    review_rows = briefing.get("score_review", [])
    if review_rows:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "rank": row["rank"],
                        "name": row["ticker_name"],
                        "ticker": row["ticker"],
                        "total": row["total_score"],
                        "material": row["material_score"],
                        "flow": row["money_flow_score"],
                        "setup": row["setup_score"],
                        "risk": row["risk_penalty"],
                        "change_rate": row["change_rate"],
                    }
                    for row in review_rows
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, page_icon=":chart_with_upwards_trend:", layout="wide")
    init_db()
    ensure_scheduler_started()
    if "briefing" not in st.session_state:
        st.session_state["briefing"] = ensure_initial_briefing()

    st.sidebar.header("설정")
    candidate_limit = st.sidebar.number_input("candidate_limit", min_value=1, max_value=5, value=DEFAULT_CANDIDATE_COUNT, step=1)
    min_score = st.sidebar.slider("min_score", min_value=50, max_value=90, value=DEFAULT_MIN_SCORE, step=1)
    if st.sidebar.button("브리핑 새로고침", use_container_width=True):
        with st.spinner("시장/뉴스 수집 중..."):
            st.session_state["briefing"] = generate_briefing(int(candidate_limit), float(min_score))
        st.rerun()

    render_ui(st.session_state["briefing"], scheduler_status())


if __name__ == "__main__":
    main()
