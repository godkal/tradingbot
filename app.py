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
PAGE_TITLE = "Premarket Briefing"
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


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS briefings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                candidate_limit INTEGER NOT NULL,
                min_score REAL NOT NULL,
                recommended_count INTEGER NOT NULL,
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
            INSERT INTO briefings (trade_date, created_at, candidate_limit, min_score, recommended_count, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["trade_date"],
                payload["created_at"],
                payload["candidate_limit"],
                payload["min_score"],
                len(payload["recommendations"]),
                json.dumps(payload, ensure_ascii=False),
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
            INSERT INTO scheduler_runs (job_name, started_at, finished_at, status, message, trade_date, briefing_created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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


def build_news_search_url(keyword: str) -> str:
    return f"https://search.naver.com/search.naver?where=news&query={quote_plus(keyword)}"


def google_rss_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


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


def keyword_strength(title: str, summary: str) -> float:
    text = f"{title} {summary}".lower()
    score = 6.0
    for keyword in ["order", "contract", "guidance", "approval", "ai", "hbm", "investment", "demand", "supply"]:
        if keyword in text:
            score += 0.7
    return min(score, 9.5)


def fetch_news_for_ticker(snapshot: Snapshot, created_at: datetime) -> list[NewsItem]:
    response = requests.get(google_rss_url(f'"{snapshot.name}" when:1d'), headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
    response.raise_for_status()
    root = ET.fromstring(response.text)
    rows: list[NewsItem] = []
    for item in root.findall(".//item")[:2]:
        title = unescape(item.findtext("title", default="")).strip()
        if not title:
            continue
        summary = unescape(item.findtext("description", default="")).strip()
        pub_date = item.findtext("pubDate", default="")
        published_at = parsedate_to_datetime(pub_date) if pub_date else created_at
        rows.append(
            NewsItem(
                title=title,
                source="Google News RSS",
                summary=summary or f"News related to {snapshot.name}",
                published_at=published_at.astimezone(KST),
                related_tickers=[snapshot.ticker],
                news_strength=keyword_strength(title, summary),
                url=item.findtext("link", default="").strip(),
            )
        )
    return rows


def collect_news(snapshots: list[Snapshot], max_tickers: int = 15) -> tuple[list[NewsItem], str]:
    created_at = now_kst()
    candidates = snapshots[:max_tickers]
    try:
        collected: list[NewsItem] = []
        with ThreadPoolExecutor(max_workers=min(6, len(candidates)) or 1) as executor:
            futures = [executor.submit(fetch_news_for_ticker, s, created_at) for s in candidates]
            for future in as_completed(futures):
                try:
                    collected.extend(future.result())
                except Exception:
                    continue
        if collected:
            return collected, "actual"
    except Exception:
        pass
    return [
        NewsItem(title=t, source="Sample", summary=s, published_at=created_at - timedelta(minutes=i * 10), related_tickers=tk, news_strength=n, url=build_news_search_url(t))
        for i, (t, s, tk, n) in enumerate(SAMPLE_NEWS_ROWS)
    ], "fallback_sample"


def score_candidates(snapshots: list[Snapshot], news_items: list[NewsItem]) -> list[ScoreRow]:
    news_map: dict[str, dict[str, Any]] = {}
    for item in news_items:
        payload = {
            "title": item.title,
            "source": item.source,
            "summary": item.summary,
            "published_at": to_storage_time(item.published_at),
            "news_strength": item.news_strength,
            "url": item.url,
        }
        for ticker in item.related_tickers:
            row = news_map.setdefault(ticker, {"max_strength": 0.0, "count": 0, "items": []})
            row["max_strength"] = max(row["max_strength"], item.news_strength)
            row["count"] += 1
            row["items"].append(payload)

    result: list[ScoreRow] = []
    for s in snapshots:
        n = news_map.get(s.ticker, {"max_strength": 0.0, "count": 0, "items": []})
        material = clamp((n["max_strength"] * 5.5) + (n["count"] * 1.2), 0, 35)
        flow = clamp((s.change_rate * 2.4) + min(s.trading_value / 120_000_000_000, 18), 0, 35)
        intraday_range = ((s.high_price - s.low_price) / s.close_price * 100) if s.close_price > 0 else 0
        setup = clamp((intraday_range * 1.3) + min(s.volume / 1_800_000, 10), 0, 20)
        risk = 0.0
        notes: list[str] = []
        if s.change_rate > 20:
            risk += 7.0
            notes.append("Daily change above 20%")
        if intraday_range > 18:
            risk += 5.0
            notes.append("Intraday range above 18%")

        total = clamp(material + flow + setup - risk, 0, 100)
        reasons: list[str] = []
        if n["items"]:
            reasons.append(f"News strength {n['max_strength']:.1f}, {n['count']} item(s)")
            reasons.extend([x["title"] for x in n["items"][:2]])
        else:
            reasons.append("No ticker-linked news in this cycle")

        result.append(
            ScoreRow(
                ticker=s.ticker,
                ticker_name=s.name,
                material_score=round(material, 2),
                money_flow_score=round(flow, 2),
                setup_score=round(setup, 2),
                risk_penalty=round(risk, 2),
                total_score=round(total, 2),
                reason_text=" | ".join(reasons[:3]),
                score_breakdown={"material": round(material, 2), "flow": round(flow, 2), "setup": round(setup, 2), "risk": round(risk, 2)},
                material_reasons=reasons,
                risk_notes=notes,
            )
        )

    result.sort(key=lambda x: x.total_score, reverse=True)
    return result


def generate_briefing(candidate_limit: int, min_score: float) -> dict[str, Any]:
    created = now_kst()
    trade_date = current_trade_date()

    market_rows, market_mode = collect_market_data(limit=60)
    news_rows, news_mode = collect_news(market_rows, max_tickers=15)
    scores = score_candidates(market_rows, news_rows)

    selected = [row for row in scores if row.total_score >= min_score][:candidate_limit]

    recs: list[dict[str, Any]] = []
    for row in selected:
        snapshot = next((x for x in market_rows if x.ticker == row.ticker), None)
        close = snapshot.close_price if snapshot else 0
        recs.append(
            {
                "ticker": row.ticker,
                "ticker_name": row.ticker_name,
                "total_score": row.total_score,
                "status": "candidate",
                "buy_min": round(close * 0.985, 2),
                "buy_max": round(close * 0.997, 2),
                "stop_loss": round(close * 0.975, 2),
                "material_reasons": row.material_reasons,
                "risk_notes": row.risk_notes,
            }
        )

    payload = {
        "trade_date": trade_date,
        "created_at": to_storage_time(created),
        "candidate_limit": candidate_limit,
        "min_score": float(min_score),
        "recommendations": recs,
        "collection_meta": {
            "market_collection_mode": market_mode,
            "news_collection_mode": news_mode,
            "market_rows": len(market_rows),
            "news_rows": len(news_rows),
            "refresh_interval_hours": DEFAULT_REFRESH_HOURS,
        },
        "collected_news": [
            {
                "title": n.title,
                "source": n.source,
                "summary": n.summary,
                "published_at": to_storage_time(n.published_at),
                "related_tickers": n.related_tickers,
                "news_strength": n.news_strength,
                "url": n.url,
            }
            for n in news_rows
        ],
        "score_review": [
            {
                "rank": idx + 1,
                "ticker": s.ticker,
                "ticker_name": s.ticker_name,
                "total_score": s.total_score,
                "material_score": s.material_score,
                "money_flow_score": s.money_flow_score,
                "setup_score": s.setup_score,
                "risk_penalty": s.risk_penalty,
                "reason_text": s.reason_text,
                "score_breakdown": s.score_breakdown,
                "material_reasons": s.material_reasons,
                "risk_notes": s.risk_notes,
                "change_rate": next((m.change_rate for m in market_rows if m.ticker == s.ticker), 0.0),
            }
            for idx, s in enumerate(scores)
        ],
    }

    save_briefing(payload)
    return payload


def run_background_refresh() -> None:
    started = to_storage_time(now_kst())
    trade_date = current_trade_date()
    try:
        briefing = generate_briefing(DEFAULT_CANDIDATE_COUNT, DEFAULT_MIN_SCORE)
        save_scheduler_run("success", started, to_storage_time(now_kst()), "briefing refreshed", trade_date, briefing.get("created_at"))
    except Exception as exc:
        save_scheduler_run("error", started, to_storage_time(now_kst()), str(exc), trade_date, None)


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
    status = load_latest_scheduler_run() or {"status": "idle", "finished_at": None, "message": ""}
    job = ensure_scheduler_started().get_job(JOB_NAME)
    status["next_run_at"] = to_storage_time(job.next_run_time.astimezone(KST)) if job and job.next_run_time else None
    return status


def ensure_initial_briefing() -> dict[str, Any]:
    today = current_trade_date()
    return load_latest_briefing_for_trade_date(today) or load_latest_briefing() or generate_briefing(DEFAULT_CANDIDATE_COUNT, DEFAULT_MIN_SCORE)


def render_ui(briefing: dict[str, Any], status: dict[str, Any]) -> None:
    st.title(PAGE_TITLE)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trade date", briefing["trade_date"])
    c2.metric("Created", to_display_time(briefing["created_at"]))
    c3.metric("Candidates", len(briefing["recommendations"]))
    c4.metric("Min score", f"{briefing['min_score']:.0f}")

    s1, s2, s3 = st.columns(3)
    s1.metric("Scheduler", status.get("status", "idle"))
    s2.metric("Last run", to_display_time(status.get("finished_at")))
    s3.metric("Next run", to_display_time(status.get("next_run_at")))

    st.subheader("Top picks")
    if not briefing["recommendations"]:
        st.info("No recommendation for current threshold")
    for item in briefing["recommendations"][:3]:
        st.markdown(f"**{item['ticker_name']} ({item['ticker']})** - score {item['total_score']:.1f}")
        st.write(f"Buy range: {item['buy_min']:,.0f} - {item['buy_max']:,.0f} | Stop: {item['stop_loss']:,.0f}")
        if item.get("material_reasons"):
            st.caption("Reasons: " + " | ".join(item["material_reasons"][:3]))

    st.subheader("Collected news")
    rows = briefing.get("collected_news", [])[:30]
    if rows:
        st.dataframe(
            pd.DataFrame([
                {
                    "time": to_display_time(x["published_at"]),
                    "strength": x["news_strength"],
                    "source": x["source"],
                    "ticker": ",".join(x.get("related_tickers", [])),
                    "title": x["title"],
                }
                for x in rows
            ]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.write("No collected news")

    st.subheader("Score review")
    review = briefing.get("score_review", [])
    if review:
        st.dataframe(
            pd.DataFrame([
                {
                    "rank": x["rank"],
                    "name": x["ticker_name"],
                    "ticker": x["ticker"],
                    "total": x["total_score"],
                    "material": x["material_score"],
                    "flow": x["money_flow_score"],
                    "setup": x["setup_score"],
                    "risk": x["risk_penalty"],
                    "change_rate": x["change_rate"],
                    "reason": x["reason_text"],
                }
                for x in review
            ]),
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, page_icon=":chart_with_upwards_trend:", layout="wide")
    init_db()
    ensure_scheduler_started()

    if "briefing" not in st.session_state:
        st.session_state["briefing"] = ensure_initial_briefing()

    st.sidebar.header("Controls")
    candidate_limit = st.sidebar.number_input("candidate_limit", min_value=1, max_value=5, value=DEFAULT_CANDIDATE_COUNT, step=1)
    min_score = st.sidebar.slider("min_score", min_value=50, max_value=90, value=DEFAULT_MIN_SCORE, step=1)

    if st.sidebar.button("refresh briefing", use_container_width=True):
        with st.spinner("Collecting market and news"):
            st.session_state["briefing"] = generate_briefing(int(candidate_limit), float(min_score))
        st.rerun()

    render_ui(st.session_state["briefing"], scheduler_status())


if __name__ == "__main__":
    main()
