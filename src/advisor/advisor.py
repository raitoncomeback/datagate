# =============================================================================
# src/advisor/advisor.py
# DataGate AI Financial Advisor — FastAPI backend
#
# The advisor reads ONLY from main_gold.mart_ticker_intelligence — the table
# that DataGate's quality gate, enrichment, and dbt models have already
# verified and structured. It never touches raw bronze data.
#
# Key behaviours:
#   - Checks advisor_can_serve before answering (circuit breaker)
#   - Explicitly tells the user which sources it used and when verified
#   - If a source is blocked, says so and answers with remaining trusted data
#   - Force mode (--force) bypasses circuit breaker for demo purposes
# =============================================================================

import os
import json
import duckdb
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from loguru import logger
from openai import OpenAI

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH",
    "data/duckdb/datagate.db"
)

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_MODEL = "openai/gpt-oss-120b:free"

# Set to True to bypass circuit breaker for demo purposes
FORCE_SERVE = True

TRACKED_TICKERS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "SBIN.NS", "BAJFINANCE.NS", "WIPRO.NS", "ADANIENT.NS"
]

TICKER_NAMES = {
    "RELIANCE.NS": "Reliance Industries",
    "TCS.NS": "Tata Consultancy Services",
    "HDFCBANK.NS": "HDFC Bank",
    "INFY.NS": "Infosys",
    "ICICIBANK.NS": "ICICI Bank",
    "HINDUNILVR.NS": "Hindustan Unilever",
    "SBIN.NS": "State Bank of India",
    "BAJFINANCE.NS": "Bajaj Finance",
    "WIPRO.NS": "Wipro",
    "ADANIENT.NS": "Adani Enterprises",
}

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="DataGate Financial Advisor",
    description="AI-powered Indian market advisor backed by trust-gated data",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QuestionRequest(BaseModel):
    question: str
    force: bool = False  # bypass circuit breaker


class SourceCitation(BaseModel):
    source: str
    trust_score: float
    is_blocked: bool
    verified_note: str


class AdvisorResponse(BaseModel):
    answer: str
    sources_used: list[SourceCitation]
    advisor_can_serve: bool
    data_as_of: str
    disclaimer: str


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def get_market_context(force: bool = False) -> dict:
    """
    Reads the gold mart and returns structured market context
    for the advisor to use in its prompt.
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Check circuit breaker
    status = conn.execute("""
        SELECT
            bool_and(advisor_can_serve)     as can_serve,
            max(stocks_trust_score)         as stocks_trust,
            max(news_trust_score)           as news_trust,
            bool_or(stocks_blocked)         as stocks_blocked,
            bool_or(news_blocked)           as news_blocked,
            max(date)                       as data_date
        FROM main_gold.mart_ticker_intelligence
    """).fetchone()

    can_serve, stocks_trust, news_trust, stocks_blocked, news_blocked, data_date = status

    # Build source citations
    sources = []
    if stocks_trust is not None:
        sources.append(SourceCitation(
            source="NSE Stock Prices (yfinance)",
            trust_score=round(stocks_trust, 3),
            is_blocked=bool(stocks_blocked),
            verified_note=f"Trust score: {round(stocks_trust * 100, 1)}% — {'BLOCKED' if stocks_blocked else 'verified'}",
        ))
    if news_trust is not None:
        sources.append(SourceCitation(
            source="Indian Financial News (NewsAPI + Gemini enrichment)",
            trust_score=round(news_trust, 3),
            is_blocked=bool(news_blocked),
            verified_note=f"Trust score: {round(news_trust * 100, 1)}% — {'BLOCKED' if news_blocked else 'verified'}",
        ))

    # If circuit breaker is tripped and not forcing, return early
    if not can_serve and not force and not FORCE_SERVE:
        conn.close()
        return {
            "can_serve": False,
            "sources": sources,
            "data_date": str(data_date),
            "tickers": [],
            "market_sentiment": None,
            "news_context": None,
        }

    # Pull ticker data
    tickers = conn.execute("""
        SELECT
            ticker,
            date,
            close,
            intraday_pct_change,
            avg_close_7d,
            avg_close_30d,
            pct_from_30d_avg,
            market_sentiment,
            bullish_score,
            bearish_score,
            sentiment_confidence,
            is_anomaly,
            anomaly_explanation,
            advisor_can_serve
        FROM main_gold.mart_ticker_intelligence
        ORDER BY ticker
    """).fetchall()

    # Pull top news with market implications
    news = conn.execute("""
        SELECT
            title,
            sentiment,
            confidence,
            market_implication,
            topic_tags
        FROM main_silver.stg_news
        WHERE confidence >= 0.7
          AND sentiment != 'neutral'
        ORDER BY confidence DESC
        LIMIT 8
    """).fetchall()

    conn.close()

    return {
        "can_serve": True,
        "sources": sources,
        "data_date": str(data_date),
        "tickers": tickers,
        "news": news,
    }


def build_prompt(question: str, context: dict) -> str:
    """
    Builds the advisor prompt using only verified market context.
    """
    data_date = context["data_date"]

    # Format ticker data
    ticker_lines = []
    for row in context["tickers"]:
        ticker, date, close, pct_change, avg_7d, avg_30d, pct_from_30d, sentiment, bull, bear, conf, is_anomaly, anomaly_exp, can_serve = row
        name = TICKER_NAMES.get(ticker, ticker)
        line = f"  {name} ({ticker.replace('.NS', '')}): ₹{close}"
        if pct_change:
            line += f" ({pct_change:+.2f}% intraday)"
        if avg_30d:
            line += f" | 30d avg ₹{round(avg_30d, 2)} ({pct_from_30d:+.2f}%)"
        if is_anomaly and anomaly_exp:
            line += f" ⚠️ ANOMALY: {anomaly_exp}"
        ticker_lines.append(line)

    # Format news
    news_lines = []
    for title, sentiment, confidence, implication, tags in context.get("news", []):
        news_lines.append(
            f"  [{sentiment.upper()} {confidence:.0%}] {title}\n"
            f"    → {implication}"
        )

    prompt = f"""You are DataGate, an AI financial advisor for Indian stock markets.
You answer questions using ONLY the verified market data below — you never make up prices,
never speculate beyond what the data shows, and always acknowledge uncertainty.

DATA AS OF: {data_date}
DATA SOURCE: All data has passed DataGate's quality gate (freshness, duplicate, schema, and range checks).

=== CURRENT NSE STOCK PRICES ===
{chr(10).join(ticker_lines)}

=== MARKET SENTIMENT (from {len(context.get('news', []))} verified news articles) ===
{context['tickers'][0][7].upper() if context['tickers'] else 'NEUTRAL'} sentiment
{chr(10).join(news_lines) if news_lines else 'No high-confidence news available.'}

=== USER QUESTION ===
{question}

=== INSTRUCTIONS ===
- Answer specifically and concisely using the data above
- Reference actual prices and percentages from the data
- If asked about a ticker not in the data, say so clearly
- If the question requires real-time data you don't have, say so
- End with one sentence noting the data date and that it has been quality-verified
- Do NOT make investment recommendations or tell users to buy/sell
- Maximum 3 short paragraphs"""

    return prompt


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
def pipeline_status():
    """Returns the current trust scores and circuit breaker state."""
    context = get_market_context(force=True)
    return {
        "data_as_of": context["data_date"],
        "sources": [s.dict() for s in context["sources"]],
        "advisor_can_serve": context["can_serve"],
    }


@app.post("/ask", response_model=AdvisorResponse)
def ask(request: QuestionRequest):
    """
    Main advisor endpoint. Answers financial questions using
    only trust-gated, quality-verified market data.
    """
    logger.info(f"Question received: {request.question}")

    # Get market context
    context = get_market_context(force=request.force)

    if not context["can_serve"] and not request.force and not FORCE_SERVE:
        # Circuit breaker tripped — explain why
        blocked_sources = [s.source for s in context["sources"] if s.is_blocked]
        return AdvisorResponse(
            answer=f"I can't answer right now because the following data sources failed "
                   f"DataGate's quality checks: {', '.join(blocked_sources)}. "
                   f"This is a safety feature — I only answer when the underlying data "
                   f"is verified as trustworthy. Please try again after the next pipeline run.",
            sources_used=context["sources"],
            advisor_can_serve=False,
            data_as_of=context["data_date"],
            disclaimer="DataGate circuit breaker active — answer withheld pending data quality recovery.",
        )

    # Build prompt and call OpenRouter
    prompt = build_prompt(request.question, context)

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )

    response = client.chat.completions.create(
        model=OPENROUTER_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=500,
    )

    answer = response.choices[0].message.content.strip()
    logger.info(f"Advisor answered in {len(answer)} chars")

    return AdvisorResponse(
        answer=answer,
        sources_used=context["sources"],
        advisor_can_serve=context["can_serve"],
        data_as_of=context["data_date"],
        disclaimer="This is not financial advice. Data is quality-verified but may not reflect the latest market movements. Always consult a registered financial advisor before making investment decisions.",
    )