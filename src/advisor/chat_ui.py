# =============================================================================
# src/advisor/chat_ui.py
# Streamlit chat interface for the DataGate AI financial advisor
# Run with: streamlit run src/advisor/chat_ui.py
# =============================================================================

import streamlit as st
import requests
import json
from datetime import datetime

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="DataGate — AI Financial Advisor",
    page_icon="🛡️",
    layout="centered",
)

ADVISOR_URL = "http://localhost:8000"

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🛡️ DataGate Financial Advisor")
st.caption(
    "AI-powered Indian market insights backed by a real-time data quality gate. "
    "This advisor only answers when its data has been verified as trustworthy."
)

# ---------------------------------------------------------------------------
# Pipeline status sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Pipeline Status")

    try:
        status = requests.get(f"{ADVISOR_URL}/status", timeout=5).json()

        st.caption(f"Data as of: **{status['data_as_of']}**")
        st.divider()

        for source in status["sources"]:
            trust_pct = round(source["trust_score"] * 100, 1)
            if source["is_blocked"]:
                st.error(f"🔴 {source['source'].split('(')[0].strip()}\n\nTrust: {trust_pct}% — BLOCKED")
            else:
                st.success(f"🟢 {source['source'].split('(')[0].strip()}\n\nTrust: {trust_pct}%")

        st.divider()
        if status["advisor_can_serve"]:
            st.success("✅ Advisor: Online")
        else:
            st.warning("⚠️ Advisor: Limited mode (force enabled for demo)")

    except Exception as e:
        st.error(f"Pipeline status unavailable: {e}")

    st.divider()
    st.caption(
        "**How DataGate works**\n\n"
        "Every data point shown has passed 4 quality checks:\n"
        "1. Freshness — data within SLA window\n"
        "2. Duplicates — no double-counting\n"
        "3. Schema — required fields present\n"
        "4. Range — values are physically plausible\n\n"
        "If any source fails, the trust score drops. "
        "Below 85%, that source is blocked from the advisor."
    )

# ---------------------------------------------------------------------------
# Suggested questions
# ---------------------------------------------------------------------------

st.subheader("Ask about Indian markets")

suggestions = [
    "How is HDFC Bank looking today?",
    "What is the overall market sentiment right now?",
    "Which stocks are showing unusual movement?",
    "How is Infosys performing compared to its 30-day average?",
    "What are the key risks in the market today?",
]

cols = st.columns(2)
for i, suggestion in enumerate(suggestions[:4]):
    if cols[i % 2].button(suggestion, use_container_width=True):
        st.session_state["prefill"] = suggestion

# ---------------------------------------------------------------------------
# Chat interface
# ---------------------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sources"):
            with st.expander("📊 Data sources used"):
                for source in message["sources"]:
                    trust_pct = round(source["trust_score"] * 100, 1)
                    icon = "🔴" if source["is_blocked"] else "🟢"
                    st.caption(f"{icon} {source['source']} — Trust: {trust_pct}%")
            st.caption(f"🗓️ Data as of: {message.get('data_as_of', 'unknown')}")

# Chat input
prefill = st.session_state.pop("prefill", "")
question = st.chat_input(
    "Ask about Nifty, specific stocks, or market sentiment...",
)

if question or prefill:
    q = question or prefill

    # Show user message
    with st.chat_message("user"):
        st.markdown(q)
    st.session_state.messages.append({"role": "user", "content": q})

    # Get advisor response
    with st.chat_message("assistant"):
        with st.spinner("Checking data quality and generating answer..."):
            try:
                response = requests.post(
                    f"{ADVISOR_URL}/ask",
                    json={"question": q, "force": False},
                    timeout=60,
                ).json()

                answer = response["answer"]
                sources = response["sources_used"]
                data_as_of = response["data_as_of"]
                disclaimer = response["disclaimer"]

                st.markdown(answer)

                with st.expander("📊 Data sources used"):
                    for source in sources:
                        trust_pct = round(source["trust_score"] * 100, 1)
                        icon = "🔴" if source["is_blocked"] else "🟢"
                        st.caption(f"{icon} {source['source']} — Trust: {trust_pct}%")

                st.caption(f"🗓️ Data as of: {data_as_of}")
                st.caption(f"⚠️ {disclaimer}")

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources,
                    "data_as_of": data_as_of,
                })

            except requests.exceptions.ConnectionError:
                st.error(
                    "Cannot connect to the advisor backend. "
                    "Make sure the FastAPI server is running: "
                    "`uvicorn src.advisor.advisor:app --port 8000`"
                )
            except Exception as e:
                st.error(f"Error: {e}")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    "**DataGate** | Built with Python, DuckDB, dbt, FastAPI, Streamlit, and OpenRouter | "
    "[GitHub](https://github.com/raitoncomeback/datagate)"
)