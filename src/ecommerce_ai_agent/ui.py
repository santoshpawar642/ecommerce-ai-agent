"""Streamlit UI for the AI analytics agent."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ecommerce_ai_agent.agent import AgentApp
from ecommerce_ai_agent.config import load_snowflake_config
from ecommerce_ai_agent.database import fetch_all
from ecommerce_ai_agent.sql_safety import extract_tables


def run_streamlit_app():
    config = load_snowflake_config()
    agent = AgentApp(config)

    st.set_page_config(
        page_title="Autonomous Sales Agent",
        page_icon="🤖",
        layout="wide",
    )

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "last_sql" not in st.session_state:
        st.session_state.last_sql = ""

    with st.sidebar:
        st.title("Data Warehouse")

        if st.session_state.last_sql:
            tables = extract_tables(st.session_state.last_sql)
            st.success(f"Tables Used: {', '.join(tables)}")
        else:
            st.info("Tables Used: Waiting for query...")

        if st.button("Preview Fact Sales"):
            try:
                preview = fetch_all(config, "SELECT * FROM FACT_SALES LIMIT 10")
                df = pd.DataFrame(preview)
                st.dataframe(df, hide_index=True)
            except Exception as exc:
                st.error(f"Connection Error: {exc}")

        st.markdown("---")
        st.markdown("### Agent Capabilities")
        st.write("Filtering by category and city")
        st.write("Trend and aggregation analysis")
        st.write("Top-N and ranking questions")
        st.write("Monitoring anomaly explanations")

    st.title("Autonomous Sales Agent")
    st.caption("Natural Language to SQL to Insights | Powered by Snowflake Cortex")

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about revenue, trends, anomalies..."):
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.status("Understanding question and generating SQL...", expanded=True):
                try:
                    final_state = agent.invoke({"query": prompt})
                    if final_state.get("generated_sql"):
                        st.session_state.last_sql = final_state["generated_sql"]
                        st.write("### Generated SQL")
                        st.code(final_state["generated_sql"], language="sql")
                    st.success("Query executed successfully")
                except Exception as exc:
                    st.error(f"Error: {exc}")
                    st.stop()

            result = final_state.get("db_result")
            context = final_state.get("context")

            if result:
                if len(result) == 1 and len(result[0]) == 1:
                    value = result[0][0]
                    st.metric("Result", value)
                    answer_text = f"Result: **{value}**"
                else:
                    st.write("### Query Result")
                    st.write(result)
                    answer_text = f"Retrieved {len(result)} rows."
            elif context:
                st.success("Explanation")
                st.write(context)
                answer_text = context
            else:
                st.error("No result found. Try refining your query.")
                answer_text = "No result found."

            if context and result:
                st.info(context)

            st.session_state.messages.append({"role": "assistant", "content": answer_text})

