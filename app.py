import streamlit as st
import pandas as pd
import snowflake.connector
from agent_final import app, SNOWFLAKE_CONFIG # Importing the graph and config

# --- 1. PAGE SETUP ---
st.set_page_config(
    page_title="Snowflake AI Data Agent", 
    page_icon="❄️", 
    layout="wide"
)

# --- 2. SIDEBAR: DATA PREVIEW ---
with st.sidebar:
    st.title("📂 Data Warehouse")
    st.info("Querying Table: `ECOM_DB.GOLD.FACT_SALES`")
    
    if st.button("🔍 Preview Raw Data"):
        try:
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            df = pd.read_sql("SELECT * FROM ECOM_DB.GOLD.FACT_SALES LIMIT 10", conn)
            st.dataframe(df, hide_index=True)
            conn.close()
        except Exception as e:
            st.error(f"Connection Error: {e}")

    st.markdown("---")
    st.markdown("### 🛠️ Agent Capabilities")
    st.write("✅ Filtering by City/Category")
    st.write("✅ Date-based Trends")
    st.write("✅ Top-N Rankings")
    st.write("✅ Autonomous Self-Healing")

# --- 3. MAIN CHAT INTERFACE ---
st.title("🤖 Autonomous Sales Agent")
st.caption("Natural Language to Snowflake Insights | Powered by Llama 3.1 70B")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display Chat History
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# User Input
if prompt := st.chat_input("Ask me about revenue, cities, or categories..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # Status container for the LangGraph workflow
        with st.status("🧠 Agent is analyzing schema and generating SQL...", expanded=True) as status:
            
            # RUN THE AGENT
            final_state = app.invoke({
                "query": prompt, 
                "retry_count": 0, 
                "error_log": ""
            })
            
            # Show the logic inside the expander
            st.write("### 🛠️ Generated Logic")
            st.code(final_state['generated_sql'], language="sql")
            
            if final_state.get('retry_count', 0) > 1:
                st.warning(f"⚠️ Agent self-corrected after {final_state['retry_count']-1} error(s).")
            
            status.update(label="✅ Query Executed Successfully", state="complete", expanded=False)

        # 4. DISPLAY FINAL RESULT
        result = final_state.get("db_result")
        
        if result:
            st.balloons()
            # If the result is a single value, show a metric
            if len(result) == 1:
                st.metric(label="Calculated Result", value=f"{result[0]}")
                answer_text = f"The answer is **{result[0]}**."
            else:
                st.write("### 📊 Query Result")
                st.write(result)
                answer_text = f"Retrieved data: {result}"
                
            st.session_state.messages.append({"role": "assistant", "content": answer_text})
        else:
            st.error("The agent could not find a valid answer. Try checking your column names in the sidebar!")