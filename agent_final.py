import snowflake.connector
from typing import TypedDict
from langgraph.graph import StateGraph, END

# --- 1. CONFIGURATION ---
SNOWFLAKE_CONFIG = {
    "user": "SANTO642",
    "password": "Kkomal@9975155925", # <--- UPDATE THIS
    "account": "BIJXYOG-TR26698",
    "warehouse": "COMPUTE_WH",
    "database": "ECOM_DB",
    "schema": "GOLD"
}

# --- 2. STATE DEFINITION ---
class AgentState(TypedDict):
    query: str
    generated_sql: str
    error_log: str
    retry_count: int
    db_result: any

# --- 3. HELPER: THE AI BRAIN (CORTEX) ---
def call_cortex(prompt):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        escaped_prompt = prompt.replace("'", "''")
        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{escaped_prompt}')"
        cursor.execute(sql)
        return cursor.fetchone()[0]
    finally:
        conn.close()

# --- 4. NODES ---
def sql_generator_node(state: AgentState):
    print(f"\n[BRAIN] Generating SQL (Attempt {state.get('retry_count', 0) + 1})...")
    
    schema_info = """
    Table: ECOM_DB.GOLD.FACT_SALES
    Columns: TRANSACTION_ID (STRING), CITY (STRING), TOTAL_AMOUNT (FLOAT), TRANSACTION_DATE (DATE)
    """
    
    examples = """
    User: What are the total sales for Pune?
    SQL: SELECT SUM(TOTAL_AMOUNT) FROM ECOM_DB.GOLD.FACT_SALES WHERE CITY = 'Pune';
    
    User: Highest average revenue city?
    SQL: SELECT CITY, AVG(TOTAL_AMOUNT) as avg_rev FROM ECOM_DB.GOLD.FACT_SALES GROUP BY CITY ORDER BY avg_rev DESC LIMIT 1;
    """

    error_context = f"\nFIX THIS ERROR: {state['error_log']}" if state.get('error_log') else ""

    prompt = f"{schema_info}\n{examples}\n{error_context}\nQuestion: {state['query']}\nSQL (Code only):"
    
    response = call_cortex(prompt)
    clean_sql = response.strip().replace("```sql", "").replace("```", "").strip()
    
    return {"generated_sql": clean_sql, "retry_count": state.get('retry_count', 0) + 1}

def validate_node(state: AgentState):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN {state['generated_sql']}")
        return {"error_log": ""}
    except Exception as e:
        return {"error_log": str(e)}
    finally:
        conn.close()

def execute_node(state: AgentState):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(state['generated_sql'])
        return {"db_result": cursor.fetchone()}
    finally:
        conn.close()

# --- 5. GRAPH CONSTRUCTION ---
workflow = StateGraph(AgentState)
workflow.add_node("generator", sql_generator_node)
workflow.add_node("validator", validate_node)
workflow.add_node("executor", execute_node)

workflow.set_entry_point("generator")
workflow.add_edge("generator", "validator")

def should_continue(state):
    if state['error_log'] != "" and state['retry_count'] < 3: return "retry"
    return "execute" if state['error_log'] == "" else "fail"

workflow.add_conditional_edges("validator", should_continue, {"retry": "generator", "execute": "executor", "fail": END})
workflow.add_edge("executor", END)

# IMPORTANT: This 'app' variable is what Streamlit imports
app = workflow.compile()

if __name__ == "__main__":
    user_query = input("Ask your data: ")
    print(app.invoke({"query": user_query, "retry_count": 0, "error_log": ""}))