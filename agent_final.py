import os
import snowflake.connector
from typing import TypedDict, Annotated, List
from langgraph.graph import StateGraph, END

# --- 1. CONFIGURATION ---
# Replace these with your actual credentials
SNOWFLAKE_CONFIG = {
    "user": "SANTO642",
    "password": "Kkomal@9975155925", # Use your new Snowflake password here
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
    """Calls Snowflake's built-in Llama 3.1 70B model."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        # Escape single quotes to prevent SQL breakage
        escaped_prompt = prompt.replace("'", "''")
        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{escaped_prompt}')"
        cursor.execute(sql)
        return cursor.fetchone()[0]
    finally:
        conn.close()

# --- 4. NODES ---

def sql_generator_node(state: AgentState):
    """LLM Generates SQL with Schema Context and Few-Shot Examples."""
    print(f"\n[BRAIN] Generating SQL (Attempt {state['retry_count'] + 1})...")

    schema_info = """
    Table: ECOM_DB.GOLD.FACT_SALES
    Columns:
    - TRANSACTION_ID (STRING): Unique ID for each sale.
    - CITY (STRING): City name (e.g., 'Pune', 'Mumbai').
    - TOTAL_AMOUNT (FLOAT): Revenue value of the sale.
    - TRANSACTION_DATE (DATE): Date of purchase.
    """

    examples = """
    User: What are the total sales for Pune?
    SQL: SELECT SUM(TOTAL_AMOUNT) FROM ECOM_DB.GOLD.FACT_SALES WHERE CITY = 'Pune';

    User: Which city had the highest revenue?
    SQL: SELECT CITY, SUM(TOTAL_AMOUNT) as rev FROM ECOM_DB.GOLD.FACT_SALES GROUP BY CITY ORDER BY rev DESC LIMIT 1;
    """

    error_context = ""
    if state['error_log']:
        error_context = f"\nFIX PREVIOUS ERROR: {state['error_log']}"

    prompt = f"""
    You are a Snowflake SQL Expert. Use this schema:
    {schema_info}
    
    Examples:
    {examples}
    
    {error_context}
    
    User Question: {state['query']}
    SQL (Code only, no markdown):
    """

    response = call_cortex(prompt)
    clean_sql = response.strip().replace("```sql", "").replace("```", "").strip()
    
    return {"generated_sql": clean_sql, "retry_count": state['retry_count'] + 1}

def validate_node(state: AgentState):
    """Uses Snowflake EXPLAIN to check if the SQL is valid."""
    print(f"[VALIDATE] Checking SQL syntax...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN {state['generated_sql']}")
        print("[+] SQL Validated Successfully.")
        return {"error_log": ""}
    except Exception as e:
        print(f"[-] SQL Failed: {str(e)}")
        return {"error_log": str(e)}
    finally:
        conn.close()

def execute_node(state: AgentState):
    """Pulls real data from Snowflake."""
    print("[EXECUTE] Fetching data from Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(state['generated_sql'])
        result = cursor.fetchone()
        return {"db_result": result}
    finally:
        conn.close()

# --- 5. ROUTING LOGIC ---
def should_continue(state: AgentState):
    if state['error_log'] != "" and state['retry_count'] < 3:
        return "retry"
    elif state['error_log'] != "" and state['retry_count'] >= 3:
        return "fail"
    else:
        return "execute"

# --- 6. GRAPH CONSTRUCTION ---
workflow = StateGraph(AgentState)

workflow.add_node("generator", sql_generator_node)
workflow.add_node("validator", validate_node)
workflow.add_node("executor", execute_node)

workflow.set_entry_point("generator")
workflow.add_edge("generator", "validator")

workflow.add_conditional_edges(
    "validator",
    should_continue,
    {
        "retry": "generator",
        "execute": "executor",
        "fail": END
    }
)

workflow.add_edge("executor", END)
app = workflow.compile()

# --- 7. RUNTIME ---
if __name__ == "__main__":
    print("--- 🤖 AUTONOMOUS DATA AGENT ONLINE ---")
    user_input = input("What would you like to know from your sales data? ")
    
    final_state = app.invoke({
        "query": user_input,
        "retry_count": 0,
        "error_log": ""
    })

    if "db_result" in final_state:
        print("\n" + "="*50)
        print(f"QUESTION: {user_input}")
        print(f"GENERATED SQL: {final_state['generated_sql']}")
        print(f"DATABASE RESULT: {final_state['db_result']}")
        print("="*50)
    else:
        print("\n[!] Agent failed to generate valid SQL after 3 attempts.")