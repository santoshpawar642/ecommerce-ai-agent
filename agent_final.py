import snowflake.connector
import json
from typing import TypedDict
from langgraph.graph import StateGraph, END

# --- 1. CONFIGURATION ---
SNOWFLAKE_CONFIG = {
    "user": "SANTO642",              # Ensure this matches your login exactly
    "password": "YOUR_SNOWFLAKE_PASSWORD", 
    "account": "BIJXYOG-TR26698",    # Try this first
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
    result: str

# --- 3. THE SNOWFLAKE BRAIN (Cortex AI) ---
def call_snowflake_ai(prompt):
    """Uses Snowflake's internal LLM to generate SQL."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        # We use Llama 3.1 70B - it's highly optimized for SQL in Snowflake
        # We use a f-string with triple quotes to handle complex prompts
        escaped_prompt = prompt.replace("'", "''")
        ai_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{escaped_prompt}')"
        cursor.execute(ai_query)
        return cursor.fetchone()[0]
    finally:
        conn.close()

def run_snowflake_query(sql, is_explain=False):
    """Executes or Validates SQL in Snowflake."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        # EXPLAIN is the 'Senior' way to validate syntax without burning credits
        query_to_run = f"EXPLAIN {sql}" if is_explain else sql
        cursor.execute(query_to_run)
        
        if is_explain:
            return True, None
        else:
            results = cursor.fetchall()
            return True, results
    except Exception as e:
        return False, str(e)
    finally:
        if 'conn' in locals():
            conn.close()

# --- 4. GRAPH NODES ---
def sql_generator_node(state: AgentState):
    current_retry = state.get("retry_count", 0)
    print(f"\n[CORTEX AI] Generating SQL (Attempt {current_retry + 1})...")
    
    # Metadata context is crucial so the AI doesn't hallucinate column names
    context = """
    Target Table: ECOM_DB.GOLD.FACT_SALES
    Columns: TRANSACTION_ID, CITY, TOTAL_AMOUNT
    Note: 'Revenue' or 'Sales' refers to TOTAL_AMOUNT.
    Instructions: Provide ONLY the raw SQL code. No markdown, no backticks.
    """
    
    error_msg = f"\nYour previous attempt failed with this error: {state['error_log']}. Please correct the SQL." if state['error_log'] else ""
    prompt = f"{context}{error_msg}\nQuestion: {state['query']}\nSQL:"
    
    raw_sql = call_snowflake_ai(prompt)
    # Basic cleaning in case the AI adds backticks
    clean_sql = raw_sql.replace("```sql", "").replace("```", "").strip()
    return {"generated_sql": clean_sql}

def validator_node(state: AgentState):
    print(f"[VALIDATE] Verifying SQL syntax with Snowflake Engine...")
    success, error = run_snowflake_query(state['generated_sql'], is_explain=True)
    
    if not success:
        print(f"[!] Syntax Error Found: {error}")
        return {"error_log": error, "retry_count": state['retry_count'] + 1}
    
    print("[+] SQL Validated successfully.")
    return {"error_log": ""}

def execution_node(state: AgentState):
    print(f"[EXECUTE] Pulling real data...")
    success, data = run_snowflake_query(state['generated_sql'])
    return {"result": str(data) if success else f"Execution Error: {data}"}

# --- 5. AGENT LOGIC (The Router) ---
def router(state: AgentState):
    if state["error_log"] == "":
        return "execute"
    if state["retry_count"] < 3:
        return "retry"
    return "fail"

# --- 6. BUILD THE GRAPH ---
builder = StateGraph(AgentState)
builder.add_node("generate", sql_generator_node)
builder.add_node("validate", validator_node)
builder.add_node("execute", execution_node)

builder.set_entry_point("generate")
builder.add_edge("generate", "validate")
builder.add_conditional_edges("validate", router, {
    "execute": "execute", 
    "retry": "generate", 
    "fail": END
})
builder.add_edge("execute", END)

app = builder.compile()

# --- 7. RUN THE SYSTEM ---
if __name__ == "__main__":
    print("--- 🤖 AUTONOMOUS DATA ANALYST ONLINE ---")
    user_question = input("\nWhat would you like to know from your sales data? ")
    
    final_state = app.invoke({
        "query": user_question, 
        "retry_count": 0, 
        "error_log": "", 
        "generated_sql": "", 
        "result": ""
    })
    
    print("\n" + "="*50)
    print(f"QUESTION: {user_question}")
    print(f"GENERATED SQL: {final_state['generated_sql']}")
    print(f"DATABASE RESULT: {final_state['result']}")
    print("="*50)