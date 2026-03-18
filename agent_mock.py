import snowflake.connector
from typing import TypedDict
from langgraph.graph import StateGraph, END

# --- 1. Configuration (Updated with your Screenshot details) ---
SNOWFLAKE_CONFIG = {
    "user": "SANTO642",             # Your Login name from screenshot
    "password": "Kkomal@9975155925", 
    "account": "BIJXYOG-TR26698",   # Your Account identifier
    "warehouse": "COMPUTE_WH",
    "database": "ECOM_DB",
    "schema": "GOLD"
}

# --- 2. State Definition ---
class AgentState(TypedDict):
    query: str
    generated_sql: str
    error_log: str
    retry_count: int
    result: str

# --- 3. Snowflake Utility ---
def run_snowflake_query(sql, is_explain=False):
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()
        
        # We use EXPLAIN to validate syntax without spending money/credits
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

# --- 4. Nodes ---
def sql_generator_node(state: AgentState):
    current_retry = state.get("retry_count", 0)
    print(f"\n[GENERATE] Attempt {current_retry + 1}")
    
    # MOCK LLM LOGIC: 
    # Attempt 1: Intentionally bad SQL (missing single quotes around 'Pune')
    # Attempt 2: Correct SQL
    if current_retry == 0:
        sql = "SELECT * FROM ECOM_DB.GOLD.FACT_SALES WHERE CITY = Pune"
    else:
        sql = "SELECT * FROM ECOM_DB.GOLD.FACT_SALES WHERE CITY = 'Pune'"
        
    return {"generated_sql": sql}

def validator_node(state: AgentState):
    sql = state['generated_sql']
    print(f"[VALIDATE] Testing Syntax in Snowflake for: {sql}")
    
    # This calls REAL Snowflake to check if the SQL is valid
    success, error = run_snowflake_query(sql, is_explain=True)
    
    if not success:
        print(f"[!] Snowflake found an error: {error}")
        new_count = state.get("retry_count", 0) + 1
        return {"error_log": error, "retry_count": new_count}
    
    print("[+] Syntax Validated by Snowflake!")
    return {"error_log": ""}

def execution_node(state: AgentState):
    sql = state['generated_sql']
    print(f"[EXECUTE] Fetching data from Snowflake...")
    
    success, data = run_snowflake_query(sql)
    if success:
        return {"result": f"Fetched {len(data)} rows successfully from your GOLD table."}
    return {"result": f"Execution failed: {data}"}

# --- 5. Graph Logic ---
def router(state: AgentState):
    if state["error_log"] == "":
        return "execute"
    if state["retry_count"] < 3:
        return "retry"
    return "fail"

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

# --- 6. Main Execution ---
if __name__ == "__main__":
    print("--- STARTING AGENTIC LOOP ---")
    inputs = {"query": "Find sales for Pune", "retry_count": 0}
    final_state = app.invoke(inputs)
    
    print("\n--- FINAL SYSTEM REPORT ---")
    print(f"Final SQL: {final_state['generated_sql']}")
    print(f"Outcome: {final_state['result']}")