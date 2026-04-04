"""Core NL-to-SQL agent logic."""

from __future__ import annotations

from typing import Any

from typing_extensions import TypedDict

from ecommerce_ai_agent.config import SnowflakeConfig
from ecommerce_ai_agent.database import fetch_all, fetch_one
from ecommerce_ai_agent.sql_safety import validate_generated_sql


SCHEMA_PROMPT = """
You are a Snowflake SQL expert.

STRICT RULES:
- Use ONLY provided columns
- NEVER hallucinate columns
- Return ONE SELECT statement only

TABLES:

FACT_SALES f(
    order_id,
    order_date,
    customer_sk,
    product_sk,
    category,
    city,
    total_amount,
    order_status,
    year,
    month
)

DIM_CUSTOMER_360 c(
    customer_sk,
    customer_id,
    total_spend,
    avg_order_value,
    last_order_date,
    total_orders,
    days_since_last_order,
    loyalty_tier
)

DIM_PRODUCT_CATALOG p(
    product_sk,
    product_id,
    product_name,
    category,
    price,
    profit_margin
)

KPI_METRICS k(
    aov,
    total_revenue,
    total_customers,
    total_orders,
    avg_ltv,
    year,
    month
)

PIPELINE_ANOMALIES a(
    run_id,
    task_name,
    layer,
    anomaly_type,
    metric_name,
    metric_value,
    expected_value,
    deviation_percent,
    anomaly_flag,
    run_date,
    created_at
)

RELATIONSHIPS:
f.customer_sk = c.customer_sk
f.product_sk = p.product_sk

IMPORTANT:
- Revenue = SUM(f.total_amount)
- Average revenue = AVG(f.total_amount)
- Use fully-qualified names only when necessary
"""


class AgentState(TypedDict):
    query: str
    generated_sql: str
    db_result: list[Any]
    error_log: str
    context: str


class AgentApp:
    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def retrieve_context(self, query: str) -> str:
        q = query.lower()
        is_debug = any(word in q for word in ["why", "issue", "anomaly", "problem", "fail"])
        if not is_debug:
            return ""

        filter_clause = ""
        if "sales" in q or "revenue" in q:
            filter_clause = "AND task_name ILIKE '%SALES%'"
        elif "customer" in q:
            filter_clause = "AND task_name ILIKE '%CUSTOMER%'"
        elif "product" in q:
            filter_clause = "AND task_name ILIKE '%PRODUCT%'"

        sql = f"""
            SELECT task_name, metric_name, deviation_percent
            FROM PIPELINE_ANOMALIES
            WHERE anomaly_flag = TRUE
            {filter_clause}
            ORDER BY created_at DESC
            LIMIT 5
        """

        try:
            rows = fetch_all(self.config, sql)
        except Exception:
            return ""

        if not rows:
            return ""

        lines = [
            f"- {task_name} has {metric_name} anomaly ({round(deviation, 2)}% deviation)"
            for task_name, metric_name, deviation in rows
        ]
        return "Relevant Pipeline Anomalies:\n" + "\n".join(lines)

    def generate_sql(self, query: str) -> str:
        prompt = f"""
{SCHEMA_PROMPT}

Convert this question into SQL:
{query}

Return ONLY SQL.
"""

        cortex_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
            $$ {prompt} $$
        )
        """

        result = fetch_one(self.config, cortex_sql)
        sql = (result[0] if result else "").replace("```sql", "").replace("```", "").strip()
        return validate_generated_sql(sql)

    def execute_sql(self, sql: str):
        return fetch_all(self.config, sql)

    def invoke(self, inputs: dict[str, str]) -> AgentState:
        original_query = inputs["query"]
        q = original_query.lower()
        is_explain = any(word in q for word in ["why", "issue", "anomaly", "problem"])
        is_data = any(word in q for word in ["show", "total", "top", "count", "calculate", "highest", "lowest", "average"])

        state: AgentState = {
            "query": original_query,
            "generated_sql": "",
            "db_result": [],
            "error_log": "",
            "context": "",
        }

        sql_query = original_query
        if is_explain:
            for word in ["why", "explain", "anomaly", "issue", "problem"]:
                sql_query = sql_query.replace(word, "")
        sql_query = sql_query.replace(",", "").strip()
        if len(sql_query) < 5:
            sql_query = "show total revenue"

        if is_explain and is_data:
            state["generated_sql"] = self.generate_sql(sql_query)
            state["db_result"] = self.execute_sql(state["generated_sql"])
            state["context"] = self.retrieve_context(original_query)
            return state

        if is_explain:
            state["context"] = self.retrieve_context(original_query)
            return state

        state["generated_sql"] = self.generate_sql(sql_query)
        state["db_result"] = self.execute_sql(state["generated_sql"])
        return state

