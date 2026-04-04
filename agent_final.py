"""Backward-compatible entrypoint for the refactored AI agent."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ecommerce_ai_agent.agent import AgentApp
from ecommerce_ai_agent.config import load_snowflake_config


SNOWFLAKE_CONFIG = load_snowflake_config().to_connector_kwargs()
app = AgentApp(load_snowflake_config())


def main():
    query = input("What would you like to know from your sales data? ").strip()
    if not query:
        print("No query provided.")
        return

    final_state = app.invoke({"query": query})
    print("\nQUESTION:", query)
    if final_state.get("generated_sql"):
        print("GENERATED SQL:", final_state["generated_sql"])
    if final_state.get("context"):
        print("CONTEXT:")
        print(final_state["context"])
    print("DATABASE RESULT:", final_state.get("db_result"))


if __name__ == "__main__":
    main()


