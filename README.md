# Ecommerce AI Agent

Natural-language analytics agent for querying the ecommerce Gold layer in Snowflake.

## What it does

- converts English questions into Snowflake SQL with Cortex
- executes read-only queries against curated analytics tables
- explains recent monitoring anomalies for debugging-style questions
- exposes both a Python entrypoint and a Streamlit chat UI

## Project structure

```text
src/ecommerce_ai_agent/
  config.py
  database.py
  sql_safety.py
  agent.py
  ui.py

agent_final.py   # backward-compatible Python entrypoint
app.py           # Streamlit entrypoint
```

## Local setup

1. Use Python 3.10 or 3.11.
2. Create and activate a virtual environment.
3. Install dependencies.
4. Copy `.env.example` to `.env` and fill in your Snowflake values.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Required environment variables

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SPARK_LAKEHOUSE_DB
SNOWFLAKE_SCHEMA=GOLD_LAYER
```

## Run the agent

CLI-style invocation:

```bash
python agent_final.py
```

Streamlit UI:

```bash
streamlit run app.py
```

## Safety notes

- generated SQL is restricted to read-only `SELECT` statements
- only approved analytics tables are allowed
- credentials are loaded from environment variables instead of source code

## Demo

![Agent Execution Result](result_screenshot.png)

![Highest Average Revenue Test](agent_logic_test.png)
