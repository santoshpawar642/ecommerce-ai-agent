"""Snowflake helpers used by the AI analytics agent."""

from __future__ import annotations

from contextlib import closing

import snowflake.connector

from ecommerce_ai_agent.config import SnowflakeConfig


def fetch_all(config: SnowflakeConfig, sql: str):
    with closing(snowflake.connector.connect(**config.to_connector_kwargs())) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


def fetch_one(config: SnowflakeConfig, sql: str):
    with closing(snowflake.connector.connect(**config.to_connector_kwargs())) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(sql)
            return cursor.fetchone()

