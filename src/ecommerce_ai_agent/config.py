"""Configuration loading for the AI analytics agent."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str

    def to_connector_kwargs(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }


def _required_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_snowflake_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        user=_required_env("SNOWFLAKE_USER"),
        password=_required_env("SNOWFLAKE_PASSWORD"),
        account=_required_env("SNOWFLAKE_ACCOUNT"),
        warehouse=_required_env("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=_required_env("SNOWFLAKE_DATABASE"),
        schema=_required_env("SNOWFLAKE_SCHEMA"),
    )

