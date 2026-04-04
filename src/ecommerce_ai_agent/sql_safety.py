"""Guardrails for executing model-generated SQL."""

from __future__ import annotations

import re


ALLOWED_TABLES = {
    "FACT_SALES",
    "DIM_CUSTOMER_360",
    "DIM_PRODUCT_CATALOG",
    "KPI_METRICS",
    "PIPELINE_ANOMALIES",
}

FORBIDDEN_SQL_PATTERNS = [
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bCREATE\b",
    r"\bMERGE\b",
    r"\bTRUNCATE\b",
    r"\bCOPY\b",
]


def extract_tables(sql: str) -> list[str]:
    matches = re.findall(r"(?:FROM|JOIN)\s+([A-Z0-9_\.]+)", sql.upper())
    return [match.split(".")[-1] for match in matches]


def validate_generated_sql(sql: str) -> str:
    normalized = sql.strip()

    if ";" in normalized:
        raise ValueError("Semicolons are not allowed in generated SQL")

    if not normalized.lower().startswith("select"):
        raise ValueError("Only SELECT statements are allowed")

    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            raise ValueError(f"Generated SQL contains forbidden operation: {pattern}")

    tables = extract_tables(normalized)
    if not tables:
        raise ValueError("Generated SQL must reference at least one approved table")

    unapproved = sorted(set(tables) - ALLOWED_TABLES)
    if unapproved:
        raise ValueError(f"Generated SQL references unapproved tables: {unapproved}")

    return normalized

