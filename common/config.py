"""Centralized configuration — loads all env vars and builds derived URLs."""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"ERROR: Missing required env var: {name}", file=sys.stderr)
        sys.exit(1)
    return val


# ── Core Snowflake credentials (required) ──────────────────────────
SNOWFLAKE_ACCOUNT_URL: str = _require("SNOWFLAKE_ACCOUNT_URL").rstrip("/")
SNOWFLAKE_PAT: str = _require("SNOWFLAKE_PAT")

# ── Agent‑mode settings ────────────────────────────────────────────
SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "").strip()
SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "").strip()
SNOWFLAKE_AGENT_NAME: str = os.getenv("SNOWFLAKE_AGENT_NAME", "").strip()

# ── Orchestrator‑mode settings ─────────────────────────────────────
CORTEX_SEARCH_SERVICE: str = os.getenv("CORTEX_SEARCH_SERVICE", "").strip()
CORTEX_SEARCH_DB: str = os.getenv("CORTEX_SEARCH_DB", "").strip()
CORTEX_SEARCH_SCHEMA: str = os.getenv("CORTEX_SEARCH_SCHEMA", "").strip()
CORTEX_SEARCH_COLUMNS: list[str] = [
    c.strip()
    for c in os.getenv("CORTEX_SEARCH_COLUMNS", "").split(",")
    if c.strip()
]
CORTEX_LLM_MODEL: str = os.getenv("CORTEX_LLM_MODEL", "claude-sonnet-4-5").strip()
SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "").strip()
SEMANTIC_MODEL: str = os.getenv("SEMANTIC_MODEL", "").strip()

# ── Derived URLs ───────────────────────────────────────────────────
# Agent mode
AGENT_RUN_URL: str = (
    f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/databases/{SNOWFLAKE_DATABASE}"
    f"/schemas/{SNOWFLAKE_SCHEMA}/agents/{SNOWFLAKE_AGENT_NAME}:run"
)
THREAD_URL: str = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/threads"

# Cortex Search REST endpoint
CORTEX_SEARCH_URL: str = (
    f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/databases/{CORTEX_SEARCH_DB}"
    f"/schemas/{CORTEX_SEARCH_SCHEMA}"
    f"/cortex-search-services/{CORTEX_SEARCH_SERVICE}:query"
)

# SQL Statements API (used for Cortex COMPLETE)
SQL_API_URL: str = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/statements"
