"""Shared Cortex tool clients used by both agent_mode and orchestrator_mode."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import httpx

from common.config import (
    CORTEX_LLM_MODEL,
    CORTEX_SEARCH_COLUMNS,
    CORTEX_SEARCH_URL,
    SNOWFLAKE_ACCOUNT_URL,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PAT,
    SNOWFLAKE_WAREHOUSE,
    SQL_API_URL,
)

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────

def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {SNOWFLAKE_PAT.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    }


# ── Cortex Search Tool ────────────────────────────────────────────

class CortexSearchTool:
    """Query a Snowflake Cortex Search Service via REST API."""

    def __init__(self, timeout: float = 60.0) -> None:
        self._timeout = timeout

    def query(
        self,
        query: str,
        columns: list[str] | None = None,
        filter: dict | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Run a search query and return the JSON response.

        Returns dict with 'results' key containing list of matching rows.
        """
        columns = columns or CORTEX_SEARCH_COLUMNS
        payload: dict[str, Any] = {
            "query": query,
            "columns": columns,
            "limit": limit,
        }
        if filter:
            payload["filter"] = filter

        logger.info("Cortex Search query: %s (limit=%d)", query, limit)
        logger.debug("Cortex Search payload: %s", json.dumps(payload))

        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(
                CORTEX_SEARCH_URL,
                headers=_auth_headers(),
                json=payload,
            )
            if resp.status_code != 200:
                logger.error(
                    "Cortex Search failed (%d): %s", resp.status_code, resp.text
                )
                raise RuntimeError(
                    f"Cortex Search failed ({resp.status_code}): {resp.text}"
                )

        data = resp.json()
        results = data.get("results", [])
        logger.info("Cortex Search returned %d results", len(results))
        return data


# ── Cortex Analyst Tool ────────────────────────────────────────────

class CortexAnalystTool:
    """Run text-to-SQL via the Cortex Analyst REST API.

    Uses the ``POST /api/v2/cortex/analyst/message`` endpoint.
    """

    def __init__(self, timeout: float = 120.0) -> None:
        self._url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/analyst/message"
        self._timeout = timeout

    def query(self, question: str, semantic_model: str = "") -> dict[str, Any]:
        """Send a natural-language question and return the Analyst response.

        The response typically contains SQL + result set.
        """
        payload: dict[str, Any] = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
            "stream": False,
        }
        # Attach semantic model reference if provided
        if semantic_model:
            payload["semantic_model_file"] = semantic_model

        logger.info("Cortex Analyst query: %s", question)

        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(
                self._url,
                headers=_auth_headers(),
                json=payload,
            )
            if resp.status_code != 200:
                logger.error(
                    "Cortex Analyst failed (%d): %s", resp.status_code, resp.text
                )
                raise RuntimeError(
                    f"Cortex Analyst failed ({resp.status_code}): {resp.text}"
                )

        data = resp.json()
        content = data.get("message", {}).get("content", [])
        logger.info("Cortex Analyst responded (content items: %d)", len(content))
        return data


# ── SQL Executor Tool ──────────────────────────────────────────────

class CortexSQLExecutor:
    """Execute SQL queries via the Snowflake SQL Statements API."""

    def __init__(self, timeout: float = 60.0) -> None:
        self._timeout = timeout

    def execute(self, sql: str) -> dict[str, Any]:
        """Execute a SQL statement and return column names + rows.

        Returns {"columns": [...], "rows": [[...], ...], "row_count": int}.
        """
        payload: dict[str, Any] = {
            "statement": sql,
            "timeout": int(self._timeout),
            "database": SNOWFLAKE_DATABASE or "SNOWFLAKE_INTELLIGENCE",
            "schema": "PUBLIC",
        }
        if SNOWFLAKE_WAREHOUSE:
            payload["warehouse"] = SNOWFLAKE_WAREHOUSE

        logger.info("SQL execute: %s", sql[:120])

        with httpx.Client(timeout=self._timeout + 30) as client:
            resp = client.post(SQL_API_URL, headers=_auth_headers(), json=payload)

            if resp.status_code == 202:
                body = resp.json()
                handle = body.get("statementHandle", "")
                if not handle:
                    raise RuntimeError("SQL API returned 202 but no statementHandle")
                data = self._poll_result(client, handle)
            elif resp.status_code == 200:
                data = resp.json()
            else:
                raise RuntimeError(
                    f"SQL execute failed ({resp.status_code}): {resp.text}"
                )

        meta = data.get("resultSetMetaData", {})
        columns = [col["name"] for col in meta.get("rowType", [])]
        rows = data.get("data", [])

        logger.info("SQL returned %d rows, %d columns", len(rows), len(columns))
        return {"columns": columns, "rows": rows, "row_count": len(rows)}

    def _poll_result(
        self, client: httpx.Client, handle: str, max_wait: float = 120.0
    ) -> dict[str, Any]:
        poll_url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/statements/{handle}"
        deadline = time.time() + max_wait
        while time.time() < deadline:
            resp = client.get(poll_url, headers=_auth_headers())
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code != 202:
                raise RuntimeError(
                    f"SQL poll failed ({resp.status_code}): {resp.text}"
                )
            time.sleep(1)
        raise RuntimeError("SQL statement timed out")


# ── Cortex LLM Tool ───────────────────────────────────────────────

class CortexLLMTool:
    """Call Snowflake Cortex COMPLETE via the SQL Statements API.

    Uses ``SNOWFLAKE.CORTEX.COMPLETE(model, messages, options)`` executed
    through the ``/api/v2/statements`` REST endpoint, which avoids the
    Chat Completions endpoint that some accounts cannot access.
    """

    def __init__(self, model: str | None = None, timeout: float = 120.0) -> None:
        self._model = model or CORTEX_LLM_MODEL
        self._timeout = timeout

    # ── internal helpers ───────────────────────────────────────────

    def _execute_complete(
        self,
        messages: list[dict[str, Any]],
        options: dict[str, Any],
    ) -> str:
        """Run SNOWFLAKE.CORTEX.COMPLETE() and return the assistant text."""
        statement = (
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, PARSE_JSON(?), PARSE_JSON(?)) AS response"
        )
        payload: dict[str, Any] = {
            "statement": statement,
            "timeout": int(self._timeout),
            "database": SNOWFLAKE_DATABASE or "SNOWFLAKE_INTELLIGENCE",
            "schema": "PUBLIC",
            "bindings": {
                "1": {"type": "TEXT", "value": self._model},
                "2": {"type": "TEXT", "value": json.dumps(messages)},
                "3": {"type": "TEXT", "value": json.dumps(options)},
            },
        }
        if SNOWFLAKE_WAREHOUSE:
            payload["warehouse"] = SNOWFLAKE_WAREHOUSE

        logger.info(
            "Cortex COMPLETE request (model=%s, msgs=%d)", self._model, len(messages)
        )

        with httpx.Client(timeout=self._timeout + 30) as client:
            resp = client.post(SQL_API_URL, headers=_auth_headers(), json=payload)

            if resp.status_code == 202:
                # Async execution — poll for result
                body = resp.json()
                handle = body.get("statementHandle", "")
                if not handle:
                    raise RuntimeError("SQL API returned 202 but no statementHandle")
                data = self._poll_result(client, handle)
            elif resp.status_code == 200:
                data = resp.json()
            else:
                raise RuntimeError(
                    f"SQL API failed ({resp.status_code}): {resp.text}"
                )

        rows = data.get("data", [])
        if not rows or not rows[0]:
            raise RuntimeError("SNOWFLAKE.CORTEX.COMPLETE returned empty result")

        # The cell is a JSON string of the COMPLETE response object
        complete_resp = json.loads(rows[0][0])
        content = complete_resp.get("choices", [{}])[0].get("messages", "")

        logger.info("Cortex COMPLETE responded (%d chars)", len(content))
        logger.debug("Cortex COMPLETE response: %s", content[:500])
        return content

    def _poll_result(
        self, client: httpx.Client, handle: str, max_wait: float = 120.0
    ) -> dict[str, Any]:
        """Poll ``GET /api/v2/statements/{handle}`` until the query completes."""
        poll_url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/statements/{handle}"
        deadline = time.time() + max_wait
        while time.time() < deadline:
            resp = client.get(poll_url, headers=_auth_headers())
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code != 202:
                raise RuntimeError(
                    f"SQL poll failed ({resp.status_code}): {resp.text}"
                )
            time.sleep(1)
        raise RuntimeError("SQL statement timed out waiting for COMPLETE result")

    # ── public API (same signatures as before) ─────────────────────

    def chat(
        self,
        messages: list[dict[str, Any]],
        temperature: float = 0.1,
        max_tokens: int = 4096,
        **_kwargs: Any,
    ) -> str:
        """Send messages and return the assistant's text reply."""
        options = {"temperature": temperature, "max_tokens": max_tokens}
        return self._execute_complete(messages, options)

    def chat_json(
        self,
        messages: list[dict[str, Any]],
        schema: dict[str, Any],
        schema_name: str = "response",
        temperature: float = 0.0,
    ) -> dict[str, Any]:
        """Chat and parse the response as JSON conforming to *schema*.

        The schema is injected into the system prompt so the LLM produces
        valid JSON output.  A fallback extractor handles cases where the
        model wraps the JSON in markdown code fences or adds preamble text.
        """
        schema_instruction = (
            "\n\n--- OUTPUT FORMAT RULES ---\n"
            "You MUST respond with ONLY a raw JSON object.\n"
            "Do NOT include any explanation, markdown, code fences, or extra text.\n"
            "Do NOT wrap the JSON in ```json ... ``` or any other formatting.\n"
            "The ENTIRE response must be parseable by json.loads().\n"
            f"Required JSON schema:\n{json.dumps(schema, indent=2)}\n"
            "--- END RULES ---"
        )

        enriched: list[dict[str, Any]] = []
        schema_added = False
        for msg in messages:
            if msg["role"] == "system" and not schema_added:
                enriched.append({
                    "role": "system",
                    "content": msg["content"] + schema_instruction,
                })
                schema_added = True
            else:
                enriched.append(msg)
        if not schema_added:
            enriched.insert(0, {
                "role": "system",
                "content": "You are a helpful assistant." + schema_instruction,
            })

        # Add a final user nudge to reinforce JSON-only output
        enriched.append({
            "role": "user",
            "content": "Remember: respond with ONLY the raw JSON object, nothing else.",
        })

        options: dict[str, Any] = {
            "temperature": temperature,
            "max_tokens": 4096,
        }
        raw = self._execute_complete(enriched, options)
        return self._extract_json(raw)

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any]:
        """Best-effort JSON extraction from LLM output."""
        import re

        text = text.strip()

        # 1. Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2. Strip markdown code fences
        m = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1).strip())
            except json.JSONDecodeError:
                pass

        # 3. Find first { ... last }
        first = text.find("{")
        last = text.rfind("}")
        if first != -1 and last > first:
            try:
                return json.loads(text[first : last + 1])
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not extract JSON from LLM response: {text[:300]}")
