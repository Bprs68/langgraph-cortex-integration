"""Cortex Analyst node — text-to-SQL via Cortex Analyst."""

from __future__ import annotations

import logging
import time

from common.config import SEMANTIC_MODEL
from common.cortex_tools import CortexAnalystTool
from common.query_rewriter import rewrite_if_needed
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)


def _extract_analyst_parts(response: dict) -> dict:
    """Pull text, SQL queries, and suggestions from a Cortex Analyst response.

    Response format: {"message": {"role": "analyst", "content": [...]}}
    Content types: "text", "sql", "suggestions"
    """
    text_parts: list[str] = []
    sql_queries: list[str] = []
    suggestions: list[str] = []

    content = response.get("message", {}).get("content", [])
    for item in content:
        itype = item.get("type")
        if itype == "text":
            text_parts.append(item.get("text", ""))
        elif itype == "sql":
            stmt = item.get("statement", "")
            if stmt:
                sql_queries.append(stmt)
        elif itype == "suggestions":
            suggestions.extend(item.get("suggestions", []))

    return {
        "text": "\n".join(text_parts),
        "sql_queries": sql_queries,
        "suggestions": suggestions,
    }


def analyst_node(state: OrchestratorState) -> dict:
    """Run text-to-SQL via Cortex Analyst and store results."""
    tool = CortexAnalystTool()
    original = state["user_message"]
    question = rewrite_if_needed(original, state.get("messages", []))

    trace = list(state.get("thinking_trace", []))
    if question != original:
        trace.append({
            "step": len(trace) + 1,
            "node": "analyst",
            "summary": "Rewrote follow-up into standalone query",
            "detail": f"Original: {original}\nRewritten: {question}",
            "timestamp": time.time(),
        })

    logger.info("Analyst node executing for: %s", question[:120])

    try:
        raw = tool.query(question=question, semantic_model=SEMANTIC_MODEL)
        parsed = _extract_analyst_parts(raw)
    except Exception as exc:
        logger.error("Analyst node failed: %s", exc)
        parsed = {"text": f"Analyst error: {exc}", "sql_queries": [], "tables": []}

    # Accumulate into tool_outputs for the synthesizer
    tool_outputs = list(state.get("tool_outputs", []))
    tool_outputs.append({
        "tool": "cortex_analyst",
        "query": question,
        "text": parsed["text"],
        "sql_queries": parsed["sql_queries"],
        "suggestions": parsed["suggestions"],
    })

    sql_summary = f"Generated {len(parsed['sql_queries'])} SQL query(s)" if parsed["sql_queries"] else "No SQL generated"
    trace.append({
        "step": len(trace) + 1,
        "node": "analyst",
        "summary": f"Cortex Analyst: {sql_summary}",
        "detail": parsed["text"] + ("\n\n" + parsed["sql_queries"][0] if parsed["sql_queries"] else ""),
        "timestamp": time.time(),
    })

    return {
        "analyst_results": parsed,
        "tool_outputs": tool_outputs,
        "thinking_trace": trace,
    }
