"""Cortex Search node — retrieves unstructured documents."""

from __future__ import annotations

import logging
import time

from common.cortex_tools import CortexSearchTool
from common.query_rewriter import rewrite_if_needed
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)


def search_node(state: OrchestratorState) -> dict:
    """Query Cortex Search and store results in state."""
    tool = CortexSearchTool()
    original = state["user_message"]
    query = rewrite_if_needed(original, state.get("messages", []))

    trace = list(state.get("thinking_trace", []))
    if query != original:
        trace.append({
            "step": len(trace) + 1,
            "node": "search",
            "summary": "Rewrote follow-up into standalone query",
            "detail": f"Original: {original}\nRewritten: {query}",
            "timestamp": time.time(),
        })

    logger.info("Search node executing for: %s", query[:120])

    try:
        data = tool.query(query=query, limit=10)
        results = data.get("results", [])
    except Exception as exc:
        logger.error("Search node failed: %s", exc)
        results = []

    # Accumulate into tool_outputs for the synthesizer
    tool_outputs = list(state.get("tool_outputs", []))
    tool_outputs.append({
        "tool": "cortex_search",
        "query": query,
        "result_count": len(results),
        "results": results,
    })

    trace.append({
        "step": len(trace) + 1,
        "node": "search",
        "summary": f"Cortex Search returned **{len(results)}** results",
        "detail": f"Query: {query}",
        "timestamp": time.time(),
    })

    return {
        "search_results": results,
        "tool_outputs": tool_outputs,
        "thinking_trace": trace,
    }
