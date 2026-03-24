"""Build the LangGraph StateGraph for orchestrator mode.

Graph flow:
    START → router → (conditional) → search / analyst / both / llm / human_review
    search → synthesizer
    analyst → synthesizer
    llm → END
    human_review → END
    synthesizer → (conditional) → END or router (multi-step, max 3)
"""

from __future__ import annotations

import logging
from typing import Literal

from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from orchestrator_mode.state import OrchestratorState
from orchestrator_mode.nodes.router import router_node
from orchestrator_mode.nodes.search import search_node
from orchestrator_mode.nodes.analyst import analyst_node
from orchestrator_mode.nodes.sql_executor import sql_executor_node
from orchestrator_mode.nodes.llm import llm_node
from orchestrator_mode.nodes.synthesizer import synthesizer_node
from orchestrator_mode.nodes.human_review import human_review_node

logger = logging.getLogger(__name__)


# ── Conditional edge functions ─────────────────────────────────────

def _route_intent(state: OrchestratorState) -> str:
    """Decide which node to call based on router's intent classification."""
    intent = state.get("intent", "general")
    logger.info("Routing to: %s", intent)
    if intent in ("search", "sql", "both", "general", "clarify"):
        return intent
    return "general"


def _after_synthesizer(state: OrchestratorState) -> Literal["complete", "needs_more"]:
    """Decide whether to loop back for more info or finish."""
    intent = state.get("intent", "complete")
    iteration = state.get("iteration", 0)
    if intent == "needs_more" and iteration < 3:
        logger.info("Synthesizer requests more info (iteration %d)", iteration)
        return "needs_more"
    return "complete"


# ── Graph construction ─────────────────────────────────────────────

def build_orchestrator_graph():
    """Construct and compile the orchestrator‑mode LangGraph."""
    logger.info("Building orchestrator-mode graph")
    graph = StateGraph(OrchestratorState)

    # Register nodes
    graph.add_node("router", router_node)
    graph.add_node("search", search_node)
    graph.add_node("analyst", analyst_node)
    graph.add_node("sql_executor", sql_executor_node)
    graph.add_node("llm", llm_node)
    graph.add_node("human_review", human_review_node)
    graph.add_node("synthesizer", synthesizer_node)

    # For "both" intent — run search then analyst sequentially
    # (LangGraph Send API requires more complex setup; sequential is simpler
    #  and avoids race conditions on tool_outputs accumulation)
    graph.add_node("search_then_analyst", _search_then_analyst)

    # START → router
    graph.add_edge(START, "router")

    # Router → conditional edges
    graph.add_conditional_edges(
        "router",
        _route_intent,
        {
            "search": "search",
            "sql": "analyst",
            "both": "search_then_analyst",
            "general": "llm",
            "clarify": "human_review",
        },
    )

    # Tool nodes → synthesizer
    graph.add_edge("search", "synthesizer")
    graph.add_edge("analyst", "sql_executor")
    graph.add_edge("sql_executor", "synthesizer")
    graph.add_edge("search_then_analyst", "synthesizer")

    # LLM and human_review → END (no synthesis needed)
    graph.add_edge("llm", END)
    graph.add_edge("human_review", END)

    # Synthesizer → conditional (loop or finish)
    graph.add_conditional_edges(
        "synthesizer",
        _after_synthesizer,
        {
            "complete": END,
            "needs_more": "router",
        },
    )

    checkpointer = MemorySaver()
    return graph.compile(checkpointer=checkpointer)


def _search_then_analyst(state: OrchestratorState) -> dict:
    """Run search, analyst, and SQL executor sequentially for 'both' intent."""
    logger.info("Running search + analyst + sql_executor (both intent)")
    s1 = search_node(state)
    # Pass accumulated tool_outputs and trace to analyst
    merged_state = {**state, **s1}
    s2 = analyst_node(merged_state)
    # Execute SQL from analyst results
    exec_state = {**merged_state, **s2}
    s3 = sql_executor_node(exec_state)
    return {
        "search_results": s1.get("search_results", []),
        "analyst_results": s2.get("analyst_results", {}),
        "tool_outputs": s3.get("tool_outputs", s2.get("tool_outputs", [])),
        "thinking_trace": s3.get("thinking_trace", s2.get("thinking_trace", s1.get("thinking_trace", []))),
    }
