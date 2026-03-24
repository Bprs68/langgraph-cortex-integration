"""LangGraph state for the orchestrator mode."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict


def _replace(a, b):
    """Reducer that always replaces the old value with the new one."""
    return b


class OrchestratorState(TypedDict, total=False):
    # Input
    user_message: str
    messages: Annotated[list[dict], operator.add]  # conversation history — appends across turns

    # Router output
    intent: str                   # "search" | "sql" | "both" | "general" | "clarify"

    # Tool outputs
    search_results: list[dict]    # rows from Cortex Search
    analyst_results: dict         # SQL + result set from Cortex Analyst
    llm_response: str             # direct LLM answer (general intent)

    # Synthesis
    tool_outputs: list[dict]      # aggregated tool results for synthesizer
    answer: str                   # final synthesized answer
    display_text: str             # formatted output for UI

    # Thinking trace — step-by-step agent reasoning for the UI
    thinking_trace: list[dict]    # [{step, node, summary, detail, timestamp}, ...]

    # Control
    iteration: int                # multi-step loop counter (max 3)
    needs_clarification: str      # question to ask user if intent is "clarify"
