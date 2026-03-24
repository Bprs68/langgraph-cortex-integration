"""Router node — classifies user intent via Cortex LLM structured output."""

from __future__ import annotations

import logging
import time

from common.cortex_tools import CortexLLMTool
from common.query_rewriter import rewrite_if_needed
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)

_ROUTER_SYSTEM = """\
You are a query router for a sales intelligence system.
Classify the user's query into exactly ONE intent:

- "search"  — needs unstructured document / conversation retrieval (e.g. find meeting notes, search conversations, lookup discussion topics)
- "sql"     — needs structured data / metrics / aggregation (e.g. total revenue, deal counts, averages, comparisons across reps or time)
- "both"    — needs BOTH document retrieval AND structured data (e.g. compare what was discussed with actual deal values)
- "general" — general question that doesn't need any data tools (e.g. greetings, explanations, definitions)
- "clarify" — the query is too vague or ambiguous; you need to ask a clarifying question

Also provide a brief reasoning for your classification.
If intent is "clarify", include the clarifying question in the clarification field.

Respond ONLY with the JSON object — no extra text."""

_ROUTER_SCHEMA = {
    "type": "object",
    "properties": {
        "intent": {
            "type": "string",
            "enum": ["search", "sql", "both", "general", "clarify"],
        },
        "reasoning": {"type": "string"},
        "clarification": {"type": "string"},
    },
    "required": ["intent", "reasoning"],
}


def router_node(state: OrchestratorState) -> dict:
    """Classify user intent and route to the appropriate tool node."""
    llm = CortexLLMTool()

    # Build context from conversation history
    history = state.get("messages", [])
    messages = [{"role": "system", "content": _ROUTER_SYSTEM}]
    # Include last few turns for context
    for msg in history[-6:]:
        messages.append(msg)
    messages.append({"role": "user", "content": state["user_message"]})

    result = llm.chat_json(
        messages=messages,
        schema=_ROUTER_SCHEMA,
        schema_name="router_decision",
    )

    intent = result.get("intent", "general")
    reasoning = result.get("reasoning", "")
    clarification = result.get("clarification", "")

    logger.info("Router decision: intent=%s reason=%s", intent, reasoning)

    trace = list(state.get("thinking_trace", []))
    trace.append({
        "step": len(trace) + 1,
        "node": "router",
        "summary": f"Classified intent as **{intent}**",
        "detail": reasoning,
        "timestamp": time.time(),
    })

    return {
        "intent": intent,
        "needs_clarification": clarification if intent == "clarify" else "",
        "iteration": state.get("iteration", 0),
        "thinking_trace": trace,
    }
