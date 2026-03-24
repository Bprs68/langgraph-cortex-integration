"""LLM node — direct answer for general questions (no data tools needed)."""

from __future__ import annotations

import logging
import time

from common.cortex_tools import CortexLLMTool
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are a helpful sales intelligence assistant. Answer the user's question
directly and concisely. If you don't know, say so."""


def llm_node(state: OrchestratorState) -> dict:
    """Answer general queries via Cortex LLM without any data tools."""
    llm = CortexLLMTool()

    history = state.get("messages", [])
    messages = [{"role": "system", "content": _SYSTEM}]
    for msg in history[-6:]:
        messages.append(msg)
    messages.append({"role": "user", "content": state["user_message"]})

    logger.info("LLM node: direct answer for general query")

    content = llm.chat(messages=messages)

    # Append only the new turn pair (operator.add reducer accumulates)
    new_messages = [
        {"role": "user", "content": state["user_message"]},
        {"role": "assistant", "content": content},
    ]

    trace = list(state.get("thinking_trace", []))
    trace.append({
        "step": len(trace) + 1,
        "node": "llm",
        "summary": "Direct LLM response generated",
        "detail": content[:200] + ("..." if len(content) > 200 else ""),
        "timestamp": time.time(),
    })

    return {
        "llm_response": content,
        "answer": content,
        "display_text": content,
        "messages": new_messages,
        "thinking_trace": trace,
    }
