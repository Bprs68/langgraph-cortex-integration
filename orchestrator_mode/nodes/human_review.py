"""Human review node — interrupts graph execution for user clarification."""

from __future__ import annotations

import logging
import time

from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)


def human_review_node(state: OrchestratorState) -> dict:
    """Set the clarification question as the answer for the user to respond to."""
    clarification = state.get("needs_clarification", "Could you please be more specific?")
    logger.info("Human review: asking for clarification: %s", clarification)

    # Append only the new turn pair (operator.add reducer accumulates)
    new_messages = [
        {"role": "user", "content": state["user_message"]},
        {"role": "assistant", "content": clarification},
    ]

    trace = list(state.get("thinking_trace", []))
    trace.append({
        "step": len(trace) + 1,
        "node": "human_review",
        "summary": "Asking user for clarification",
        "detail": clarification,
        "timestamp": time.time(),
    })

    return {
        "answer": clarification,
        "display_text": f"🔍 **Clarification needed:**\n{clarification}",
        "messages": new_messages,
        "thinking_trace": trace,
    }
