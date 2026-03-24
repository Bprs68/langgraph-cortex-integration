"""Synthesizer node — merges tool outputs into a coherent answer via Cortex LLM."""

from __future__ import annotations

import json
import logging
import time

from common.cortex_tools import CortexLLMTool
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)

_SYNTH_SYSTEM = """\
You are a sales intelligence assistant. The user asked a question and one or
more data tools were called. Below are the tool outputs.

Your job:
1. Synthesize the tool results into a clear, concise NATURAL LANGUAGE answer.
2. Present key numbers and insights clearly in plain English.
3. If search results were returned, incorporate relevant excerpts with attribution.
4. If the data is insufficient to fully answer, set needs_followup to true and
   explain what additional information is needed.

IMPORTANT: Do NOT include SQL queries, raw table data, column headers, or
result sets in your answer. Just provide a clear conversational summary of the
findings. The SQL and raw data are shown separately in the UI.

Respond ONLY with the JSON — no extra text."""

_SYNTH_SCHEMA = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "needs_followup": {"type": "boolean"},
        "followup_reason": {"type": "string"},
    },
    "required": ["answer", "needs_followup"],
}


def synthesizer_node(state: OrchestratorState) -> dict:
    """Merge all tool outputs into a final answer."""
    llm = CortexLLMTool()
    tool_outputs = state.get("tool_outputs", [])

    # Build the context message with tool results
    context_parts: list[str] = []
    for to in tool_outputs:
        tool_name = to.get("tool", "unknown")
        if tool_name == "cortex_search":
            results = to.get("results", [])
            context_parts.append(
                f"=== Cortex Search ({to.get('result_count', 0)} results) ===\n"
                + json.dumps(results[:5], indent=2, default=str)
            )
        elif tool_name == "cortex_analyst":
            part = (
                f"=== Cortex Analyst ===\n"
                f"Text: {to.get('text', '')}\n"
                f"SQL: {to.get('sql_queries', [])}\n"
            )
            # Append executed SQL results if available
            for i, sr in enumerate(to.get('sql_results', [])):
                if sr.get('error'):
                    part += f"\nQuery {i+1} error: {sr['error']}\n"
                else:
                    cols = sr.get('columns', [])
                    rows = sr.get('rows', [])
                    part += f"\nQuery {i+1} results ({len(rows)} rows):\n"
                    part += f"Columns: {cols}\n"
                    for row in rows[:20]:
                        part += f"  {dict(zip(cols, row))}\n"
                    if len(rows) > 20:
                        part += f"  ... and {len(rows) - 20} more rows\n"
            context_parts.append(part)

    context = "\n\n".join(context_parts) if context_parts else "(no tool outputs)"

    messages = [
        {"role": "system", "content": _SYNTH_SYSTEM},
        {"role": "user", "content": (
            f"User question: {state['user_message']}\n\n"
            f"Tool outputs:\n{context}"
        )},
    ]

    logger.info("Synthesizer: merging %d tool outputs", len(tool_outputs))

    result = llm.chat_json(
        messages=messages,
        schema=_SYNTH_SCHEMA,
        schema_name="synthesis",
    )

    answer = result.get("answer", "")
    needs_followup = result.get("needs_followup", False)
    iteration = state.get("iteration", 0) + 1

    # Display text is just the synthesized answer — SQL/details live in thinking trace
    display_text = answer

    # Only add messages on the first iteration to avoid duplicating the user turn
    # in multi-step loops (synthesizer → router → ... → synthesizer)
    if state.get("iteration", 0) == 0:
        new_messages = [
            {"role": "user", "content": state["user_message"]},
            {"role": "assistant", "content": answer},
        ]
    else:
        new_messages = [
            {"role": "assistant", "content": answer},
        ]

    logger.info("Synthesizer: needs_followup=%s iteration=%d", needs_followup, iteration)

    trace = list(state.get("thinking_trace", []))
    trace.append({
        "step": len(trace) + 1,
        "node": "synthesizer",
        "summary": "Synthesized final answer" if not needs_followup else "Needs follow-up",
        "detail": answer[:300] + ("..." if len(answer) > 300 else ""),
        "timestamp": time.time(),
    })

    return {
        "answer": answer,
        "display_text": display_text,
        "iteration": iteration,
        "messages": new_messages,
        # Reset tool outputs for potential next iteration
        "tool_outputs": [] if needs_followup and iteration < 3 else tool_outputs,
        # Signal for conditional edge
        "intent": "needs_more" if needs_followup and iteration < 3 else "complete",
        "thinking_trace": trace,
    }
