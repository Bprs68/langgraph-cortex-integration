"""Gradio UI — single app with mode toggle, chat, thinking trace, and auto-charts."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

import gradio as gr
import plotly.graph_objects as go

from agent_mode.graph import build_agent_graph
from orchestrator_mode.graph import build_orchestrator_graph
from common.chart_helper import auto_chart

logger = logging.getLogger(__name__)

# ── Graph singletons (built once) ─────────────────────────────────

_agent_graph = None
_orchestrator_graph = None


def _get_agent_graph():
    global _agent_graph
    if _agent_graph is None:
        _agent_graph = build_agent_graph()
    return _agent_graph


def _get_orchestrator_graph():
    global _orchestrator_graph
    if _orchestrator_graph is None:
        _orchestrator_graph = build_orchestrator_graph()
    return _orchestrator_graph


# ── State management ───────────────────────────────────────────────

def _make_session_state() -> dict:
    return {
        "thread_id": str(uuid.uuid4()),   # LangGraph checkpointer thread
        "agent_thread_id": None,          # Cortex Agent thread
        "parent_message_id": 0,
    }


# ── Chat handler ───────────────────────────────────────────────────

def _chat_stream(
    user_message: str,
    history: list[dict],
    mode: str,
    session_state: dict,
):
    """Generator that yields incremental UI updates as nodes complete."""
    if not user_message.strip():
        yield history, "", session_state, "", None, ""
        return

    logger.info("Chat request: mode=%s message=%s", mode, user_message[:100])

    # Show user message immediately with a processing indicator
    pending_history = history + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": "\u23f3 Processing..."},
    ]

    if mode == "Cortex Agent":
        # Agent mode: no streaming, just invoke
        yield pending_history, "", session_state, "", None, "_Agent mode \u2014 no trace._"
        result = _run_agent_mode(user_message, session_state)
        answer = result.get("display_text", result.get("answer", "(no response)"))
        session_state["agent_thread_id"] = result.get("thread_id", session_state["agent_thread_id"])
        session_state["parent_message_id"] = result.get(
            "parent_message_id", session_state["parent_message_id"]
        )
        raw = result.get("cortex_response", {})
        tool_info = _format_agent_tool_info(raw)
        final_history = history + [
            {"role": "user", "content": user_message},
            {"role": "assistant", "content": answer},
        ]
        yield final_history, tool_info, session_state, "", None, "_Agent mode \u2014 no trace._"
        return

    # \u2500\u2500 LangGraph Orchestrator: stream node-by-node \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    yield pending_history, "", session_state, "", None, "### \U0001f9e0 Agent Thinking Trace\n\n\u23f3 Starting..."

    app = _get_orchestrator_graph()
    config = {"configurable": {"thread_id": session_state["thread_id"]}}
    input_state = {
        "user_message": user_message,
        "iteration": 0,
        "tool_outputs": [],
        "thinking_trace": [],
    }

    accumulated_state = {}
    try:
        for chunk in app.stream(input_state, config=config, stream_mode="updates"):
            # chunk is {node_name: state_update}
            for node_name, state_update in chunk.items():
                if isinstance(state_update, dict):
                    accumulated_state.update(state_update)

            # Build live trace from accumulated state
            trace = accumulated_state.get("thinking_trace", [])
            trace_md = _format_thinking_trace(trace)

            # Show chart if available so far
            chart_fig = _extract_chart(accumulated_state)

            # Update assistant message with latest answer or keep processing
            current_answer = accumulated_state.get("display_text",
                             accumulated_state.get("answer", "\u23f3 Processing..."))
            live_history = history + [
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": current_answer},
            ]

            tool_info = _format_orchestrator_tool_info(accumulated_state)
            yield live_history, tool_info, session_state, "", chart_fig, trace_md

    except Exception as exc:
        logger.error("Orchestrator stream error: %s", exc)
        accumulated_state["answer"] = f"Error: {exc}"
        accumulated_state["display_text"] = f"Error: {exc}"

    # Final update — messages are persisted by LangGraph's checkpointer
    answer = accumulated_state.get("display_text", accumulated_state.get("answer", "(no response)"))
    final_history = history + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": answer},
    ]
    chart_fig = _extract_chart(accumulated_state)
    trace_md = _format_thinking_trace(accumulated_state.get("thinking_trace", []))
    tool_info = _format_orchestrator_tool_info(accumulated_state)
    yield final_history, tool_info, session_state, "", chart_fig, trace_md


def _run_agent_mode(user_message: str, session: dict) -> dict:
    app = _get_agent_graph()
    try:
        return app.invoke({
            "user_message": user_message,
            "messages": [],  # agent mode manages its own thread
            "thread_id": session.get("agent_thread_id"),
            "parent_message_id": session["parent_message_id"],
        })
    except Exception as exc:
        logger.error("Agent mode error: %s", exc)
        return {"answer": f"Error: {exc}", "display_text": f"Error: {exc}"}




# ── Thinking trace formatter ──────────────────────────────────────

_NODE_ICONS = {
    "router": "🧭",
    "search": "🔍",
    "analyst": "📊",
    "sql_executor": "⚙️",
    "llm": "🤖",
    "synthesizer": "✨",
    "human_review": "👤",
}


def _format_thinking_trace(trace: list[dict]) -> str:
    if not trace:
        return "_No thinking trace._"
    lines = ["### 🧠 Agent Thinking Trace\n"]
    start = trace[0].get("timestamp", 0)
    for entry in trace:
        icon = _NODE_ICONS.get(entry.get("node", ""), "▶️")
        elapsed = entry.get("timestamp", 0) - start
        step = entry.get("step", "?")
        node = entry.get("node", "unknown")
        summary = entry.get("summary", "")
        detail = entry.get("detail", "")
        lines.append(f"**{icon} Step {step} — {node}** _(+{elapsed:.1f}s)_")
        lines.append(f"> {summary}")
        if detail:
            # Keep detail compact: show first 500 chars
            truncated = detail[:500] + ("..." if len(detail) > 500 else "")
            lines.append(f"<details><summary>Details</summary>\n\n```\n{truncated}\n```\n</details>")
        lines.append("")
    return "\n".join(lines)


# ── Chart extraction ──────────────────────────────────────────────

def _extract_chart(result: dict) -> go.Figure | None:
    """Try to build a chart from the first SQL result set available."""
    tool_outputs = result.get("tool_outputs", [])
    for to in tool_outputs:
        for sr in to.get("sql_results", []):
            if sr.get("error"):
                continue
            columns = sr.get("columns", [])
            rows = sr.get("rows", [])
            fig = auto_chart(columns, rows)
            if fig:
                return fig
    return None


# ── Tool info formatting ──────────────────────────────────────────

def _format_agent_tool_info(raw: dict) -> str:
    if not raw:
        return "No tool details available."
    parts = []
    for item in raw.get("content", []):
        itype = item.get("type")
        if itype == "tool_result":
            tr = item.get("tool_result", {})
            tool_name = tr.get("tool_use_id", "unknown")
            parts.append(f"**Tool:** {tool_name}")
            for piece in tr.get("content", []):
                if piece.get("type") == "json":
                    sql = piece.get("json", {}).get("sql")
                    if sql:
                        parts.append(f"```sql\n{sql}\n```")
        elif itype == "table":
            tbl = item.get("table", {})
            title = tbl.get("title", "Table")
            rs = tbl.get("result_set", {})
            rows = rs.get("data", [])
            parts.append(f"**{title}:** {len(rows)} rows")
    return "\n\n".join(parts) if parts else "No tool calls detected."


def _format_orchestrator_tool_info(result: dict) -> str:
    parts = []
    intent = result.get("intent", "unknown")
    parts.append(f"**Router Intent:** `{intent}`")

    tool_outputs = result.get("tool_outputs", [])
    for to in tool_outputs:
        tool_name = to.get("tool", "unknown")
        if tool_name == "cortex_search":
            count = to.get("result_count", 0)
            parts.append(f"\n**Cortex Search:** {count} results")
            for i, r in enumerate(to.get("results", [])[:3], 1):
                parts.append(f"  {i}. {json.dumps(r, default=str)[:200]}")
        elif tool_name == "cortex_analyst":
            parts.append("\n**Cortex Analyst:**")
            for sql in to.get("sql_queries", []):
                parts.append(f"```sql\n{sql}\n```")
            for i, sr in enumerate(to.get("sql_results", []), 1):
                if sr.get("error"):
                    parts.append(f"  Query {i} error: {sr['error']}")
                else:
                    parts.append(f"  Query {i}: {sr.get('row_count', 0)} rows returned")

    if result.get("needs_clarification"):
        parts.append(f"\n**Clarification:** {result['needs_clarification']}")

    return "\n".join(parts) if parts else "No tool details available."


# ── Gradio app builder ────────────────────────────────────────────

def create_app() -> gr.Blocks:
    """Build and return the Gradio Blocks application."""
    with gr.Blocks(
        title="Cortex × LangGraph",
    ) as app:
        gr.Markdown("# ❄️ Snowflake Cortex × LangGraph\nAsk questions about your sales data.")

        session_state = gr.State(_make_session_state)

        with gr.Row():
            with gr.Column(scale=3):
                mode = gr.Dropdown(
                    choices=["Cortex Agent", "LangGraph Orchestrator"],
                    value="LangGraph Orchestrator",
                    label="Mode",
                    interactive=True,
                )

            with gr.Column(scale=1):
                clear_btn = gr.Button("🗑️ Clear Chat", variant="secondary")

        chatbot = gr.Chatbot(
            label="Conversation",
            height=450,
        )

        with gr.Row():
            msg_input = gr.Textbox(
                placeholder="Ask a question about your sales data...",
                label="Message",
                scale=5,
                lines=1,
            )
            send_btn = gr.Button("Send", variant="primary", scale=1)

        with gr.Row():
            with gr.Column(scale=1):
                with gr.Accordion("🧠 Agent Thinking", open=True):
                    trace_display = gr.Markdown("_No thinking trace yet._")

            with gr.Column(scale=1):
                with gr.Accordion("📊 Chart", open=True):
                    chart_display = gr.Plot(label="Auto-generated Chart")

        with gr.Accordion("🔧 Tool Outputs", open=False):
            tool_display = gr.Markdown("_No tool outputs yet._")

        # ── Event handlers ─────────────────────────────────────────

        send_btn.click(
            fn=_chat_stream,
            inputs=[msg_input, chatbot, mode, session_state],
            outputs=[chatbot, tool_display, session_state, msg_input, chart_display, trace_display],
        )
        msg_input.submit(
            fn=_chat_stream,
            inputs=[msg_input, chatbot, mode, session_state],
            outputs=[chatbot, tool_display, session_state, msg_input, chart_display, trace_display],
        )

        def on_clear():
            return [], "_No tool outputs yet._", _make_session_state(), None, "_No thinking trace yet._"

        clear_btn.click(
            fn=on_clear,
            outputs=[chatbot, tool_display, session_state, chart_display, trace_display],
        )

    return app
