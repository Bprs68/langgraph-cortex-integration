"""Build the LangGraph StateGraph for Cortex Agent wrapper mode."""

from __future__ import annotations

import logging

from langgraph.graph import StateGraph, START, END

from agent_mode.state import AgentState
from agent_mode.cortex_client import CortexAgentClient
from agent_mode.response_parser import parse_cortex_response, format_for_display

logger = logging.getLogger(__name__)


def _call_cortex_agent(state: AgentState) -> dict:
    client = CortexAgentClient()
    thread_id = state.get("thread_id")
    parent_message_id = state.get("parent_message_id", 0)

    if thread_id is None:
        thread_id = client.create_thread()

    raw_response = client.run_agent(
        user_message=state["user_message"],
        thread_id=thread_id,
        parent_message_id=parent_message_id,
    )
    return {"cortex_response": raw_response, "thread_id": thread_id}


def _parse_response(state: AgentState) -> dict:
    raw = state.get("cortex_response") or {}
    parsed = parse_cortex_response(raw)

    next_parent = parsed.message_id
    if next_parent is None:
        next_parent = state.get("parent_message_id", 0) + 2

    messages = list(state.get("messages", []))
    messages.append({"role": "user", "content": state["user_message"]})
    messages.append({"role": "assistant", "content": parsed.text})

    return {
        "answer": parsed.text,
        "tool_results": parsed.tool_results,
        "parent_message_id": next_parent,
        "messages": messages,
    }


def _format_output(state: AgentState) -> dict:
    raw = state.get("cortex_response") or {}
    parsed = parse_cortex_response(raw)
    return {"display_text": format_for_display(parsed)}


def build_agent_graph():
    """Construct and compile the Cortex Agent LangGraph."""
    logger.info("Building agent-mode graph")
    graph = StateGraph(AgentState)

    graph.add_node("call_cortex_agent", _call_cortex_agent)
    graph.add_node("parse_response", _parse_response)
    graph.add_node("format_output", _format_output)

    graph.add_edge(START, "call_cortex_agent")
    graph.add_edge("call_cortex_agent", "parse_response")
    graph.add_edge("parse_response", "format_output")
    graph.add_edge("format_output", END)

    return graph.compile()
