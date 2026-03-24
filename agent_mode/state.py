"""LangGraph state for the Cortex Agent wrapper mode."""

from __future__ import annotations

from typing import TypedDict


class AgentState(TypedDict, total=False):
    user_message: str
    messages: list[dict]
    thread_id: int | None
    parent_message_id: int
    cortex_response: dict | None
    answer: str
    tool_results: list[dict]
    display_text: str
