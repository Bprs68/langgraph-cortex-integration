"""Unified entry point — launches Gradio UI or headless CLI.

Usage:
    python main.py                          # Gradio UI (default)
    python main.py --cli --mode agent       # CLI, Cortex Agent wrapper
    python main.py --cli --mode orchestrator # CLI, LangGraph orchestrator
"""

from __future__ import annotations

import argparse
import logging
import sys

import gradio as gr

from agent_mode.graph import build_agent_graph
from common.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_AGENT_NAME
from common.logging_config import setup_logging
from orchestrator_mode.graph import build_orchestrator_graph
from ui.app import create_app


def _run_cli(mode: str) -> None:
    """Run the headless CLI loop."""
    logger = logging.getLogger(__name__)

    if mode == "agent":
        app = build_agent_graph()
        state: dict = {"messages": [], "thread_id": None, "parent_message_id": 0}

        print("=" * 60)
        print("  Snowflake Cortex Agent  ↔  LangGraph")
        print(f"  Agent: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_AGENT_NAME}")
        print("  Type your question, or 'quit' to exit.")
        print("=" * 60)

        while True:
            try:
                user_input = input("\nYou: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break

            try:
                result = app.invoke({
                    "user_message": user_input,
                    "messages": state["messages"],
                    "thread_id": state["thread_id"],
                    "parent_message_id": state["parent_message_id"],
                })
            except Exception as exc:
                logger.error("Agent error: %s", exc)
                print(f"\nERROR: {exc}")
                continue

            display = result.get("display_text", result.get("answer", "(no response)"))
            print(f"\nAgent: {display}")
            state["messages"] = result.get("messages", state["messages"])
            state["thread_id"] = result.get("thread_id", state["thread_id"])
            state["parent_message_id"] = result.get("parent_message_id", state["parent_message_id"])

    else:  # orchestrator
        app = build_orchestrator_graph()
        state = {"messages": [], "iteration": 0, "tool_outputs": []}

        print("=" * 60)
        print("  LangGraph Orchestrator  ↔  Cortex Tools")
        print("  Type your question, or 'quit' to exit.")
        print("=" * 60)

        while True:
            try:
                user_input = input("\nYou: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break

            try:
                result = app.invoke({
                    "user_message": user_input,
                    "messages": state["messages"],
                    "iteration": 0,
                    "tool_outputs": [],
                })
            except Exception as exc:
                logger.error("Orchestrator error: %s", exc)
                print(f"\nERROR: {exc}")
                continue

            display = result.get("display_text", result.get("answer", "(no response)"))
            print(f"\nAssistant: {display}")
            state["messages"] = result.get("messages", state["messages"])


def _run_ui() -> None:
    """Launch the Gradio web UI."""
    app = create_app()
    app.launch(share=False, theme=gr.themes.Soft())


def main() -> None:
    parser = argparse.ArgumentParser(description="Cortex + LangGraph")
    parser.add_argument("--cli", action="store_true", help="Run in headless CLI mode")
    parser.add_argument(
        "--mode",
        choices=["agent", "orchestrator"],
        default="orchestrator",
        help="Which backend to use (default: orchestrator)",
    )
    args = parser.parse_args()

    setup_logging()

    if args.cli:
        _run_cli(args.mode)
    else:
        _run_ui()


if __name__ == "__main__":
    main()
