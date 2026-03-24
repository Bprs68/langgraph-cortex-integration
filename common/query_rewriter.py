"""Rewrite follow-up questions into standalone queries using conversation history."""

from __future__ import annotations

import logging

from common.cortex_tools import CortexLLMTool

logger = logging.getLogger(__name__)

_REWRITE_SYSTEM = """\
You are a query rewriter. Given a conversation history and a follow-up question,
rewrite the follow-up into a STANDALONE question that includes all necessary
context from the conversation. The rewritten question should be self-contained
and understandable without the conversation history.

Rules:
- If the question is already standalone, return it unchanged.
- Resolve pronouns (their, it, those, etc.) using conversation context.
- Keep the rewritten question concise and natural.
- Respond with ONLY the rewritten question — no explanation or extra text."""


def rewrite_if_needed(user_message: str, messages: list[dict]) -> str:
    """Return a standalone version of user_message using conversation history.

    If there's no history or the message is already self-contained, returns
    the original message unchanged.
    """
    if not messages:
        return user_message

    # Only rewrite if there's prior context
    llm = CortexLLMTool()
    history_text = []
    for msg in messages[-6:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        # Trim long assistant responses
        if role == "assistant" and len(content) > 300:
            content = content[:300] + "..."
        history_text.append(f"{role}: {content}")

    prompt_messages = [
        {"role": "system", "content": _REWRITE_SYSTEM},
        {"role": "user", "content": (
            f"Conversation history:\n"
            + "\n".join(history_text)
            + f"\n\nFollow-up question: {user_message}"
        )},
    ]

    try:
        rewritten = llm.chat(messages=prompt_messages).strip()
        if rewritten:
            logger.info("Query rewritten: '%s' -> '%s'", user_message[:80], rewritten[:80])
            return rewritten
    except Exception as exc:
        logger.warning("Query rewrite failed, using original: %s", exc)

    return user_message
