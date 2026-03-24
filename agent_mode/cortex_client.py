"""Thin wrapper around the Snowflake Cortex Agent REST API."""

from __future__ import annotations

import json
import logging

import httpx

from common.config import AGENT_RUN_URL, THREAD_URL, SNOWFLAKE_PAT

logger = logging.getLogger(__name__)


class CortexAgentClient:
    """Call the Cortex Agent :run endpoint and manage threads."""

    def __init__(self, timeout: float = 120.0) -> None:
        pat = SNOWFLAKE_PAT.strip()
        self._headers = {
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        }
        self._timeout = timeout

    # ── Thread management ──────────────────────────────────────────

    def create_thread(self) -> int:
        """Create a new conversation thread and return its thread_id."""
        logger.info("Creating new Cortex thread")
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(THREAD_URL, headers=self._headers, json={})
            if resp.status_code != 200:
                logger.error("Thread creation failed (%d): %s",
                             resp.status_code, resp.text)
                raise RuntimeError(
                    f"Thread creation failed ({resp.status_code}): {resp.text}"
                )
            tid = resp.json()["thread_id"]
            logger.info("Created thread %d", tid)
            return tid

    # ── Agent invocation ───────────────────────────────────────────

    def run_agent(
        self,
        user_message: str,
        thread_id: int | None = None,
        parent_message_id: int = 0,
    ) -> dict:
        """Send a user message to the Cortex Agent and return the raw JSON."""
        payload: dict = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_message}],
                }
            ],
            "stream": False,
        }

        if thread_id is not None:
            payload["thread_id"] = thread_id
            payload["parent_message_id"] = parent_message_id

        logger.info("Agent run: thread=%s parent_msg=%d query=%s",
                     thread_id, parent_message_id, user_message[:120])

        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(
                AGENT_RUN_URL, headers=self._headers, json=payload
            )
            if resp.status_code != 200:
                logger.error("Agent run failed (%d): %s",
                             resp.status_code, resp.text)
                raise RuntimeError(
                    f"Agent run failed ({resp.status_code}): {resp.text}"
                )

        data = resp.json()
        logger.info("Agent responded (content items: %d)",
                     len(data.get("content", [])))
        return data

    # ── Streaming variant ──────────────────────────────────────────

    def run_agent_stream(
        self,
        user_message: str,
        thread_id: int | None = None,
        parent_message_id: int = 0,
    ):
        """Yield ``(event_type, data_dict)`` tuples via SSE streaming."""
        payload: dict = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_message}],
                }
            ],
            "stream": True,
        }
        if thread_id is not None:
            payload["thread_id"] = thread_id
            payload["parent_message_id"] = parent_message_id

        headers = {**self._headers, "Accept": "text/event-stream"}
        logger.info("Agent stream: thread=%s query=%s",
                     thread_id, user_message[:120])

        with httpx.Client(timeout=self._timeout) as client:
            with client.stream(
                "POST", AGENT_RUN_URL, headers=headers, json=payload
            ) as resp:
                resp.raise_for_status()
                event_type = ""
                for line in resp.iter_lines():
                    if line.startswith("event:"):
                        event_type = line[len("event:"):].strip()
                    elif line.startswith("data:"):
                        data_str = line[len("data:"):].strip()
                        try:
                            data = json.loads(data_str)
                        except json.JSONDecodeError:
                            data = {"raw": data_str}
                        yield event_type, data
                        event_type = ""
