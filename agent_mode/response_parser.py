"""Parse non-streaming JSON responses from the Cortex Agent :run API."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ParsedResponse:
    """Structured representation of a Cortex Agent response."""

    text: str = ""
    tool_results: list[dict] = field(default_factory=list)
    tables: list[dict] = field(default_factory=list)
    citations: list[dict] = field(default_factory=list)
    sql_queries: list[str] = field(default_factory=list)
    message_id: int | None = None


def parse_cortex_response(response: dict) -> ParsedResponse:
    """Extract structured data from the agent:run non-streaming response."""
    result = ParsedResponse()
    content_items: list[dict] = response.get("content", [])

    for item in content_items:
        item_type = item.get("type")

        if item_type == "text":
            result.text += item.get("text", "")
            for ann in item.get("annotations", []):
                if ann.get("type") == "cortex_search_citation":
                    result.citations.append(ann)

        elif item_type == "tool_result":
            tr = item.get("tool_result", {})
            result.tool_results.append(tr)
            for content_piece in tr.get("content", []):
                if content_piece.get("type") == "json":
                    json_data = content_piece.get("json", {})
                    sql = json_data.get("sql")
                    if sql:
                        result.sql_queries.append(sql)

        elif item_type == "table":
            result.tables.append(item.get("table", {}))

    metadata = response.get("metadata", {})
    if isinstance(metadata, dict):
        result.message_id = metadata.get("message_id")

    logger.debug("Parsed response: text=%d chars, tables=%d, citations=%d, sql=%d",
                 len(result.text), len(result.tables),
                 len(result.citations), len(result.sql_queries))
    return result


def format_for_display(parsed: ParsedResponse) -> str:
    """Format a ParsedResponse into a human-readable string."""
    parts: list[str] = []

    if parsed.text:
        parts.append(parsed.text)

    if parsed.sql_queries:
        parts.append("\n--- SQL Queries ---")
        for i, sql in enumerate(parsed.sql_queries, 1):
            parts.append(f"[{i}] {sql}")

    if parsed.tables:
        parts.append("\n--- Tables ---")
        for tbl in parsed.tables:
            title = tbl.get("title", "Untitled")
            rs = tbl.get("result_set", {})
            meta = rs.get("resultSetMetaData", {})
            col_names = [c["name"] for c in meta.get("rowType", [])]
            rows = rs.get("data", [])
            parts.append(f"  {title}: {len(rows)} rows, columns: {col_names}")

    if parsed.citations:
        parts.append("\n--- Citations ---")
        for cit in parsed.citations:
            parts.append(
                f"  [{cit.get('index', '?')}] {cit.get('doc_title', 'N/A')}"
            )

    return "\n".join(parts)
