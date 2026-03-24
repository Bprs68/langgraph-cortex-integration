"""SQL Executor node — runs SQL from Cortex Analyst against Snowflake."""

from __future__ import annotations

import logging
import time

from common.cortex_tools import CortexSQLExecutor
from orchestrator_mode.state import OrchestratorState

logger = logging.getLogger(__name__)


def sql_executor_node(state: OrchestratorState) -> dict:
    """Execute SQL queries produced by the analyst and attach result rows."""
    executor = CortexSQLExecutor()
    tool_outputs = list(state.get("tool_outputs", []))

    updated = []
    for output in tool_outputs:
        if output.get("tool") == "cortex_analyst" and output.get("sql_queries"):
            sql_results = []
            for sql in output["sql_queries"]:
                try:
                    result = executor.execute(sql)
                    sql_results.append(result)
                    logger.info(
                        "Executed SQL (%d rows, %d cols)",
                        result["row_count"],
                        len(result["columns"]),
                    )
                except Exception as exc:
                    logger.error("SQL execution failed: %s", exc)
                    sql_results.append({"error": str(exc), "columns": [], "rows": []})

            output = {**output, "sql_results": sql_results}
        updated.append(output)

    trace = list(state.get("thinking_trace", []))
    total_rows = sum(sr.get("row_count", 0) for o in updated for sr in o.get("sql_results", []))
    errors = sum(1 for o in updated for sr in o.get("sql_results", []) if sr.get("error"))
    summary = f"Executed SQL: **{total_rows}** rows returned"
    if errors:
        summary += f" ({errors} error(s))"
    detail_parts = []
    for o in updated:
        for sr in o.get("sql_results", []):
            if sr.get("error"):
                detail_parts.append(f"Error: {sr['error']}")
            else:
                cols = sr.get("columns", [])
                rows = sr.get("rows", [])
                detail_parts.append(f"Columns: {', '.join(cols)}")
                for row in rows[:10]:
                    detail_parts.append(f"  {dict(zip(cols, row))}")
                if len(rows) > 10:
                    detail_parts.append(f"  ... and {len(rows) - 10} more rows")
    trace.append({
        "step": len(trace) + 1,
        "node": "sql_executor",
        "summary": summary,
        "detail": "\n".join(detail_parts),
        "timestamp": time.time(),
    })

    return {"tool_outputs": updated, "thinking_trace": trace}
