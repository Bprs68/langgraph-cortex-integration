"""Auto-detect and generate Plotly charts from SQL result data."""

from __future__ import annotations

import logging
from typing import Any

import plotly.graph_objects as go

logger = logging.getLogger(__name__)

# Heuristic thresholds
_MAX_PIE_SLICES = 8
_MAX_BAR_CATEGORIES = 30


def auto_chart(columns: list[str], rows: list[list[Any]]) -> go.Figure | None:
    """Return a Plotly Figure for the data, or None if no chart makes sense."""
    if not columns or not rows or len(columns) < 2:
        return None

    try:
        return _build_chart(columns, rows)
    except Exception:
        logger.warning("Chart generation failed", exc_info=True)
        return None


def _build_chart(columns: list[str], rows: list[list[Any]]) -> go.Figure | None:
    """Core chart builder with auto-detection logic."""
    # Classify columns as numeric or categorical
    numeric_cols: list[int] = []
    categorical_cols: list[int] = []
    date_cols: list[int] = []

    for i, col in enumerate(columns):
        sample_values = [r[i] for r in rows[:20] if r[i] is not None]
        if not sample_values:
            categorical_cols.append(i)
            continue
        # Check column name for date patterns FIRST (before numeric check)
        # because date columns may contain numeric epoch values
        if _is_date_like(col):
            date_cols.append(i)
        elif _is_numeric(sample_values):
            numeric_cols.append(i)
        else:
            categorical_cols.append(i)

    if not numeric_cols:
        return None

    # Date + numeric → line chart
    if date_cols and numeric_cols:
        return _line_chart(columns, rows, date_cols[0], numeric_cols)

    # 1 categorical + 1 numeric with few values → pie
    if len(categorical_cols) == 1 and len(numeric_cols) == 1 and len(rows) <= _MAX_PIE_SLICES:
        return _pie_chart(columns, rows, categorical_cols[0], numeric_cols[0])

    # Categorical + numeric → bar chart
    if categorical_cols and numeric_cols and len(rows) <= _MAX_BAR_CATEGORIES:
        return _bar_chart(columns, rows, categorical_cols[0], numeric_cols)

    # Fallback: just pick first two columns
    if len(numeric_cols) >= 2:
        return _scatter_chart(columns, rows, numeric_cols[0], numeric_cols[1])

    return None


# ── Chart builders ─────────────────────────────────────────────────


def _bar_chart(columns, rows, cat_idx, num_indices):
    labels = [str(r[cat_idx]) for r in rows]
    fig = go.Figure()
    for ni in num_indices:
        vals = [_to_float(r[ni]) for r in rows]
        fig.add_trace(go.Bar(x=labels, y=vals, name=columns[ni]))
    fig.update_layout(
        title=f"{columns[num_indices[0]]} by {columns[cat_idx]}",
        xaxis_title=columns[cat_idx],
        yaxis_title=columns[num_indices[0]] if len(num_indices) == 1 else "Value",
        template="plotly_white",
    )
    return fig


def _pie_chart(columns, rows, cat_idx, num_idx):
    labels = [str(r[cat_idx]) for r in rows]
    values = [_to_float(r[num_idx]) for r in rows]
    fig = go.Figure(go.Pie(labels=labels, values=values, textinfo="label+percent"))
    fig.update_layout(
        title=f"{columns[num_idx]} by {columns[cat_idx]}",
        template="plotly_white",
    )
    return fig


def _line_chart(columns, rows, date_idx, num_indices):
    x_vals = [str(r[date_idx]) for r in rows]
    fig = go.Figure()
    for ni in num_indices:
        vals = [_to_float(r[ni]) for r in rows]
        fig.add_trace(go.Scatter(x=x_vals, y=vals, mode="lines+markers", name=columns[ni]))
    fig.update_layout(
        title=f"{columns[num_indices[0]]} over {columns[date_idx]}",
        xaxis_title=columns[date_idx],
        yaxis_title=columns[num_indices[0]] if len(num_indices) == 1 else "Value",
        template="plotly_white",
    )
    return fig


def _scatter_chart(columns, rows, x_idx, y_idx):
    x_vals = [_to_float(r[x_idx]) for r in rows]
    y_vals = [_to_float(r[y_idx]) for r in rows]
    fig = go.Figure(go.Scatter(x=x_vals, y=y_vals, mode="markers"))
    fig.update_layout(
        title=f"{columns[y_idx]} vs {columns[x_idx]}",
        xaxis_title=columns[x_idx],
        yaxis_title=columns[y_idx],
        template="plotly_white",
    )
    return fig


# ── Helpers ────────────────────────────────────────────────────────

def _is_numeric(values: list) -> bool:
    try:
        for v in values:
            float(v)
        return True
    except (ValueError, TypeError):
        return False


def _is_date_like(col_name: str) -> bool:
    lower = col_name.lower()
    return any(kw in lower for kw in ("date", "time", "month", "year", "week", "day", "quarter"))


def _to_float(v) -> float:
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0
