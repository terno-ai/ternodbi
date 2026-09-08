"""Parse filter expressions into a normalised AST.

The parser implements the shared filter grammar so connectors only need to
translate the AST into their provider-specific syntax. This keeps parsing and
validation in one place.

Supported expressions are flat conditions joined by a single `AND` or `OR`.
Parentheses and mixed operator precedence are intentionally not supported.

Examples:
    country == US AND clicks > 100
    campaign_name =@ "Black Friday" OR spend >= 1000
    country [] US,CA,GB
"""

from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List, Literal, Union
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

Operator = Literal["==", "!=", ">", ">=", "<", "<=", "=@", "!@", "=~", "!~", "[]"]

# Longest first, so '>=' is matched before '>' and '=@' before '='.
_OPERATORS = ["==", "!=", ">=", "<=", "=@", "!@", "=~", "!~", ">", "<", "[]"]

_FIELD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


@dataclass(frozen=True)
class Condition:
    field: str
    operator: str
    value: Union[str, List[str]]   # list only for the '[]' (in) operator


@dataclass(frozen=True)
class FilterAst:
    conjunction: Literal["AND", "OR"]
    conditions: List[Condition]


def _bad(message: str, expr: str) -> ApiError:
    return ApiError(
        ErrorCode.INVALID_FILTER,
        f"{message} in filter expression: {expr!r}",
        details={"expression": expr},
    )


def _split_top_level(expr: str) -> tuple[str, List[str]]:
    """Split on AND / OR that sit outside quotes.

    Returns the single conjunction used and the raw condition strings. A mix of
    AND and OR is rejected rather than silently choosing a precedence.
    """
    tokens: List[str] = []
    conjunctions: List[str] = []
    buf: List[str] = []
    i, n = 0, len(expr)
    quote: str | None = None

    while i < n:
        ch = expr[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "\"'":
            quote = ch
            buf.append(ch)
            i += 1
            continue
        # Match AND / OR only on word boundaries, case-insensitively.
        rest = expr[i:]
        m = re.match(r"(?i)\b(AND|OR)\b", rest)
        if m:
            tokens.append("".join(buf).strip())
            conjunctions.append(m.group(1).upper())
            buf = []
            i += m.end()
            continue
        buf.append(ch)
        i += 1

    if quote:
        raise _bad("Unterminated quoted value", expr)

    tokens.append("".join(buf).strip())

    if conjunctions and len(set(conjunctions)) > 1:
        raise _bad(
            "Mixed AND/OR is not supported; split into separate queries", expr
        )
    conjunction = conjunctions[0] if conjunctions else "AND"
    return conjunction, [t for t in tokens if t]


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] in "\"'" and value[-1] == value[0]:
        return value[1:-1]
    return value


def _parse_condition(raw: str, expr: str) -> Condition:
    m = _FIELD_RE.match(raw.strip())
    if not m:
        raise _bad(f"Expected a field name at {raw!r}", expr)
    field = m.group(0)
    rest = raw.strip()[m.end():].lstrip()

    for op in _OPERATORS:
        if rest.startswith(op):
            value_part = rest[len(op):].strip()
            if not value_part:
                raise _bad(f"Missing value after {op!r}", expr)
            if op == "[]":
                items = [
                    _strip_quotes(v) for v in value_part.split(",") if v.strip()
                ]
                if not items:
                    raise _bad("Empty list for '[]' operator", expr)
                return Condition(field, op, items)
            return Condition(field, op, _strip_quotes(value_part))

    raise _bad(f"No known operator in condition {raw!r}", expr)


def parse_filters(expr: str | None) -> FilterAst | None:
    """Parse a filter expression, or return None for an empty one.

    Raises `ApiError(INVALID_FILTER)` on malformed input, so the failure is a
    clean, coded response rather than an opaque upstream error.
    """
    if expr is None or not expr.strip():
        return None
    conjunction, raw_conditions = _split_top_level(expr)
    if not raw_conditions:
        raise _bad("No conditions found", expr)
    conditions = [_parse_condition(c, expr) for c in raw_conditions]
    return FilterAst(conjunction, conditions)


__all__ = ["Condition", "FilterAst", "Operator", "parse_filters"]
