import re

from anno_sql_test.errors import ParseError

_ISO_PATTERN = re.compile(
    r'^P'
    r'(?:(\d+(?:\.\d+)?)Y)?'
    r'(?:(\d+(?:\.\d+)?)M)?'
    r'(?:(\d+(?:\.\d+)?)D)?'
    r'(?:T'
    r'(?:(\d+(?:\.\d+)?)H)?'
    r'(?:(\d+(?:\.\d+)?)M)?'
    r'(?:(\d+(?:\.\d+)?)S)?'
    r')?$',
)


def _parse_iso_duration_to_seconds(duration: str, line: str) -> float:
    m = _ISO_PATTERN.match(duration)
    if not m:
        raise ParseError(f"Invalid ISO 8601 duration '{duration}' in: {line}")

    y, mo, d, h, mi, s = m.groups()
    if y is not None or mo is not None:
        raise ValueError(
            f"Years and months are ambiguous in temporal approx: {duration}",
        )

    total = 0.0
    if d:
        total += float(d) * 86400
    if h:
        total += float(h) * 3600
    if mi:
        total += float(mi) * 60
    if s:
        total += float(s)
    return total


def _parse_float(value: str, label: str, line: str) -> float:
    try:
        return float(value)
    except ValueError:
        raise ParseError(f"Invalid {label} '{value}' in: {line}") from None


def _smart_split(s: str) -> list[str]:
    parts = []
    current: list[str] = []
    paren_depth = 0
    in_single_quote = False
    in_double_quote = False
    escape = False

    for ch in s:
        if escape:
            current.append(ch)
            escape = False
            continue
        if ch == '\\' and (in_single_quote or in_double_quote):
            current.append(ch)
            escape = True
            continue
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(ch)
            continue
        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
            continue
        if ch == '(' and not in_single_quote and not in_double_quote:
            paren_depth += 1
            current.append(ch)
            continue
        if ch == ')' and not in_single_quote and not in_double_quote:
            paren_depth -= 1
            current.append(ch)
            continue
        if ch == ',' and paren_depth == 0 and not in_single_quote and not in_double_quote:
            parts.append(''.join(current).strip())
            current = []
            continue
        current.append(ch)

    remaining = ''.join(current).strip()
    if remaining:
        parts.append(remaining)

    return [p for p in parts if p]


def _parse_field_list(s: str) -> list[str]:
    fields = _smart_split(s)
    if not fields:
        raise ParseError("Empty field list")
    return fields


def _parse_dual_join_assert(rest: str, line: str):
    rest = rest.strip()
    if not rest.lower().startswith("on "):
        raise ParseError(f"Expected 'on <keys> values <vals>' in: {line}")
    rest = rest[3:]
    values_idx = rest.lower().find(" values ")
    if values_idx == -1:
        raise ParseError(f"Expected 'on <keys> values <vals>' in: {line}")
    keys_str = rest[:values_idx].strip()
    vals_str = rest[values_idx + len(" values "):].strip()
    keys = _smart_split(keys_str)
    vals = _smart_split(vals_str)
    if not keys or not vals:
        raise ParseError(f"Empty keys or values in: {line}")
    return keys, vals


def _parse_sql_lines(sql_lines: list[str]) -> list[str]:
    raw = "\n".join(sql_lines).strip()
    if not raw:
        return []
    return [s.strip() for s in raw.split(";") if s.strip()]
