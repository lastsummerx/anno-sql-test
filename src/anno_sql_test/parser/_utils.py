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


def _parse_iso_duration_to_seconds(duration: str, source: str) -> float:
    m = _ISO_PATTERN.match(duration)
    if not m:
        raise ParseError(f"Invalid ISO 8601 duration '{duration}' in: {source}")

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


def _parse_float(value: str, label: str, source: str) -> float:
    try:
        return float(value)
    except ValueError:
        raise ParseError(f"Invalid {label} '{value}' in: {source}") from None


def _calc_top_before(s: str) -> list[bool]:
    top_before = []
    depth = 0
    in_single = False
    in_double = False
    escape = False

    for ch in s:
        top_before.append(depth == 0 and not in_single and not in_double)

        if escape:
            escape = False
            continue
        if ch == '\\' and (in_single or in_double):
            escape = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue
        if ch == '(' and not in_single and not in_double:
            depth += 1
            continue
        if ch == ')' and not in_single and not in_double:
            if depth > 0:
                depth -= 1
            # Ignore extra closing parens; avoid negative depth.
            continue
    return top_before


def _smart_split(s: str, sep: str, maxsplit: int = -1) -> list[str]:
    """
    Split a string by a regex separator, but ignore separators that lie
    inside parentheses, single quotes, or double quotes.

    Args:
        s:          Input string.
        sep:        Separator regex pattern (string, will be compiled).
        maxsplit:   Maximum number of splits to perform.
                    -1 (default) means no limit (split everywhere).
                    0  means no split (return the whole string as one piece).
                    n  (positive) means at most n splits, returning n+1 pieces.

    Returns:
        List of pieces.
    """
    # Step 1: Record "top-level-before" state for each character index.
    # top_before[i] is True if, just before processing character s[i],
    # we are at parenthesis depth 0 and not inside any quotes.
    top_before = _calc_top_before(s)

    # Step 2: Find all separator matches and keep only those where the
    # entire match span is at top level.
    pattern = re.compile(sep)
    splits = []
    for m in pattern.finditer(s):
        start, end = m.start(), m.end()
        if all(top_before[start:end]):
            splits.append((start, end))

    # Step 3: Cut according to maxsplit.
    if maxsplit < 0:
        # -1 means unlimited splits – use all valid split points.
        maxsplit = len(splits)

    parts = []
    last_end = 0
    for start, end in splits[:maxsplit]:
        parts.append(s[last_end:start].strip())
        last_end = end
    parts.append(s[last_end:].strip())

    return parts


def _parse_field_list(s: str) -> list[str]:
    fields = _smart_split(s, ",")
    if not fields:
        raise ParseError("Empty field list")
    return fields
