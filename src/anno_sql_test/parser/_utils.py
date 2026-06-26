import re
from dataclasses import dataclass
from enum import Enum

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    ColumnSpec,
    ExprColumn,
    FieldType,
    GlobTemplateColumn,
)

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


_WORD_CHARS = re.compile(r'[\w.]+')

_FIELD_TYPE_ALIASES: dict[str, FieldType] = {
    'numeric': FieldType.NUMERIC,
    'number': FieldType.NUMERIC,
    'string': FieldType.STRING,
    'temporal': FieldType.TEMPORAL,
}


class TokenKind(Enum):
    STRING = 'string'
    STAR = 'star'
    COLON = 'colon'
    WORD = 'word'
    LPAREN = 'lparen'  # (
    RPAREN = 'rparen'  # )
    OTHER = 'other'


_SINGLE_CHAR_MAP = {
    '*': TokenKind.STAR,
    ':': TokenKind.COLON,
    '(': TokenKind.LPAREN,
    ')': TokenKind.RPAREN,
}


@dataclass
class Token:
    kind: TokenKind
    value: str
    start: int
    end: int


def skip_string(s: str, i: int) -> int:
    if i >= len(s) or s[i] not in ("'", '"'):
        return i
    quote = s[i]
    j = i + 1
    while j < len(s):
        if s[j] == '\\':
            j += 2
            continue
        if s[j] == quote:
            return j + 1
        j += 1
    return j


def tokenize(s: str) -> list[Token]:
    tokens = []
    i = 0
    while i < len(s):
        if s[i] in ("'", '"'):
            end = skip_string(s, i)
            tokens.append(Token(TokenKind.STRING, s[i:end], i, end))
            i = end
        elif s[i] in _SINGLE_CHAR_MAP:
            tokens.append(Token(_SINGLE_CHAR_MAP[s[i]], s[i], i, i + 1))
            i += 1
        elif m := _WORD_CHARS.match(s, i):
            tokens.append(Token(TokenKind.WORD, m.group(), i, m.end()))
            i = m.end()
        else:
            tokens.append(Token(TokenKind.OTHER, s[i], i, i + 1))
            i += 1
    return tokens


def _extract_glob_and_template(expr: str) -> tuple[str, str]:
    tokens = tokenize(expr)
    star_idx = next((i for i, t in enumerate(tokens) if t.kind == TokenKind.STAR), None)
    if star_idx is None:
        return expr, '{col}'

    start = tokens[star_idx].start
    end = tokens[star_idx].end

    j = star_idx - 1
    while j >= 0 and tokens[j].kind == TokenKind.WORD:
        start = tokens[j].start
        j -= 1

    j = star_idx + 1
    while j < len(tokens) and tokens[j].kind == TokenKind.WORD:
        end = tokens[j].end
        j += 1

    glob_pattern = expr[start:tokens[star_idx].start] + '*' + expr[tokens[star_idx].end:end]
    template = expr[:start] + '{col}' + expr[end:]

    return glob_pattern, template


def _parse_except_patterns(s: str) -> list[str]:
    raw = s.strip()
    if raw.startswith('(') and raw.endswith(')'):
        raw = raw[1:-1]
    return [p.strip() for p in raw.split(',') if p.strip()]


def _find_matching_paren(tokens: list, start: int) -> int | None:
    """
    Given a list of tokens and the index of an LPAREN token,
    return the index of the matching RPAREN token, or None if not found.
    """
    if start >= len(tokens) or tokens[start].kind != TokenKind.LPAREN:
        return None
    depth = 1
    for i in range(start + 1, len(tokens)):
        if tokens[i].kind == TokenKind.LPAREN:
            depth += 1
        elif tokens[i].kind == TokenKind.RPAREN:
            depth -= 1
            if depth == 0:
                return i
    return None


def _find_columns(expr: str) -> tuple[int, int, str] | None:
    """
    Locate a top-level COLUMNS(...) call in the expression.
    Returns (start, end, inner_text) or None if not found.
    """
    tokens = tokenize(expr)
    for i, t in enumerate(tokens):
        if (t.kind == TokenKind.WORD and t.value.upper() == 'COLUMNS'
                and i + 1 < len(tokens) and tokens[i + 1].kind == TokenKind.LPAREN):
            j = _find_matching_paren(tokens, i + 1)
            if j is None:
                return None
            inner = expr[tokens[i + 2].start:tokens[j].start].strip()
            return t.start, tokens[j].end, inner
    return None


def _parse_inner_except(inner: str) -> tuple[str, list[str]]:
    """
    Parse EXCEPT clause inside a COLUMNS() inner expression.
    Returns (inner_expr_without_except, list_of_except_patterns).
    """
    inner_tokens = tokenize(inner)
    except_pos = next(
        (i for i, t in enumerate(inner_tokens)
         if t.kind == TokenKind.WORD and t.value.upper() == 'EXCEPT'),
        -1,
    )
    if except_pos == -1:
        return inner.strip(), []

    after = inner_tokens[except_pos + 1:]
    except_arg_start = inner_tokens[except_pos].end

    # Determine the end of the EXCEPT argument
    if after and after[0].kind == TokenKind.LPAREN:
        j = _find_matching_paren(after, 0)
        except_arg_end = after[j].end if j is not None else len(inner)
    else:
        # No parentheses: grab until the next unbalanced RPAREN or end
        depth = 0
        i = 0
        while i < len(after):
            if after[i].kind == TokenKind.LPAREN:
                depth += 1
            elif after[i].kind == TokenKind.RPAREN:
                if depth == 0:
                    break
                depth -= 1
            i += 1
        except_arg_end = after[i].start if i < len(after) else len(inner)

    tail = inner[except_arg_start:except_arg_end].strip()
    except_patterns = _parse_except_patterns(tail)

    # Remove the EXCEPT clause from the inner expression
    inner_expr = (
        inner[:inner_tokens[except_pos].start].rstrip()
        + inner[except_arg_end:]
    ).strip()
    return inner_expr, except_patterns


def parse_column_spec(s: str) -> ColumnSpec:
    raw = s.strip()

    tokens = tokenize(raw)

    # Optional type filter prefix (e.g. "num: ...")
    type_filter = None
    prefix_end = 0
    if (len(tokens) >= 2 and tokens[0].kind == TokenKind.WORD
            and tokens[1].kind == TokenKind.COLON):
        ft = _FIELD_TYPE_ALIASES.get(tokens[0].value)
        if ft is not None:
            type_filter = ft
            prefix_end = tokens[1].end

    rest = raw[prefix_end:]

    # columns() marker required for glob expansion
    cm = _find_columns(rest)
    if cm is None:
        return ExprColumn(raw)

    cs, ce, inner = cm

    # EXCEPT outside columns() is invalid glob syntax
    rest_tokens = tokenize(rest)
    if any(
        t.kind == TokenKind.WORD and t.value.upper() == 'EXCEPT'
        and (t.start < cs or t.start >= ce)
        for t in rest_tokens
    ):
        return ExprColumn(raw)

    # Parse EXCEPT clause inside columns()
    inner_expr, except_patterns = _parse_inner_except(inner)

    glob, _ = _extract_glob_and_template(inner_expr)
    template = rest[:cs] + '{col}' + rest[ce:]
    return GlobTemplateColumn(
        glob=glob,
        type_filter=type_filter,
        excepts=except_patterns,
        expr=template,
    )


def _parse_field_list(s: str, label: str = "fields") -> list[ColumnSpec]:
    raw = [x.strip() for x in _smart_split(s, ",")]
    if not raw or '' in raw:
        raise ParseError(f"Empty {label} in: {s}")
    return [parse_column_spec(f) for f in raw]
