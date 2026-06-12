import re
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DateType,
    NumericType,
    StringType,
    StructType,
    TimestampType,
)

from anno_sql_test.evaluators._field_parser import Token, TokenKind, tokenize

_TEMPORAL_TYPES = (DateType, TimestampType)
try:
    from pyspark.sql.types import TimeType
    _TEMPORAL_TYPES = _TEMPORAL_TYPES + (TimeType,)
except ImportError:
    pass
try:
    from pyspark.sql.types import TimestampNTZType
    _TEMPORAL_TYPES = _TEMPORAL_TYPES + (TimestampNTZType,)
except ImportError:
    pass


@dataclass
class NamedColumn:
    name: str
    column: Column
    namespace: str = ""


type ColumnComparator = Callable[[Column, Column], Column]
type ColumnTypeChecker = Callable[[str, DataType], str | None]


_NON_WORD = re.compile(r"\W")
_IS_GLOB = re.compile(r'^[\w.*]+$')
_TYPE_PREFIX_MAP = MappingProxyType({
    'numeric': NumericType,
    'number': NumericType,       # 别名
    'string': StringType,
    'temporal': _TEMPORAL_TYPES,  # 元组 isinstance 同样兼容
})


def _filter_by_type(cols: list[str], type_name: str, schema: StructType) -> list[str]:
    type_rule = _TYPE_PREFIX_MAP.get(type_name)
    if type_rule is None:
        return cols
    return [c for c in cols if isinstance(schema[c].dataType, type_rule)]


def _filter_by_glob(cols: list[str], pattern: str) -> list[str]:
    regex = re.escape(pattern).replace(r'\*', '.*')
    return [c for c in cols if re.match(f'^{regex}$', c)]


def _expand_glob(
    inner: str, tokens: list[Token], cols: list[str],
) -> list[str]:
    if _IS_GLOB.match(inner):
        return _filter_by_glob(cols, inner)

    star_indices = [i for i, t in enumerate(tokens) if t.kind == TokenKind.STAR]
    if not star_indices:
        return [inner]

    result = [inner]
    for si in star_indices:
        start = tokens[si].start
        left = ''
        j = si - 1
        while j >= 0 and tokens[j].kind == TokenKind.WORD:
            left = tokens[j].value + left
            start = tokens[j].start
            j -= 1

        end = tokens[si].end
        right = ''
        j = si + 1
        while j < len(tokens) and tokens[j].kind == TokenKind.WORD:
            right += tokens[j].value
            end = tokens[j].end
            j += 1

        glob_pattern = left + '*' + right

        new_result = []
        for r in result:
            for col in _filter_by_glob(cols, glob_pattern):
                new_result.append(r[:start] + col + r[end:])
        result = new_result
        if not result:
            return [inner]

    return result


def resolve_fields(values: list[str], dataframes: list[DataFrame]) -> list[str]:
    if not values:
        return values

    common_cols = sorted(set(dataframes[0].columns).intersection(
        *(set(df.columns) for df in dataframes[1:]),
    ))
    schema = dataframes[0].schema

    result = []
    for v in values:
        tokens = tokenize(v)

        type_prefix = None
        inner_tokens = tokens
        inner_offset = 0
        if len(tokens) >= 2 and tokens[0].kind == TokenKind.WORD and tokens[1].kind == TokenKind.COLON:
            candidate = tokens[0].value
            if candidate in _TYPE_PREFIX_MAP:
                type_prefix = candidate
                inner_tokens = tokens[2:]
                inner_offset = inner_tokens[0].start if inner_tokens else len(v)

        cols = _filter_by_type(common_cols, type_prefix, schema) if type_prefix else common_cols
        if not cols:
            continue

        if not inner_tokens:
            result.append(v)
            continue

        inner = v[inner_offset:]
        adjusted = [Token(t.kind, t.value, t.start - inner_offset, t.end - inner_offset) for t in inner_tokens]
        expanded = _expand_glob(inner, adjusted, cols)
        if expanded == [inner]:
            result.append(v)
        else:
            result.extend(expanded)

    return result


def _to_literal_name(expr: str) -> str:
    return re.sub(_NON_WORD, '_', expr)


def _build_aliased_columns(exprs: list[str], prefix: str) -> list[Column]:
    resolved = []
    for expr in exprs:
        column = F.expr(expr).alias(f"{prefix}{_to_literal_name(expr)}")
        resolved.append(column)
    return resolved


def _check_numeric(expr: str, data_type: DataType) -> str | None:
    if not isinstance(data_type, NumericType):
        return f"'{expr}' is not numeric, got {data_type.simpleString()}"
    return None


def _check_temporal(expr: str, data_type: DataType) -> str | None:
    if not isinstance(data_type, _TEMPORAL_TYPES):
        return f"'{expr}' is not temporal, got {data_type.simpleString()}"
    return None


def _batch_validate_types(
    checker: ColumnTypeChecker,
    fields: list[str],
    dataframes: list[DataFrame],
) -> list[str]:
    errors = []
    for i, df in enumerate(dataframes):
        if not fields:
            continue
        exprs = [F.expr(f) for f in fields]
        result_schema = df.select(*exprs).schema
        for expr, f in zip(fields, result_schema.fields):
            err = checker(expr, f.dataType)
            if err:
                errors.append(f"{err} in df[{i}]")
    return errors
