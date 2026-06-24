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

from anno_sql_test.models import ColumnSpec, ExprColumn, FieldType, GlobTemplateColumn

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


_WORD_RE = re.compile(r'\w+')
_NON_WORD = re.compile(r"\W")
_TYPE_PREFIX_MAP = MappingProxyType({
    FieldType.NUMERIC: NumericType,
    FieldType.STRING: StringType,
    FieldType.TEMPORAL: _TEMPORAL_TYPES,
})


def _filter_by_type(cols: list[str], type_filter: FieldType, schema: StructType) -> list[str]:
    type_rule = _TYPE_PREFIX_MAP.get(type_filter)
    if type_rule is None:
        return cols
    return [c for c in cols if isinstance(schema[c].dataType, type_rule)]


def _filter_by_glob(cols: list[str], pattern: str) -> list[str]:
    regex = re.escape(pattern).replace(r'\*', '.*')
    return [c for c in cols if re.match(f'^{regex}$', c)]


def _filter_except(columns: list[str], patterns: list[str]) -> list[str]:
    return [
        c for c in columns
        if not any(_filter_by_glob([c], p) for p in patterns)
    ]


def resolve_fields(values: list[ColumnSpec], dataframes: list[DataFrame]) -> list[str]:
    if not values:
        return []

    common_cols = sorted(set(dataframes[0].columns).intersection(
        *(set(df.columns) for df in dataframes[1:]),
    ))
    schema = dataframes[0].schema

    result = []
    for v in values:
        match v:
            case ExprColumn(expr):
                result.append(expr)

            case GlobTemplateColumn(glob=glob, type_filter=tf, excepts=exc, expr=template):
                cols = _filter_by_type(common_cols, tf, schema) if tf else common_cols
                if not cols:
                    continue
                expanded = _filter_by_glob(cols, glob)
                if exc:
                    expanded = _filter_except(expanded, exc)
                result.extend(template.format(col=col) for col in expanded)

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


def extract_word_fields(expressions: list[str], all_columns: list[str]) -> list[str]:
    fields = set()
    for expr in expressions:
        fields.update(_WORD_RE.findall(expr))
    return sorted(f for f in fields if f in all_columns)
