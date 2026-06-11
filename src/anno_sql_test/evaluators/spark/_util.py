import re
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, NumericType, StructType, TimestampType

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
type ColumnTypeChecker = Callable[[StructType, str], str | None]


_NON_WORD = re.compile(r"\W")


def _resolve_fields(
    fields: list[str],
    dataframes: list[DataFrame],
) -> list[str]:
    if fields == ["*"]:
        common = list(set(dataframes[0].columns).intersection(
            *[set(df.columns) for df in dataframes[1:]],
        ))
        return common
    return fields


def _to_literal_name(expr: str) -> str:
    return re.sub(_NON_WORD, '_', expr)


def _build_aliased_columns(exprs: list[str], prefix: str) -> list[Column]:
    resolved = []
    for expr in exprs:
        column = F.expr(expr).alias(f"{prefix}{_to_literal_name(expr)}")
        resolved.append(column)
    return resolved


def _col_type_name(schema: StructType, col_name: str) -> str:
    return schema[col_name].dataType.simpleString()


def _check_numeric(schema: StructType, col_name: str) -> str | None:
    if not isinstance(schema[col_name].dataType, NumericType):
        return (
            f"Column '{col_name}' is not numeric, "
            f"got {_col_type_name(schema, col_name)}"
        )
    return None


def _check_temporal(schema: StructType, col_name: str) -> str | None:
    if not isinstance(schema[col_name].dataType, _TEMPORAL_TYPES):
        return (
            f"Column '{col_name}' is not temporal, "
            f"got {_col_type_name(schema, col_name)}"
        )
    return None
