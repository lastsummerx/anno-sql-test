import re
from collections.abc import Callable

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

_SIMPLE_COL = re.compile(r'^[\w.]+$')


def _is_simple_column(s: str) -> bool:
    return bool(_SIMPLE_COL.match(s))


def _resolve_col(alias: str, v: str):
    if _is_simple_column(v):
        return F.col(f"{alias}.`{v}`")
    return F.expr(f"{alias}.({v})")


def _prepare_df(df: DataFrame, keys: list[str], values: list[str]):
    rkeys = []
    for i, k in enumerate(keys):
        if _is_simple_column(k):
            rkeys.append(k)
        else:
            df = df.withColumn(f"_key_{i}", F.expr(k))
            rkeys.append(f"_key_{i}")

    rvals = []
    for i, v in enumerate(values):
        if _is_simple_column(v):
            rvals.append(v)
        else:
            df = df.withColumn(f"_val_{i}", F.expr(v))
            rvals.append(f"_val_{i}")

    return df, rkeys, rvals


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


type ColumnComparator = Callable[[Column, Column], Column]


type ColumnTypeChecker = Callable[[StructType, str], str | None]
