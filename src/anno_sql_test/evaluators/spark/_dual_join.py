from abc import abstractmethod
from functools import reduce
from operator import and_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from anno_sql_test.evaluators.spark._base import BaseSparkEvaluator
from anno_sql_test.evaluators.spark._util import (
    ColumnComparator,
    ColumnTypeChecker,
    _check_numeric,
    _check_temporal,
    _is_simple_column,
    _prepare_df,
    _resolve_col,
)
from anno_sql_test.models import (
    AssertionResult,
    DualJoinAssertEqual,
    DualJoinAssertion,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
)


def _resolve_star_values(
    values: list[str], left: DataFrame, right: DataFrame, keys: list[str],
) -> list[str] | None:
    if values == ["*"]:
        keys_set = set(keys)
        common = list(set(left.columns).intersection(right.columns) - keys_set)
        if not common:
            return None
        return common
    return values


def _check_missing_columns(values, left, right):
    missing_left = [v for v in values if _is_simple_column(v) and v not in left.columns]
    missing_right = [v for v in values if _is_simple_column(v) and v not in right.columns]
    if missing_left or missing_right:
        parts = []
        if missing_left:
            parts.append(f"left missing: {missing_left}")
        if missing_right:
            parts.append(f"right missing: {missing_right}")
        return "; ".join(parts)
    return None


def _evaluate_dual_assertion(
    assertion: DualJoinAssertion,
    dataframes: list[DataFrame],
    comparator: ColumnComparator,
    type_checker: ColumnTypeChecker | None = None,
):
    """公共的双DataFrame断言评估逻辑"""

    left, right = dataframes[0], dataframes[1]
    keys = assertion.keys
    values = _resolve_star_values(assertion.values, left, right, keys)
    if values is None:
        return AssertionResult(
            assertion=assertion, passed=False,
            message="No common value columns across both DataFrames",
        )

    left_prep, rkeys, rvals = _prepare_df(left, keys, values)
    right_prep, _, _ = _prepare_df(right, keys, values)

    keys_only = list(rkeys)

    msg = _check_missing_columns(values, left, right)
    if msg:
        return AssertionResult(assertion=assertion, passed=False, message=msg)

    joined = left_prep.alias("l").join(right_prep.alias("r"), keys_only, "fullouter")

    per_col_ok = {}
    for v in rvals:
        lv = _resolve_col("l", v)
        rv = _resolve_col("r", v)

        if type_checker is not None:
            err = type_checker(left_prep.schema, v)
            if err:
                return AssertionResult(assertion=assertion, passed=False, message=err)

        col_ok = (F.isnull(lv) & F.isnull(rv)) | (
            F.isnotnull(lv) & F.isnotnull(rv) & comparator(lv, rv)
        )
        per_col_ok[v] = col_ok

    if not per_col_ok:
        return AssertionResult(assertion=assertion, passed=True)

    all_ok = reduce(and_, per_col_ok.values())
    counts = joined.agg(
        F.count(F.lit(1)).alias("_total_rows"),
        F.sum(F.when(~all_ok, 1).otherwise(0)).alias("_total_violated"),
        *[F.sum(F.when(~per_col_ok[v], 1).otherwise(0)).alias(f"_violated_{v}") for v in rvals],
    ).collect()[0]

    total_violated = counts["_total_violated"]
    if total_violated == 0:
        return AssertionResult(assertion=assertion, passed=True)

    total_rows = counts["_total_rows"]
    total_pct = total_violated / total_rows * 100
    details = []
    for v in rvals:
        v_cnt = counts[f"_violated_{v}"]
        if v_cnt > 0:
            v_pct = v_cnt / total_rows * 100
            details.append(f"{v}: {v_cnt} row(s) ({v_pct:.1f}%)")
    return AssertionResult(
        assertion=assertion, passed=False,
        message=f"Found {total_violated} row(s) ({total_pct:.1f}%) with mismatches: {'; '.join(details)}",
    )


class BaseDualJoinAssertEvaluator[T: DualJoinAssertion](BaseSparkEvaluator[T]):
    def evaluate(self, assertion: T, dataframes: list[DataFrame]) -> AssertionResult:
        if len(dataframes) != 2:
            return AssertionResult(
                assertion=assertion, passed=False,
                message=f"Expected exactly 2 DataFrames, got {len(dataframes)}",
            )

        comparator = self._get_comparator(assertion)
        type_checker = self._get_type_checker()
        return _evaluate_dual_assertion(assertion, dataframes, comparator, type_checker)

    @abstractmethod
    def _get_comparator(self, assertion: T) -> ColumnComparator:
        """返回比较函数 (left_col, right_col) -> Column"""
        ...

    def _get_type_checker(self) -> ColumnTypeChecker | None:
        """返回类型检查函数，默认无检查"""
        return None


class DualJoinAssertEqualEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertEqual]):
    def _get_comparator(self, assertion: DualJoinAssertEqual) -> ColumnComparator:
        return lambda lv, rv: lv == rv


class DualJoinAssertNumericRatioApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertNumericRatioApprox]):
    def _get_comparator(self, assertion: DualJoinAssertNumericRatioApprox) -> ColumnComparator:
        ratio = assertion.ratio
        return lambda lv, rv: (
            F.abs(lv.cast("double") - rv.cast("double"))
            <= ratio * F.greatest(F.abs(lv.cast("double")), F.abs(rv.cast("double")))
        )

    def _get_type_checker(self):
        return _check_numeric


class DualJoinAssertNumericDeltaApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertNumericDeltaApprox]):
    def _get_comparator(self, assertion: DualJoinAssertNumericDeltaApprox) -> ColumnComparator:
        delta = assertion.delta
        return lambda lv, rv: F.abs(lv.cast("double") - rv.cast("double")) <= delta

    def _get_type_checker(self):
        return _check_numeric


class DualJoinAssertTemporalApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertTemporalApprox]):

    def _get_comparator(self, assertion: DualJoinAssertTemporalApprox) -> ColumnComparator:
        ds = assertion.duration_seconds
        return lambda lv, rv: F.abs(lv.cast("double") - rv.cast("double")) <= F.lit(ds)

    def _get_type_checker(self):
        return _check_temporal
