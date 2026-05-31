import re
from abc import abstractmethod

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from anno_sql_test.evaluators.spark._base import BaseSparkEvaluator
from anno_sql_test.evaluators.spark._util import (
    ColumnComparator,
    ColumnTypeChecker,
    _check_numeric,
    _check_temporal,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    MultiAggAssertEqual,
    MultiAggAssertion,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
)


def _build_multi_aggs(
    dataframes: list[DataFrame], fields: list[str], agg: str,
) -> tuple[list[str], list[DataFrame]]:
    if fields == ["*"]:
        common = list(set(dataframes[0].columns).intersection(
            *[set(df.columns) for df in dataframes[1:]],
        ))
        if not common:
            return [], []
        aggs = [F.expr(f"{agg}(`{c}`)").alias(f"{agg}_{c}") for c in common]
    else:
        aggs = [
            F.expr(f"{agg}({f})").alias(f"{agg}_{re.sub(r'\\W', '_', f)}")
            for f in fields
        ]

    df_aggs = [df.agg(*aggs) for df in dataframes]
    return df_aggs[0].columns, df_aggs


def _build_pair_comparisons(
    n: int,
    col_names: list[str],
    comparator: ColumnComparator,
) -> tuple[Column, list[tuple[str, Column, int, int, str]]]:
    all_ok_expr = None
    ok_entries: list[tuple[str, Column, int, int, str]] = []
    for i in range(n):
        for j in range(i + 1, n):
            for c in col_names:
                left = F.col(f"df{i}_{c}")
                right = F.col(f"df{j}_{c}")

                col_ok = (F.isnull(left) & F.isnull(right)) | (
                    F.isnotnull(left) & F.isnotnull(right) & comparator(left, right)
                )
                alias = f"_ok_{len(ok_entries)}"
                ok_entries.append((alias, col_ok, i, j, c))
                all_ok_expr = col_ok if all_ok_expr is None else (all_ok_expr & col_ok)
    assert all_ok_expr is not None
    return all_ok_expr, ok_entries


def _compare_multi_agg(
    df_aggs: list[DataFrame],
    col_names: list[str],
    assertion: Assertion,
    comparator: ColumnComparator,
    type_checker: ColumnTypeChecker | None = None,
) -> AssertionResult:
    n = len(df_aggs)

    combined = df_aggs[0].select(*[F.col(c).alias(f"df0_{c}") for c in col_names])
    for i in range(1, n):
        combined = combined.crossJoin(
            df_aggs[i].select(*[F.col(c).alias(f"df{i}_{c}") for c in col_names]),
        )

    if type_checker is not None:
        for c in col_names:
            err = type_checker(df_aggs[0].schema, c)
            if err:
                return AssertionResult(
                    assertion=assertion, passed=False, message=err,
                )

    all_ok_expr, ok_entries = _build_pair_comparisons(n, col_names, comparator)

    value_cols = [F.col(f"df{i}_{c}") for i in range(n) for c in col_names]

    row = combined.select(
        F.when(~all_ok_expr, F.lit(1)).otherwise(F.lit(0)).alias("_violated"),
        *[col_ok.alias(alias) for alias, col_ok, _, _, _ in ok_entries],
        *value_cols,
    ).collect()[0]

    if not row["_violated"]:
        return AssertionResult(assertion=assertion, passed=True)

    bad_parts = []
    for alias, _, i, j, c in ok_entries:
        if not row[alias]:
            left_val = row[f"df{i}_{c}"]
            right_val = row[f"df{j}_{c}"]
            bad_parts.append(f"DF{i}.{c}={left_val} vs DF{j}.{c}={right_val}")

    return AssertionResult(
        assertion=assertion, passed=False,
        message=f"Aggregation mismatch: {'; '.join(bad_parts)}",
    )


class BaseMultiAggEvaluator[T: MultiAggAssertion](BaseSparkEvaluator[T]):
    """模板基类 – 消除多DataFrame聚合断言中的重复代码"""

    def evaluate(self, assertion: T, dataframes: list[DataFrame]) -> AssertionResult:
        if len(dataframes) < 2:
            return AssertionResult(
                assertion=assertion, passed=False,
                message=f"Expected at least 2 DataFrames, got {len(dataframes)}",
            )

        col_names, df_aggs = _build_multi_aggs(dataframes, assertion.fields, assertion.agg.lower())
        if not col_names:
            return AssertionResult(
                assertion=assertion, passed=False,
                message="No common columns across all DataFrames",
            )

        comparator = self._get_comparator(assertion)
        type_checker = self._get_type_checker()

        return _compare_multi_agg(
            df_aggs, col_names, assertion, comparator,
            type_checker=type_checker,
        )

    @abstractmethod
    def _get_comparator(self, assertion: T) -> ColumnComparator:
        ...

    def _get_type_checker(self) -> ColumnTypeChecker | None:
        """返回类型检查函数（如 _check_numeric），默认无检查"""
        return None


class MultiAggAssertEqualEvaluator(BaseMultiAggEvaluator[MultiAggAssertEqual]):
    def _get_comparator(self, assertion: MultiAggAssertEqual) -> ColumnComparator:
        return lambda ac, bc: ac == bc


class MultiAggAssertNumericRatioApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertNumericRatioApprox]):
    def _get_comparator(self, assertion: MultiAggAssertNumericRatioApprox) -> ColumnComparator:
        ratio = assertion.ratio
        return lambda ac, bc: (
            F.abs(ac.cast("double") - bc.cast("double"))
            <= ratio * F.greatest(F.abs(ac.cast("double")), F.abs(bc.cast("double")))
        )

    def _get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class MultiAggAssertNumericDeltaApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertNumericDeltaApprox]):
    def _get_comparator(self, assertion: MultiAggAssertNumericDeltaApprox) -> ColumnComparator:
        delta = assertion.delta
        return lambda ac, bc: F.abs(ac.cast("double") - bc.cast("double")) <= F.lit(delta)

    def _get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class MultiAggAssertTemporalApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertTemporalApprox]):
    def _get_comparator(self, assertion: MultiAggAssertTemporalApprox) -> ColumnComparator:
        ds = assertion.duration_seconds
        return lambda ac, bc: (
            F.abs(ac.cast("double") - bc.cast("double")) <= F.lit(ds)
        )

    def _get_type_checker(self) -> ColumnTypeChecker:
        return _check_temporal
