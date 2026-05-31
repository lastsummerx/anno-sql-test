from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from anno_sql_test.evaluators.spark._base import BaseSparkEvaluator
from anno_sql_test.evaluators.spark._util import _is_simple_column
from anno_sql_test.models import (
    AssertionResult,
    SingleAssert,
    SingleAssertEmpty,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)


class SingleAssertEvaluator(BaseSparkEvaluator[SingleAssert]):
    def evaluate(self, assertion: SingleAssert, dataframes: list[DataFrame]) -> AssertionResult:
        df = dataframes[0]
        total_rows = df.count()
        violated = df.filter(f"not ({assertion.predicate})")
        total_violated = violated.count()
        if total_violated == 0:
            return AssertionResult(assertion=assertion, passed=True)
        pct = total_violated / total_rows * 100
        return AssertionResult(
            assertion=assertion, passed=False,
            message=f"Found {total_violated} row(s) ({pct:.1f}%) violating: {assertion.predicate}",
        )


class SingleAssertEmptyEvaluator(BaseSparkEvaluator[SingleAssertEmpty]):
    def evaluate(self, assertion: SingleAssertEmpty, dataframes: list[DataFrame]) -> AssertionResult:
        df = dataframes[0]
        count = df.count()
        if count == 0:
            return AssertionResult(assertion=assertion, passed=True)
        return AssertionResult(
            assertion=assertion, passed=False,
            message=f"DataFrame is not empty, has {count} row(s)",
        )


class SingleAssertNotEmptyEvaluator(BaseSparkEvaluator[SingleAssertNotEmpty]):
    def evaluate(self, assertion: SingleAssertNotEmpty, dataframes: list[DataFrame]) -> AssertionResult:
        df = dataframes[0]
        if not df.isEmpty():
            return AssertionResult(assertion=assertion, passed=True)
        return AssertionResult(assertion=assertion, passed=False, message="DataFrame is empty")


class SingleAssertUniqueEvaluator(BaseSparkEvaluator[SingleAssertUnique]):
    def evaluate(self, assertion: SingleAssertUnique, dataframes: list[DataFrame]) -> AssertionResult:
        cols = assertion.fields
        col_expr = ", ".join(cols)
        df = dataframes[0]
        group_cols = [F.expr(c) if not _is_simple_column(c) else F.col(c) for c in cols]
        cnt_df = df.groupBy(*group_cols).agg(F.count("*").alias("_cnt"))
        row = cnt_df.agg(
            F.sum("_cnt").alias("total_rows"),
            F.count(F.lit(1)).alias("total_groups"),
            F.sum(F.when(F.col("_cnt") > 1, F.col("_cnt")).otherwise(0)).alias("dup_rows"),
            F.count(F.when(F.col("_cnt") > 1, 1)).alias("dup_groups"),
        ).collect()[0]
        dup_groups = row["dup_groups"]
        if dup_groups == 0:
            return AssertionResult(assertion=assertion, passed=True)
        pct_groups = dup_groups / row["total_groups"] * 100
        pct_rows = row["dup_rows"] / row["total_rows"] * 100
        return AssertionResult(
            assertion=assertion, passed=False,
            message=(
                f"Found {row['dup_rows']} row(s) ({pct_rows:.1f}%) "
                f"in {dup_groups} group(s) ({pct_groups:.1f}% of keys) "
                f"with duplicate values for columns: {col_expr}"
            ),
        )
