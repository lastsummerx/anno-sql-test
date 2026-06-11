from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace as replace_field
from itertools import chain
from typing import Any

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F

from anno_sql_test.evaluators.base import (
    BaseStepwiseAssertionEvaluator,
    StepResult,
)
from anno_sql_test.evaluators.spark._base import (
    BaseStepwiseSparkEvaluator,
    DelegatingStepwiseSparkFusedEvaluator,
)
from anno_sql_test.evaluators.spark._util import NamedColumn, _to_literal_name
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    FusedAssertion,
    SingleAssertAll,
    SingleAssertEmpty,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)


@dataclass
class SingleAssertContext:
    dataframe: DataFrame
    total: Column
    namespace: str = ""


type BaseSingleAssrtion = SingleAssertAll | SingleAssertEmpty | SingleAssertNotEmpty


class BaseSingleDataFrameEvaluator[T: Assertion](
    BaseStepwiseSparkEvaluator[T, SingleAssertContext, list[NamedColumn]],
):
    """处理单 DataFrame 断言的基础评估器"""

    TOTAL_COL = "_total"

    def validate(self, assertion: T, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if not dataframes:
            return [("No DataFrames provided", assertion)]
        return []

    @classmethod
    def _column_prefix(cls, namespace: str) -> str:
        return f"_{namespace}_" if namespace else ""

    @classmethod
    def prepare_shared(cls, dataframes: list[DataFrame]) -> SingleAssertContext:
        return SingleAssertContext(
            dataframe=dataframes[0],
            total=F.count(F.lit(1)).alias(cls.TOTAL_COL),
        )

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> SingleAssertContext:
        return self.prepare_shared(dataframes)

    def execute(self, prepared: SingleAssertContext, plan: list[NamedColumn]) -> Row:
        return prepared.dataframe.agg(prepared.total, *(p.column for p in plan)).collect()[0]


class SingleAssertEvaluator(BaseSingleDataFrameEvaluator[SingleAssertAll]):
    def build(self, assertion: SingleAssertAll, prepared: SingleAssertContext) -> list[NamedColumn]:
        name = self._column_prefix(prepared.namespace) + _to_literal_name(assertion.predicate)
        return [
            NamedColumn(name=name, column=F.count(F.when(~F.expr(assertion.predicate), 1)).alias(name)),
        ]

    def finalize(
        self, assertion: SingleAssertAll, step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        violated = exec_result[step_result.plan[0].name]
        if violated == 0:
            return [AssertionResult(assertion=assertion, passed=True)]
        total = exec_result[self.TOTAL_COL]
        pct = violated / total * 100
        return [AssertionResult(
            assertion=assertion, passed=False,
            message=f"Found {violated} row(s) ({pct:.1f}%) violating: {assertion.predicate}",
        )]


class SingleAssertEmptyEvaluator(BaseSingleDataFrameEvaluator[SingleAssertEmpty]):
    def build(self, assertion: SingleAssertEmpty, prepared: SingleAssertContext) -> list[NamedColumn]:
        return []

    def finalize(
        self, assertion: SingleAssertEmpty, step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> list[AssertionResult]:
        count = step_result.executed[self.TOTAL_COL]
        if count == 0:
            return [AssertionResult(assertion=assertion, passed=True)]
        return [AssertionResult(
            assertion=assertion, passed=False,
            message=f"DataFrame is not empty, has {count} row(s)",
        )]


class SingleAssertNotEmptyEvaluator(BaseSingleDataFrameEvaluator[SingleAssertNotEmpty]):
    def build(self, assertion: SingleAssertNotEmpty, prepared: SingleAssertContext) -> list[NamedColumn]:
        return []

    def finalize(
        self, assertion: SingleAssertNotEmpty, step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> list[AssertionResult]:
        count = step_result.executed[self.TOTAL_COL]
        if count > 0:
            return [AssertionResult(assertion=assertion, passed=True)]
        return [AssertionResult(assertion=assertion, passed=False, message="DataFrame is empty")]


class SingleAssertUniqueEvaluator(
    BaseStepwiseSparkEvaluator[SingleAssertUnique, DataFrame, list[Column]],
):
    COUNT_COL = "_cnt"
    TOTAL_ROWS_COL = "_total_rows"
    TOTAL_GROUPS_COL = "_total_groups"
    DUP_ROWS_COL = "_dup_rows"
    DUP_GROUPS_COL = "_dup_groups"

    def validate(self, assertion: SingleAssertUnique, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if not dataframes:
            return [("No DataFrames provided", assertion)]
        return []

    def prepare(self, assertion: SingleAssertUnique, dataframes: list[DataFrame]) -> DataFrame:
        fields = (F.expr(field) for field in assertion.fields)
        return dataframes[0].groupBy(*fields).agg(F.count(F.lit(1)).alias(self.COUNT_COL))

    def build(self, assertion: SingleAssertUnique, prepared: DataFrame) -> list[Column]:
        is_dup = F.col(self.COUNT_COL) > 1
        return [
            F.sum(self.COUNT_COL).alias(self.TOTAL_ROWS_COL),
            F.count(F.lit(1)).alias(self.TOTAL_GROUPS_COL),
            F.sum(F.when(is_dup, F.col(self.COUNT_COL)).otherwise(0)).alias(self.DUP_ROWS_COL),
            F.count(F.when(is_dup, 1)).alias(self.DUP_GROUPS_COL),
        ]

    def execute(self, prepared: DataFrame, plan: list[Column]) -> Row:
        return prepared.agg(*plan).collect()[0]

    def finalize(
        self, assertion: SingleAssertUnique, step_result: StepResult[DataFrame, list[Column], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        dup_groups = exec_result[self.DUP_GROUPS_COL]
        if dup_groups == 0:
            return [AssertionResult(assertion=assertion, passed=True)]
        col_expr = ", ".join(assertion.fields)
        pct_groups = dup_groups / exec_result[self.TOTAL_GROUPS_COL] * 100
        pct_rows = exec_result[self.DUP_ROWS_COL] / exec_result[self.TOTAL_ROWS_COL] * 100
        return [AssertionResult(
            assertion=assertion, passed=False,
            message=(
                f"Found {exec_result[self.DUP_ROWS_COL]} row(s) ({pct_rows:.1f}%) "
                f"in {dup_groups} group(s) ({pct_groups:.1f}% of keys) "
                f"with duplicate values for columns: {col_expr}"
            ),
        )]


class SinglePredicateFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[BaseSingleAssrtion, SingleAssertContext, list[NamedColumn]],
):
    def __init__(self) -> None:
        self._assertion_evaluators: dict[type[BaseSingleAssrtion], BaseSingleDataFrameEvaluator[Any]] = {
            SingleAssertAll: SingleAssertEvaluator(),
            SingleAssertEmpty: SingleAssertEmptyEvaluator(),
            SingleAssertNotEmpty: SingleAssertNotEmptyEvaluator(),
        }

    def get_evaluator_map(self) -> Mapping[
        type[BaseSingleAssrtion],
        BaseStepwiseAssertionEvaluator[BaseSingleAssrtion, DataFrame, SingleAssertContext, list[NamedColumn], Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[BaseSingleAssrtion], dataframes: list[DataFrame],
    ) -> list[SingleAssertContext]:
        prepared = BaseSingleDataFrameEvaluator.prepare_shared(dataframes)
        return [
            replace_field(prepared, namespace=f"asrt{i}")
            for i in range(len(assertion.assertions))
        ]

    def execute(self, prepared: list[SingleAssertContext], plan: list[list[NamedColumn]]) -> Row:
        df = prepared[0].dataframe
        columns = [x.column for x in chain.from_iterable(plan)]
        return df.select(prepared[0].total, *columns).collect()[0]
