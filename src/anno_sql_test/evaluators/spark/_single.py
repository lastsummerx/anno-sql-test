from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace as replace_field
from functools import reduce
from itertools import chain
from typing import Any

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from anno_sql_test.evaluators.base import (
    BaseStepwiseAssertionEvaluator,
    StepResult,
)
from anno_sql_test.evaluators.spark._base import (
    BaseStepwiseSparkEvaluator,
    DelegatingStepwiseSparkFusedEvaluator,
)
from anno_sql_test.evaluators.spark._utils import (
    NamedColumn,
    _to_literal_name,
    extract_word_fields,
    resolve_fields,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    FusedAssertion,
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertion,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertPredicate,
    SingleAssertUnique,
)


@dataclass
class SingleAssertContext:
    dataframe: DataFrame
    total: Column
    namespace: str = ""


class BaseSingleDataFrameEvaluator[T: SingleAssertion](
    BaseStepwiseSparkEvaluator[T, SingleAssertContext, list[NamedColumn]],
):
    """处理单 DataFrame 断言的基础评估器"""

    TOTAL_COL = "_total"

    def __init__(self, sample_count: int = 0):
        self._sample_count = sample_count

    def validate(self, assertion: T, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if not dataframes:
            self.logger.warning("No DataFrames provided for %s", type(assertion).__name__)
            return [("No DataFrames provided", assertion)]
        self.logger.debug("%s validated, %d DataFrame(s)", type(assertion).__name__, len(dataframes))
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
        ctx = self.prepare_shared(dataframes)
        if self._sample_count > 0:
            ctx.dataframe.persist(StorageLevel.DISK_ONLY)
        return ctx

    def cleanup(self, prepared: SingleAssertContext) -> None:
        if self._sample_count > 0:
            prepared.dataframe.unpersist()

    def execute(self, prepared: SingleAssertContext, plan: list[NamedColumn]) -> Row:
        return prepared.dataframe.agg(prepared.total, *(p.column for p in plan)).collect()[0]

    def _sample_key_columns(self, assertion: T, all_columns: list[str]) -> list[str]:
        pred = getattr(assertion, 'predicate', None)
        if pred:
            return extract_word_fields([pred.expr], all_columns)
        return []


class SingleAssertPredicateEvaluator[T: SingleAssertPredicate](
    BaseSingleDataFrameEvaluator[T],
):
    @abstractmethod
    def _predicate_column(self, pred: str) -> Column:
        ...

    @abstractmethod
    def _is_violation(self, count: int) -> bool:
        ...

    @abstractmethod
    def _failure_detail(self, name: str, cnt: int, total: int) -> str:
        ...

    @abstractmethod
    def _failure_message(self, assertion: T, details: str) -> str:
        ...

    @abstractmethod
    def _sample_condition(self, pred: str) -> Column:
        ...

    def build(self, assertion: T, prepared: SingleAssertContext) -> list[NamedColumn]:
        predicates = resolve_fields([assertion.predicate], [prepared.dataframe])
        result = []
        for pred in predicates:
            name = self._column_prefix(prepared.namespace) + _to_literal_name(pred)
            col = F.count(self._predicate_column(pred)).alias(name)
            result.append(NamedColumn(name=name, column=col))
        return result

    def finalize(
        self, assertion: T,
        step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        failures = []
        for plan_entry in step_result.plan:
            cnt = exec_result[plan_entry.name]
            if self._is_violation(cnt):
                failures.append((plan_entry.name, cnt))
        if not failures:
            return [AssertionResult(assertion=assertion, passed=True)]
        total = exec_result[self.TOTAL_COL]
        details = "; ".join(
            self._failure_detail(name, cnt, total) for name, cnt in failures
        )
        return [AssertionResult(
            assertion=assertion, passed=False,
            message=self._failure_message(assertion, details),
        )]

    def sample_failure(
        self, assertion: T,
        step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> list[dict] | None:
        if self._sample_count <= 0:
            return None
        df = step_result.prepared.dataframe
        exec_result = step_result.executed
        plan = step_result.plan
        predicates = resolve_fields([assertion.predicate], [df])

        conditions = []
        for pred, nc in zip(predicates, plan):
            if self._is_violation(exec_result[nc.name]):
                conditions.append(self._sample_condition(pred))
        if not conditions:
            return None
        filter_cond = reduce(lambda a, b: a | b, conditions, F.lit(False))

        key_cols = extract_word_fields(predicates, df.columns) or df.columns
        rows = df.filter(filter_cond).select(*key_cols).take(self._sample_count)
        return [row.asDict() for row in rows]


class SingleAssertAllEvaluator(SingleAssertPredicateEvaluator[SingleAssertAll]):
    def _predicate_column(self, pred: str) -> Column:
        return F.when(~F.expr(pred), 1)

    def _is_violation(self, count: int) -> bool:
        return count > 0

    def _failure_detail(self, name: str, cnt: int, total: int) -> str:
        return f"{name} {cnt} row(s) ({cnt / total * 100:.1f}%) violated"

    def _failure_message(self, assertion, details: str) -> str:
        return f"Expected all rows to match {assertion.predicate}, but got: {details}"

    def _sample_condition(self, pred: str) -> Column:
        return ~F.expr(pred)


class SingleAssertAnyEvaluator(SingleAssertPredicateEvaluator[SingleAssertAny]):
    def _predicate_column(self, pred: str) -> Column:
        return F.when(F.expr(pred), 1)

    def _is_violation(self, count: int) -> bool:
        return count == 0

    def _failure_detail(self, name: str, cnt: int, total: int) -> str:
        return f"{name} {cnt} row(s) ({cnt / total * 100:.1f}%) violated"

    def _failure_message(self, assertion, details: str) -> str:
        return f"Expected at least 1 row to match {assertion.predicate}, but got: {details}"

    def _sample_condition(self, pred: str) -> Column:
        return ~F.expr(pred)


class SingleAssertNoneEvaluator(SingleAssertPredicateEvaluator[SingleAssertNone]):
    def _predicate_column(self, pred: str) -> Column:
        return F.when(F.expr(pred), 1)

    def _is_violation(self, count: int) -> bool:
        return count > 0

    def _failure_detail(self, name: str, cnt: int, total: int) -> str:
        return f"Found {name} {cnt} row(s) ({cnt / total * 100:.1f}%) matching"

    def _failure_message(self, assertion, details: str) -> str:
        return f"Expected 0 rows to match {assertion.predicate}, but got: {details}"

    def _sample_condition(self, pred: str) -> Column:
        return F.expr(pred)


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

    def sample_failure(
        self, assertion: SingleAssertEmpty, step_result: StepResult[SingleAssertContext, list[NamedColumn], Row],
    ) -> Any | None:
        rows = step_result.prepared.dataframe.tail(self._sample_count)
        return [row.asDict() for row in rows]


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


@dataclass
class AssertUniqueContext:
    dataframe: DataFrame
    field_names: list[str]


class SingleAssertUniqueEvaluator(
    BaseStepwiseSparkEvaluator[SingleAssertUnique, AssertUniqueContext, list[Column]],
):
    COUNT_COL = "_cnt"
    TOTAL_ROWS_COL = "_total_rows"
    TOTAL_GROUPS_COL = "_total_groups"
    DUP_ROWS_COL = "_dup_rows"
    DUP_GROUPS_COL = "_dup_groups"

    def __init__(self, sample_count: int = 0):
        self._sample_count = sample_count

    def validate(self, assertion: SingleAssertUnique, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if not dataframes:
            return [("No DataFrames provided", assertion)]
        return []

    def prepare(self, assertion: SingleAssertUnique, dataframes: list[DataFrame]) -> AssertUniqueContext:
        field_names = resolve_fields(assertion.fields, dataframes)
        exprs = [F.expr(f) for f in field_names]
        prepared = dataframes[0].groupBy(*exprs).agg(F.count(F.lit(1)).alias(self.COUNT_COL))
        if self._sample_count > 0:
            prepared.persist(StorageLevel.DISK_ONLY)
        return AssertUniqueContext(dataframe=prepared, field_names=field_names)

    def cleanup(self, prepared: AssertUniqueContext) -> None:
        if self._sample_count > 0:
            prepared.dataframe.unpersist()

    def build(self, assertion: SingleAssertUnique, prepared: AssertUniqueContext) -> list[Column]:
        is_dup = F.col(self.COUNT_COL) > 1
        return [
            F.sum(self.COUNT_COL).alias(self.TOTAL_ROWS_COL),
            F.count(F.lit(1)).alias(self.TOTAL_GROUPS_COL),
            F.sum(F.when(is_dup, F.col(self.COUNT_COL)).otherwise(0)).alias(self.DUP_ROWS_COL),
            F.count(F.when(is_dup, 1)).alias(self.DUP_GROUPS_COL),
        ]

    def execute(self, prepared: AssertUniqueContext, plan: list[Column]) -> Row:
        return prepared.dataframe.agg(*plan).collect()[0]

    def finalize(
        self, assertion: SingleAssertUnique, step_result: StepResult[AssertUniqueContext, list[Column], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        dup_groups = exec_result[self.DUP_GROUPS_COL]
        if dup_groups == 0:
            return [AssertionResult(assertion=assertion, passed=True)]
        col_expr = ", ".join(step_result.prepared.field_names)
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

    def sample_failure(
        self, assertion: SingleAssertUnique, step_result: StepResult[AssertUniqueContext, list[Column], Row],
    ) -> list[dict] | None:
        if self._sample_count <= 0:
            return None
        dup_df = step_result.prepared.dataframe.filter(F.col(self.COUNT_COL) > 1)
        rows = dup_df.take(self._sample_count)
        return [row.asDict() for row in rows]


class SinglePredicateFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[SingleAssertion, SingleAssertContext, list[NamedColumn]],
):
    def __init__(self, sample_count: int = 0) -> None:
        self._sample_count = sample_count
        self._assertion_evaluators: dict[type[SingleAssertion], BaseSingleDataFrameEvaluator[Any]] = {
            SingleAssertAll: SingleAssertAllEvaluator(sample_count=sample_count),
            SingleAssertAny: SingleAssertAnyEvaluator(sample_count=sample_count),
            SingleAssertNone: SingleAssertNoneEvaluator(sample_count=sample_count),
            SingleAssertEmpty: SingleAssertEmptyEvaluator(sample_count=sample_count),
            SingleAssertNotEmpty: SingleAssertNotEmptyEvaluator(sample_count=sample_count),
        }

    def get_evaluator_map(self) -> Mapping[
        type[SingleAssertion],
        BaseStepwiseAssertionEvaluator[SingleAssertion, DataFrame, SingleAssertContext, list[NamedColumn], Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[SingleAssertion], dataframes: list[DataFrame],
    ) -> list[SingleAssertContext]:
        self.logger.debug("Preparing %d SingleAssertion assertions", len(assertion.assertions))
        prepared = BaseSingleDataFrameEvaluator.prepare_shared(dataframes)
        if self._sample_count > 0:
            prepared.dataframe.persist(StorageLevel.DISK_ONLY)
        return [
            replace_field(prepared, namespace=f"asrt{i}")
            for i in range(len(assertion.assertions))
        ]

    def cleanup(self, prepared: list[SingleAssertContext]) -> None:
        if self._sample_count > 0:
            prepared[0].dataframe.unpersist()

    def execute(self, prepared: list[SingleAssertContext], plan: list[list[NamedColumn]]) -> Row:
        df = prepared[0].dataframe
        columns = [x.column for x in chain.from_iterable(plan)]
        return df.select(prepared[0].total, *columns).collect()[0]
