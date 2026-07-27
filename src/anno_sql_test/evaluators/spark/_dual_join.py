from abc import abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from operator import and_
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
    ColumnTypeChecker,
    NamedColumn,
    _batch_validate_types,
    _build_aliased_columns,
    _check_numeric,
    _check_temporal,
    resolve_fields,
    sample_failure_distribute,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    ColumnSpec,
    DualJoinAssertEqual,
    DualJoinAssertion,
    DualJoinAssertLambda,
    DualJoinAssertNumericApprox,
    DualJoinAssertTemporalApprox,
    DualRowsAssertEqual,
    DualRowsAssertion,
    ExprColumn,
    FusedAssertion,
    LambdaFunc,
)


@dataclass
class DualJoinContext:
    dataframe: DataFrame
    total: Column
    original_keys: list[str]
    original_values: list[str]
    col_for: Callable[[str, str], str]
    namespace: str = ""


class BaseDualJoinAssertEvaluator[T: DualJoinAssertion](
    BaseStepwiseSparkEvaluator[T, DualJoinContext, list[Column]],
):
    KEY_PREFIX = "_key_"
    LEFT_DF_ALIAS = "l"
    RIGHT_DF_ALIAS = "r"
    TOTAL_COL = "_total"
    TOTAL_VIOLATED_COL = "_total_violated"
    _JOIN_TYPE_COL = "_join_type"

    def __init__(self, sample_count: int = 0):
        self._sample_count = sample_count

    def validate(self, assertion: T, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if len(dataframes) != 2:
            self.logger.warning("Expected 2 DataFrames, got %d for %s", len(dataframes), type(assertion).__name__)
            return [(f"Expected exactly 2 DataFrames, got {len(dataframes)}", assertion)]
        fields = resolve_fields(assertion.values, dataframes)
        if not fields:
            self.logger.warning("No common columns for %s", type(assertion).__name__)
            return [("No common columns across all DataFrames", assertion)]
        type_checker = self.get_type_checker()
        if type_checker is None:
            return []
        errors = _batch_validate_types(type_checker, fields, dataframes)
        if errors:
            self.logger.warning("Type validation failed for %s: %s", type(assertion).__name__, errors)
        return [(";".join(errors), assertion)] if errors else []

    @classmethod
    def prepare_values(
        cls, dataframes: list[DataFrame], values: list[ColumnSpec], namespace: str = "",
    ) -> list[NamedColumn]:
        original_values = resolve_fields(values, dataframes)
        prefix = f"_{namespace}_val_" if namespace else "_val_"
        value_cols = _build_aliased_columns(original_values, prefix)
        return [NamedColumn(name=ov, column=vc, namespace=namespace) for ov, vc in zip(original_values, value_cols)]

    @classmethod
    def prepare_shared(
        cls, dataframes: list[DataFrame], keys: list[ColumnSpec], values: list[NamedColumn], join_type: str,
    ) -> DualJoinContext:
        left, right = dataframes[0], dataframes[1]
        resolved_keys = resolve_fields(keys, dataframes)
        key_cols = _build_aliased_columns(resolved_keys, cls.KEY_PREFIX)
        original_values = [x.name for x in values]
        value_cols = [x.column for x in values]

        left_prep = left.select(F.lit(1).alias("_lm"), *key_cols, *value_cols).alias(cls.LEFT_DF_ALIAS)
        right_prep = right.select(F.lit(1).alias("_rm"), *key_cols, *value_cols).alias(cls.RIGHT_DF_ALIAS)

        key_names = left_prep.columns[1:1 + len(key_cols)]
        val_names = left_prep.columns[1 + len(key_cols):]
        if key_names:
            df = left_prep.join(right_prep, key_names, join_type)
        else:
            df = left_prep.crossJoin(right_prep)
        has_left = F.expr(f"{cls.LEFT_DF_ALIAS}._lm").isNotNull()
        has_right = F.expr(f"{cls.RIGHT_DF_ALIAS}._rm").isNotNull()
        df = df.withColumn(
            cls._JOIN_TYPE_COL,
            F.when(has_left & ~has_right, -1)
             .when(~has_left & has_right, 1)
             .otherwise(0),
        )
        total = F.count(F.lit(1)).alias(cls.TOTAL_COL)
        values_with_ns = [(x.namespace, x.name) for x in values]
        col_dict = dict(zip(values_with_ns, val_names))

        def col_for(c: str, ns: str = "") -> str:
            return col_dict[(ns, c)]
        return DualJoinContext(
            dataframe=df,
            total=total,
            original_keys=key_names,
            original_values=original_values,
            col_for=col_for,
        )

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> DualJoinContext:
        self.logger.debug("Preparing %s: keys=%s", type(assertion).__name__, assertion.keys)
        values = self.prepare_values(dataframes, list(assertion.values))
        ctx = self.prepare_shared(dataframes, list(assertion.keys), values, assertion.join_type)
        if self._sample_count > 0:
            ctx.dataframe.persist(StorageLevel.DISK_ONLY)
        return ctx

    def cleanup(self, prepared: DualJoinContext) -> None:
        if self._sample_count > 0:
            prepared.dataframe.unpersist()

    def _total_violated_col(self, ns: str = "") -> str:
        return f"{self.TOTAL_VIOLATED_COL}_{ns}" if ns else self.TOTAL_VIOLATED_COL

    def _build_cmps(self, assertion: T, prepared: DualJoinContext) -> list[Column]:
        comparator = self.get_comparator(assertion)

        cmps = []
        for name in prepared.original_values:
            v = prepared.col_for(name, prepared.namespace)
            lv_str = f"{self.LEFT_DF_ALIAS}.{v}"
            rv_str = f"{self.RIGHT_DF_ALIAS}.{v}"
            lv = F.expr(lv_str)
            rv = F.expr(rv_str)

            cmp = (lv.isNull() & rv.isNull()) | (
                lv.isNotNull() & rv.isNotNull() & F.expr(comparator.format(lv_str, rv_str))
            )

            cmps.append(cmp)

        cmps.append(F.col(self._JOIN_TYPE_COL) == 0)

        return cmps

    def build(self, assertion: T, prepared: DualJoinContext) -> list[Column]:
        cmps = self._build_cmps(assertion, prepared)
        tv_name = self._total_violated_col(prepared.namespace)
        total_violated = F.count(F.when(~reduce(and_, cmps, F.lit(True)), 1)).alias(tv_name)

        rst = [
            F.count(F.when(~cmp, 1)).alias(prepared.col_for(name, prepared.namespace))
            for name, cmp in zip(prepared.original_values, cmps[:len(prepared.original_values)])
        ]
        rst.append(total_violated)
        return rst

    def execute(self, prepared: DualJoinContext, plan: list[Column]) -> Row:
        return prepared.dataframe.agg(prepared.total, *plan).collect()[0]

    @staticmethod
    def _check_row_tolerance(assertion: T, total_rows: int, total_violated: int) -> bool:
        row_ratio = assertion.row_ratio
        row_delta = assertion.row_delta
        if row_delta > 0:
            return total_violated <= row_delta
        if row_ratio > 0.0:
            return total_violated <= total_rows * row_ratio if total_rows > 0 else True
        return total_violated == 0

    def finalize(
        self, assertion: T, step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        prepared = step_result.prepared
        tv_name = self._total_violated_col(prepared.namespace)
        total_violated = exec_result[tv_name]
        self.logger.debug("Finalizing %s: %d violated rows", type(assertion).__name__, total_violated)

        total_rows = exec_result[self.TOTAL_COL]
        if self._check_row_tolerance(assertion, total_rows, total_violated):
            return [AssertionResult(assertion=assertion, passed=True)]

        details = []
        for original_name in prepared.original_values:
            name = prepared.col_for(original_name, prepared.namespace)
            if exec_result[name] > 0:
                details.append(f"{original_name}: {self._format_violated(total_rows, exec_result[name])}")

        return [AssertionResult(
            assertion=assertion, passed=False,
            message=f"Found {self._format_violated(total_rows, total_violated)} with mismatches: {'; '.join(details)}",
        )]

    def _format_violated(self, total, violated) -> str:
        pct = violated / total * 100 if total > 0 else 0
        return f"{violated} row(s) ({pct:.1f}%)"

    def sample_failure(
        self, assertion: T, step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[dict] | None:
        if self._sample_count <= 0:
            return None
        prepared = step_result.prepared
        exec_result = step_result.executed
        df = prepared.dataframe
        conditions = self._build_cmps(assertion, step_result.prepared)

        violated_indices = [
            idx for idx, name in enumerate(prepared.original_values)
            if exec_result[prepared.col_for(name, prepared.namespace)] > 0
        ]
        if not violated_indices:
            return None

        violation_cond = reduce(lambda a, b: a | ~b, [conditions[i] for i in violated_indices], F.lit(False))

        jt = F.col(self._JOIN_TYPE_COL)
        sub_filters = {
            "left-only": violation_cond & (jt == -1),
            "right-only": violation_cond & (jt == 1),
            "mismatch": violation_cond & (jt == 0),
        }

        counts_row = df.agg(*[
            F.count(F.when(f, 1)).alias(k)
            for k, f in sub_filters.items()
        ]).collect()[0]

        case_counts = {k: int(counts_row[k]) for k in sub_filters if int(counts_row[k]) > 0}
        if not case_counts:
            return None

        if len(case_counts) > self._sample_count:
            self.logger.warning(
                f"Number of failure cases ({len(case_counts)}) exceeds sample_count ({self._sample_count}), "
                "sampling will be limited")
        per_case = sample_failure_distribute(case_counts, self._sample_count)

        key_cols_base = [F.col(c).alias(c.removeprefix(self.KEY_PREFIX)) for c in step_result.prepared.original_keys]
        failed_names = [prepared.original_values[i] for i in violated_indices]
        value_cols = []
        for name in failed_names:
            v = prepared.col_for(name, prepared.namespace)
            value_cols.append(F.expr(f"{self.LEFT_DF_ALIAS}.{v}").alias(f"{self.LEFT_DF_ALIAS}.{name}"))
            value_cols.append(F.expr(f"{self.RIGHT_DF_ALIAS}.{v}").alias(f"{self.RIGHT_DF_ALIAS}.{name}"))
        all_cols = key_cols_base + value_cols

        result = []
        for k, n in per_case.items():
            rows = df.filter(sub_filters[k]).select(*all_cols).take(n)
            result.extend(row.asDict() for row in rows)
        return result if result else None

    @abstractmethod
    def get_comparator(self, assertion: T) -> LambdaFunc:
        ...

    def get_type_checker(self) -> ColumnTypeChecker | None:
        return None


class DualJoinAssertLambdaEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertLambda]):
    def get_comparator(self, assertion: DualJoinAssertLambda) -> LambdaFunc:
        return assertion.comparator


class DualJoinAssertEqualEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertEqual]):
    def get_comparator(self, assertion: DualJoinAssertEqual) -> LambdaFunc:
        return LambdaFunc.from_str("(a, b) -> a = b")


class DualJoinAssertNumericApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertNumericApprox]):
    def get_comparator(self, assertion: DualJoinAssertNumericApprox) -> LambdaFunc:
        if assertion.val_ratio:
            comparator = LambdaFunc.from_str(
                f'(a, b) -> abs(a - b) <= {assertion.val_ratio} * greatest(abs(a), abs(b))',
            )
        elif assertion.val_delta:
            comparator = LambdaFunc.from_str(f"(a, b) -> abs(a - b) <= {assertion.val_delta}")
        else:
            comparator = LambdaFunc.from_str("(a, b) -> a = b")
        return comparator

    def get_type_checker(self):
        return _check_numeric


class DualJoinAssertTemporalApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertTemporalApprox]):
    def get_comparator(self, assertion: DualJoinAssertTemporalApprox) -> LambdaFunc:
        return LambdaFunc.from_str(
            f"(a, b) -> abs(cast(a as double) - cast(b as double)) <= {assertion.duration_seconds}",
        )

    def get_type_checker(self):
        return _check_temporal


class BaseRowsAssertEvaluator[T: DualRowsAssertion](
    BaseStepwiseSparkEvaluator[T, DualJoinContext, list[Column]],
):
    _COUNT_COL = "_cnt"
    _TOTAL_ROWS_COL = "_total_rows"
    _TOTAL_VIOLATED_ROWS_COL = "_total_violated_rows"

    def __init__(self, sample_count: int = 0):
        self._sample_count = sample_count
        self._join_evaluator = DualJoinAssertLambdaEvaluator(sample_count)

    @abstractmethod
    def _failure_message(self, assertion: T, total_count: int, total_violated: int) -> str | None:
        ...

    def _convert_assertion(self, assertion: T) -> DualJoinAssertLambda:
        return DualJoinAssertLambda(
            keys=assertion.fields,
            values=(ExprColumn(expr=self._COUNT_COL),),
            comparator=LambdaFunc.from_str("(a, b) -> a = b"),
            row_ratio=assertion.row_ratio,
            row_delta=assertion.row_delta,
        )

    def validate(self, assertion: Any, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if len(dataframes) != 2:
            return [(f"Expected exactly 2 DataFrames, got {len(dataframes)}", assertion)]
        return []

    def prepare(self, assertion: Any, dataframes: list[DataFrame]) -> DualJoinContext:
        self.logger.debug("Preparing %s: fields=%s", type(assertion).__name__, assertion.fields)
        fields = resolve_fields(assertion.fields, dataframes)
        if not fields:
            fields = sorted(set(dataframes[0].columns).intersection(dataframes[1].columns))
        field_specs: list[ColumnSpec] = [ExprColumn(expr=f) for f in fields]
        values = [NamedColumn(name=self._COUNT_COL, column=F.col(self._COUNT_COL))]
        left_agg = dataframes[0].groupBy(*fields).agg(F.count(F.lit(1)).alias(self._COUNT_COL))
        right_agg = dataframes[1].groupBy(*fields).agg(F.count(F.lit(1)).alias(self._COUNT_COL))
        ctx = BaseDualJoinAssertEvaluator.prepare_shared([left_agg, right_agg], field_specs, values, "full")
        if self._sample_count > 0:
            ctx.dataframe.persist(StorageLevel.DISK_ONLY)
        return ctx

    def build(self, assertion: Any, prepared: DualJoinContext) -> list[Column]:
        join_assertion = self._convert_assertion(assertion)
        self._join_evaluator.get_comparator(join_assertion)
        rst = self._join_evaluator.build(join_assertion, prepared)
        left_count = F.expr(f"coalesce({BaseDualJoinAssertEvaluator.LEFT_DF_ALIAS}.{self._COUNT_COL}, 0)")
        right_count = F.expr(f"coalesce({BaseDualJoinAssertEvaluator.RIGHT_DF_ALIAS}.{self._COUNT_COL}, 0)")
        rst.append(F.sum(F.greatest(left_count, right_count)).alias(self._TOTAL_ROWS_COL))
        rst.append(F.sum(F.abs(left_count - right_count)).alias(self._TOTAL_VIOLATED_ROWS_COL))
        return rst

    def execute(self, prepared: DualJoinContext, plan: list[Column]) -> Row:
        return self._join_evaluator.execute(prepared, plan)

    def finalize(
        self, assertion: Any,
        step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        total_count = int(exec_result[self._TOTAL_ROWS_COL])
        total_violated = int(exec_result[self._TOTAL_VIOLATED_ROWS_COL])
        msg = self._failure_message(assertion, total_count, total_violated)
        if msg is None:
            return [AssertionResult(assertion=assertion, passed=True)]
        return [AssertionResult(assertion=assertion, passed=False, message=msg)]

    def cleanup(self, prepared: DualJoinContext) -> None:
        return self._join_evaluator.cleanup(prepared)

    def sample_failure(
        self, assertion: Any,
        step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[dict] | None:
        join_assertion = self._convert_assertion(assertion)
        return self._join_evaluator.sample_failure(join_assertion, step_result)


class DualRowsAssertEqualEvaluator(BaseRowsAssertEvaluator[DualRowsAssertEqual]):
    def _failure_message(
        self, assertion: DualRowsAssertEqual, total_count: int, total_violated: int,
    ) -> str | None:
        if BaseDualJoinAssertEvaluator._check_row_tolerance(assertion, total_count, total_violated):
            return None
        return (
            f"{total_violated} of {total_count} group(s) "
            f"({total_violated / total_count * 100:.1f}%) have count mismatch"
        )


class DualJoinFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[DualJoinAssertion, DualJoinContext, list[Column]],
):
    def __init__(self, sample_count: int = 0) -> None:
        self._sample_count = sample_count
        self._assertion_evaluators: dict[type[DualJoinAssertion], BaseDualJoinAssertEvaluator[Any]] = {
            DualJoinAssertEqual: DualJoinAssertEqualEvaluator(sample_count=sample_count),
            DualJoinAssertNumericApprox: DualJoinAssertNumericApproxEvaluator(sample_count=sample_count),
            DualJoinAssertTemporalApprox: DualJoinAssertTemporalApproxEvaluator(sample_count=sample_count),
            DualJoinAssertLambda: DualJoinAssertLambdaEvaluator(sample_count=sample_count),
        }

    def get_evaluator_map(self) -> Mapping[
        type[DualJoinAssertion],
        BaseStepwiseAssertionEvaluator[DualJoinAssertion, DataFrame, DualJoinContext, list[Column], Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[DualJoinAssertion], dataframes: list[DataFrame],
    ) -> list[DualJoinContext]:
        self.logger.debug("Fused prepare for %d DualJoinAssertion assertions", len(assertion.assertions))
        all_values = []
        keys = list(assertion.assertions[0].keys)
        for i, asrt in enumerate(assertion.assertions):
            evaluator = self._assertion_evaluators[type(asrt)]
            values = evaluator.prepare_values(dataframes, list(asrt.values), namespace=f"asrt{i}")
            all_values.append(values)
        prepared_all = BaseDualJoinAssertEvaluator.prepare_shared(
            dataframes, keys, list(chain.from_iterable(all_values)), assertion.assertions[0].join_type,
        )
        if self._sample_count > 0:
            prepared_all.dataframe.persist(StorageLevel.DISK_ONLY)
        ctxs = []
        for idx, values in enumerate(all_values):
            sub_original_values = [x.name for x in values]
            ns = f"asrt{idx}"
            ctxs.append(DualJoinContext(
                dataframe=prepared_all.dataframe,
                total=prepared_all.total,
                original_keys=prepared_all.original_keys,
                original_values=sub_original_values,
                col_for=lambda c, ns=ns: prepared_all.col_for(c, ns),
                namespace=ns,
            ))
        return ctxs

    def cleanup(self, prepared: list[DualJoinContext]) -> None:
        if self._sample_count > 0:
            prepared[0].dataframe.unpersist()

    def execute(self, prepared: list[DualJoinContext], plan: list[list[Column]]) -> Row:
        p = prepared[0]
        return p.dataframe.select(p.total, *chain.from_iterable(plan)).collect()[0]
