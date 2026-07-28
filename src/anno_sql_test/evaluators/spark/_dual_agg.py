from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import replace as replace_field
from itertools import chain
from typing import Any, cast

from pyspark import StorageLevel
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
from anno_sql_test.evaluators.spark._dual_join import (
    BaseDualJoinAssertEvaluator,
    DualJoinAssertLambdaEvaluator,
    DualJoinContext,
)
from anno_sql_test.evaluators.spark._utils import (
    ColumnTypeChecker,
    NamedColumn,
    _batch_validate_types,
    _check_numeric,
    _check_temporal,
    _to_literal_name,
    resolve_fields,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    ColumnSpec,
    DualAggAssertEqual,
    DualAggAssertion,
    DualAggAssertNumericDeltaApprox,
    DualAggAssertNumericRatioApprox,
    DualAggAssertTemporalApprox,
    DualJoinAssertLambda,
    ExprColumn,
    FusedAssertion,
    LambdaFunc,
)

type DualAggContext = tuple[DualJoinContext, DualJoinAssertLambda]


class BaseDualAggEvaluator[T: DualAggAssertion](
    BaseStepwiseSparkEvaluator[T, DualAggContext, list[Column]],
):
    def __init__(self, sample_count: int = 0):
        self._sample_count = sample_count
        self._dual_evaluator = DualJoinAssertLambdaEvaluator(sample_count=sample_count)

    def validate(self, assertion: T, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if len(dataframes) != 2:
            self.logger.warning("Expected 2 DataFrames, got %d for %s", len(dataframes), type(assertion).__name__)
            return [(f"Expected exactly 2 DataFrames, got {len(dataframes)}", assertion)]
        fields = resolve_fields(assertion.fields, dataframes)
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

    def prepare_values(self, assertion: T, dataframes: list[DataFrame], namespace: str = "") -> list[NamedColumn]:
        fields = resolve_fields(assertion.fields, dataframes)
        agg_exprs = [assertion.agg.format(f) for f in fields]
        prefix = f"_{namespace}_agg" if namespace else "_agg"
        aliases = [f"{prefix}_{_to_literal_name(a)}" for a in agg_exprs]
        return [NamedColumn(al, F.expr(a).alias(al), namespace) for a, al in zip(agg_exprs, aliases)]

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> DualAggContext:
        self.logger.debug("Preparing %s: agg=%s, fields=%s, keys=%s",
                          type(assertion).__name__, assertion.agg, assertion.fields, assertion.keys)
        keys = resolve_fields(assertion.keys, dataframes)
        aggs = self.prepare_values(assertion, dataframes)

        left_agg = dataframes[0].groupBy(*keys).agg(*[a.column for a in aggs])
        right_agg = dataframes[1].groupBy(*keys).agg(*[a.column for a in aggs])
        key_specs = tuple(ExprColumn(expr=k) for k in keys)
        values = tuple(ExprColumn(expr=a.name) for a in aggs)

        join_assertion = DualJoinAssertLambda(key_specs, values, 0, 0, "full", self.get_comparator(assertion))
        ctx = self._dual_evaluator.prepare(join_assertion, [left_agg, right_agg])
        return (ctx, join_assertion)

    def build(self, assertion: T, prepared: DualAggContext) -> list[Column]:
        return self._dual_evaluator.build(prepared[1], prepared[0])

    def execute(self, prepared: DualAggContext, plan: list[Column]) -> Row:
        return self._dual_evaluator.execute(prepared[0], plan)

    def finalize(
        self, assertion: T, step_result: StepResult[DualAggContext, list[Column], Row],
    ) -> list[AssertionResult]:
        join_step_result = StepResult(step_result.prepared[0], step_result.plan, step_result.executed)
        results = self._dual_evaluator.finalize(step_result.prepared[1], join_step_result)
        return [AssertionResult(assertion=assertion, passed=r.passed, message=r.message) for r in results]

    def sample_failure(
        self, assertion: T, step_result: StepResult[DualAggContext, list[Column], Row],
    ) -> list[dict] | None:
        join_step_result = StepResult(step_result.prepared[0], step_result.plan, step_result.executed)
        return self._dual_evaluator.sample_failure(step_result.prepared[1], join_step_result)

    def cleanup(self, prepared: DualAggContext) -> None:
        self._dual_evaluator.cleanup(prepared[0])

    @abstractmethod
    def get_comparator(self, assertion: T) -> LambdaFunc:
        ...

    def get_type_checker(self) -> ColumnTypeChecker | None:
        return None


class DualAggAssertEqualEvaluator(BaseDualAggEvaluator[DualAggAssertEqual]):
    def get_comparator(self, assertion: DualAggAssertEqual) -> LambdaFunc:
        return LambdaFunc.from_str('(a, b) -> a = b')


class DualAggAssertNumericRatioApproxEvaluator(BaseDualAggEvaluator[DualAggAssertNumericRatioApprox]):
    def get_comparator(self, assertion: DualAggAssertNumericRatioApprox) -> LambdaFunc:
        ratio = assertion.ratio
        return LambdaFunc.from_str(
            f'(ac, bc) -> abs(ac - bc) <= {ratio} * greatest(abs(ac), abs(bc))',
        )

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class DualAggAssertNumericDeltaApproxEvaluator(BaseDualAggEvaluator[DualAggAssertNumericDeltaApprox]):
    def get_comparator(self, assertion: DualAggAssertNumericDeltaApprox) -> LambdaFunc:
        delta = assertion.delta
        return LambdaFunc.from_str(f'(ac, bc) -> abs(ac - bc) <= {delta}')

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class DualAggAssertTemporalApproxEvaluator(BaseDualAggEvaluator[DualAggAssertTemporalApprox]):
    def get_comparator(self, assertion: DualAggAssertTemporalApprox) -> LambdaFunc:
        ds = assertion.duration_seconds
        return LambdaFunc.from_str(f'(ac, bc) -> abs(ac - bc) <= {ds}')

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_temporal


class DualAggFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[DualAggAssertion, DualAggContext, list[Column]],
):
    def __init__(self, sample_count: int = 0) -> None:
        self._sample_count = sample_count
        self._assertion_evaluators: dict[type[DualAggAssertion], BaseDualAggEvaluator[Any]] = {
            DualAggAssertEqual: DualAggAssertEqualEvaluator(sample_count=sample_count),
            DualAggAssertNumericRatioApprox: DualAggAssertNumericRatioApproxEvaluator(sample_count=sample_count),
            DualAggAssertNumericDeltaApprox: DualAggAssertNumericDeltaApproxEvaluator(sample_count=sample_count),
            DualAggAssertTemporalApprox: DualAggAssertTemporalApproxEvaluator(sample_count=sample_count),
        }

    def get_evaluator_map(self) -> Mapping[
        type[DualAggAssertion],
        BaseStepwiseAssertionEvaluator[DualAggAssertion, DataFrame, DualAggContext, list[Column], Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[DualAggAssertion], dataframes: list[DataFrame],
    ) -> list[DualAggContext]:
        self.logger.debug("Fused prepare for %d DualiAggAssertion assertions", len(assertion.assertions))
        keys = resolve_fields(assertion.assertions[0].keys, dataframes)
        all_agg_values: list[list[NamedColumn]] = []
        all_agg_cols: list[Column] = []
        for i, asrt in enumerate(assertion.assertions):
            vals = self._assertion_evaluators[type(asrt)].prepare_values(asrt, dataframes, f"asrt{i}")
            all_agg_values.append(vals)
            all_agg_cols.extend(a.column for a in vals)

        left_agg = dataframes[0].groupBy(*keys).agg(*all_agg_cols)
        right_agg = dataframes[1].groupBy(*keys).agg(*all_agg_cols)

        key_specs = [cast(ColumnSpec, ExprColumn(expr=k)) for k in keys]
        all_values = [replace_field(v, column=F.col(v.name)) for v in chain.from_iterable(all_agg_values)]
        prepared_all = BaseDualJoinAssertEvaluator.prepare_shared([left_agg, right_agg], key_specs, all_values, "full")
        if self._sample_count > 0:
            prepared_all.dataframe.persist(StorageLevel.DISK_ONLY)

        ctxs = []
        for idx, (asrt, values) in enumerate(zip(assertion.assertions, all_agg_values)):
            ns = f"asrt{idx}"
            join_values = tuple(ExprColumn(a.name) for a in values)
            join_assertion = DualJoinAssertLambda(
                tuple(key_specs), join_values, 0, 0, "full",
                self._assertion_evaluators[type(asrt)].get_comparator(asrt),
            )
            ctxs.append((
                DualJoinContext(
                    dataframe=prepared_all.dataframe,
                    total=prepared_all.total,
                    original_keys=prepared_all.original_keys,
                    original_values=[x.name for x in values],
                    col_for=lambda c, ns=ns: prepared_all.col_for(c, ns),
                    namespace=ns,
                ),
                join_assertion,
            ))
        return ctxs

    def cleanup(self, prepared: list[DualAggContext]) -> None:
        if self._sample_count > 0:
            prepared[0][0].dataframe.unpersist()

    def execute(self, prepared: list[DualAggContext], plan: list[list[Column]]) -> Row:
        p = prepared[0][0]
        return p.dataframe.select(p.total, *chain.from_iterable(plan)).collect()[0]
