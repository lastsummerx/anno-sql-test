from abc import abstractmethod
from collections.abc import Mapping
from itertools import chain
from typing import Any, cast

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


class BaseDualAggEvaluator[T: DualAggAssertion](
    BaseStepwiseSparkEvaluator[T, DualJoinContext, list[Column]],
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

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> DualJoinContext:
        self.logger.debug("Preparing %s: agg=%s, fields=%s, keys=%s",
                          type(assertion).__name__, assertion.agg, assertion.fields, assertion.keys)
        keys = resolve_fields(assertion.keys, dataframes)
        fields = resolve_fields(assertion.fields, dataframes)
        agg_exprs = [assertion.agg.format(f) for f in fields]
        aliases = [_to_literal_name(a) for a in agg_exprs]

        left_agg = dataframes[0].groupBy(*keys).agg(*[
            F.expr(a).alias(al) for a, al in zip(agg_exprs, aliases)
        ])
        right_agg = dataframes[1].groupBy(*keys).agg(*[
            F.expr(a).alias(al) for a, al in zip(agg_exprs, aliases)
        ])

        key_specs = [cast(ColumnSpec, ExprColumn(expr=k)) for k in keys]
        values = [NamedColumn(name=f, column=F.col(al)) for f, al in zip(fields, aliases)]
        return self._dual_evaluator.prepare_shared([left_agg, right_agg], key_specs, values, "full")

    def build(self, assertion: T, prepared: DualJoinContext) -> list[Column]:
        dj = DualJoinAssertLambda(keys=(), values=(), comparator=self.get_comparator(assertion))
        return self._dual_evaluator.build(dj, prepared)

    def execute(self, prepared: DualJoinContext, plan: list[Column]) -> Row:
        return self._dual_evaluator.execute(prepared, plan)

    def finalize(
        self, assertion: T, step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[AssertionResult]:
        dj = DualJoinAssertLambda(keys=(), values=(), comparator=self.get_comparator(assertion))
        results = self._dual_evaluator.finalize(dj, step_result)
        return [AssertionResult(assertion=assertion, passed=r.passed, message=r.message) for r in results]

    def sample_failure(
        self, assertion: T, step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[dict] | None:
        dj = DualJoinAssertLambda(keys=(), values=(), comparator=self.get_comparator(assertion))
        return self._dual_evaluator.sample_failure(dj, step_result)

    def cleanup(self, prepared: DualJoinContext) -> None:
        self._dual_evaluator.cleanup(prepared)

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
    DelegatingStepwiseSparkFusedEvaluator[DualAggAssertion, DualJoinContext, list[Column]],
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
        BaseStepwiseAssertionEvaluator[DualAggAssertion, DataFrame, DualJoinContext, list[Column], Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[DualAggAssertion], dataframes: list[DataFrame],
    ) -> list[DualJoinContext]:
        self.logger.debug("Fused prepare for %d DualiAggAssertion assertions", len(assertion.assertions))
        keys = resolve_fields(assertion.assertions[0].keys, dataframes)
        all_agg_values: list[list[NamedColumn]] = []
        all_agg_cols: list[Column] = []
        for i, asrt in enumerate(assertion.assertions):
            fields = resolve_fields(asrt.fields, dataframes)
            agg_exprs = [asrt.agg.format(f) for f in fields]
            aliases = [_to_literal_name(a) for a in agg_exprs]
            ns = f"asrt{i}"
            vals = [NamedColumn(name=f, column=F.col(al), namespace=ns) for f, al in zip(fields, aliases)]
            all_agg_values.append(vals)
            all_agg_cols.extend(F.expr(a).alias(al) for a, al in zip(agg_exprs, aliases))

        left_agg = dataframes[0].groupBy(*keys).agg(*all_agg_cols)
        right_agg = dataframes[1].groupBy(*keys).agg(*all_agg_cols)

        key_specs = [cast(ColumnSpec, ExprColumn(expr=k)) for k in keys]
        all_values = list(chain.from_iterable(all_agg_values))
        prepared_all = BaseDualJoinAssertEvaluator.prepare_shared([left_agg, right_agg], key_specs, all_values, "full")

        ctxs = []
        for idx, values in enumerate(all_agg_values):
            ns = f"asrt{idx}"
            ctxs.append(DualJoinContext(
                dataframe=prepared_all.dataframe,
                total=prepared_all.total,
                original_keys=prepared_all.original_keys,
                original_values=[x.name for x in values],
                col_for=lambda c, ns=ns: prepared_all.col_for(c, ns),
                namespace=ns,
            ))
        return ctxs

    def execute(self, prepared: list[DualJoinContext], plan: list[list[Column]]) -> Row:
        p = prepared[0]
        return p.dataframe.select(p.total, *chain.from_iterable(plan)).collect()[0]
