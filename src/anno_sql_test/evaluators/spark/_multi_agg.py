from abc import abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
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
from anno_sql_test.evaluators.spark._util import (
    ColumnComparator,
    ColumnTypeChecker,
    NamedColumn,
    _build_aliased_columns,
    _check_numeric,
    _check_temporal,
    _resolve_fields,
    _to_literal_name,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    FusedAssertion,
    MultiAggAssertEqual,
    MultiAggAssertion,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
)


@dataclass
class MultiAggContext:
    dataframe: DataFrame
    original_values: list[str]
    n: int
    col_for: Callable[[int, str, str], str]
    namespace: str = ""


@dataclass
class CrossDFComparation:
    name: str
    df_i: int
    df_j: int
    df_i_column: str
    df_j_column: str
    original_name: str


@dataclass
class MultiAggPlan:
    comperations: list[CrossDFComparation]
    select_columns: list[Column]


class BaseMultiAggEvaluator[T: MultiAggAssertion](
    BaseStepwiseSparkEvaluator[T, MultiAggContext, MultiAggPlan],
):
    def validate(self, assertion: T, dataframes: list[DataFrame]) -> list[tuple[str, Assertion]]:
        if len(dataframes) < 2:
            return [(f"Expected at least 2 DataFrames, got {len(dataframes)}", assertion)]
        fields = _resolve_fields(assertion.fields, dataframes)
        if not fields:
            return [("No common columns across all DataFrames", assertion)]
        type_checker = self.get_type_checker()
        if type_checker is None:
            return []
        errors = []
        for i, df in enumerate(dataframes):
            for f in fields:
                err = type_checker(df.schema, f)
                if err:
                    errors.append(f"{err} in df[{i}]")
        return [(";".join(errors), assertion)] if errors else []

    @classmethod
    def prepare_values(
        cls, dataframes: list[DataFrame], agg: str, values: list[str], namespace: str = "",
    ) -> list[NamedColumn]:
        fields = _resolve_fields(values, dataframes)
        agg_fields = [f"{agg}({x})" for x in fields]
        prefix = f"_{namespace}_agg_" if namespace else "_agg_"
        agg_cols = _build_aliased_columns(agg_fields, prefix)
        return [NamedColumn(name=f, column=c, namespace=namespace) for f, c in zip(fields, agg_cols)]

    @classmethod
    def add_prefix(cls, df: DataFrame, prefix: str) -> DataFrame:
        cols = [F.col(x).alias(f"{prefix}{x}") for x in df.columns]
        return df.select(*cols)

    @classmethod
    def _comparison_name(cls, i: int, j: int, c: str, ns: str = "") -> str:
        prefix = f"{ns}_" if ns else ""
        return f"_ok_{prefix}{i}_{j}_{_to_literal_name(c)}"

    @classmethod
    def prepare_shared(cls, dataframes: list[DataFrame], values: list[NamedColumn]) -> MultiAggContext:
        original_values = [x.name for x in values]
        dfs = [
            cls.add_prefix(df.agg(*(x.column for x in values)), f"_df{i}")
            for i, df in enumerate(dataframes)
        ]
        result_df = dfs[0]
        n = len(dfs)
        for i in range(1, n):
            result_df = result_df.crossJoin(dfs[i])
        values_with_ns = [(x.namespace, x.name) for x in values]
        df_col_dict = {
            i: dict(zip(values_with_ns, df.columns))
            for i, df in enumerate(dfs)
        }

        def col_for(i: int, c: str, ns: str = "") -> str:
            return df_col_dict[i][(ns, c)]
        return MultiAggContext(dataframe=result_df, original_values=original_values, n=n, col_for=col_for)

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> MultiAggContext:
        values = self.prepare_values(dataframes, assertion.agg, assertion.fields)
        return self.prepare_shared(dataframes, values)

    def build(self, assertion: T, prepared: MultiAggContext) -> MultiAggPlan:
        comparator = self.get_comparator(assertion)
        comparisons = []
        value_cols = []
        cmp_cols = []

        for i in range(prepared.n):
            for c in prepared.original_values:
                value_cols.append(F.col(prepared.col_for(i, c, prepared.namespace)))
            for j in range(i + 1, prepared.n):
                for c in prepared.original_values:
                    left = F.col(prepared.col_for(i, c, prepared.namespace))
                    right = F.col(prepared.col_for(j, c, prepared.namespace))
                    name = self._comparison_name(i, j, c, prepared.namespace)
                    col_ok = (F.isnull(left) & F.isnull(right)) | (
                        F.isnotnull(left) & F.isnotnull(right) & comparator(left, right)
                    )
                    cmp_cols.append(col_ok.alias(name))
                    comparisons.append(CrossDFComparation(
                        name=name,
                        df_i=i,
                        df_j=j,
                        df_i_column=prepared.col_for(i, c, prepared.namespace),
                        df_j_column=prepared.col_for(j, c, prepared.namespace),
                        original_name=c,
                    ))
        return MultiAggPlan(comperations=comparisons, select_columns=value_cols + cmp_cols)

    def execute(self, prepared: MultiAggContext, plan: MultiAggPlan) -> Row:
        return prepared.dataframe.select(*plan.select_columns).collect()[0]

    def finalize(
        self, assertion: T, step_result: StepResult[MultiAggContext, MultiAggPlan, Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        bad_parts = []
        agg = assertion.agg
        for cmp in step_result.plan.comperations:
            if not exec_result[cmp.name]:
                left_val = exec_result[cmp.df_i_column]
                right_val = exec_result[cmp.df_j_column]
                left_str = f"DF{cmp.df_i}.{agg}({cmp.original_name})={left_val}"
                right_str = f"DF{cmp.df_j}.{agg}({cmp.original_name})={right_val}"
                bad_parts.append(
                    f"{left_str} vs {right_str}",
                )
        if not bad_parts:
            return [AssertionResult(assertion=assertion, passed=True)]
        return [AssertionResult(
            assertion=assertion, passed=False,
            message=f"Aggregation mismatch: {'; '.join(bad_parts)}",
        )]

    @abstractmethod
    def get_comparator(self, assertion: T) -> ColumnComparator:
        ...

    def get_type_checker(self) -> ColumnTypeChecker | None:
        return None


class MultiAggAssertEqualEvaluator(BaseMultiAggEvaluator[MultiAggAssertEqual]):
    def get_comparator(self, assertion: MultiAggAssertEqual) -> ColumnComparator:
        return lambda ac, bc: ac == bc


class MultiAggAssertNumericRatioApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertNumericRatioApprox]):
    def get_comparator(self, assertion: MultiAggAssertNumericRatioApprox) -> ColumnComparator:
        ratio = assertion.ratio
        return lambda ac, bc: (
            F.abs(ac.cast("double") - bc.cast("double"))
            <= ratio * F.greatest(F.abs(ac.cast("double")), F.abs(bc.cast("double")))
        )

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class MultiAggAssertNumericDeltaApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertNumericDeltaApprox]):
    def get_comparator(self, assertion: MultiAggAssertNumericDeltaApprox) -> ColumnComparator:
        delta = assertion.delta
        return lambda ac, bc: F.abs(ac.cast("double") - bc.cast("double")) <= F.lit(delta)

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_numeric


class MultiAggAssertTemporalApproxEvaluator(BaseMultiAggEvaluator[MultiAggAssertTemporalApprox]):
    def get_comparator(self, assertion: MultiAggAssertTemporalApprox) -> ColumnComparator:
        ds = assertion.duration_seconds
        return lambda ac, bc: (
            F.abs(ac.cast("double") - bc.cast("double")) <= F.lit(ds)
        )

    def get_type_checker(self) -> ColumnTypeChecker:
        return _check_temporal


class MultiAggFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[MultiAggAssertion, MultiAggContext, MultiAggPlan],
):
    def __init__(self) -> None:
        self._assertion_evaluators: dict[type[MultiAggAssertion], BaseMultiAggEvaluator[Any]] = {
            MultiAggAssertEqual: MultiAggAssertEqualEvaluator(),
            MultiAggAssertNumericRatioApprox: MultiAggAssertNumericRatioApproxEvaluator(),
            MultiAggAssertNumericDeltaApprox: MultiAggAssertNumericDeltaApproxEvaluator(),
            MultiAggAssertTemporalApprox: MultiAggAssertTemporalApproxEvaluator(),
        }

    def get_evaluator_map(self) -> Mapping[
        type[MultiAggAssertion],
        BaseStepwiseAssertionEvaluator[MultiAggAssertion, DataFrame, MultiAggContext, MultiAggPlan, Row],
    ]:
        return self._assertion_evaluators

    def prepare(
        self, assertion: FusedAssertion[MultiAggAssertion], dataframes: list[DataFrame],
    ) -> list[MultiAggContext]:
        all_values = []
        for i, asrt in enumerate(assertion.assertions):
            evaluator = self._assertion_evaluators[type(asrt)]
            values = evaluator.prepare_values(dataframes, asrt.agg, asrt.fields, namespace=f"asrt{i}")
            all_values.append(values)
        prepared_all = BaseMultiAggEvaluator.prepare_shared(dataframes, list(chain.from_iterable(all_values)))
        ctxs = []
        for idx, values in enumerate(all_values):
            sub_original_values = [x.name for x in values]
            ns = f"asrt{idx}"
            ctxs.append(MultiAggContext(
                dataframe=prepared_all.dataframe,
                original_values=sub_original_values,
                n=len(dataframes),
                col_for=lambda i, c, ns=ns: prepared_all.col_for(i, c, ns),
                namespace=ns,
            ))
        return ctxs

    def execute(self, prepared: list[MultiAggContext], plan: list[MultiAggPlan]) -> Row:
        df = prepared[0].dataframe
        return df.select(*chain.from_iterable(p.select_columns for p in plan)).collect()[0]
