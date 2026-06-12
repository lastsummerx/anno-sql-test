from abc import abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import reduce
from itertools import chain
from operator import and_
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
    _batch_validate_types,
    _build_aliased_columns,
    _check_numeric,
    _check_temporal,
    resolve_fields,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    DualJoinAssertEqual,
    DualJoinAssertion,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    FusedAssertion,
)


@dataclass
class DualJoinContext:
    dataframe: DataFrame
    total: Column
    original_values: list[str]
    col_for: Callable[[str, str], str]
    namespace: str = ""


class BaseDualJoinAssertEvaluator[T: DualJoinAssertion](
    BaseStepwiseSparkEvaluator[T, DualJoinContext, list[Column]],
):
    LEFT_DF_ALIAS = "l"
    RIGHT_DF_ALIAS = "r"
    TOTAL_COL = "_total"
    TOTAL_VIOLATED_COL = "_total_violated"

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
    def prepare_values(cls, dataframes: list[DataFrame], values: list[str], namespace: str = "") -> list[NamedColumn]:
        original_values = resolve_fields(values, dataframes)
        prefix = f"_{namespace}_val_" if namespace else "_val_"
        value_cols = _build_aliased_columns(original_values, prefix)
        return [NamedColumn(name=ov, column=vc, namespace=namespace) for ov, vc in zip(original_values, value_cols)]

    @classmethod
    def prepare_shared(cls, dataframes: list[DataFrame], keys: list[str], values: list[NamedColumn]) -> DualJoinContext:
        left, right = dataframes[0], dataframes[1]
        key_prefix = "_key_"
        key_cols = _build_aliased_columns(keys, key_prefix)
        original_values = [x.name for x in values]
        value_cols = [x.column for x in values]

        left_prep = left.select(*key_cols, *value_cols).alias(cls.LEFT_DF_ALIAS)
        right_prep = right.select(*key_cols, *value_cols).alias(cls.RIGHT_DF_ALIAS)

        key_names = left_prep.columns[:len(key_cols)]
        val_names = left_prep.columns[len(key_cols):]
        df = left_prep.join(right_prep, key_names, "fullouter")
        total = F.count(F.lit(1)).alias(cls.TOTAL_COL)
        values_with_ns = [(x.namespace, x.name) for x in values]
        col_dict = dict(zip(values_with_ns, val_names))

        def col_for(c: str, ns: str = "") -> str:
            return col_dict[(ns, c)]
        return DualJoinContext(
            dataframe=df, total=total, original_values=original_values, col_for=col_for,
        )

    def prepare(self, assertion: T, dataframes: list[DataFrame]) -> DualJoinContext:
        self.logger.debug("Preparing %s: keys=%s", type(assertion).__name__, assertion.keys)
        values = self.prepare_values(dataframes, assertion.values)
        return self.prepare_shared(dataframes, assertion.keys, values)

    def _total_violated_col(self, ns: str = "") -> str:
        return f"{self.TOTAL_VIOLATED_COL}_{ns}" if ns else self.TOTAL_VIOLATED_COL

    def build(self, assertion: T, prepared: DualJoinContext) -> list[Column]:
        comparator = self.get_comparator(assertion)

        cmps = []
        for name in prepared.original_values:
            v = prepared.col_for(name, prepared.namespace)
            lv = F.expr(f"{self.LEFT_DF_ALIAS}.{v}")
            rv = F.expr(f"{self.RIGHT_DF_ALIAS}.{v}")
            both_null = F.isnull(lv) & F.isnull(rv)
            both_not_null_and_comp = F.isnotnull(lv) & F.isnotnull(rv) & comparator(lv, rv)
            cmps.append(both_null | both_not_null_and_comp)
        tv_name = self._total_violated_col(prepared.namespace)
        total_violated = F.count(F.when(~reduce(and_, cmps, F.lit(True)), 1)).alias(tv_name)

        rst = [
            F.count(F.when(~cmp, 1)).alias(prepared.col_for(name, prepared.namespace))
            for name, cmp in zip(prepared.original_values, cmps)
        ]
        rst.append(total_violated)
        return rst

    def execute(self, prepared: DualJoinContext, plan: list[Column]) -> Row:
        return prepared.dataframe.agg(prepared.total, *plan).collect()[0]

    def finalize(
        self, assertion: T, step_result: StepResult[DualJoinContext, list[Column], Row],
    ) -> list[AssertionResult]:
        exec_result = step_result.executed
        prepared = step_result.prepared
        tv_name = self._total_violated_col(prepared.namespace)
        total_violated = exec_result[tv_name]
        self.logger.debug("Finalizing %s: %d violated rows", type(assertion).__name__, total_violated)
        if total_violated == 0:
            return [AssertionResult(assertion=assertion, passed=True)]

        total_rows = exec_result[self.TOTAL_COL]
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

    @abstractmethod
    def get_comparator(self, assertion: T) -> ColumnComparator:
        ...

    def get_type_checker(self) -> ColumnTypeChecker | None:
        return None


class DualJoinAssertEqualEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertEqual]):
    def get_comparator(self, assertion: DualJoinAssertEqual) -> ColumnComparator:
        return lambda lv, rv: lv == rv


class DualJoinAssertNumericRatioApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertNumericRatioApprox]):
    def get_comparator(self, assertion: DualJoinAssertNumericRatioApprox) -> ColumnComparator:
        ratio = assertion.ratio
        return lambda lv, rv: (
            F.abs(lv.cast("double") - rv.cast("double"))
            <= ratio * F.greatest(F.abs(lv.cast("double")), F.abs(rv.cast("double")))
        )

    def get_type_checker(self):
        return _check_numeric


class DualJoinAssertNumericDeltaApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertNumericDeltaApprox]):
    def get_comparator(self, assertion: DualJoinAssertNumericDeltaApprox) -> ColumnComparator:
        delta = assertion.delta
        return lambda lv, rv: F.abs(lv.cast("double") - rv.cast("double")) <= delta

    def get_type_checker(self):
        return _check_numeric


class DualJoinAssertTemporalApproxEvaluator(BaseDualJoinAssertEvaluator[DualJoinAssertTemporalApprox]):

    def get_comparator(self, assertion: DualJoinAssertTemporalApprox) -> ColumnComparator:
        ds = assertion.duration_seconds
        return lambda lv, rv: F.abs(lv.cast("double") - rv.cast("double")) <= F.lit(ds)

    def get_type_checker(self):
        return _check_temporal


class DualJoinFusedAssertionEvaluator(
    DelegatingStepwiseSparkFusedEvaluator[DualJoinAssertion, DualJoinContext, list[Column]],
):
    def __init__(self) -> None:
        self._assertion_evaluators: dict[type[DualJoinAssertion], BaseDualJoinAssertEvaluator[Any]] = {
            DualJoinAssertEqual: DualJoinAssertEqualEvaluator(),
            DualJoinAssertNumericRatioApprox: DualJoinAssertNumericRatioApproxEvaluator(),
            DualJoinAssertNumericDeltaApprox: DualJoinAssertNumericDeltaApproxEvaluator(),
            DualJoinAssertTemporalApprox: DualJoinAssertTemporalApproxEvaluator(),
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
        keys = assertion.assertions[0].keys
        for i, asrt in enumerate(assertion.assertions):
            evaluator = self._assertion_evaluators[type(asrt)]
            values = evaluator.prepare_values(dataframes, asrt.values, namespace=f"asrt{i}")
            all_values.append(values)
        prepared_all = BaseDualJoinAssertEvaluator.prepare_shared(
            dataframes, keys, list(chain.from_iterable(all_values)),
        )
        ctxs = []
        for idx, values in enumerate(all_values):
            sub_original_values = [x.name for x in values]
            ns = f"asrt{idx}"
            ctxs.append(DualJoinContext(
                dataframe=prepared_all.dataframe,
                total=prepared_all.total,
                original_values=sub_original_values,
                col_for=lambda c, ns=ns: prepared_all.col_for(c, ns),
                namespace=ns,
            ))
        return ctxs

    def execute(self, prepared: list[DualJoinContext], plan: list[list[Column]]) -> Row:
        p = prepared[0]
        return p.dataframe.select(p.total, *chain.from_iterable(plan)).collect()[0]
