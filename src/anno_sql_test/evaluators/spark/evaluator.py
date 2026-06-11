from typing import cast

from pyspark.sql import DataFrame

from anno_sql_test.evaluators.base import (
    BaseFusedAssertionEvaluator,
    SimpleFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._base import (
    BaseSparkEvaluator,
    BaseSparkFusedEvaluator,
    DelegatingStepwiseSparkFusedEvaluator,
)
from anno_sql_test.evaluators.spark._dual_join import (
    DualJoinAssertEqualEvaluator,
    DualJoinAssertNumericDeltaApproxEvaluator,
    DualJoinAssertNumericRatioApproxEvaluator,
    DualJoinAssertTemporalApproxEvaluator,
    DualJoinFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._multi_agg import (
    MultiAggAssertEqualEvaluator,
    MultiAggAssertNumericDeltaApproxEvaluator,
    MultiAggAssertNumericRatioApproxEvaluator,
    MultiAggAssertTemporalApproxEvaluator,
    MultiAggFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._single import (
    SingleAssertAnyEvaluator,
    SingleAssertEmptyEvaluator,
    SingleAssertEvaluator,
    SingleAssertNoneEvaluator,
    SingleAssertNotEmptyEvaluator,
    SingleAssertUniqueEvaluator,
    SinglePredicateFusedAssertionEvaluator,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    DualJoinAssertEqual,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    FusedAssertion,
    MultiAggAssertEqual,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)


class SparkAssertionEvaluator(BaseSparkEvaluator[Assertion]):
    def __init__(self):
        _handlers = [
            (SingleAssertAll, SingleAssertEvaluator()),
            (SingleAssertAny, SingleAssertAnyEvaluator()),
            (SingleAssertNone, SingleAssertNoneEvaluator()),
            (SingleAssertEmpty, SingleAssertEmptyEvaluator()),
            (SingleAssertNotEmpty, SingleAssertNotEmptyEvaluator()),
            (SingleAssertUnique, SingleAssertUniqueEvaluator()),
            (MultiAggAssertEqual, MultiAggAssertEqualEvaluator()),
            (MultiAggAssertNumericRatioApprox, MultiAggAssertNumericRatioApproxEvaluator()),
            (MultiAggAssertNumericDeltaApprox, MultiAggAssertNumericDeltaApproxEvaluator()),
            (MultiAggAssertTemporalApprox, MultiAggAssertTemporalApproxEvaluator()),
            (DualJoinAssertEqual, DualJoinAssertEqualEvaluator()),
            (DualJoinAssertNumericRatioApprox, DualJoinAssertNumericRatioApproxEvaluator()),
            (DualJoinAssertNumericDeltaApprox, DualJoinAssertNumericDeltaApproxEvaluator()),
            (DualJoinAssertTemporalApprox, DualJoinAssertTemporalApproxEvaluator()),
        ]
        self._handlers: dict[type[Assertion], BaseSparkEvaluator[Assertion]] = {
            k: cast(BaseSparkEvaluator[Assertion], v)
            for k, v in _handlers
        }

    def evaluate(self, assertion: Assertion, dataframes: list[DataFrame]) -> AssertionResult:
        handler = self._handlers.get(type(assertion))
        if handler:
            return handler.evaluate(assertion, dataframes)
        return AssertionResult(assertion=assertion, passed=False, message="Unknown assertion type")


class SparkFusedAssertionEvaluator(BaseSparkFusedEvaluator[Assertion]):
    def __init__(self, fallback: SimpleFusedAssertionEvaluator | None = None):
        self._fallback = fallback or SimpleFusedAssertionEvaluator(SparkAssertionEvaluator())
        self._handlers: dict[type[Assertion], BaseFusedAssertionEvaluator] = {}
        evaluators: list[DelegatingStepwiseSparkFusedEvaluator] = [
            SinglePredicateFusedAssertionEvaluator(),
            MultiAggFusedAssertionEvaluator(),
            DualJoinFusedAssertionEvaluator(),
        ]
        for evaluator in evaluators:
            for k in evaluator.get_evaluator_map().keys():
                self._handlers[k] = evaluator

    def support_assertions(self) -> set[type[Assertion]]:
        return set(self._handlers.keys())

    def evaluate(self, assertion: FusedAssertion[Assertion], dataframes: list[DataFrame]) -> list[AssertionResult]:
        first_type = type(assertion.assertions[0])
        if first_type in self._handlers:
            return self._handlers[first_type].evaluate(assertion, dataframes)
        else:
            return self._fallback.evaluate(assertion, dataframes)
