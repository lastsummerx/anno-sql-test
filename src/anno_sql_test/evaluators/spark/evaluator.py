import logging
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
from anno_sql_test.evaluators.spark._dual_agg import (
    DualAggAssertEqualEvaluator,
    DualAggAssertNumericDeltaApproxEvaluator,
    DualAggAssertNumericRatioApproxEvaluator,
    DualAggAssertTemporalApproxEvaluator,
    DualAggFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._dual_join import (
    DualJoinAssertEqualEvaluator,
    DualJoinAssertLambdaEvaluator,
    DualJoinAssertNumericApproxEvaluator,
    DualJoinAssertTemporalApproxEvaluator,
    DualJoinFusedAssertionEvaluator,
    DualRowsAssertEqualEvaluator,
)
from anno_sql_test.evaluators.spark._single import (
    SingleAssertAllEvaluator,
    SingleAssertAnyEvaluator,
    SingleAssertEmptyEvaluator,
    SingleAssertNoneEvaluator,
    SingleAssertNotEmptyEvaluator,
    SingleAssertSetEqualEvaluator,
    SingleAssertUniqueEvaluator,
    SinglePredicateFusedAssertionEvaluator,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    DualAggAssertEqual,
    DualAggAssertNumericDeltaApprox,
    DualAggAssertNumericRatioApprox,
    DualAggAssertTemporalApprox,
    DualJoinAssertEqual,
    DualJoinAssertLambda,
    DualJoinAssertNumericApprox,
    DualJoinAssertTemporalApprox,
    DualRowsAssertEqual,
    FusedAssertion,
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertSetEqual,
    SingleAssertUnique,
)

_logger = logging.getLogger(__name__)


class SparkAssertionEvaluator(BaseSparkEvaluator[Assertion]):
    def __init__(self, sample_count: int = 0):
        _handlers = [
            (SingleAssertAll, SingleAssertAllEvaluator(sample_count=sample_count)),
            (SingleAssertAny, SingleAssertAnyEvaluator(sample_count=sample_count)),
            (SingleAssertNone, SingleAssertNoneEvaluator(sample_count=sample_count)),
            (SingleAssertEmpty, SingleAssertEmptyEvaluator(sample_count=sample_count)),
            (SingleAssertNotEmpty, SingleAssertNotEmptyEvaluator()),
            (SingleAssertSetEqual, SingleAssertSetEqualEvaluator(sample_count=sample_count)),
            (SingleAssertUnique, SingleAssertUniqueEvaluator(sample_count=sample_count)),
            (DualAggAssertEqual, DualAggAssertEqualEvaluator(sample_count=sample_count)),
            (DualAggAssertNumericRatioApprox, DualAggAssertNumericRatioApproxEvaluator(sample_count=sample_count)),
            (DualAggAssertNumericDeltaApprox, DualAggAssertNumericDeltaApproxEvaluator(sample_count=sample_count)),
            (DualAggAssertTemporalApprox, DualAggAssertTemporalApproxEvaluator(sample_count=sample_count)),
            (DualJoinAssertEqual, DualJoinAssertEqualEvaluator(sample_count=sample_count)),
            (DualJoinAssertNumericApprox, DualJoinAssertNumericApproxEvaluator(sample_count=sample_count)),
            (DualJoinAssertTemporalApprox, DualJoinAssertTemporalApproxEvaluator(sample_count=sample_count)),
            (DualJoinAssertLambda, DualJoinAssertLambdaEvaluator(sample_count=sample_count)),
            (DualRowsAssertEqual, DualRowsAssertEqualEvaluator(sample_count=sample_count)),
        ]
        self._handlers: dict[type[Assertion], BaseSparkEvaluator[Assertion]] = {
            k: cast(BaseSparkEvaluator[Assertion], v)
            for k, v in _handlers
        }

    def evaluate(self, assertion: Assertion, dataframes: list[DataFrame]) -> AssertionResult:
        handler = self._handlers.get(type(assertion))
        if handler:
            _logger.debug("Dispatch %s -> %s", type(assertion).__name__, type(handler).__name__)
            return handler.evaluate(assertion, dataframes)
        _logger.warning("No handler for assertion type: %s", type(assertion).__name__)
        return AssertionResult(assertion=assertion, passed=False, message="Unknown assertion type")


class SparkFusedAssertionEvaluator(BaseSparkFusedEvaluator[Assertion]):
    def __init__(self, sample_count: int = 0, fallback: SimpleFusedAssertionEvaluator | None = None):
        self._fallback = fallback or SimpleFusedAssertionEvaluator(SparkAssertionEvaluator(sample_count=sample_count))
        self._handlers: dict[type[Assertion], BaseFusedAssertionEvaluator] = {}
        evaluators: list[DelegatingStepwiseSparkFusedEvaluator] = [
            SinglePredicateFusedAssertionEvaluator(sample_count=sample_count),
            DualAggFusedAssertionEvaluator(sample_count=sample_count),
            DualJoinFusedAssertionEvaluator(sample_count=sample_count),
        ]
        for evaluator in evaluators:
            for k in evaluator.get_evaluator_map().keys():
                self._handlers[k] = evaluator

    def support_assertions(self) -> set[type[Assertion]]:
        return set(self._handlers.keys())

    def evaluate(self, assertion: FusedAssertion[Assertion], dataframes: list[DataFrame]) -> list[AssertionResult]:
        first_type = type(assertion.assertions[0])
        n = len(assertion.assertions)
        if first_type in self._handlers:
            handler_name = type(self._handlers[first_type]).__name__
            _logger.debug("Fused evaluate %d x %s -> %s", n, first_type.__name__, handler_name)
            return self._handlers[first_type].evaluate(assertion, dataframes)
        else:
            _logger.debug("Fused fallback for %d x %s", n, first_type.__name__)
            return self._fallback.evaluate(assertion, dataframes)
