from pyspark.sql import DataFrame

from anno_sql_test.evaluators.spark._base import BaseSparkEvaluator
from anno_sql_test.evaluators.spark._dual_join import (
    DualJoinAssertEqualEvaluator,
    DualJoinAssertNumericDeltaApproxEvaluator,
    DualJoinAssertNumericRatioApproxEvaluator,
    DualJoinAssertTemporalApproxEvaluator,
)
from anno_sql_test.evaluators.spark._multi_agg import (
    MultiAggAssertEqualEvaluator,
    MultiAggAssertNumericDeltaApproxEvaluator,
    MultiAggAssertNumericRatioApproxEvaluator,
    MultiAggAssertTemporalApproxEvaluator,
)
from anno_sql_test.evaluators.spark._single import (
    SingleAssertEmptyEvaluator,
    SingleAssertEvaluator,
    SingleAssertNotEmptyEvaluator,
    SingleAssertUniqueEvaluator,
)
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    DualJoinAssertEqual,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    MultiAggAssertEqual,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
    SingleAssert,
    SingleAssertEmpty,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)


class SparkAssertionEvaluator(BaseSparkEvaluator[Assertion]):
    def __init__(self):
        self._handlers: dict[type[Assertion], BaseSparkEvaluator] = {
            SingleAssert: SingleAssertEvaluator(),
            SingleAssertEmpty: SingleAssertEmptyEvaluator(),
            SingleAssertNotEmpty: SingleAssertNotEmptyEvaluator(),
            SingleAssertUnique: SingleAssertUniqueEvaluator(),
            MultiAggAssertEqual: MultiAggAssertEqualEvaluator(),
            MultiAggAssertNumericRatioApprox: MultiAggAssertNumericRatioApproxEvaluator(),
            MultiAggAssertNumericDeltaApprox: MultiAggAssertNumericDeltaApproxEvaluator(),
            MultiAggAssertTemporalApprox: MultiAggAssertTemporalApproxEvaluator(),
            DualJoinAssertEqual: DualJoinAssertEqualEvaluator(),
            DualJoinAssertNumericRatioApprox: DualJoinAssertNumericRatioApproxEvaluator(),
            DualJoinAssertNumericDeltaApprox: DualJoinAssertNumericDeltaApproxEvaluator(),
            DualJoinAssertTemporalApprox: DualJoinAssertTemporalApproxEvaluator(),
        }

    def evaluate(self, assertion: Assertion, dataframes: list[DataFrame]) -> AssertionResult:
        handler = self._handlers.get(type(assertion))
        if handler:
            return handler.evaluate(assertion, dataframes)
        return AssertionResult(assertion=assertion, passed=False, message="Unknown assertion type")
