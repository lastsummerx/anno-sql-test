from pyspark.sql import DataFrame, Row

from anno_sql_test.evaluators.base import (
    Assertion,
    BaseAssertionEvaluator,
    BaseFusedAssertionEvaluator,
    BaseStepwiseAssertionEvaluator,
    DelegatingStepwiseFusedAssertionEvaluator,
)


class BaseSparkEvaluator[T: Assertion](BaseAssertionEvaluator[T, DataFrame]):
    ...


class BaseSparkFusedEvaluator[T: Assertion](BaseFusedAssertionEvaluator[T, DataFrame]):
    ...


class BaseStepwiseSparkEvaluator[T: Assertion, DAT, QRY](
    BaseStepwiseAssertionEvaluator[T, DataFrame, DAT, QRY, Row],
):
    ...


class DelegatingStepwiseSparkFusedEvaluator[T: Assertion, DAT, QRY](
    DelegatingStepwiseFusedAssertionEvaluator[T, DataFrame, DAT, QRY, Row],
):
    ...
