from pyspark.sql import DataFrame

from anno_sql_test.evaluators.base import Assertion, BaseAssertionEvaluator


class BaseSparkEvaluator[T: Assertion](BaseAssertionEvaluator[T, DataFrame]):
    ...
