from pyspark.sql import SparkSession

from anno_sql_test.evaluators.spark.evaluator import (
    SparkFusedAssertionEvaluator,
)
from anno_sql_test.models import (
    DualJoinAssertEqual,
    FusedAssertion,
    MultiAggAssertEqual,
    SingleAssert,
    SingleAssertEmpty,
    SingleAssertNotEmpty,
)

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())


class TestSparkFusedAssertionEvaluator:

    def test_batch_evaluate_single_predicates(self):
        evaluator = SparkFusedAssertionEvaluator()
        df = spark.createDataFrame([(1,), (2,)], ["a"])
        fused = FusedAssertion([SingleAssert(predicate="a > 0"), SingleAssert(predicate="a < 10")])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_batch_evaluate_mixed_types(self):
        evaluator = SparkFusedAssertionEvaluator()
        df = spark.createDataFrame([(1,)], ["a"])
        fused = FusedAssertion([
            SingleAssert(predicate="a > 0"),
            SingleAssertEmpty(),
            SingleAssertNotEmpty(),
        ])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 3
        assert results[0].passed is True
        assert results[1].passed is False
        assert results[2].passed is True

    def test_batch_evaluate_multi_agg(self):
        evaluator = SparkFusedAssertionEvaluator()
        df1 = spark.createDataFrame([(1,)], ["a"])
        df2 = spark.createDataFrame([(2,)], ["a"])
        fused = FusedAssertion([MultiAggAssertEqual(agg="count", fields=["*"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is True

    def test_batch_evaluate_dual_join(self):
        evaluator = SparkFusedAssertionEvaluator()
        df1 = spark.createDataFrame([(1, 10)], ["id", "a"])
        df2 = spark.createDataFrame([(1, 10)], ["id", "a"])
        fused = FusedAssertion([DualJoinAssertEqual(keys=["id"], values=["a"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is True

    def test_batch_evaluate_dual_join_same_keys_fused(self):
        evaluator = SparkFusedAssertionEvaluator()
        df = spark.createDataFrame([(1, 10, 100)], ["id", "a", "b"])
        fused = FusedAssertion([
            DualJoinAssertEqual(keys=["id"], values=["a"]),
            DualJoinAssertEqual(keys=["id"], values=["b"]),
        ])
        results = evaluator.evaluate(fused, [df, df])
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is True

    def test_batch_evaluate_dual_join_different_keys_separate_batches(self):
        evaluator = SparkFusedAssertionEvaluator()
        df = spark.createDataFrame([(1, 10, 100)], ["id", "a", "b"])
        fused = FusedAssertion([
            DualJoinAssertEqual(keys=["id"], values=["a"]),
            DualJoinAssertEqual(keys=["a"], values=["b"]),
        ])
        results = evaluator.evaluate(fused, [df, df])
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is True
