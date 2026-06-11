from pyspark.sql import SparkSession

from anno_sql_test.evaluators.spark._dual_join import (
    DualJoinFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._multi_agg import (
    MultiAggFusedAssertionEvaluator,
)
from anno_sql_test.evaluators.spark._single import (
    SinglePredicateFusedAssertionEvaluator,
)
from anno_sql_test.models import (
    DualJoinAssertEqual,
    DualJoinAssertNumericRatioApprox,
    FusedAssertion,
    MultiAggAssertEqual,
    SingleAssert,
)

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())


class TestSinglePredicateFusedAssertionEvaluator:

    def test_single_predicate_pass(self):
        df = spark.createDataFrame([(1,), (2,)], ["a"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[SingleAssert(predicate="a > 0")])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 1
        assert results[0].passed is True

    def test_single_predicate_fail(self):
        df = spark.createDataFrame([(1,), (None,)], ["a"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[SingleAssert(predicate="a is not null")])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 1
        assert results[0].passed is False
        assert "violating" in results[0].message

    def test_multiple_predicates_fused(self):
        df = spark.createDataFrame([(1, 10), (2, 20), (3, 2)], ["a", "b"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            SingleAssert(predicate="a > 0"),
            SingleAssert(predicate="b > a"),
        ])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False
        assert "violating" in results[1].message

    def test_all_predicates_pass(self):
        df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            SingleAssert(predicate="a > 0"),
            SingleAssert(predicate="b > 0"),
        ])
        results = evaluator.evaluate(fused, [df])
        assert all(r.passed for r in results)

    def test_all_predicates_fail(self):
        df = spark.createDataFrame([(1,), (2,)], ["a"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            SingleAssert(predicate="a < 0"),
            SingleAssert(predicate="a > 10"),
        ])
        results = evaluator.evaluate(fused, [df])
        assert all(not r.passed for r in results)

    def test_empty_dataframe(self):
        df = spark.createDataFrame([], schema="a: int")
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[SingleAssert(predicate="a > 0")])
        results = evaluator.evaluate(fused, [df])
        assert len(results) == 1
        assert results[0].passed is True

    def test_failure_message_accuracy(self):
        df = spark.createDataFrame([(1,), (0,), (-1,)], ["a"])
        evaluator = SinglePredicateFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[SingleAssert(predicate="a > 0")])
        results = evaluator.evaluate(fused, [df])
        assert results[0].passed is False
        assert "2" in results[0].message
        assert "66.7" in results[0].message


class TestMultiAggFusedAssertionEvaluator:

    def test_single_agg_pass(self):
        df1 = spark.createDataFrame([(1,)], ["a"])
        df2 = spark.createDataFrame([(2,)], ["a"])
        evaluator = MultiAggFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[MultiAggAssertEqual(agg="count", fields=["*"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is True

    def test_single_agg_fail(self):
        df1 = spark.createDataFrame([(1,)], ["a"])
        df2 = spark.createDataFrame([(1,), (2,)], ["a"])
        evaluator = MultiAggFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[MultiAggAssertEqual(agg="count", fields=["*"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is False

    def test_multiple_aggs_fused(self):
        df1 = spark.createDataFrame([(1, 10)], ["a", "b"])
        df2 = spark.createDataFrame([(2, 20)], ["a", "b"])
        evaluator = MultiAggFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            MultiAggAssertEqual(agg="count", fields=["*"]),
            MultiAggAssertEqual(agg="sum", fields=["a"]),
        ])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False

    def test_specific_fields(self):
        df1 = spark.createDataFrame([(1, 10)], ["a", "b"])
        df2 = spark.createDataFrame([(2, 10)], ["a", "b"])
        evaluator = MultiAggFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            MultiAggAssertEqual(agg="sum", fields=["a"]),
            MultiAggAssertEqual(agg="sum", fields=["b"]),
        ])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 2
        assert results[0].passed is False
        assert results[1].passed is True

    def test_less_than_two_dataframes(self):
        df1 = spark.createDataFrame([(1,)], ["a"])
        evaluator = MultiAggFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[MultiAggAssertEqual(agg="count", fields=["*"])])
        results = evaluator.evaluate(fused, [df1])
        assert len(results) == 1
        assert results[0].passed is False
        assert "Expected at least 2 DataFrames" in results[0].message


class TestDualJoinFusedAssertionEvaluator:

    def test_single_equal_pass(self):
        df1 = spark.createDataFrame([(1, 10)], ["id", "a"])
        df2 = spark.createDataFrame([(1, 10)], ["id", "a"])
        evaluator = DualJoinFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[DualJoinAssertEqual(keys=["id"], values=["a"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is True

    def test_single_equal_fail(self):
        df1 = spark.createDataFrame([(1, 10)], ["id", "a"])
        df2 = spark.createDataFrame([(1, 20)], ["id", "a"])
        evaluator = DualJoinFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[DualJoinAssertEqual(keys=["id"], values=["a"])])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 1
        assert results[0].passed is False

    def test_equal_and_approx_same_keys(self):
        df1 = spark.createDataFrame([(1, 10.0)], ["id", "a"])
        df2 = spark.createDataFrame([(1, 10.5)], ["id", "a"])
        evaluator = DualJoinFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            DualJoinAssertEqual(keys=["id"], values=["a"]),
            DualJoinAssertNumericRatioApprox(keys=["id"], values=["a"], ratio=0.1),
        ])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 2
        assert results[0].passed is False
        assert results[1].passed is True

    def test_different_value_columns(self):
        df1 = spark.createDataFrame([(1, 10, 100)], ["id", "a", "b"])
        df2 = spark.createDataFrame([(1, 10, 200)], ["id", "a", "b"])
        evaluator = DualJoinFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[
            DualJoinAssertEqual(keys=["id"], values=["a"]),
            DualJoinAssertEqual(keys=["id"], values=["b"]),
        ])
        results = evaluator.evaluate(fused, [df1, df2])
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False

    def test_not_exactly_two_dataframes(self):
        df1 = spark.createDataFrame([(1,)], ["a"])
        evaluator = DualJoinFusedAssertionEvaluator()
        fused = FusedAssertion(assertions=[DualJoinAssertEqual(keys=["a"], values=["a"])])
        results = evaluator.evaluate(fused, [df1])
        assert len(results) == 1
        assert results[0].passed is False
        assert "exactly 2" in results[0].message.lower()
