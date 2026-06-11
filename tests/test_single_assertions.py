from pyspark.sql import SparkSession

from anno_sql_test.evaluators.spark import SparkAssertionEvaluator
from anno_sql_test.models import (
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())
evaluator = SparkAssertionEvaluator()


def test_assert_predicate_pass():
    df = spark.createDataFrame([(1,), (2,)], ["a"])
    result = evaluator.evaluate(SingleAssertAll(predicate="a > 0"), [df])
    assert result.passed is True


def test_assert_predicate_fail():
    df = spark.createDataFrame([(1,), (None,)], ["a"])
    result = evaluator.evaluate(SingleAssertAll(predicate="a is not null"), [df])
    assert result.passed is False


def test_assert_empty_pass():
    df = spark.createDataFrame([], schema="a: int")
    result = evaluator.evaluate(SingleAssertEmpty(), [df])
    assert result.passed is True


def test_assert_empty_fail():
    df = spark.createDataFrame([(1,)], ["a"])
    result = evaluator.evaluate(SingleAssertEmpty(), [df])
    assert result.passed is False


def test_assert_not_empty_pass():
    df = spark.createDataFrame([(1,)], ["a"])
    result = evaluator.evaluate(SingleAssertNotEmpty(), [df])
    assert result.passed is True


def test_assert_not_empty_fail():
    df = spark.createDataFrame([], schema="a: int")
    result = evaluator.evaluate(SingleAssertNotEmpty(), [df])
    assert result.passed is False


def test_assert_unique_single_column_pass():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["a"]), [df])
    assert result.passed is True


def test_assert_unique_single_column_fail():
    df = spark.createDataFrame([(1,), (1,), (2,)], ["a"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["a"]), [df])
    assert result.passed is False


def test_assert_unique_composite_pass():
    df = spark.createDataFrame([(1, "x"), (1, "y"), (2, "x")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["a", "b"]), [df])
    assert result.passed is True


def test_assert_unique_composite_fail():
    df = spark.createDataFrame([(1, "x"), (1, "x"), (2, "y")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["a", "b"]), [df])
    assert result.passed is False


def test_assert_any_pass():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertAny(predicate="a > 2"), [df])
    assert result.passed is True


def test_assert_any_fail():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertAny(predicate="a > 10"), [df])
    assert result.passed is False


def test_assert_any_empty_df_fail():
    df = spark.createDataFrame([], schema="a: int")
    result = evaluator.evaluate(SingleAssertAny(predicate="a > 0"), [df])
    assert result.passed is False


def test_assert_any_all_rows_match():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertAny(predicate="a > 0"), [df])
    assert result.passed is True


def test_assert_none_pass():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertNone(predicate="a > 10"), [df])
    assert result.passed is True


def test_assert_none_fail():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertNone(predicate="a > 2"), [df])
    assert result.passed is False


def test_assert_none_empty_df_pass():
    df = spark.createDataFrame([], schema="a: int")
    result = evaluator.evaluate(SingleAssertNone(predicate="a > 0"), [df])
    assert result.passed is True


def test_assert_none_all_rows_match_fail():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = evaluator.evaluate(SingleAssertNone(predicate="a > 0"), [df])
    assert result.passed is False


def test_assert_unique_with_count_column():
    df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 1)], ["count", "x"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["count"]), [df])
    assert result.passed is True

    result = evaluator.evaluate(SingleAssertUnique(fields=["count", "x"]), [df])
    assert result.passed is True

    df_dup = spark.createDataFrame([("a", 1), ("a", 1), ("b", 2)], ["count", "x"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["count", "x"]), [df_dup])
    assert result.passed is False
