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


def test_assert_all_star_is_not_null_pass():
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="* is not null"), [df])
    assert result.passed is True


def test_assert_all_star_is_not_null_fail():
    df = spark.createDataFrame([(1, "a"), (None, "b")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="* is not null"), [df])
    assert result.passed is False


def test_assert_all_star_expr_pass():
    df = spark.createDataFrame([("a", "x"), ("b", "y")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="nvl(*, '@') != ''"), [df])
    assert result.passed is True


def test_assert_all_star_expr_fail():
    df = spark.createDataFrame([("a", "x"), ("", "y")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="nvl(*, '@') != ''"), [df])
    assert result.passed is False


def test_assert_any_star_is_not_null_pass():
    df = spark.createDataFrame([(None, 1), (1, None)], schema="a: int, b: int")
    result = evaluator.evaluate(SingleAssertAny(predicate="* is not null"), [df])
    assert result.passed is True


def test_assert_any_star_is_not_null_fail():
    df = spark.createDataFrame([(None, None), (1, None)], schema="a: int, b: int")
    result = evaluator.evaluate(SingleAssertAny(predicate="* is not null"), [df])
    assert result.passed is False


def test_assert_none_star_is_null_pass():
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertNone(predicate="* is null"), [df])
    assert result.passed is True


def test_assert_none_star_is_null_fail():
    df = spark.createDataFrame([(1, "a"), (None, "b")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertNone(predicate="* is null"), [df])
    assert result.passed is False


def test_assert_all_numeric_star_is_not_null_pass():
    df = spark.createDataFrame([(1, "a", 3.0), (2, "b", 4.0)], ["a", "b", "c"])
    result = evaluator.evaluate(SingleAssertAll(predicate="numeric:* is not null"), [df])
    assert result.passed is True


def test_assert_all_numeric_star_is_not_null_fail():
    df = spark.createDataFrame([(1, "a", None), (2, "b", 4.0)], ["a", "b", "c"])
    result = evaluator.evaluate(SingleAssertAll(predicate="numeric:* is not null"), [df])
    assert result.passed is False


def test_assert_all_glob_suffix_pass():
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["a_cnt", "b_other"])
    result = evaluator.evaluate(SingleAssertAll(predicate="*_cnt is not null"), [df])
    assert result.passed is True


def test_assert_all_glob_suffix_fail():
    df = spark.createDataFrame([(None, "x"), (2, "y")], ["a_cnt", "b_other"])
    result = evaluator.evaluate(SingleAssertAll(predicate="*_cnt is not null"), [df])
    assert result.passed is False


def test_assert_all_type_glob_combined():
    df = spark.createDataFrame([(1, "x", 3.0), (2, "y", None)], ["a_cnt", "b_str", "c_cnt"])
    result = evaluator.evaluate(SingleAssertAll(predicate="numeric:*_cnt is not null"), [df])
    assert result.passed is False


def test_assert_all_glob_prefix_pass():
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["cnt_a", "other_b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="cnt_* is not null"), [df])
    assert result.passed is True


def test_assert_all_star_quoted_literal():
    df = spark.createDataFrame([("a", "x"), ("b", "y")], ["a", "b"])
    result = evaluator.evaluate(SingleAssertAll(predicate="nvl(*, '*:') != ''"), [df])
    assert result.passed is True


def test_assert_all_string_star_pass():
    df = spark.createDataFrame([(1, "a", "x"), (2, "b", "y")], ["a", "b", "c"])
    result = evaluator.evaluate(SingleAssertAll(predicate="string:* is not null"), [df])
    assert result.passed is True


def test_assert_unique_with_count_column():
    df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 1)], ["count", "x"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["count"]), [df])
    assert result.passed is True

    result = evaluator.evaluate(SingleAssertUnique(fields=["count", "x"]), [df])
    assert result.passed is True

    df_dup = spark.createDataFrame([("a", 1), ("a", 1), ("b", 2)], ["count", "x"])
    result = evaluator.evaluate(SingleAssertUnique(fields=["count", "x"]), [df_dup])
    assert result.passed is False


def test_assert_all_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=3)
    df = spark.createDataFrame([(1,), (None,), (3,)], ["a"])
    result = sample_eval.evaluate(SingleAssertAll(predicate="a is not null"), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    assert isinstance(result.failure_sample, list)
    assert len(result.failure_sample) == 1
    assert result.failure_sample[0]["a"] is None


def test_assert_all_failure_sample_multiple_rows():
    sample_eval = SparkAssertionEvaluator(sample_count=10)
    df = spark.createDataFrame([(1,), (None,), (None,), (4,)], ["a"])
    result = sample_eval.evaluate(SingleAssertAll(predicate="a is not null"), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    assert len(result.failure_sample) == 2
    for row in result.failure_sample:
        assert row["a"] is None


def test_assert_all_failure_sample_disabled():
    sample_eval = SparkAssertionEvaluator(sample_count=0)
    df = spark.createDataFrame([(1,), (None,)], ["a"])
    result = sample_eval.evaluate(SingleAssertAll(predicate="a is not null"), [df])
    assert result.passed is False
    assert result.failure_sample is None


def test_assert_empty_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=3)
    df = spark.createDataFrame([(1,), (2,)], ["a"])
    result = sample_eval.evaluate(SingleAssertEmpty(), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    assert isinstance(result.failure_sample, list)
    assert len(result.failure_sample) <= 3


def test_assert_any_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=3)
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = sample_eval.evaluate(SingleAssertAny(predicate="a > 10"), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    assert len(result.failure_sample) == 3


def test_assert_none_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=2)
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    result = sample_eval.evaluate(SingleAssertNone(predicate="a > 2"), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    assert len(result.failure_sample) == 1
    assert result.failure_sample[0]["a"] == 3


def test_assert_unique_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=5)
    df = spark.createDataFrame([(1,), (1,), (2,), (3,), (3,)], ["a"])
    result = sample_eval.evaluate(SingleAssertUnique(fields=["a"]), [df])
    assert result.passed is False
    assert result.failure_sample is not None
    vals = {row["a"] for row in result.failure_sample}
    assert vals == {1, 3}


def test_assert_not_empty_no_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=3)
    df = spark.createDataFrame([], schema="a: int")
    result = sample_eval.evaluate(SingleAssertNotEmpty(), [df])
    assert result.passed is False
    assert result.failure_sample is None


def test_assert_pass_has_no_failure_sample():
    sample_eval = SparkAssertionEvaluator(sample_count=3)
    df = spark.createDataFrame([(1,), (2,)], ["a"])
    result = sample_eval.evaluate(SingleAssertAll(predicate="a > 0"), [df])
    assert result.passed is True
    assert result.failure_sample is None
