from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from anno_sql_test.evaluators.spark import SparkAssertionEvaluator
from anno_sql_test.models import (
    DualJoinAssertEqual,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    MultiAggAssertEqual,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
)

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())
evaluator = SparkAssertionEvaluator()


def test_agg_equal_count_all_pass():
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(3, "c"), (4, "d")], ["id", "name"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="count", fields=["*"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_count_all_fail():
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(3,)], ["id"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="count", fields=["*"]),
        [df1, df2],
    )
    assert result.passed is False


def test_agg_equal_numeric_star_pass():
    df1 = spark.createDataFrame([(10, "a", 1), (20, "b", 2)], ["val", "name", "id"])
    df2 = spark.createDataFrame([(10, "c", 1), (20, "d", 2)], ["val", "name", "id"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["numeric:*"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_numeric_star_fail():
    df1 = spark.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "name", "val"])
    df2 = spark.createDataFrame([(3, "c", 5), (4, "d", 25)], ["id", "name", "val"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["numeric:*"]),
        [df1, df2],
    )
    assert result.passed is False


def test_agg_equal_glob_suffix_pass():
    df1 = spark.createDataFrame([(10, 1), (20, 2)], ["val_cnt", "id"])
    df2 = spark.createDataFrame([(10, 1), (20, 2)], ["val_cnt", "id"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["*_cnt"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_type_glob_combined_pass():
    df1 = spark.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "name", "val_cnt"])
    df2 = spark.createDataFrame([(1, "c", 10), (2, "d", 20)], ["id", "name", "val_cnt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["numeric:*_cnt"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_sum_pass():
    df1 = spark.createDataFrame([(100,), (200,)], ["amt"])
    df2 = spark.createDataFrame([(150,), (150,)], ["amt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["amt"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_sum_fail():
    df1 = spark.createDataFrame([(100,), (200,)], ["amt"])
    df2 = spark.createDataFrame([(100,), (100,)], ["amt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["amt"]),
        [df1, df2],
    )
    assert result.passed is False


def test_agg_numeric_ratio_approx_pass():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(100.000001,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericRatioApprox(agg="sum", fields=["v"], ratio=0.01),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_numeric_ratio_approx_fail():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(200.0,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericRatioApprox(agg="sum", fields=["v"], ratio=0.01),
        [df1, df2],
    )
    assert result.passed is False


def test_agg_equal_three_dfs():
    df1 = spark.createDataFrame([(1,)], ["a"])
    df2 = spark.createDataFrame([(1,)], ["a"])
    df3 = spark.createDataFrame([(1,)], ["a"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="count", fields=["*"]),
        [df1, df2, df3],
    )
    assert result.passed is True


def test_agg_equal_three_dfs_one_diff():
    df1 = spark.createDataFrame([(1, 100), (2, 200)], ["id", "amt"])
    df2 = spark.createDataFrame([(3, 50), (4, 250)], ["id", "amt"])
    df3 = spark.createDataFrame([(5, 500)], ["id", "amt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["amt"]),
        [df1, df2, df3],
    )
    assert result.passed is False
    assert "DF2" in result.message


def test_agg_equal_three_dfs_all_diff():
    df1 = spark.createDataFrame([(100,)], ["amt"])
    df2 = spark.createDataFrame([(200,)], ["amt"])
    df3 = spark.createDataFrame([(300,)], ["amt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["amt"]),
        [df1, df2, df3],
    )
    assert result.passed is False
    assert "DF0" in result.message
    assert "DF1" in result.message
    assert "DF2" in result.message


def test_agg_equal_four_dfs_mixed():
    df1 = spark.createDataFrame([(100,), (100,)], ["amt"])
    df2 = spark.createDataFrame([(200,)], ["amt"])
    df3 = spark.createDataFrame([(300,)], ["amt"])
    df4 = spark.createDataFrame([(400,)], ["amt"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["amt"]),
        [df1, df2, df3, df4],
    )
    assert result.passed is False
    assert "DF0" in result.message
    assert "DF3" in result.message


def test_agg_ratio_approx_three_dfs_fail():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(101.0,)], ["v"])
    df3 = spark.createDataFrame([(200.0,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericRatioApprox(agg="sum", fields=["v"], ratio=0.01),
        [df1, df2, df3],
    )
    assert result.passed is False


def test_agg_delta_approx_three_dfs_fail():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(105.0,)], ["v"])
    df3 = spark.createDataFrame([(150.0,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericDeltaApprox(agg="sum", fields=["v"], delta=10.0),
        [df1, df2, df3],
    )
    assert result.passed is False


def test_equal_by_key_pass():
    left = spark.createDataFrame([(1, "a", 100), (2, "b", 200)], ["id", "name", "amt"])
    right = spark.createDataFrame([(1, "a", 100), (2, "b", 200)], ["id", "name", "amt"])
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["name", "amt"]),
        [left, right],
    )
    assert result.passed is True


def test_equal_by_key_fail():
    left = spark.createDataFrame([(1, "a", 100)], ["id", "name", "amt"])
    right = spark.createDataFrame([(1, "a", 999)], ["id", "name", "amt"])
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["amt"]),
        [left, right],
    )
    assert result.passed is False


def test_equal_by_key_extra_left_row():
    left = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    right = spark.createDataFrame([(1, "a")], ["id", "name"])
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["name"]),
        [left, right],
    )
    assert result.passed is False


def test_numeric_ratio_approx_by_key_pass():
    left = spark.createDataFrame([(1, 100.0)], ["id", "v"])
    right = spark.createDataFrame([(1, 100.000001)], ["id", "v"])
    result = evaluator.evaluate(
        DualJoinAssertNumericRatioApprox(keys=["id"], values=["v"]),
        [left, right],
    )
    assert result.passed is True


def test_agg_equal_sum_multi_field_pass():
    df1 = spark.createDataFrame([(100, 10), (200, 20)], ["a", "b"])
    df2 = spark.createDataFrame([(150, 15), (150, 15)], ["a", "b"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["a", "b"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_sum_multi_field_fail():
    df1 = spark.createDataFrame([(100, 10), (200, 20)], ["a", "b"])
    df2 = spark.createDataFrame([(100, 15), (100, 15)], ["a", "b"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["a", "b"]),
        [df1, df2],
    )
    assert result.passed is False


def test_agg_equal_sum_expression_pass():
    df1 = spark.createDataFrame([(100, 10), (200, 20)], ["a", "b"])
    df2 = spark.createDataFrame([(50, 5), (250, 25)], ["a", "b"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["a + b"]),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_equal_sum_expression_fail():
    df1 = spark.createDataFrame([(100, 10), (200, 20)], ["a", "b"])
    df2 = spark.createDataFrame([(50, 5), (250, 20)], ["a", "b"])
    result = evaluator.evaluate(
        MultiAggAssertEqual(agg="sum", fields=["a + b"]),
        [df1, df2],
    )
    assert result.passed is False


def test_equal_by_key_expression_values_pass():
    left = spark.createDataFrame([(1, 100, 10), (2, 200, 20)], ["id", "a", "b"])
    right = spark.createDataFrame([(1, 50, 60), (2, 100, 120)], ["id", "a", "b"])
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["a + b"]),
        [left, right],
    )
    assert result.passed is True


def test_equal_by_key_expression_values_fail():
    left = spark.createDataFrame([(1, 100, 10), (2, 200, 20)], ["id", "a", "b"])
    right = spark.createDataFrame([(1, 50, 60), (2, 100, 5)], ["id", "a", "b"])
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["a + b"]),
        [left, right],
    )
    assert result.passed is False


def test_assert_join_equal_requires_two_dfs():
    result = evaluator.evaluate(
        DualJoinAssertEqual(keys=["id"], values=["name"]),
        [
            spark.createDataFrame([(1,)], ["id"]),
            spark.createDataFrame([(1,)], ["id"]),
            spark.createDataFrame([(1,)], ["id"]),
        ],
    )
    assert result.passed is False
    assert "exactly 2" in result.message


def test_numeric_delta_approx_pass():
    left = spark.createDataFrame([(1, 100.0)], ["id", "v"])
    right = spark.createDataFrame([(1, 105.0)], ["id", "v"])
    result = evaluator.evaluate(
        DualJoinAssertNumericDeltaApprox(keys=["id"], values=["v"], delta=10.0),
        [left, right],
    )
    assert result.passed is True


def test_numeric_delta_approx_fail():
    left = spark.createDataFrame([(1, 100.0)], ["id", "v"])
    right = spark.createDataFrame([(1, 120.0)], ["id", "v"])
    result = evaluator.evaluate(
        DualJoinAssertNumericDeltaApprox(keys=["id"], values=["v"], delta=10.0),
        [left, right],
    )
    assert result.passed is False


def test_numeric_delta_approx_non_numeric_fails():
    left = spark.createDataFrame([(1, "foo")], ["id", "v"])
    right = spark.createDataFrame([(1, "bar")], ["id", "v"])
    result = evaluator.evaluate(
        DualJoinAssertNumericDeltaApprox(keys=["id"], values=["v"], delta=10.0),
        [left, right],
    )
    assert result.passed is False
    assert "not numeric" in result.message


def test_agg_numeric_delta_approx_pass():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(105.0,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericDeltaApprox(agg="sum", fields=["v"], delta=10.0),
        [df1, df2],
    )
    assert result.passed is True


def test_agg_numeric_delta_approx_fail():
    df1 = spark.createDataFrame([(100.0,)], ["v"])
    df2 = spark.createDataFrame([(120.0,)], ["v"])
    result = evaluator.evaluate(
        MultiAggAssertNumericDeltaApprox(agg="sum", fields=["v"], delta=10.0),
        [df1, df2],
    )
    assert result.passed is False


def test_temporal_approx_pass():
    left = spark.createDataFrame(
        [(1, "2024-01-01 10:00:00")], ["id", "ts"],
    )
    right = spark.createDataFrame(
        [(1, "2024-01-01 10:00:30")], ["id", "ts"],
    )
    left = left.withColumn("ts", F.col("ts").cast("timestamp"))
    right = right.withColumn("ts", F.col("ts").cast("timestamp"))
    result = evaluator.evaluate(
        DualJoinAssertTemporalApprox(keys=["id"], values=["ts"], duration_seconds=60.0),
        [left, right],
    )
    assert result.passed is True


def test_temporal_approx_fail():
    left = spark.createDataFrame(
        [(1, "2024-01-01 10:00:00")], ["id", "ts"],
    )
    right = spark.createDataFrame(
        [(1, "2024-01-01 10:05:00")], ["id", "ts"],
    )
    left = left.withColumn("ts", F.col("ts").cast("timestamp"))
    right = right.withColumn("ts", F.col("ts").cast("timestamp"))
    result = evaluator.evaluate(
        DualJoinAssertTemporalApprox(keys=["id"], values=["ts"], duration_seconds=60.0),
        [left, right],
    )
    assert result.passed is False


def test_temporal_approx_non_temporal_fails():
    left = spark.createDataFrame([(1, 100)], ["id", "v"])
    right = spark.createDataFrame([(1, 200)], ["id", "v"])
    result = evaluator.evaluate(
        DualJoinAssertTemporalApprox(keys=["id"], values=["v"], duration_seconds=60.0),
        [left, right],
    )
    assert result.passed is False
    assert "not temporal" in result.message
