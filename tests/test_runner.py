from pathlib import Path

from pyspark.sql import SparkSession

from anno_sql_test.models import (
    MultiAggAssertEqual,
    SingleAssertAll,
    SingleAssertNotEmpty,
    SqlTestCase,
    SqlTestSuite,
    SqlTestSuiteResult,
)
from anno_sql_test.runner import SparkRunner

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())
runner = SparkRunner(spark)


def test_run_single_pass():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(
        name="test1",
        assertions=[SingleAssertAll(predicate="a > 0")],
        sql_statements=["SELECT 1 AS a"],
    ))
    result = runner.run(suite)
    assert isinstance(result, SqlTestSuiteResult)
    assert len(result.results) == 1
    assert result.results[0].passed is True


def test_run_single_fail():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(
        name="test1",
        assertions=[SingleAssertAll(predicate="a > 10")],
        sql_statements=["SELECT 1 AS a"],
    ))
    result = runner.run(suite)
    assert result.results[0].passed is False


def test_run_dual_agg():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(
        name="test_agg",
        assertions=[MultiAggAssertEqual(agg="count({col})", fields=["*"])],
        sql_statements=["SELECT 1 AS a", "SELECT 2 AS a"],
    ))
    result = runner.run(suite)
    assert result.results[0].passed is True


def test_run_skip_dependency():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(
        name="base",
        assertions=[SingleAssertAll(predicate="a > 10")],
        sql_statements=["SELECT 1 AS a"],
    ))
    suite.blocks.append(SqlTestCase(
        name="dependent",
        dependencies=["base"],
        assertions=[SingleAssertAll(predicate="a > 0")],
        sql_statements=["SELECT 1 AS a"],
    ))
    result = runner.run(suite)
    assert result.results[0].passed is False
    assert result.results[1].skipped is True


def test_run_empty_suite():
    suite = SqlTestSuite(path=Path("/fake/empty.sql"))
    result = runner.run(suite)
    assert len(result.results) == 0


def test_run_multiple_assertions():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(
        name="multi",
        assertions=[
            SingleAssertAll(predicate="a > 0"),
            SingleAssertNotEmpty(),
        ],
        sql_statements=["SELECT 1 AS a"],
    ))
    result = runner.run(suite)
    assert result.results[0].passed is True


def test_run_cycle_dependency_no_crash():
    suite = SqlTestSuite(path=Path("/fake/test.sql"))
    suite.blocks.append(SqlTestCase(name="a", dependencies=["b"]))
    suite.blocks.append(SqlTestCase(name="b", dependencies=["a"]))
    result = runner.run(suite)
    assert len(result.results) == 2
