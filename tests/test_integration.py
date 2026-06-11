from pathlib import Path

from pyspark.sql import SparkSession

from anno_sql_test.discover import discover_sql_files
from anno_sql_test.parser import parse_suite
from anno_sql_test.reporter import ConsoleReporter
from anno_sql_test.runner import SparkRunner

spark = (SparkSession.builder.master("local[1]")
    .appName("test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())


FILE_CONTENT = """-- @TEST test_predicate
-- @assert_all a > 0
SELECT 1 AS a;

-- @TEST test_not_empty
-- @assert_not_empty
SELECT 1 AS a;

-- @TEST test_empty
-- @assert_empty
SELECT 1 AS a WHERE 1 = 0;

-- @TEST test_unique
-- @assert_unique a
SELECT 1 AS a UNION ALL SELECT 2 AS a;

-- @TEST test_agg_equal
-- @assert_agg_equal count *
SELECT 1 AS a, 2 AS b;
SELECT 1 AS a, 2 AS b;

-- @TEST test_dependent
-- @dependency test_predicate
-- @assert_all a > 0
SELECT 1 AS a;
"""


def test_integration_full_pipeline(tmp_path: Path):
    filepath = tmp_path / "test.sql"
    filepath.write_text(FILE_CONTENT)

    files = discover_sql_files(filepath)
    assert len(files) == 1

    suites = parse_suite(files)
    assert len(suites) == 1
    suite = suites[0]
    assert len(suite.cases) == 6

    runner = SparkRunner(spark)
    result = runner.run(suite)
    assert len(result.results) == 6

    assert result.results[0].passed is True
    assert result.results[1].passed is True
    assert result.results[2].passed is True
    assert result.results[3].passed is True
    assert result.results[4].passed is True
    assert result.results[5].passed is True
    assert result.results[5].skipped is False

    ec = ConsoleReporter().report(result)
    assert ec == 0


def test_integration_failure(tmp_path: Path):
    filepath = tmp_path / "fail.sql"
    filepath.write_text("-- @TEST fail\n-- @assert_all a > 10\nSELECT 1 AS a;\n")

    suites = parse_suite(discover_sql_files(filepath))
    runner = SparkRunner(spark)
    result = runner.run(suites[0])
    assert result.results[0].passed is False
    ec = ConsoleReporter().report(result)
    assert ec == 1
