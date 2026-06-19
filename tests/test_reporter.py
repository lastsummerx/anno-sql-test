from pathlib import Path

from anno_sql_test.models import (
    AssertionResult,
    SingleAssertAll,
    SqlTestCase,
    SqlTestResult,
    SqlTestSuite,
    SqlTestSuiteResult,
)
from anno_sql_test.reporter import ConsoleReporter


def _make_suite_result(name: str, passed: bool, skip: bool = False):
    case = SqlTestCase(name=name)
    tr = SqlTestResult(case=case, passed=passed, skipped=skip)
    suite = SqlTestSuite(path=Path(f"/fake/{name}.sql"), blocks=[case])
    return SqlTestSuiteResult(suite=suite, results=[tr])


def test_report_pass(capsys):
    sr = _make_suite_result("test_pass", passed=True)
    ec = ConsoleReporter().report([sr])
    captured = capsys.readouterr()
    assert "PASS" in captured.out
    assert ec == 0


def test_report_fail(capsys):
    sr = _make_suite_result("test_fail", passed=False)
    tr = sr.results[0]
    tr.assertion_results.append(AssertionResult(
        assertion=SingleAssertAll(predicate="a > 0"),
        passed=False, message="values not > 0",
    ))
    ec = ConsoleReporter().report([sr])
    captured = capsys.readouterr()
    assert "FAIL" in captured.out
    assert "values not > 0" in captured.out
    assert ec == 1


def test_report_skip(capsys):
    sr = _make_suite_result("test_skip", passed=False, skip=True)
    sr.results[0].skip_reason = "dependency failed"
    ec = ConsoleReporter().report([sr])
    captured = capsys.readouterr()
    assert "SKIP" in captured.out
    assert "dependency failed" in captured.out
    assert ec == 0


def test_report_mixed(capsys):
    suite = SqlTestSuite(path=Path("/fake/mixed.sql"))
    case1 = SqlTestCase(name="pass1")
    case2 = SqlTestCase(name="fail2")
    suite.blocks.extend([case1, case2])
    results = [
        SqlTestResult(case=case1, passed=True),
        SqlTestResult(case=case2, passed=False, assertion_results=[
            AssertionResult(
                assertion=SingleAssertAll(predicate="a > 0"), passed=False, message="fail msg",
            ),
        ]),
    ]
    sr = SqlTestSuiteResult(suite=suite, results=results)
    ec = ConsoleReporter().report([sr])
    captured = capsys.readouterr()
    assert "1 passed, 1 failed" in captured.out
    assert ec == 1


def test_report_summary_line(capsys):
    suite = SqlTestSuite(path=Path("/fake/summary.sql"))
    results = []
    for i in range(3):
        c = SqlTestCase(name=f"t{i}")
        suite.blocks.append(c)
        results.append(SqlTestResult(case=c, passed=True))
    sr = SqlTestSuiteResult(suite=suite, results=results)
    ec = ConsoleReporter().report([sr])
    captured = capsys.readouterr()
    assert "3 passed in" in captured.out
    assert "summary.sql" in captured.out
    assert ec == 0
