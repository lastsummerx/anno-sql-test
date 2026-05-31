from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from anno_sql_test.evaluators.base import BaseAssertionEvaluator
from anno_sql_test.evaluators.spark import SparkAssertionEvaluator
from anno_sql_test.models import (
    Assertion,
    AssertionResult,
    SqlNonTestBlock,
    SqlTestCase,
    SqlTestResult,
    SqlTestSuite,
    SqlTestSuiteResult,
)


class BaseRunner(ABC):
    @abstractmethod
    def run(self, suite: SqlTestSuite) -> SqlTestSuiteResult:
        ...


class SparkRunner(BaseRunner):
    def __init__(self, spark, evaluator: BaseAssertionEvaluator | None = None):
        self._spark = spark
        self._evaluator = evaluator or SparkAssertionEvaluator()

    def run(self, suite: SqlTestSuite) -> SqlTestSuiteResult:
        name_to_result: dict = {}
        results: list[SqlTestResult] = []
        non_test_errors: list[str] = []

        for block in suite.blocks:
            if isinstance(block, SqlNonTestBlock):
                # Collect errors from non-test SQL blocks
                non_test_errors.extend(self._handle_non_test_block(block))
            else:
                # Process each test case
                result = self._process_test_case(block, name_to_result)
                name_to_result[result.case.name] = result
                results.append(result)

        return SqlTestSuiteResult(suite=suite, non_test_errors=non_test_errors, results=results)

    def _handle_non_test_block(self, block: SqlNonTestBlock) -> list[str]:
        """Execute all SQL statements in a non-test block and return error messages."""
        errors = []
        for sql in block.sql_statements:
            try:
                self._spark.sql(sql)
            except Exception as e:
                errors.append(f"Non-test SQL error: {e}")
        return errors

    def _should_skip(self, case: SqlTestCase, name_to_result: dict) -> tuple[bool, str]:
        """Check if a test case should be skipped due to failing dependencies."""
        for dep in case.dependencies:
            dep_result = name_to_result.get(dep)
            if dep_result is not None and not dep_result.passed:
                return True, f"dependency '{dep}' failed"
        return False, ""

    def _execute_sql_statements(self, case: SqlTestCase) -> tuple[list[DataFrame], SqlTestResult | None]:
        """
        Execute all SQL statements for a test case.
        Returns (dataframes, None) on success, or (None, error_result) on failure.
        """
        dataframes = []
        try:
            for sql in case.sql_statements:
                df = self._spark.sql(sql)
                dataframes.append(df)
            return dataframes, None
        except Exception as e:
            error_result = SqlTestResult(
                case=case,
                passed=False,
                assertion_results=[
                    AssertionResult(
                        assertion=Assertion(),
                        passed=False,
                        message=f"SQL execution error: {e}",
                    ),
                ],
            )
            return [], error_result

    def _evaluate_assertions(self, case: SqlTestCase, dataframes: list[DataFrame]) -> list[AssertionResult]:
        """Evaluate all assertions of a test case. Returns (assertion_results, all_passed)."""
        assertion_results = []

        for assertion in case.assertions:
            try:
                ar = self._evaluator.evaluate(assertion, dataframes)
            except Exception as e:
                ar = AssertionResult(assertion=assertion, passed=False, message=f"Evaluator error: {e}")
            assertion_results.append(ar)
        return assertion_results

    def _process_test_case(self, case: SqlTestCase, name_to_result: dict) -> SqlTestResult:
        """
        Process a single test case:
        1. Dependency check
        2. SQL execution
        3. Assertion evaluation
        """
        # 1. Check dependencies
        skip, skip_reason = self._should_skip(case, name_to_result)
        if skip:
            return SqlTestResult(case=case, passed=False, skipped=True, skip_reason=skip_reason)

        # 2. Execute SQL statements
        dataframes, sql_error_result = self._execute_sql_statements(case)
        if sql_error_result is not None:
            return sql_error_result

        # 3. Evaluate assertions
        assertion_results = self._evaluate_assertions(case, dataframes)
        all_passed = all(ar.passed for ar in assertion_results)
        return SqlTestResult(case=case, passed=all_passed, assertion_results=assertion_results)
