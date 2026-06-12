import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from anno_sql_test.evaluators.optimizer import group_as_fused
from anno_sql_test.evaluators.spark import (
    SparkAssertionEvaluator,
    SparkFusedAssertionEvaluator,
)
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
    def __init__(self, spark, evaluator: SparkAssertionEvaluator | None = None):
        self._spark = spark
        self._evaluator = evaluator or SparkFusedAssertionEvaluator()

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
        errors = []
        for sql in block.sql_statements:
            try:
                logging.debug("Executing non-test SQL: %s", sql[:80])
                self._spark.sql(sql)
            except Exception as e:
                logging.warning("Non-test SQL error: %s", e)
                errors.append(f"Non-test SQL error: {e}")
        return errors

    def _should_skip(self, case: SqlTestCase, name_to_result: dict) -> tuple[bool, str]:
        for dep in case.dependencies:
            dep_result = name_to_result.get(dep)
            if dep_result is not None and not dep_result.passed:
                logging.info("Skipping '%s': dependency '%s' failed", case.name, dep)
                return True, f"dependency '{dep}' failed"
        return False, ""

    def _execute_sql_statements(self, case: SqlTestCase) -> tuple[list[DataFrame], SqlTestResult | None]:
        dataframes = []
        try:
            for i, sql in enumerate(case.sql_statements):
                logging.debug("Executing SQL statement %d for '%s': %s", i + 1, case.name, sql[:80])
                df = self._spark.sql(sql)
                dataframes.append(df)
            logging.debug("Successfully executed %d SQL statements for '%s'", len(dataframes), case.name)
            return dataframes, None
        except Exception as e:
            logging.warning("SQL execution failed for '%s': %s", case.name, e)
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
        logging.debug("Evaluating %d assertions for '%s'", len(case.assertions), case.name)
        if isinstance(self._evaluator, SparkFusedAssertionEvaluator):
            fused_assertions = group_as_fused(case.assertions)
            result = []
            for fused in fused_assertions:
                try:
                    result.extend(self._evaluator.evaluate(fused, dataframes))
                except Exception as e:
                    for a in fused.assertions:
                        ar = AssertionResult(assertion=a, passed=False, message=f"Fused evaluation error: {e}")
                        result.append(ar)
            return result
        assertion_results = []

        for assertion in case.assertions:
            try:
                ar = self._evaluator.evaluate(assertion, dataframes)
            except Exception as e:
                ar = AssertionResult(assertion=assertion, passed=False, message=f"Evaluator error: {e}")
            assertion_results.append(ar)
        return assertion_results

    def _process_test_case(self, case: SqlTestCase, name_to_result: dict) -> SqlTestResult:
        logging.info("Running test: %s", case.name)
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
