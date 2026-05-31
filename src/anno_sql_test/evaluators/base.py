from abc import ABC, abstractmethod

from anno_sql_test.models import Assertion, AssertionResult


class BaseAssertionEvaluator[T: Assertion, DF](ABC):
    @abstractmethod
    def evaluate(self, assertion: T, dataframes: list[DF]) -> AssertionResult:
        ...
