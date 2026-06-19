import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from anno_sql_test.models import Assertion, AssertionResult, FusedAssertion


class BaseAssertionEvaluator[T: Assertion, DF](ABC):
    @abstractmethod
    def evaluate(self, assertion: T, dataframes: list[DF]) -> AssertionResult:
        ...


@dataclass(frozen=True)
class StepResult[DAT, QRY, RST]:
    prepared: DAT
    plan: QRY
    executed: RST


class StepwiseAssertionMixin[T, DF, DAT, QRY, RST](ABC):
    def validate(self, assertion: T, dataframes: list[DF]) -> list[tuple[str, Assertion]]:
        return []

    @abstractmethod
    def prepare(self, assertion: T, dataframes: list[DF]) -> DAT:
        ...

    @abstractmethod
    def build(self, assertion: T, prepared: DAT) -> QRY:
        ...

    @abstractmethod
    def execute(self, prepared: DAT, plan: QRY) -> RST:
        ...

    @abstractmethod
    def finalize(self, assertion: T, step_result: StepResult[DAT, QRY, RST]) -> list[AssertionResult]:
        ...

    @abstractmethod
    def on_error(self, assertion: T, error: Exception) -> list[AssertionResult]:
        ...

    def sample_failure(self, assertion: T, step_result: StepResult[DAT, QRY, RST]) -> Any | None:
        return None

    def cleanup(self, prepared: DAT) -> None:
        pass

    def step_evaluate(self, assertion: T, dataframes: list[DF]) -> list[AssertionResult]:
        self.logger.debug("Validating %s", type(assertion).__name__)
        validation_error = self.validate(assertion, dataframes)
        if validation_error:
            self.logger.warning("Validation failed: %s", validation_error)
            return [AssertionResult(assertion=a, passed=False, message=msg) for msg, a in validation_error]
        prepared = None
        try:
            self.logger.debug("Preparing %s", type(assertion).__name__)
            prepared = self.prepare(assertion, dataframes)
            self.logger.debug("Building plan for %s", type(assertion).__name__)
            plan = self.build(assertion, prepared)
            self.logger.debug("Executing plan for %s", type(assertion).__name__)
            exec_result = self.execute(prepared, plan)
            step_result = StepResult(prepared=prepared, plan=plan, executed=exec_result)
            assertion_results = self.finalize(assertion, step_result)
            self.logger.debug("Enriching result for %s", type(assertion).__name__)
            return self._enrich_with_failure_samples(assertion, step_result, assertion_results)
        except Exception as e:
            self.logger.exception("Error evaluating %s: %s", type(assertion).__name__, e)
            return self.on_error(assertion, e)
        finally:
            if prepared is not None:
                self.cleanup(prepared)

    @abstractmethod
    def _enrich_with_failure_samples(
        self, assertion: T, step_result: StepResult[DAT, QRY, RST], results: list[AssertionResult],
    ) -> list[AssertionResult]:
        ...

    @property
    def logger(self):
        return logging.getLogger(type(self).__module__)


class BaseStepwiseAssertionEvaluator[T: Assertion, DF, DAT, QRY, RST](
    BaseAssertionEvaluator[T, DF],
    StepwiseAssertionMixin[T, DF, DAT, QRY, RST],
):
    def evaluate(self, assertion: T, dataframes: list[DF]) -> AssertionResult:
        return self.step_evaluate(assertion, dataframes)[0]

    def on_error(self, assertion: T, error: Exception) -> list[AssertionResult]:
        return [AssertionResult(assertion=assertion, passed=False, message=str(error))]

    def _enrich_with_failure_samples(
        self, assertion: T, step_result: StepResult[DAT, QRY, RST], results: list[AssertionResult],
    ) -> list[AssertionResult]:
        result = results[0]
        if not result.passed:
            try:
                sample = self.sample_failure(assertion, step_result)
                if sample is not None:
                    result = replace(result, failure_sample=sample)
            except Exception as e:
                self.logger.warning("Failure sampling failed: %s", e)
        return [result]


class BaseFusedAssertionEvaluator[T: Assertion, DF](ABC):
    @abstractmethod
    def evaluate(self, assertion: FusedAssertion[T], dataframes: list[DF]) -> list[AssertionResult]:
        ...

    @abstractmethod
    def support_assertions(self) -> set[type[Assertion]]:
        ...


class BaseStepwiseFusedAssertionEvaluator[T: Assertion, DF, DAT, QRY, RST](
    BaseFusedAssertionEvaluator[T, DF],
    StepwiseAssertionMixin[FusedAssertion[T], DF, DAT, QRY, RST],
):
    def evaluate(self, assertion: FusedAssertion[T], dataframes: list[DF]) -> list[AssertionResult]:
        return self.step_evaluate(assertion, dataframes)

    def on_error(self, assertion: FusedAssertion[T], error: Exception) -> list[AssertionResult]:
        return [AssertionResult(assertion=assertion, passed=False, message=str(error))]


class SimpleFusedAssertionEvaluator[DF](BaseFusedAssertionEvaluator[Assertion, DF]):
    def __init__(self, evalurator: BaseAssertionEvaluator[Assertion, DF]):
        self._evalurator = evalurator

    def evaluate(self, assertion: FusedAssertion[Assertion], dataframes: list[DF]) -> list[AssertionResult]:
        return [self._evalurator.evaluate(a, dataframes) for a in assertion.assertions]

    def support_assertions(self) -> set[type[Assertion]]:
        return {Assertion}


class DelegatingStepwiseFusedAssertionEvaluator[T: Assertion, DF, DAT, QRY, RST](
    BaseStepwiseFusedAssertionEvaluator[T, DF, list[DAT], list[QRY], RST],
):
    @abstractmethod
    def get_evaluator_map(self) -> Mapping[type[T], BaseStepwiseAssertionEvaluator[T, DF, DAT, QRY, RST]]:
        ...

    def validate(
        self, assertion: FusedAssertion[T], dataframes: list[DF],
    ) -> list[tuple[str, Assertion]]:
        rst = []
        for asrt in assertion.assertions:
            evaluator = self.get_evaluator_map()[type(asrt)]
            self.logger.debug("Delegating validate %s -> %s", type(asrt).__name__, type(evaluator).__name__)
            rst.extend(evaluator.validate(asrt, dataframes))
        return rst

    def build(
        self, assertion: FusedAssertion[T], prepared: list[DAT],
    ) -> list[QRY]:
        rst = []
        for asrt, sub_prepared in zip(assertion.assertions, prepared):
            evaluator = self.get_evaluator_map()[type(asrt)]
            self.logger.debug("Delegating build %s -> %s", type(asrt).__name__, type(evaluator).__name__)
            rst.append(evaluator.build(asrt, sub_prepared))
        return rst

    def finalize(
        self, assertion: FusedAssertion[T],step_result: StepResult[list[DAT], list[QRY], RST],
    ) -> list[AssertionResult]:
        rst = []
        for i, asrt in enumerate(assertion.assertions):
            evaluator = self.get_evaluator_map()[type(asrt)]
            self.logger.debug("Delegating finalize %s -> %s", type(asrt).__name__, type(evaluator).__name__)
            sub_step_result = StepResult(
                prepared=step_result.prepared[i],
                plan=step_result.plan[i],
                executed=step_result.executed,
            )
            rst.extend(evaluator.finalize(asrt, sub_step_result))
        return rst

    def support_assertions(self) -> set[type[Assertion]]:
        return set(self.get_evaluator_map().keys())

    def sample_failure(
        self, assertion: FusedAssertion[T], step_result: StepResult[list[DAT], list[QRY], RST],
    ) -> list[Any | None]:
        rst = []
        for i, asrt in enumerate(assertion.assertions):
            evaluator = self.get_evaluator_map()[type(asrt)]
            self.logger.debug("Delegating sample_failure %s -> %s", type(asrt).__name__, type(evaluator).__name__)
            sub_step_result = StepResult(
                prepared=step_result.prepared[i],
                plan=step_result.plan[i],
                executed=step_result.executed,
            )
            rst.append(evaluator.sample_failure(asrt, sub_step_result))
        return rst

    def _enrich_with_failure_samples(
        self,
        assertion: FusedAssertion[T],
        step_result: StepResult[list[DAT], list[QRY], RST],
        results: list[AssertionResult],
    ) -> list[AssertionResult]:
        enriched = []
        samples = self.sample_failure(assertion, step_result)
        for result, sample in zip(results, samples):
            if sample is not None:
                result = replace(result, failure_sample=sample)
            enriched.append(result)
        return enriched
