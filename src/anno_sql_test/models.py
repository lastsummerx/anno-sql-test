from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, TypeVar


@dataclass
class Assertion:
    pass


@dataclass
class SingleAssertion(Assertion):
    pass


@dataclass
class DualJoinAssertion(Assertion):
    keys: list[str]
    values: list[str]


@dataclass
class MultiAggAssertion(Assertion):
    fields: list[str]
    agg: str


@dataclass
class SingleAssertAll(SingleAssertion):
    predicate: str


@dataclass
class SingleAssertAny(SingleAssertion):
    predicate: str


@dataclass
class SingleAssertNone(SingleAssertion):
    predicate: str


@dataclass
class SingleAssertEmpty(SingleAssertion):
    pass


@dataclass
class SingleAssertNotEmpty(SingleAssertion):
    pass


@dataclass
class SingleAssertUnique(Assertion):
    fields: list[str]


@dataclass
class MultiAggAssertEqual(MultiAggAssertion):
    pass


@dataclass
class MultiAggAssertNumericRatioApprox(MultiAggAssertion):
    ratio: float = 0.01


@dataclass
class MultiAggAssertNumericDeltaApprox(MultiAggAssertion):
    delta: float


@dataclass
class MultiAggAssertTemporalApprox(MultiAggAssertion):
    duration_seconds: float


@dataclass
class DualJoinAssertEqual(DualJoinAssertion):
    pass


@dataclass
class DualJoinAssertNumericRatioApprox(DualJoinAssertion):
    ratio: float = 0.01


@dataclass
class DualJoinAssertNumericDeltaApprox(DualJoinAssertion):
    delta: float


@dataclass
class DualJoinAssertTemporalApprox(DualJoinAssertion):
    duration_seconds: float


T_co = TypeVar('T_co', bound=Assertion, covariant=True)


@dataclass
class FusedAssertion(Generic[T_co]):
    assertions: list[T_co]


type GeneralAssertion = Assertion | FusedAssertion[Assertion]


@dataclass
class SqlTestCase:
    name: str
    dependencies: list[str] = field(default_factory=list)
    assertions: list[Assertion] = field(default_factory=list)
    sql_statements: list[str] = field(default_factory=list)


@dataclass
class SqlNonTestBlock:
    sql_statements: list[str] = field(default_factory=list)


@dataclass
class AssertionResult:
    assertion: GeneralAssertion
    passed: bool
    message: str = ""


@dataclass
class SqlTestResult:
    case: SqlTestCase
    passed: bool
    skipped: bool = False
    skip_reason: str = ""
    assertion_results: list[AssertionResult] = field(default_factory=list)
    duration: float = 0.0


@dataclass
class SqlTestSuite:
    path: Path
    blocks: list[SqlTestCase | SqlNonTestBlock] = field(default_factory=list)

    @property
    def cases(self) -> list[SqlTestCase]:
        return [b for b in self.blocks if isinstance(b, SqlTestCase)]


@dataclass
class SqlTestSuiteResult:
    suite: SqlTestSuite
    non_test_errors: list[str] = field(default_factory=list)
    results: list[SqlTestResult] = field(default_factory=list)
    start_time: float = 0.0
    duration: float = 0.0
