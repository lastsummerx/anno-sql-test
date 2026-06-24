from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Generic, TypeVar


class FieldType(Enum):
    NUMERIC = 'numeric'
    STRING = 'string'
    TEMPORAL = 'temporal'


@dataclass
class ExprColumn:
    expr: str


@dataclass
class GlobTemplateColumn:
    glob: str
    type_filter: FieldType | None = None
    excepts: list[str] = field(default_factory=list)
    expr: str = "{col}"

    def format(self, field: str) -> str:
        return self.expr.format(col=field)


@dataclass
class AggFunc:
    func: str

    def format(self, field: str) -> str:
        return self.func.format(col=field)


type ColumnSpec = ExprColumn | GlobTemplateColumn


@dataclass
class Assertion:
    pass


@dataclass
class SingleAssertion(Assertion):
    pass


@dataclass
class DualJoinAssertion(Assertion):
    keys: list[ColumnSpec]
    values: list[ColumnSpec]


@dataclass
class MultiAggAssertion(Assertion):
    fields: list[ColumnSpec]
    agg: AggFunc


@dataclass
class SingleAssertPredicate(SingleAssertion):
    predicate: ColumnSpec


@dataclass
class SingleAssertAll(SingleAssertPredicate):
    pass


@dataclass
class SingleAssertAny(SingleAssertPredicate):
    pass


@dataclass
class SingleAssertNone(SingleAssertPredicate):
    pass


@dataclass
class SingleAssertEmpty(SingleAssertion):
    pass


@dataclass
class SingleAssertNotEmpty(SingleAssertion):
    pass


@dataclass
class SingleAssertUnique(Assertion):
    fields: list[ColumnSpec]


@dataclass
class MultiAggAssertEqual(MultiAggAssertion):
    pass


@dataclass
class MultiAggAssertNumericRatioApprox(MultiAggAssertion):
    ratio: float


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
    ratio: float


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
    failure_sample: Any | None = None


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
