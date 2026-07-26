import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Generic, Literal, TypeVar


class FieldType(Enum):
    NUMERIC = 'numeric'
    STRING = 'string'
    TEMPORAL = 'temporal'


@dataclass(frozen=True)
class LambdaFunc:
    param_names: tuple[str, ...]
    template: str

    NAME_REGEX: ClassVar[re.Pattern[str]] = re.compile(r'\w+')

    def format(self, *param_values: str) -> str:
        if len(param_values) != len(self.param_names):
            raise ValueError(f"Expected {len(self.param_names)} values, got {len(param_values)}")
        kwargs = dict(zip(self.param_names, param_values))
        return self.template.format(**kwargs)

    @classmethod
    def escape(cls, expr: str) -> str:
        return expr.replace('{', '{{').replace('}', '}}')  # Escape braces for str.format

    @classmethod
    def from_str(cls, expr: str) -> "LambdaFunc":
        if '->' not in expr:
            raise ValueError(f"Invalid lambda expression: {expr}")

        left, body = expr.split('->', 1)
        left = left.strip()
        body = cls.escape(body.strip())

        if left.startswith('(') and left.endswith(')'):
            param_names = tuple(name.strip() for name in left[1:-1].split(','))
        else:
            param_names = (left,)
        for name in param_names:
            if not cls.NAME_REGEX.fullmatch(name):
                raise ValueError(f"Invalid parameter name: {name}")
            body = re.sub(rf'\b{name}\b', f'{{{name}}}', body)

        return cls(param_names=param_names, template=body)

    @classmethod
    def from_segments(cls, segments: list[tuple[Literal['var', 'txt'], str]]) -> "LambdaFunc":
        param_name_list = []
        template_seg = []
        for t, x in segments:
            if t == 'var':
                if not cls.NAME_REGEX.fullmatch(x):
                    raise ValueError(f"Invalid parameter name: {x}")
                param_name_list.append(x)
                template_seg.append(f"{{{x}}}")
            else:
                template_seg.append(cls.escape(x))
        template = ''.join(template_seg)
        return LambdaFunc(param_names=tuple(param_name_list), template=template)


@dataclass(frozen=True)
class ExprColumn:
    expr: str


@dataclass(frozen=True)
class GlobTemplateColumn:
    glob: str
    type_filter: FieldType | None = None
    excepts: tuple[str, ...] = ()
    expr: LambdaFunc = LambdaFunc.from_str('col -> col')


type ColumnSpec = ExprColumn | GlobTemplateColumn


@dataclass
class Assertion:
    pass


@dataclass
class SingleAssertion(Assertion):
    pass


@dataclass
class DualJoinAssertion(Assertion):
    keys: tuple[ColumnSpec, ...]
    values: tuple[ColumnSpec, ...]
    row_ratio: float = 0.0
    row_delta: int = 0
    join_type: str = "full"

    def grouping_key(self):
        return (frozenset(self.keys), self.join_type, self.row_delta, self.row_ratio)


@dataclass
class MultiAggAssertion(Assertion):
    fields: tuple[ColumnSpec, ...]
    agg: LambdaFunc


@dataclass
class DualRowsAssertion(Assertion):
    fields: tuple[ColumnSpec, ...]
    row_ratio: float = 0.0
    row_delta: int = 0


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
    fields: tuple[ColumnSpec, ...]


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
class DualRowsAssertEqual(DualRowsAssertion):
    pass


@dataclass
class DualJoinAssertEqual(DualJoinAssertion):
    pass


@dataclass
class DualJoinAssertNumericApprox(DualJoinAssertion):
    val_ratio: float = 0.0
    val_delta: float = 0.0


@dataclass
class DualJoinAssertTemporalApprox(DualJoinAssertion):
    duration_seconds: float = 0.0


@dataclass
class DualJoinAssertLambda(DualJoinAssertion):
    comparator: LambdaFunc = LambdaFunc.from_str('(a, b) -> a = b')


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
