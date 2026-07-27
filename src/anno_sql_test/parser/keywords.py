import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Self

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    Assertion,
    ColumnSpec,
    DualAggAssertEqual,
    DualAggAssertNumericDeltaApprox,
    DualAggAssertNumericRatioApprox,
    DualAggAssertTemporalApprox,
    DualJoinAssertEqual,
    DualJoinAssertion,
    DualJoinAssertLambda,
    DualJoinAssertNumericApprox,
    DualJoinAssertTemporalApprox,
    DualRowsAssertEqual,
    DualRowsAssertion,
    GlobTemplateColumn,
    LambdaFunc,
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertSetEqual,
    SingleAssertUnique,
)
from anno_sql_test.parser._utils import (
    _parse_fields,
    _parse_float,
    _parse_int,
    _parse_iso_duration_to_seconds,
    _smart_split,
    parse_column_spec,
)


@dataclass(frozen=True)
class ParseInput:
    rest: str
    source: str
    start_line: int
    end_line: int


@dataclass
class AnnotationKeyword[T](ABC):
    @abstractmethod
    def build(self, parse_input: ParseInput) -> T:
        ...


@dataclass
class AssertKeyword(AnnotationKeyword[Assertion]):
    pass


class SingleAssertAllKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertAll(predicate=parse_column_spec(parse_input.rest))


class SingleAssertAnyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertAny(predicate=parse_column_spec(parse_input.rest))


class SingleAssertNoneKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertNone(predicate=parse_column_spec(parse_input.rest))


class SingleAssertEmptyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertEmpty()


class SingleAssertNotEmptyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertNotEmpty()


class SingleAssertUniqueKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        cols = tuple(parse_column_spec(c.strip()) for c in _smart_split(parse_input.rest, ",") if c.strip())
        return SingleAssertUnique(fields=cols)


class SingleAssertSetEqualKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        split = parse_input.rest.split(None, 1)
        if len(split) != 2:
            raise ParseError(f"Expected '<col> <set>' in: {parse_input.source}")
        col_str = split[0].strip()
        set_str = split[1].strip()
        if set_str[0] != '(' or set_str[-1] != ')':
            raise ParseError(f"Expected <set> surround by () but {set_str}")
        set_str = set_str[1:-1]
        values = {v.strip() for v in _smart_split(set_str, ',') if v.strip()}
        if not values:
            raise ParseError(f"Empty set in: {parse_input.source}")
        return SingleAssertSetEqual(column=parse_column_spec(col_str), set_values=tuple(values))


class _BaseDualAggAssertKeyword(AssertKeyword):
    _COL = "col"
    _GROUP_BY_REGEX: ClassVar[re.Pattern[str]] = re.compile(r'\bgroup\s+by\b', re.IGNORECASE)

    @classmethod
    def _parse_group_by(
        cls, fields_str: str,
    ) -> tuple[tuple[ColumnSpec, ...], tuple[ColumnSpec, ...]]:
        m = cls._GROUP_BY_REGEX.search(fields_str)
        if m:
            before = fields_str[:m.start()].strip()
            after = fields_str[m.end():].strip()
            fields = _parse_fields(before) if before else ()
            keys = _parse_fields(after) if after else ()
            return fields, keys
        return _parse_fields(fields_str), ()

    @classmethod
    def _parse_agg_fields(
        cls, parse_input: ParseInput,
    ) -> tuple[LambdaFunc, tuple[ColumnSpec, ...], tuple[ColumnSpec, ...]]:
        parts = _smart_split(parse_input.rest.strip(), r'\s+', 1)
        if len(parts) < 2 or '' in parts:
            raise ParseError(f"Expected '<agg> <fields>' in: {parse_input.source}")
        agg = cls._make_agg_template(parts[0])
        fields, keys = cls._parse_group_by(parts[1])
        return agg, fields, keys

    @classmethod
    def _parse_agg_param_fields(
        cls, parse_input: ParseInput, param_label: str = "param",
    ) -> tuple[LambdaFunc, str, tuple[ColumnSpec, ...], tuple[ColumnSpec, ...]]:
        parts = _smart_split(parse_input.rest.strip(), r'\s+', 2)
        if len(parts) < 3 or '' in parts:
            raise ParseError(f"Expected '<agg> <{param_label}> <fields>' in: {parse_input.source}")
        agg = cls._make_agg_template(parts[0])
        fields, keys = cls._parse_group_by(parts[2])
        return agg, parts[1], fields, keys

    @classmethod
    def _make_agg_template(cls, agg: str) -> LambdaFunc:
        if '->' in agg:
            return LambdaFunc.from_str(agg[1:-1])
        return LambdaFunc.from_str(f'{cls._COL} -> {agg}({cls._COL})')


class DualAggAssertEqualKeyword(_BaseDualAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, fields, keys = self._parse_agg_fields(parse_input)
        return DualAggAssertEqual(agg=agg, fields=fields, keys=keys)


class DualAggAssertNumericRatioKeyword(_BaseDualAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, ratio_str, fields, keys = self._parse_agg_param_fields(parse_input, "ratio")
        ratio = _parse_float(ratio_str, "ratio", parse_input.source)
        return DualAggAssertNumericRatioApprox(agg=agg, fields=fields, keys=keys, ratio=ratio)


class DualAggAssertNumericDeltaKeyword(_BaseDualAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, delta_str, fields, keys = self._parse_agg_param_fields(parse_input, "delta")
        delta = _parse_float(delta_str, "delta", parse_input.source)
        return DualAggAssertNumericDeltaApprox(agg=agg, fields=fields, keys=keys, delta=delta)


class DualAggAssertTemporalKeyword(_BaseDualAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, duration, fields, keys = self._parse_agg_param_fields(parse_input, "duration")
        duration_seconds = _parse_iso_duration_to_seconds(duration, parse_input.source)
        return DualAggAssertTemporalApprox(agg=agg, fields=fields, keys=keys, duration_seconds=duration_seconds)


@dataclass
class _RowParam:
    row_ratio: float = 0.0
    row_delta: int = 0

    row_ratio_match: re.Match | None = None
    row_delta_match: re.Match | None = None

    ROW_RATIO_REGEX: ClassVar[re.Pattern[str]] = re.compile(r'\brow_ratio=(\S+)\s*')
    ROW_DELTA_REGEX: ClassVar[re.Pattern[str]] = re.compile(r'\brow_delta=(\S+)\s*')

    @classmethod
    def from_str(cls, s: str, source: str) -> Self:
        row_ratio_match = re.search(cls.ROW_RATIO_REGEX, s)
        row_delta_match = re.search(cls.ROW_DELTA_REGEX, s)
        row_ratio = _parse_float(row_ratio_match.group(1), "row_ratio", source) if row_ratio_match else 0.0
        row_delta = _parse_int(row_delta_match.group(1), "row_delta", source) if row_delta_match else 0

        if row_ratio > 0 and row_delta > 0:
            raise ParseError(f"Cannot specify both row_ratio and row_delta in: {source}")

        if row_delta < 0:
            raise ParseError(f"row_delta must be non-negative in: {source}")

        if row_ratio < 0 or row_ratio > 1:
            raise ParseError(f"row_ratio must be between 0 and 1 in: {source}")

        return cls(
            row_ratio=row_ratio, row_delta=row_delta,
            row_ratio_match=row_ratio_match, row_delta_match=row_delta_match,
        )


class _BaseDualJoinAssertKeyword(AssertKeyword):
    JOIN_REGEX = re.compile(r'\b(?:(\w{4,5})?\s+join)?$', re.IGNORECASE)
    ON_REGEX = re.compile(r'\bon\b', re.IGNORECASE)
    VALUES_REGEX = re.compile(r'\bvalues\b', re.IGNORECASE)

    @classmethod
    def _parse_dual_join_assert(cls, rest: str, source: str) -> tuple[DualJoinAssertion, str]:
        if not re.search(cls.ON_REGEX, rest):
            raise ParseError(f"Expected 'on <keys> [values <vals>]' in: {source}")
        before_on, after_on = re.split(cls.ON_REGEX, rest, maxsplit=1)
        before_on = before_on.strip()
        after_on = after_on.strip()

        values_match = re.search(cls.VALUES_REGEX, after_on)
        if values_match is None:
            keys = _parse_fields(after_on, "keys")
            values = ()
        else:
            keys = _parse_fields(after_on[:values_match.start()], "keys")
            values = _parse_fields(after_on[values_match.end():], "values")
        join_match = re.search(cls.JOIN_REGEX, before_on)
        join_type = join_match.group(1).lower() if join_match and join_match.group(1) else "full"
        before_join = before_on[:join_match.start()].strip() if join_match else before_on.strip()

        row_param = _RowParam.from_str(before_join, source)

        base_assertion = DualJoinAssertion(
            keys=keys,
            values=values,
            join_type=join_type,
            row_ratio=row_param.row_ratio,
            row_delta=row_param.row_delta,
        )
        return base_assertion, before_join


class DualJoinAssertEqualKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        p, _ = self._parse_dual_join_assert(parse_input.rest, parse_input.source)
        return DualJoinAssertEqual(**p.__dict__)


class DualJoinAssertNumericApproxKeyword(_BaseDualJoinAssertKeyword):
    VAL_RATIO_REGEX = re.compile(r'\bval_ratio=(\S+)\s*')
    VAL_DELTA_REGEX = re.compile(r'\bval_delta=(\S+)\s*')

    def build(self, parse_input: ParseInput) -> Assertion:
        p, before_join = self._parse_dual_join_assert(parse_input.rest, parse_input.source)
        val_ratio_match = re.search(self.VAL_RATIO_REGEX, before_join)
        val_delta_match = re.search(self.VAL_DELTA_REGEX, before_join)
        print(before_join, val_delta_match)
        val_ratio = _parse_float(val_ratio_match.group(1), "val_ratio", before_join) if val_ratio_match else 0.0
        val_delta = _parse_float(val_delta_match.group(1), "val_delta", before_join) if val_delta_match else 0.0
        return DualJoinAssertNumericApprox(val_ratio=val_ratio, val_delta=val_delta, **p.__dict__)


class DualJoinAssertTemporalKeyword(_BaseDualJoinAssertKeyword):
    DURATION_REGEX = re.compile(r'\bduration=(\S+)\s*')

    def build(self, parse_input: ParseInput) -> Assertion:
        p, before_join = self._parse_dual_join_assert(parse_input.rest, parse_input.source)
        duration_match = re.search(self.DURATION_REGEX, before_join)
        if not duration_match:
            raise ParseError(f"Expected 'duration=<iso>' in: {parse_input.source}")
        duration_seconds = _parse_iso_duration_to_seconds(duration_match.group(1), parse_input.source)
        return DualJoinAssertTemporalApprox(duration_seconds=duration_seconds, **p.__dict__)


class DualJoinAssertLambdaKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        p, before_join = self._parse_dual_join_assert(parse_input.rest, parse_input.source)
        lambda_str = _smart_split(before_join, r'\s+')[-1]

        try:
            comparator = LambdaFunc.from_str(lambda_str[1:-1])
        except ValueError as e:
            raise ParseError(f"Invalid lambda comparator in: {parse_input.source}") from e
        return DualJoinAssertLambda(comparator=comparator, **p.__dict__)


class _BaseDualRowsAssertKeyword(AssertKeyword):
    @classmethod
    def _parse_rows_assert(cls, rest: str) -> DualRowsAssertion:
        rest = rest.strip()
        if not rest:
            return DualRowsAssertion(fields=(GlobTemplateColumn(glob="*"),))
        row_param = _RowParam.from_str(rest, rest)

        start = row_param.row_ratio_match.end() if row_param.row_ratio_match else 0
        start = max(start, row_param.row_delta_match.end() if row_param.row_delta_match else 0)
        after_param = rest[start:]
        return DualRowsAssertion(
            fields=_parse_fields(after_param),
            row_ratio=row_param.row_ratio, row_delta=row_param.row_delta,
        )


class DualRowsAssertEqualKeyword(_BaseDualRowsAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        p = self._parse_rows_assert(parse_input.rest)
        return DualRowsAssertEqual(**p.__dict__)


class DependencyKeyword(AnnotationKeyword[list[str]]):
    def build(self, parse_input: ParseInput) -> list[str]:
        targets = parse_input.rest.split() if parse_input.rest else []
        return targets


class VarKeyword(AnnotationKeyword[tuple[str, str]]):
    def build(self, parse_input: ParseInput) -> tuple[str, str]:
        parts = tuple(x.strip() for x in parse_input.rest.split("=", 1))
        if len(parts) != 2 or not parts[0].strip():
            raise ParseError(f"Invalid @var syntax, expected name=value in: {parse_input.source}")
        return parts[0], parts[1]


class TestKeyword(AnnotationKeyword[str]):
    def build(self, parse_input: ParseInput) -> str:
        name = parse_input.rest.strip() if parse_input.rest else ""
        if not name:
            raise ParseError(f"Empty test name at line: {parse_input.source}")
        return name


class NonTestKeyword(AnnotationKeyword[None]):
    def build(self, parse_input: ParseInput) -> None:
        return None


_KEYWORD_MAP: dict[str, AnnotationKeyword] = {
    "assert_all": SingleAssertAllKeyword(),
    "assert_any": SingleAssertAnyKeyword(),
    "assert_none": SingleAssertNoneKeyword(),
    "assert_empty": SingleAssertEmptyKeyword(),
    "assert_not_empty": SingleAssertNotEmptyKeyword(),
    "assert_unique": SingleAssertUniqueKeyword(),
    "assert_set_equal": SingleAssertSetEqualKeyword(),
    "assert_agg_equal": DualAggAssertEqualKeyword(),
    "assert_agg_numeric_ratio_approx": DualAggAssertNumericRatioKeyword(),
    "assert_agg_numeric_delta_approx": DualAggAssertNumericDeltaKeyword(),
    "assert_agg_temporal_approx": DualAggAssertTemporalKeyword(),
    "assert_join_equal": DualJoinAssertEqualKeyword(),
    "assert_join_numeric_approx": DualJoinAssertNumericApproxKeyword(),
    "assert_join_temporal_approx": DualJoinAssertTemporalKeyword(),
    "assert_join_lambda": DualJoinAssertLambdaKeyword(),
    "assert_rows_equal": DualRowsAssertEqualKeyword(),
    "dependency": DependencyKeyword(),
    "var": VarKeyword(),
    "test": TestKeyword(),
    "non_test": NonTestKeyword(),
}
