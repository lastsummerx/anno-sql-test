import re
from abc import ABC, abstractmethod
from dataclasses import dataclass

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    Assertion,
    DualJoinAssertEqual,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    MultiAggAssertEqual,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
    SingleAssertAll,
    SingleAssertAny,
    SingleAssertEmpty,
    SingleAssertNone,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)
from anno_sql_test.parser._utils import (
    _parse_field_list,
    _parse_float,
    _parse_iso_duration_to_seconds,
    _smart_split,
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
        return SingleAssertAll(predicate=parse_input.rest)


class SingleAssertAnyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertAny(predicate=parse_input.rest)


class SingleAssertNoneKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertNone(predicate=parse_input.rest)


class SingleAssertEmptyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertEmpty()


class SingleAssertNotEmptyKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        return SingleAssertNotEmpty()


class SingleAssertUniqueKeyword(AssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        cols = [c.strip() for c in _smart_split(parse_input.rest, ",") if c.strip()]
        return SingleAssertUnique(fields=cols)


class _BaseMultiAggAssertKeyword(AssertKeyword):
    _COL = "{col}"

    @classmethod
    def _parse_agg_fields(cls, parse_input: ParseInput) -> tuple[str, list[str]]:
        parts = _smart_split(parse_input.rest.strip(), r'\s+', 1)
        if len(parts) < 2 or '' in parts:
            raise ParseError(f"Expected '<agg> <fields>' in: {parse_input.source}")
        return cls._make_agg_template(parts[0]), _parse_field_list(parts[1])

    @classmethod
    def _parse_agg_param_fields(cls, parse_input: ParseInput, param_label: str = "param") -> tuple[str, str, list[str]]:
        parts = _smart_split(parse_input.rest.strip(), r'\s+', 2)
        if len(parts) < 3 or '' in parts:
            raise ParseError(f"Expected '<agg> <{param_label}> <fields>' in: {parse_input.source}")
        return cls._make_agg_template(parts[0]), parts[1], _parse_field_list(parts[2])

    @classmethod
    def _make_agg_template(cls, agg: str) -> str:
        if '->' in agg:
            inner = agg.removeprefix('(').removesuffix(')').strip()
            param, body = (x.strip() for x in inner.split('->', 1))
            body = body.replace('{', '{{').replace('}', '}}')
            return re.sub(r'\b' + re.escape(param) + r'\b', cls._COL, body)
        return f"{agg}({cls._COL})"


class MultiAggAssertEqualKeyword(_BaseMultiAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, fields = self._parse_agg_fields(parse_input)
        return MultiAggAssertEqual(agg=agg, fields=fields)


class MultiAggAssertNumericRatioKeyword(_BaseMultiAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, ratio_str, fields = self._parse_agg_param_fields(parse_input, "ratio")
        ratio = _parse_float(ratio_str, "ratio", parse_input.source)
        return MultiAggAssertNumericRatioApprox(agg=agg, fields=fields, ratio=ratio)


class MultiAggAssertNumericDeltaKeyword(_BaseMultiAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, delta_str, fields = self._parse_agg_param_fields(parse_input, "delta")
        delta = _parse_float(delta_str, "delta", parse_input.source)
        return MultiAggAssertNumericDeltaApprox(agg=agg, fields=fields, delta=delta)


class MultiAggAssertTemporalKeyword(_BaseMultiAggAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        agg, duration, fields = self._parse_agg_param_fields(parse_input, "duration")
        duration_seconds = _parse_iso_duration_to_seconds(duration, parse_input.source)
        return MultiAggAssertTemporalApprox(agg=agg, fields=fields, duration_seconds=duration_seconds)


class _BaseDualJoinAssertKeyword(AssertKeyword):
    @classmethod
    def _parse_dual(cls, parse_input: ParseInput) -> tuple[list[str], list[str]]:
        return cls._parse_dual_join_assert(parse_input.rest, parse_input.source)

    @classmethod
    def _parse_param_dual(cls, parse_input: ParseInput, param_label: str = "param") -> tuple[str, list[str], list[str]]:
        parts = parse_input.rest.split(None, 1)
        if len(parts) < 2:
            raise ParseError(f"Expected '<{param_label}> on <keys> values <vals>' in: {parse_input.source}")
        return parts[0], *cls._parse_dual_join_assert(parts[1], parse_input.source)

    @classmethod
    def _parse_dual_join_assert(cls, rest: str, source: str):
        rest = rest.strip()
        if not rest.lower().startswith("on "):
            raise ParseError(f"Expected 'on <keys> values <vals>' in: {source}")
        rest = rest[3:]
        values_idx = rest.lower().find(" values ")
        if values_idx == -1:
            raise ParseError(f"Expected 'on <keys> values <vals>' in: {source}")
        keys_str = rest[:values_idx].strip()
        vals_str = rest[values_idx + len(" values "):].strip()
        keys = _smart_split(keys_str, ",")
        vals = _smart_split(vals_str, ",")
        if not keys or '' in keys or not vals or '' in vals:
            raise ParseError(f"Empty keys or values in: {source}")
        return keys, vals


class DualJoinAssertEqualKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        keys, vals = self._parse_dual(parse_input)
        return DualJoinAssertEqual(keys=keys, values=vals)


class DualJoinAssertNumericRatioKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        ratio_str, keys, vals = self._parse_param_dual(parse_input, "ratio")
        ratio = _parse_float(ratio_str, "ratio", parse_input.source)
        return DualJoinAssertNumericRatioApprox(keys=keys, values=vals, ratio=ratio)


class DualJoinAssertNumericDeltaKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        delta_str, keys, vals = self._parse_param_dual(parse_input, "delta")
        delta = _parse_float(delta_str, "delta", parse_input.source)
        return DualJoinAssertNumericDeltaApprox(keys=keys, values=vals, delta=delta)


class DualJoinAssertTemporalKeyword(_BaseDualJoinAssertKeyword):
    def build(self, parse_input: ParseInput) -> Assertion:
        duration, keys, vals = self._parse_param_dual(parse_input, "duration")
        duration_seconds = _parse_iso_duration_to_seconds(duration, parse_input.source)
        return DualJoinAssertTemporalApprox(keys=keys, values=vals, duration_seconds=duration_seconds)


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
    "assert_agg_equal": MultiAggAssertEqualKeyword(),
    "assert_agg_numeric_ratio_approx": MultiAggAssertNumericRatioKeyword(),
    "assert_agg_numeric_delta_approx": MultiAggAssertNumericDeltaKeyword(),
    "assert_agg_temporal_approx": MultiAggAssertTemporalKeyword(),
    "assert_join_equal": DualJoinAssertEqualKeyword(),
    "assert_join_numeric_ratio_approx": DualJoinAssertNumericRatioKeyword(),
    "assert_join_numeric_delta_approx": DualJoinAssertNumericDeltaKeyword(),
    "assert_join_temporal_approx": DualJoinAssertTemporalKeyword(),
    "dependency": DependencyKeyword(),
    "var": VarKeyword(),
    "test": TestKeyword(),
    "non_test": NonTestKeyword(),
}
