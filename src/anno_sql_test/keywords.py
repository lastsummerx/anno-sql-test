from abc import ABC, abstractmethod

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
    SingleAssertEmpty,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)
from anno_sql_test.parser._tokenizer import (
    _parse_dual_join_assert,
    _parse_field_list,
    _parse_float,
    _parse_iso_duration_to_seconds,
    _smart_split,
)


class AssertKeyword(ABC):
    @abstractmethod
    def build(self, rest: str, line: str) -> Assertion:
        ...


class SingleAssertAllKeyword(AssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        return SingleAssertAll(predicate=rest)


class SingleAssertEmptyKeyword(AssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        return SingleAssertEmpty()


class SingleAssertNotEmptyKeyword(AssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        return SingleAssertNotEmpty()


class SingleAssertUniqueKeyword(AssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        cols = [c.strip() for c in _smart_split(rest) if c.strip()]
        return SingleAssertUnique(fields=cols)


class _BaseMultiAggAssertKeyword(AssertKeyword):
    @staticmethod
    def _parse_agg_fields(rest: str, line: str) -> tuple[str, list[str]]:
        parts = rest.split(None, 1)
        if len(parts) < 2:
            raise ParseError(f"Expected '<agg> <fields>' in: {line}")
        return parts[0], _parse_field_list(parts[1])

    @staticmethod
    def _parse_agg_param_fields(rest: str, line: str, param_label: str = "param") -> tuple[str, str, list[str]]:
        parts = rest.split(None, 2)
        if len(parts) < 3:
            raise ParseError(f"Expected '<agg> <{param_label}> <fields>' in: {line}")
        return parts[0], parts[1], _parse_field_list(parts[2])


class MultiAggAssertEqualKeyword(_BaseMultiAggAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        agg, fields = self._parse_agg_fields(rest, line)
        return MultiAggAssertEqual(agg=agg, fields=fields)


class MultiAggAssertNumericRatioKeyword(_BaseMultiAggAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        agg, ratio_str, fields = self._parse_agg_param_fields(rest, line, "ratio")
        ratio = _parse_float(ratio_str, "ratio", line)
        return MultiAggAssertNumericRatioApprox(agg=agg, fields=fields, ratio=ratio)


class MultiAggAssertNumericDeltaKeyword(_BaseMultiAggAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        agg, delta_str, fields = self._parse_agg_param_fields(rest, line, "delta")
        delta = _parse_float(delta_str, "delta", line)
        return MultiAggAssertNumericDeltaApprox(agg=agg, fields=fields, delta=delta)


class MultiAggAssertTemporalKeyword(_BaseMultiAggAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        agg, duration, fields = self._parse_agg_param_fields(rest, line, "duration")
        duration_seconds = _parse_iso_duration_to_seconds(duration, line)
        return MultiAggAssertTemporalApprox(agg=agg, fields=fields, duration_seconds=duration_seconds)


class _BaseDualJoinAssertKeyword(AssertKeyword):
    @staticmethod
    def _parse_dual(rest: str, line: str) -> tuple[list[str], list[str]]:
        return _parse_dual_join_assert(rest, line)

    @staticmethod
    def _parse_param_dual(rest: str, line: str, param_label: str = "param") -> tuple[str, list[str], list[str]]:
        parts = rest.split(None, 1)
        if len(parts) < 2:
            raise ParseError(f"Expected '<{param_label}> on <keys> values <vals>' in: {line}")
        return parts[0], *_parse_dual_join_assert(parts[1], line)


class DualJoinAssertEqualKeyword(_BaseDualJoinAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        keys, vals = self._parse_dual(rest, line)
        return DualJoinAssertEqual(keys=keys, values=vals)


class DualJoinAssertNumericRatioKeyword(_BaseDualJoinAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        ratio_str, keys, vals = self._parse_param_dual(rest, line, "ratio")
        ratio = _parse_float(ratio_str, "ratio", line)
        return DualJoinAssertNumericRatioApprox(keys=keys, values=vals, ratio=ratio)


class DualJoinAssertNumericDeltaKeyword(_BaseDualJoinAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        delta_str, keys, vals = self._parse_param_dual(rest, line, "delta")
        delta = _parse_float(delta_str, "delta", line)
        return DualJoinAssertNumericDeltaApprox(keys=keys, values=vals, delta=delta)


class DualJoinAssertTemporalKeyword(_BaseDualJoinAssertKeyword):
    def build(self, rest: str, line: str) -> Assertion:
        duration, keys, vals = self._parse_param_dual(rest, line, "duration")
        duration_seconds = _parse_iso_duration_to_seconds(duration, line)
        return DualJoinAssertTemporalApprox(keys=keys, values=vals, duration_seconds=duration_seconds)


class DependencyKeyword:
    pass


_KEYWORD_MAP: dict[str, AssertKeyword | DependencyKeyword] = {
    "assert_all": SingleAssertAllKeyword(),
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
}
