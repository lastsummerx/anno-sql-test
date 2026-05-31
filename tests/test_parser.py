import textwrap
from pathlib import Path
from typing import cast

import pytest

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    DualJoinAssertEqual,
    DualJoinAssertNumericDeltaApprox,
    DualJoinAssertNumericRatioApprox,
    DualJoinAssertTemporalApprox,
    MultiAggAssertEqual,
    MultiAggAssertNumericDeltaApprox,
    MultiAggAssertNumericRatioApprox,
    MultiAggAssertTemporalApprox,
    SingleAssert,
    SingleAssertEmpty,
    SingleAssertNotEmpty,
    SingleAssertUnique,
)
from anno_sql_test.parser import parse_file

SINGLE_FILE = textwrap.dedent("""\
    -- @TEST test_non_null
    -- @assert aaa is not null
    select aaa from table_a;
""")


DUAL_FILE = textwrap.dedent("""\
    -- @TEST test_agg
    -- @assert_agg_equal count *
    select aaa from table_a;

    select aaa from table_b;
""")


MULTI_TEST_FILE = textwrap.dedent("""\
    -- @TEST test_one
    -- @assert aaa > 0
    select aaa from table_a;

    -- @TEST test_two
    -- @assert_empty
    select bbb from table_b;
""")


DEP_FILE = textwrap.dedent("""\
    -- @TEST test_base
    -- @assert_not_empty
    select * from raw;

    -- @TEST test_check
    -- @dependency test_base
    -- @assert id is not null
    select id from raw;
""")


UNIQUE_FILE = textwrap.dedent("""\
    -- @TEST test_unique
    -- @assert_unique id, name
    select id, name from table_a;
""")


EQUAL_FILE = textwrap.dedent("""\
    -- @TEST test_equal
    -- @assert_equal on id, date values amount, status
    select id, date, amount, status from left_tbl;

    select id, date, amount, status from right_tbl;
""")


def test_parse_single_assert(tmp_path: Path):
    p = tmp_path / "test.sql"
    p.write_text(SINGLE_FILE)
    suite = parse_file(p)
    assert len(suite.cases) == 1
    case = suite.cases[0]
    assert case.name == "test_non_null"
    assert len(case.assertions) == 1
    a = cast(SingleAssert, case.assertions[0])
    assert isinstance(a, SingleAssert)
    assert a.predicate == "aaa is not null"
    assert len(case.sql_statements) == 1
    assert "select aaa from table_a" in case.sql_statements[0]


def test_parse_dual_agg(tmp_path: Path):
    p = tmp_path / "dual.sql"
    p.write_text(DUAL_FILE)
    suite = parse_file(p)
    assert len(suite.cases) == 1
    case = suite.cases[0]
    assert case.name == "test_agg"
    assert len(case.assertions) == 1
    a = cast(MultiAggAssertEqual, case.assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == "count"
    assert a.fields == ["*"]
    assert len(case.sql_statements) == 2


def test_parse_multiple_tests(tmp_path: Path):
    p = tmp_path / "multi.sql"
    p.write_text(MULTI_TEST_FILE)
    suite = parse_file(p)
    assert len(suite.cases) == 2
    assert suite.cases[0].name == "test_one"
    assert suite.cases[1].name == "test_two"


def test_parse_dependency(tmp_path: Path):
    p = tmp_path / "dep.sql"
    p.write_text(DEP_FILE)
    suite = parse_file(p)
    case = suite.cases[1]
    assert case.name == "test_check"
    assert case.dependencies == ["test_base"]


def test_parse_unique_with_multiple_columns(tmp_path: Path):
    p = tmp_path / "unique.sql"
    p.write_text(UNIQUE_FILE)
    suite = parse_file(p)
    case = suite.cases[0]
    a = cast(SingleAssertUnique, case.assertions[0])
    assert isinstance(a, SingleAssertUnique)
    assert a.fields == ["id", "name"]


def test_parse_equal_with_keys_and_values(tmp_path: Path):
    p = tmp_path / "equal.sql"
    p.write_text(EQUAL_FILE)
    suite = parse_file(p)
    case = suite.cases[0]
    a = cast(DualJoinAssertEqual, case.assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.keys == ["id", "date"]
    assert a.values == ["amount", "status"]
    assert len(case.sql_statements) == 2


def test_parse_invalid_empty_test_name(tmp_path: Path):
    p = tmp_path / "bad.sql"
    p.write_text("-- @TEST  \nselect 1;")
    with pytest.raises(ParseError, match="(?i)empty"):
        parse_file(p)


def test_parse_unknown_assert_type(tmp_path: Path):
    p = tmp_path / "bad.sql"
    p.write_text("-- @TEST t\n-- @assert_foo bar\nselect 1;")
    with pytest.raises(ParseError, match="Unknown assertion"):
        parse_file(p)


def test_parse_empty_file(tmp_path: Path):
    p = tmp_path / "empty.sql"
    p.write_text("")
    suite = parse_file(p)
    assert len(suite.cases) == 0


def test_parse_single_assert_empty(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_empty\nselect 1;")
    suite = parse_file(p)
    a = suite.cases[0].assertions[0]
    assert isinstance(a, SingleAssertEmpty)


def test_parse_single_assert_not_empty(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_not_empty\nselect 1;")
    suite = parse_file(p)
    a = suite.cases[0].assertions[0]
    assert isinstance(a, SingleAssertNotEmpty)


def test_parse_agg_numeric_ratio_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx sum 0.05 amount\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericRatioApprox)
    assert a.agg == "sum"
    assert a.ratio == pytest.approx(0.05)
    assert a.fields == ["amount"]


def test_parse_aggregation_equal_multi_field(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal sum a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == "sum"
    assert a.fields == ["a", "b"]


def test_parse_aggregation_equal_expression(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal sum a + b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == "sum"
    assert a.fields == ["a + b"]


def test_parse_agg_numeric_ratio_approx_multi_field(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx sum 0.05 a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericRatioApprox)
    assert a.agg == "sum"
    assert a.ratio == pytest.approx(0.05)
    assert a.fields == ["a", "b"]


def test_parse_numeric_ratio_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_numeric_ratio_approx 0.05 on id values total\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericRatioApprox)
    assert a.ratio == pytest.approx(0.05)
    assert a.keys == ["id"]
    assert a.values == ["total"]


def test_parse_agg_equal_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal\nselect 1;")
    with pytest.raises(ParseError, match="Expected"):
        parse_file(p)


def test_parse_agg_numeric_ratio_approx_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <ratio> <fields>"):
        parse_file(p)

    p2 = tmp_path / "f2.sql"
    p2.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx sum\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <ratio> <fields>"):
        parse_file(p2)

    p3 = tmp_path / "f3.sql"
    p3.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx sum bad\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <ratio> <fields>"):
        parse_file(p3)


def test_parse_missing_on_keyword(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_equal id values name\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Expected.*on"):
        parse_file(p)


def test_parse_missing_values_keyword(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_equal on id name\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Expected.*values"):
        parse_file(p)


def test_parse_empty_keys_or_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_equal on , values ,\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Empty"):
        parse_file(p)


def test_parse_equal_expression_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_equal on id values a + b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.keys == ["id"]
    assert a.values == ["a + b"]


def test_parse_numeric_ratio_approx_expression_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_numeric_ratio_approx 0.05 on id values a + b, a - b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericRatioApprox)
    assert a.ratio == pytest.approx(0.05)
    assert a.keys == ["id"]
    assert a.values == ["a + b", "a - b"]


def test_parse_numeric_ratio_approx_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_numeric_ratio_approx\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Expected.*ratio"):
        parse_file(p)

    p2 = tmp_path / "f2.sql"
    p2.write_text("-- @TEST t\n-- @assert_numeric_ratio_approx bad on id values total\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid ratio"):
        parse_file(p2)


def test_parse_numeric_delta_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_numeric_delta_approx 10.5 on id values total\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericDeltaApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericDeltaApprox)
    assert a.delta == pytest.approx(10.5)
    assert a.keys == ["id"]
    assert a.values == ["total"]


def test_parse_numeric_delta_approx_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_numeric_delta_approx\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Expected.*delta"):
        parse_file(p)


def test_parse_agg_numeric_delta_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_delta_approx sum 10.5 amount\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericDeltaApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericDeltaApprox)
    assert a.agg == "sum"
    assert a.delta == pytest.approx(10.5)
    assert a.fields == ["amount"]


def test_parse_agg_numeric_delta_approx_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_delta_approx sum\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <delta> <fields>"):
        parse_file(p)


def test_parse_temporal_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_temporal_approx P1DT12H on id values ts\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertTemporalApprox)
    assert a.duration_seconds == pytest.approx(129600.0)
    assert a.keys == ["id"]
    assert a.values == ["ts"]


def test_parse_temporal_approx_invalid_duration(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_temporal_approx bad on id values ts\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid ISO 8601 duration"):
        parse_file(p)


def test_parse_agg_temporal_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_temporal_approx min P1DT12H ts\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertTemporalApprox)
    assert a.agg == "min"
    assert a.duration_seconds == pytest.approx(129600.0)
    assert a.fields == ["ts"]


def test_parse_agg_temporal_approx_invalid_duration(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_temporal_approx min bad ts\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid ISO 8601 duration"):
        parse_file(p)


def test_parse_duplicate_name(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\nselect 1;\n-- @TEST t\nselect 2;")
    with pytest.raises(ParseError, match="Duplicate"):
        parse_file(p)


def test_parse_dependency_not_found(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @dependency nonexistent\nselect 1;")
    with pytest.raises(ParseError, match="not found"):
        parse_file(p)
