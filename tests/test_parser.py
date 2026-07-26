import textwrap
from pathlib import Path
from typing import cast

import pytest

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    DualJoinAssertEqual,
    DualJoinAssertLambda,
    DualJoinAssertNumericApprox,
    DualJoinAssertTemporalApprox,
    DualRowsAssertEqual,
    ExprColumn,
    GlobTemplateColumn,
    LambdaFunc,
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
from anno_sql_test.parser import parse_file

SINGLE_FILE = textwrap.dedent("""\
    -- @TEST test_non_null
    -- @assert_all aaa is not null
    select aaa from table_a;
""")


DUAL_FILE = textwrap.dedent("""\
    -- @TEST test_agg
    -- @assert_agg_equal count columns(*)
    select aaa from table_a;

    select aaa from table_b;
""")


MULTI_TEST_FILE = textwrap.dedent("""\
    -- @TEST test_one
    -- @assert_all aaa > 0
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
    -- @assert_all id is not null
    select id from raw;
""")


UNIQUE_FILE = textwrap.dedent("""\
    -- @TEST test_unique
    -- @assert_unique id, name
    select id, name from table_a;
""")


EQUAL_FILE = textwrap.dedent("""\
    -- @TEST test_equal
    -- @assert_join_equal on id, date values amount, status
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
    a = cast(SingleAssertAll, case.assertions[0])
    assert isinstance(a, SingleAssertAll)
    assert a.predicate == ExprColumn(expr="aaa is not null")
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
    assert a.agg == LambdaFunc(param_names=("col",), template="count({col})")
    assert a.fields == (GlobTemplateColumn(glob="*"),)
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
    assert a.fields == (ExprColumn(expr="id"), ExprColumn(expr="name"))


def test_parse_equal_with_keys_and_values(tmp_path: Path):
    p = tmp_path / "equal.sql"
    p.write_text(EQUAL_FILE)
    suite = parse_file(p)
    case = suite.cases[0]
    a = cast(DualJoinAssertEqual, case.assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.keys == (ExprColumn(expr="id"), ExprColumn(expr="date"))
    assert a.values == (ExprColumn(expr="amount"), ExprColumn(expr="status"))
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
    assert a.agg == LambdaFunc(param_names=("col",), template="sum({col})")
    assert a.ratio == pytest.approx(0.05)
    assert a.fields == (ExprColumn(expr="amount"),)


def test_parse_aggregation_equal_multi_field(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal  sum  a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == LambdaFunc(param_names=("col",), template="sum({col})")
    assert a.fields == (ExprColumn(expr="a"), ExprColumn(expr="b"))


def test_parse_aggregation_equal_expression(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal  sum  a + b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == LambdaFunc(param_names=("col",), template="sum({col})")
    assert a.fields == (ExprColumn(expr="a + b"),)


def test_parse_agg_numeric_ratio_approx_multi_field(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx  sum  0.05  a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericRatioApprox)
    assert a.agg == LambdaFunc(param_names=("col",), template="sum({col})")
    assert a.ratio == pytest.approx(0.05)
    assert a.fields == (ExprColumn(expr="a"), ExprColumn(expr="b"))


def test_parse_numeric_approx_ratio(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_numeric_approx val_ratio=0.05 on id values total\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericApprox)
    assert a.val_ratio == pytest.approx(0.05)
    assert a.val_delta == 0.0
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="total"),)


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

    p4 = tmp_path / "f4.sql"
    p4.write_text("-- @TEST t\n-- @assert_agg_numeric_ratio_approx sum  0.01  \nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <ratio> <fields>"):
        parse_file(p4)


def test_parse_missing_on_keyword(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal id values name\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Expected.*on"):
        parse_file(p)


def test_parse_join_equal_no_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal on id\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == ()


def test_parse_empty_keys_or_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal on , values ,\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Empty"):
        parse_file(p)


def test_parse_equal_expression_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal on id values a + b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="a + b"),)


def test_parse_numeric_approx_expression_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_join_numeric_approx val_ratio=0.05 on id values a + b, a - b\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericApprox)
    assert a.val_ratio == pytest.approx(0.05)
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="a + b"), ExprColumn(expr="a - b"))


def test_parse_numeric_approx_no_param(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_numeric_approx on id values total\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericApprox)
    assert a.val_ratio == 0.0
    assert a.val_delta == 0.0


def test_parse_numeric_approx_invalid_ratio(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_numeric_approx val_ratio=bad on id values total\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid"):
        parse_file(p)


def test_parse_numeric_approx_delta(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_numeric_approx val_delta=10.5 on id values total\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericApprox)
    assert a.val_delta == pytest.approx(10.5)
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="total"),)


def test_parse_numeric_approx_invalid_delta(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_numeric_approx val_delta=bad on id values total\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid"):
        parse_file(p)


def test_parse_agg_numeric_delta_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_delta_approx sum 10.5 amount\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericDeltaApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericDeltaApprox)
    assert a.agg == LambdaFunc(param_names=("col",), template="sum({col})")
    assert a.delta == pytest.approx(10.5)
    assert a.fields == (ExprColumn(expr="amount"),)


def test_parse_agg_numeric_delta_approx_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_numeric_delta_approx sum\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <delta> <fields>"):
        parse_file(p)


def test_parse_temporal_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_temporal_approx duration=P1DT12H on id values ts\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertTemporalApprox)
    assert a.duration_seconds == pytest.approx(129600.0)
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="ts"),)


def test_parse_temporal_approx_invalid_duration(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_temporal_approx duration=bad on id values ts\nselect 1;\nselect 2;")
    with pytest.raises(ParseError, match="Invalid ISO 8601 duration"):
        parse_file(p)


def test_parse_agg_temporal_approx(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_temporal_approx min P1DT12H ts\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertTemporalApprox)
    assert a.agg == LambdaFunc(param_names=("col",), template="min({col})")
    assert a.duration_seconds == pytest.approx(129600.0)
    assert a.fields == (ExprColumn(expr="ts"),)


def test_parse_agg_equal_lambda(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal (x -> count(distinct x)) id\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == LambdaFunc(param_names=("x",), template="count(distinct {x})")
    assert a.fields == (ExprColumn(expr="id"),)


def test_parse_agg_equal_lambda_multi_field(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal (x -> count(distinct x)) id, name\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(MultiAggAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertEqual)
    assert a.agg == LambdaFunc(param_names=("x",), template="count(distinct {x})")
    assert a.fields == (ExprColumn(expr="id"), ExprColumn(expr="name"))


def test_parse_agg_numeric_ratio_approx_lambda(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_agg_numeric_ratio_approx (x -> count(distinct x)) 0.05 amount\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericRatioApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericRatioApprox)
    assert a.agg == LambdaFunc(param_names=("x",), template="count(distinct {x})")
    assert a.ratio == pytest.approx(0.05)
    assert a.fields == (ExprColumn(expr="amount"),)


def test_parse_agg_numeric_delta_approx_lambda(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_agg_numeric_delta_approx (x -> count(distinct x)) 10.5 amount\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(MultiAggAssertNumericDeltaApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertNumericDeltaApprox)
    assert a.agg == LambdaFunc(param_names=("x",), template="count(distinct {x})")
    assert a.delta == pytest.approx(10.5)
    assert a.fields == (ExprColumn(expr="amount"),)


def test_parse_agg_temporal_approx_lambda(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_agg_temporal_approx (x -> max(x)) P1DT12H ts\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(MultiAggAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, MultiAggAssertTemporalApprox)
    assert a.agg == LambdaFunc(param_names=("x",), template="max({x})")
    assert a.duration_seconds == pytest.approx(129600.0)
    assert a.fields == (ExprColumn(expr="ts"),)


def test_parse_agg_equal_lambda_missing_fields(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_agg_equal (x -> count(distinct x))\nselect 1;")
    with pytest.raises(ParseError, match="Expected.*<agg> <fields>"):
        parse_file(p)


def test_parse_agg_numeric_ratio_approx_lambda_missing_args(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_agg_numeric_ratio_approx (x -> count(distinct x))\nselect 1;",
    )
    with pytest.raises(ParseError, match="Expected.*<agg> <ratio> <fields>"):
        parse_file(p)


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


def test_parse_assert_any(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_any a > 0\nselect 1;")
    suite = parse_file(p)
    a = cast(SingleAssertAny, suite.cases[0].assertions[0])
    assert isinstance(a, SingleAssertAny)
    assert a.predicate == ExprColumn(expr="a > 0")


def test_parse_assert_none(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_none a is null\nselect 1;")
    suite = parse_file(p)
    a = cast(SingleAssertNone, suite.cases[0].assertions[0])
    assert isinstance(a, SingleAssertNone)
    assert a.predicate == ExprColumn(expr="a is null")


def test_parse_rows_equal_default_fields(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_rows_equal\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualRowsAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualRowsAssertEqual)
    assert a.fields == (GlobTemplateColumn(glob="*"),)


def test_parse_rows_equal_with_fields(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_rows_equal a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualRowsAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualRowsAssertEqual)
    assert a.fields == (ExprColumn(expr="a"), ExprColumn(expr="b"))


def test_parse_join_lambda_basic(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_lambda ((a, b) -> a >= b) on id values amount\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertLambda, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertLambda)
    assert a.comparator == LambdaFunc(param_names=("a", "b"), template="{a} >= {b}")
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == (ExprColumn(expr="amount"),)
    assert a.join_type == "full"


def test_parse_join_lambda_no_values(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_lambda ((a, b) -> a = b) on id\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertLambda, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertLambda)
    assert a.comparator == LambdaFunc(param_names=("a", "b"), template="{a} = {b}")
    assert a.keys == (ExprColumn(expr="id"),)
    assert a.values == ()


def test_parse_join_lambda_with_join_type(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_join_lambda ((a, b) -> a >= b) left join on id values amount\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(DualJoinAssertLambda, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertLambda)
    assert a.join_type == "left"


def test_parse_join_lambda_with_row_delta(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_join_lambda row_delta=3 ((a, b) -> a >= b) on id values amount\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(DualJoinAssertLambda, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertLambda)
    assert a.row_delta == 3


def test_parse_join_equal_with_join_type(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal inner join on id values name\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.join_type == "inner"
    assert a.row_ratio == 0.0
    assert a.row_delta == 0


def test_parse_join_equal_with_row_ratio(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_join_equal row_ratio=0.1 on id values name\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualJoinAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertEqual)
    assert a.row_ratio == pytest.approx(0.1)
    assert a.row_delta == 0


def test_parse_join_numeric_approx_row_delta(tmp_path: Path):
    p = tmp_path / "f.sql"
    params = "val_ratio=0.01 row_delta=5"
    p.write_text(
        f"-- @TEST t\n-- @assert_join_numeric_approx {params} on id values amount\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(DualJoinAssertNumericApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertNumericApprox)
    assert a.val_ratio == pytest.approx(0.01)
    assert a.row_delta == 5


def test_parse_join_temporal_approx_row_ratio(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text(
        "-- @TEST t\n-- @assert_join_temporal_approx duration=P1D row_ratio=0.05 on id values ts\nselect 1;\nselect 2;",
    )
    suite = parse_file(p)
    a = cast(DualJoinAssertTemporalApprox, suite.cases[0].assertions[0])
    assert isinstance(a, DualJoinAssertTemporalApprox)
    assert a.duration_seconds == pytest.approx(86400.0)
    assert a.row_ratio == pytest.approx(0.05)


def test_parse_rows_equal_row_ratio(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @assert_rows_equal row_ratio=0.1 a, b\nselect 1;\nselect 2;")
    suite = parse_file(p)
    a = cast(DualRowsAssertEqual, suite.cases[0].assertions[0])
    assert isinstance(a, DualRowsAssertEqual)
    assert a.row_ratio == pytest.approx(0.1)
    assert a.fields == (ExprColumn(expr="a"), ExprColumn(expr="b"))


def test_parse_dependency_not_found(tmp_path: Path):
    p = tmp_path / "f.sql"
    p.write_text("-- @TEST t\n-- @dependency nonexistent\nselect 1;")
    with pytest.raises(ParseError, match="not found"):
        parse_file(p)
