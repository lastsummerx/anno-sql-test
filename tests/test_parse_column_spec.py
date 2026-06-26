from anno_sql_test.models import ExprColumn, FieldType, GlobTemplateColumn
from anno_sql_test.parser._utils import parse_column_spec


def test_expr_column():
    r = parse_column_spec('amount')
    assert isinstance(r, ExprColumn) and r.expr == 'amount'


def test_glob_star():
    r = parse_column_spec('columns(*)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*' and r.expr == '{col}'


def test_numeric_type_filter():
    r = parse_column_spec('numeric:columns(*)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.type_filter == FieldType.NUMERIC and r.expr == '{col}'


def test_star_with_template():
    r = parse_column_spec('columns(*) is not null')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.expr == '{col} is not null'


# except outside columns() is invalid syntax — falls back to ExprColumn
def test_except_outside_columns_is_expr():
    r = parse_column_spec('numeric:columns(*) EXCEPT (_*, adjustment)')
    assert isinstance(r, ExprColumn)


def test_except_outside_columns_with_template_is_expr():
    r = parse_column_spec('columns(*_cnt) is not null EXCEPT (total_cnt)')
    assert isinstance(r, ExprColumn)


def test_nvl_with_star_in_quotes():
    r = parse_column_spec("nvl(columns(*), '@') != ''")
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.expr == "nvl({col}, '@') != ''"


def test_except_outside_columns_string_filter_is_expr():
    r = parse_column_spec('string:columns(*) EXCEPT (name)')
    assert isinstance(r, ExprColumn)


def test_number_alias_for_numeric():
    r = parse_column_spec('number:columns(*)')
    assert isinstance(r, GlobTemplateColumn) and r.type_filter == FieldType.NUMERIC


def test_glob_suffix():
    r = parse_column_spec('columns(cnt_*)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == 'cnt_*' and r.expr == '{col}'


def test_literal_column():
    r = parse_column_spec('abc')
    assert isinstance(r, ExprColumn) and r.expr == 'abc'


def test_columns_wrapper_star():
    r = parse_column_spec('columns(*)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*' and r.expr == '{col}'


def test_columns_wrapper_with_suffix():
    r = parse_column_spec('columns(*_cnt)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*_cnt' and r.expr == '{col}'


# --- EXCEPT inside columns() ---

def test_except_inside_columns_with_parens():
    r = parse_column_spec('columns(today*_cnt except(today_col1_cnt, today_col2_cnt))')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == 'today*_cnt'
    assert r.excepts == ['today_col1_cnt', 'today_col2_cnt']
    assert r.expr == '{col}'


def test_except_inside_columns_without_parens():
    r = parse_column_spec('columns(today*_cnt except today_col1_cnt, today_col2_cnt)')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == 'today*_cnt'
    assert r.excepts == ['today_col1_cnt', 'today_col2_cnt']
    assert r.expr == '{col}'


def test_except_inside_columns_single_with_parens():
    r = parse_column_spec('columns(* except(col1))')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == '*'
    assert r.excepts == ['col1']
    assert r.expr == '{col}'


def test_except_inside_columns_single_without_parens():
    r = parse_column_spec('columns(* except col1)')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == '*'
    assert r.excepts == ['col1']
    assert r.expr == '{col}'


def test_except_inside_columns_with_type_filter():
    r = parse_column_spec('numeric:columns(today*_cnt except(today_col1_cnt, today_col2_cnt))')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == 'today*_cnt'
    assert r.type_filter == FieldType.NUMERIC
    assert r.excepts == ['today_col1_cnt', 'today_col2_cnt']
    assert r.expr == '{col}'


def test_except_inside_columns_lowercase():
    r = parse_column_spec('columns(today*_cnt except(today_col1_cnt, today_col2_cnt))')
    assert isinstance(r, GlobTemplateColumn)
    assert r.excepts == ['today_col1_cnt', 'today_col2_cnt']


def test_except_inside_columns_wildcard_pattern():
    r = parse_column_spec('columns(* except(_*, adjustment))')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == '*'
    assert r.excepts == ['_*', 'adjustment']
    assert r.expr == '{col}'


def test_except_inside_columns_no_except():
    r = parse_column_spec('columns(today*_cnt)')
    assert isinstance(r, GlobTemplateColumn)
    assert r.glob == 'today*_cnt'
    assert r.excepts == []
    assert r.expr == '{col}'
