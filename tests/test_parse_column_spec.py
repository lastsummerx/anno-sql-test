from anno_sql_test.models import ExprColumn, FieldType, GlobTemplateColumn
from anno_sql_test.parser._utils import parse_column_spec


def test_expr_column():
    r = parse_column_spec('amount')
    assert isinstance(r, ExprColumn) and r.expr == 'amount'


def test_glob_star():
    r = parse_column_spec('*')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*' and r.expr == '{col}'


def test_numeric_type_filter():
    r = parse_column_spec('numeric:*')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.type_filter == FieldType.NUMERIC and r.expr == '{col}'


def test_star_with_template():
    r = parse_column_spec('* is not null')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.expr == '{col} is not null'


def test_numeric_except():
    r = parse_column_spec('numeric:* EXCEPT (_*, adjustment)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.type_filter == FieldType.NUMERIC
    assert r.excepts == ['_*', 'adjustment']


def test_wildcard_prefix_with_template_and_except():
    r = parse_column_spec('*_cnt is not null EXCEPT (total_cnt)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*_cnt'
    assert r.expr == '{col} is not null'
    assert r.excepts == ['total_cnt']


def test_nvl_with_star_in_quotes():
    r = parse_column_spec("nvl(*, '@') != ''")
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.expr == "nvl({col}, '@') != ''"


def test_string_type_filter_except():
    r = parse_column_spec('string:* EXCEPT (name)')
    assert isinstance(r, GlobTemplateColumn) and r.type_filter == FieldType.STRING
    assert r.excepts == ['name']


def test_number_alias_for_numeric():
    r = parse_column_spec('number:*')
    assert isinstance(r, GlobTemplateColumn) and r.type_filter == FieldType.NUMERIC


def test_lowercase_except():
    r = parse_column_spec('* except (col1)')
    assert isinstance(r, GlobTemplateColumn) and r.glob == '*'
    assert r.excepts == ['col1']


def test_wildcard_suffix():
    r = parse_column_spec('cnt_*')
    assert isinstance(r, GlobTemplateColumn) and r.glob == 'cnt_*' and r.expr == '{col}'


def test_literal_column():
    r = parse_column_spec('abc')
    assert isinstance(r, ExprColumn) and r.expr == 'abc'
