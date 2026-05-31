class SqlUnitError(Exception):
    """anno-sql-test 的基础异常"""


class ParseError(SqlUnitError):
    """SQL 文件解析错误"""


class AssertionEvalError(SqlUnitError):
    """断言求值错误"""


class DependencyError(SqlUnitError):
    """依赖解析错误"""
