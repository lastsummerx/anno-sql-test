import re
import string
from pathlib import Path

from anno_sql_test.errors import ParseError
from anno_sql_test.models import (
    Assertion,
    SqlNonTestBlock,
    SqlTestCase,
    SqlTestSuite,
)
from anno_sql_test.parser.keywords import (
    _KEYWORD_MAP,
    AssertKeyword,
    DependencyKeyword,
    NonTestKeyword,
    ParseInput,
    TestKeyword,
    VarKeyword,
)

_HINT_PREFIX_RE = re.compile(r"--\s*@")
_HINT_RE = re.compile(r"--\s*@(\w+)(?:\s+(.*))?\s*$", re.IGNORECASE)
_BLOCK_KEYWORDS = (TestKeyword, NonTestKeyword)


def _parse_sql_lines(sql_lines: list[str]) -> list[str]:
    raw = "\n".join(sql_lines).strip()
    if not raw:
        return []
    return [s.strip() for s in raw.split(";") if s.strip()]


def _validate(suite: SqlTestSuite):
    names = set()
    cases = suite.cases
    all_names = {c.name for c in cases}
    for case in cases:
        if case.name in names:
            raise ParseError(f"Duplicate test name '{case.name}' in {suite.path}")
        names.add(case.name)
        for dep in case.dependencies:
            if dep not in all_names:
                raise ParseError(f"Dependency '{dep}' not found in {suite.path}")


def _resolve_variables(variables: dict[str, str]) -> dict[str, str]:
    pending = dict(variables)
    resolved: dict[str, str] = {}
    while pending:
        before = len(pending)
        for name, value in list(pending.items()):
            try:
                resolved[name] = string.Template(value).substitute(resolved)
                del pending[name]
            except KeyError:
                pass
        if len(pending) == before:
            unresolved = ", ".join(f"{k}={v}" for k, v in pending.items())
            raise ParseError(f"Circular or unresolved variable references: {unresolved}")
    return resolved


def _substitute_variables(sql: str, variables: dict[str, str]) -> str:
    try:
        return string.Template(sql).substitute(variables)
    except KeyError as e:
        raise ParseError(f"Undefined variable {e} in SQL: {sql[:80]}") from e


def _substitute_all(texts: list[str], variables: dict[str, str]) -> list[str]:
    return [_substitute_variables(t, variables) for t in texts]


class _Parser:
    def __init__(self, lines: list[str], filepath: Path, cli_variables: dict[str, str] | None = None):
        self._lines = lines
        self._filepath = filepath
        self._cli_variables = cli_variables or {}
        self._pos = 0

    def _peek(self) -> str:
        return "" if self._pos >= len(self._lines) else self._lines[self._pos]

    def _advance(self) -> str:
        line = self._lines[self._pos]
        self._pos += 1
        return line

    def _current_line_no(self) -> int:
        return self._pos + 1

    def _is_eof(self) -> bool:
        return self._pos >= len(self._lines)

    @staticmethod
    def _match_hint(line: str) -> re.Match | None:
        return _HINT_RE.match(line.strip())

    def parse(self) -> SqlTestSuite:
        suite = SqlTestSuite(path=self._filepath)
        file_vars, auto_sql_texts = self._parse_preamble()
        variables = _resolve_variables({**file_vars, **self._cli_variables})
        for sql_text in auto_sql_texts:
            statements = _parse_sql_lines([sql_text])
            statements = _substitute_all(statements, variables)
            if statements:
                suite.blocks.append(SqlNonTestBlock(sql_statements=statements))
        self._parse_blocks(suite, variables)
        _validate(suite)
        return suite

    def _parse_preamble(self) -> tuple[dict[str, str], list[str]]:
        file_vars: dict[str, str] = {}
        auto_sql_texts: list[str] = []
        sql_buf: list[str] = []

        while not self._is_eof():
            line = self._peek()
            stripped = line.strip()
            if not stripped:
                self._advance()
                continue
            if not stripped.startswith("--"):
                sql_buf.append(line)
                self._advance()
                continue

            m = self._match_hint(line)
            if m:
                kw = _KEYWORD_MAP.get(m.group(1).lower())
                if isinstance(kw, _BLOCK_KEYWORDS):
                    break
                if isinstance(kw, VarKeyword):
                    if sql_buf:
                        auto_sql_texts.append("\n".join(sql_buf))
                        sql_buf = []
                    pi = ParseInput(
                        rest=m.group(2) or "",
                        source=line,
                        start_line=self._current_line_no(),
                        end_line=self._current_line_no(),
                    )
                    name, value = kw.build(pi)
                    file_vars[name] = value
            self._advance()

        if sql_buf:
            auto_sql_texts.append("\n".join(sql_buf))
        return file_vars, auto_sql_texts

    def _parse_blocks(self, suite: SqlTestSuite, variables: dict[str, str]):
        while not self._is_eof():
            line = self._peek()
            stripped = line.strip()
            if not stripped:
                self._advance()
                continue
            if not stripped.startswith("--"):
                self._advance()
                continue
            m = self._match_hint(line)
            if not m:
                self._advance()
                continue
            kw = _KEYWORD_MAP.get(m.group(1).lower())
            if isinstance(kw, TestKeyword):
                suite.blocks.append(self._parse_test_block(variables))
            elif isinstance(kw, NonTestKeyword):
                block = self._parse_non_test_block(variables)
                if block is not None:
                    suite.blocks.append(block)
            else:
                self._advance()

    def _parse_test_block(self, variables: dict[str, str]) -> SqlTestCase:
        line = self._advance()
        m = self._match_hint(line)
        kw = _KEYWORD_MAP.get(m.group(1).lower()) if m else None
        if not isinstance(kw, TestKeyword) or not m:
            raise ParseError(f"Empty test name at line {self._current_line_no() - 1}")

        pi = ParseInput(
            rest=m.group(2) or "",
            source=line,
            start_line=self._current_line_no() - 1,
            end_line=self._current_line_no() - 1,
        )
        case = SqlTestCase(name=kw.build(pi))
        dependencies, assertions = self._parse_hints()
        case.dependencies = dependencies
        case.assertions = assertions

        sql_text = self._consume_sql_block()
        if sql_text:
            case.sql_statements = _substitute_all(_parse_sql_lines([sql_text]), variables)

        return case

    def _parse_non_test_block(self, variables: dict[str, str]) -> SqlNonTestBlock | None:
        self._advance()
        sql_text = self._consume_sql_block()
        if not sql_text:
            return None
        statements = _substitute_all(_parse_sql_lines([sql_text]), variables)
        return SqlNonTestBlock(sql_statements=statements) if statements else None

    def _parse_hints(self) -> tuple[list[str], list[Assertion]]:
        dependencies: list[str] = []
        assertions: list[Assertion] = []

        while not self._is_eof():
            line = self._peek()
            stripped = line.strip()
            if not stripped:
                break
            if not stripped.startswith("--"):
                break

            m = self._match_hint(line)
            if not m:
                if re.match(_HINT_PREFIX_RE, stripped):
                    raise ParseError(f"Unknown assertion hint at line {self._current_line_no()}: {line}")
                self._advance()
                continue

            kw = _KEYWORD_MAP.get(m.group(1).lower())
            if isinstance(kw, _BLOCK_KEYWORDS):
                break
            if not isinstance(kw, (AssertKeyword, DependencyKeyword)):
                if re.match(_HINT_PREFIX_RE, stripped):
                    raise ParseError(f"Unknown assertion hint at line {self._current_line_no()}: {line}")
                self._advance()
                continue

            pi = ParseInput(
                rest=m.group(2) or "",
                source=line,
                start_line=self._current_line_no(),
                end_line=self._current_line_no(),
            )
            if isinstance(kw, DependencyKeyword):
                dependencies.extend(kw.build(pi))
            else:
                assertions.append(kw.build(pi))
            self._advance()

        return dependencies, assertions

    def _consume_sql_block(self) -> str:
        sql_buf: list[str] = []
        while not self._is_eof():
            line = self._peek()
            stripped = line.strip()
            if stripped.startswith("--"):
                m = self._match_hint(line)
                if m:
                    kw = _KEYWORD_MAP.get(m.group(1).lower())
                    if isinstance(kw, _BLOCK_KEYWORDS):
                        break
            sql_buf.append(line)
            self._advance()
        return "\n".join(sql_buf).strip() if sql_buf else ""


def parse_file(filepath: Path, cli_variables: dict[str, str] | None = None) -> SqlTestSuite:
    lines = filepath.read_text(encoding="utf-8").splitlines()
    return _Parser(lines, filepath, cli_variables).parse()


def parse_suite(files: list[Path], cli_variables: dict[str, str] | None = None) -> list[SqlTestSuite]:
    return [parse_file(f, cli_variables) for f in files]
