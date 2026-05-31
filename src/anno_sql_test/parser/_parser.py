import re
from pathlib import Path

from anno_sql_test.errors import ParseError
from anno_sql_test.keywords import _KEYWORD_MAP, DependencyKeyword
from anno_sql_test.models import SqlNonTestBlock, SqlTestCase, SqlTestSuite
from anno_sql_test.parser._tokenizer import _parse_sql_lines

_HINT_RE = re.compile(r"--\s*@(\w+)(?:\s+(.*))?\s*$", re.IGNORECASE)
TEST_PATTERN = re.compile(r"--\s*@test(?:\s+(?P<name>.*))?\s*$", re.IGNORECASE)
NON_TEST_PATTERN = re.compile(r"--\s*@non_test\s*$", re.IGNORECASE)


def _parse_hints(case: SqlTestCase, hints: list[str]):
    for line in hints:
        stripped = line.strip()
        m = _HINT_RE.match(stripped)
        if m:
            name = m.group(1)
            rest = m.group(2) or ""
            kw = _KEYWORD_MAP.get(name)
            if kw is None:
                raise ParseError(f"Unknown assertion hint at line: {line}")
            if isinstance(kw, DependencyKeyword):
                if rest:
                    case.dependencies.extend(rest.split())
            else:
                case.assertions.append(kw.build(rest, line))
        else:
            if stripped.startswith("-- @"):
                raise ParseError(f"Unknown assertion hint at line: {line}")


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


def _parse_sql(obj: SqlTestCase | SqlNonTestBlock, sql_lines: list[str]):
    statements = _parse_sql_lines(sql_lines)
    obj.sql_statements = statements


def _extract_auto_sql_lines(lines: list[str]) -> tuple[list[str], int]:
    auto_lines = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        if stripped.startswith("--"):
            if TEST_PATTERN.match(stripped) or NON_TEST_PATTERN.match(stripped):
                break
            i += 1
            continue
        auto_lines.append(line)
        i += 1
    return auto_lines, i


def _is_test_annotation(line: str) -> bool:
    return bool(TEST_PATTERN.match(line))


def _is_non_test_annotation(line: str) -> bool:
    return bool(NON_TEST_PATTERN.match(line))


def _parse_test_block(lines: list[str], start_idx: int, filepath: Path) -> tuple[SqlTestCase, int]:
    line = lines[start_idx].strip()
    m = TEST_PATTERN.match(line)
    name = (m.group("name") or "").strip() if m else ""
    if not name:
        raise ParseError(f"Empty test name at line {start_idx + 1} in {filepath}")

    case = SqlTestCase(name=name)
    idx = start_idx + 1
    hint_lines = []
    sql_lines = []
    in_hints = True

    while idx < len(lines):
        cline = lines[idx]
        cstripped = cline.strip()
        if _is_test_annotation(cstripped) or _is_non_test_annotation(cstripped):
            break
        if in_hints and cstripped.startswith("--"):
            hint_lines.append(cstripped)
        else:
            in_hints = False
            sql_lines.append(cline)
        idx += 1

    _parse_hints(case, hint_lines)
    _parse_sql(case, sql_lines)
    return case, idx


def _parse_non_test_block(lines: list[str], start_idx: int) -> tuple[SqlNonTestBlock, int]:
    idx = start_idx + 1
    sql_lines = []
    while idx < len(lines):
        cline = lines[idx]
        cstripped = cline.strip()
        if _is_test_annotation(cstripped) or _is_non_test_annotation(cstripped):
            break
        sql_lines.append(cline)
        idx += 1

    statements = _parse_sql_lines(sql_lines)
    return SqlNonTestBlock(sql_statements=statements), idx


def parse_file(filepath: Path) -> SqlTestSuite:
    lines = filepath.read_text(encoding="utf-8").splitlines()
    suite = SqlTestSuite(path=filepath)

    auto_lines, start_idx = _extract_auto_sql_lines(lines)
    if auto_lines:
        statements = _parse_sql_lines(auto_lines)
        if statements:
            suite.blocks.append(SqlNonTestBlock(sql_statements=statements))

    idx = start_idx
    while idx < len(lines):
        line = lines[idx].strip()
        if not line:
            idx += 1
            continue

        if _is_test_annotation(line):
            case, idx = _parse_test_block(lines, idx, filepath)
            suite.blocks.append(case)
        elif _is_non_test_annotation(line):
            block, idx = _parse_non_test_block(lines, idx)
            if block.sql_statements:
                suite.blocks.append(block)
        else:
            idx += 1

    _validate(suite)
    return suite


def parse_suite(files: list[Path]) -> list[SqlTestSuite]:
    suites: list[SqlTestSuite] = []
    for fpath in files:
        suites.append(parse_file(fpath))
    return suites
