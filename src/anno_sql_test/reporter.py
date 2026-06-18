import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from anno_sql_test.models import SqlTestSuiteResult


class BaseReporter(ABC):
    @abstractmethod
    def report(self, results: list[SqlTestSuiteResult]) -> int:
        ...


@dataclass
class SqlTestCounts:
    passed: int = 0
    failed: int = 0
    skipped: int = 0


@dataclass
class TextReport:
    lines: list[str] = field(default_factory=list)
    passed: int = 0
    failed: int = 0
    skipped: int = 0


def _format_duration(seconds: float) -> str:
    if not seconds:
        return ""
    if seconds < 1:
        return f"{seconds:.3f}s"
    if seconds < 60:
        return f"{seconds:.3f}s"
    parts: list[str] = []
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}min")
    if secs or not parts:
        parts.append(f"{secs}s")
    return "".join(parts)


def _count_results(result: SqlTestSuiteResult) -> SqlTestCounts:
    counts = SqlTestCounts()
    for tr in result.results:
        if tr.skipped:
            counts.skipped += 1
        elif tr.passed:
            counts.passed += 1
        else:
            counts.failed += 1
    return counts


def _format_text_report(result: SqlTestSuiteResult) -> TextReport:
    report = TextReport()

    for err in result.non_test_errors:
        report.lines.append(f"  WARN  {err}")

    for tr in result.results:
        duration_str = _format_duration(tr.duration)
        duration_tag = f" ({duration_str})" if duration_str else ""
        if tr.skipped:
            report.skipped += 1
            reason = f" ({tr.skip_reason})" if tr.skip_reason else ""
            report.lines.append(f"  SKIP  {tr.case.name}{reason}{duration_tag}")
            continue
        if tr.passed:
            report.passed += 1
            report.lines.append(f"  PASS  {tr.case.name}{duration_tag}")
        else:
            report.failed += 1
            report.lines.append(f"  FAIL  {tr.case.name}{duration_tag}")
            for ar in tr.assertion_results:
                if not ar.passed:
                    report.lines.append(f"         {ar.message}")

    summary_parts = [f"{report.passed} passed"]
    if report.failed:
        summary_parts.append(f"{report.failed} failed")
    if report.skipped:
        summary_parts.append(f"{report.skipped} skipped")
    duration_str = _format_duration(result.duration)
    duration_tag = f", {duration_str}" if duration_str else ""
    report.lines.append(f"\n{', '.join(summary_parts)}{duration_tag} in {result.suite.path}")

    return report


class ConsoleReporter(BaseReporter):
    def report(self, results: list[SqlTestSuiteResult]) -> int:
        any_failed = 0
        for i, result in enumerate(results):
            if i > 0:
                print()
            report = _format_text_report(result)
            for line in report.lines:
                print(line)
            if report.failed:
                any_failed = 1
        return any_failed


class XlsxReporter(BaseReporter):
    def __init__(self, output_path: str = "test_report.xlsx"):
        self.output_path = output_path

    def report(self, results: list[SqlTestSuiteResult]) -> int:
        try:
            from openpyxl import Workbook
        except ImportError:
            logging.error("openpyxl is required for XLSX output. Install with: pip install openpyxl")
            return 1

        wb = Workbook()
        ws = wb.active
        ws.title = "Test Results"
        ws.append(["Suite", "Test Name", "Status", "Duration", "Message"])

        any_failed = 0
        for result in results:
            for tr in result.results:
                duration_str = _format_duration(tr.duration)
                if tr.skipped:
                    ws.append([str(result.suite.path), tr.case.name, "SKIP", duration_str, tr.skip_reason or ""])
                elif tr.passed:
                    ws.append([str(result.suite.path), tr.case.name, "PASS", duration_str, ""])
                else:
                    messages = "; ".join(ar.message for ar in tr.assertion_results if not ar.passed)
                    ws.append([str(result.suite.path), tr.case.name, "FAIL", duration_str, messages])
                    any_failed = 1

        wb.save(self.output_path)
        print(f"Report saved to {self.output_path}")
        return any_failed


class TxtReporter(BaseReporter):
    def __init__(self, output_path: str = "test_report.txt"):
        self.output_path = output_path

    def report(self, results: list[SqlTestSuiteResult]) -> int:
        any_failed = 0
        all_lines: list[str] = []

        for i, result in enumerate(results):
            if i > 0:
                all_lines.append("")
            report = _format_text_report(result)
            all_lines.extend(report.lines)
            if report.failed:
                any_failed = 1

        with Path(self.output_path).open("w", encoding="utf-8") as f:
            f.write("\n".join(all_lines) + "\n")

        print(f"Report saved to {self.output_path}")
        return any_failed


class JunitXmlReporter(BaseReporter):
    XML_HEADER = b'<?xml version="1.0" encoding="UTF-8"?>\n'

    def __init__(self, output_path: str = "test_report.xml"):
        self.output_path = output_path

    def report(self, results: list[SqlTestSuiteResult]) -> int:
        root, any_failed = self._build_testsuites(results)
        xml = self.XML_HEADER + tostring(root, encoding="unicode").encode("utf-8")
        self._write(xml)
        return any_failed

    def _build_testsuites(self, results: list[SqlTestSuiteResult]) -> tuple[Element, int]:
        root = Element("testsuites")
        any_failed = 0
        for result in results:
            self._build_testsuite(root, result)
            if _count_results(result).failed:
                any_failed = 1
        return root, any_failed

    def _build_testsuite(self, parent: Element, result: SqlTestSuiteResult) -> None:
        counts = _count_results(result)
        ts = SubElement(parent, "testsuite")
        ts.set("name", str(result.suite.path))
        ts.set("tests", str(len(result.results)))
        ts.set("failures", str(counts.failed))
        ts.set("errors", "0")
        ts.set("skipped", str(counts.skipped))
        if result.duration:
            ts.set("time", f"{result.duration:.3f}")
        for tr in result.results:
            self._build_testcase(ts, tr, str(result.suite.path))

    def _build_testcase(self, parent: Element, tr, suite_path: str) -> None:
        tc = SubElement(parent, "testcase")
        tc.set("name", tr.case.name)
        tc.set("classname", suite_path)
        if tr.duration:
            tc.set("time", f"{tr.duration:.3f}")
        if tr.skipped:
            skipped = SubElement(tc, "skipped")
            if tr.skip_reason:
                skipped.set("message", tr.skip_reason)
        elif not tr.passed:
            for ar in tr.assertion_results:
                if not ar.passed:
                    failure = SubElement(tc, "failure")
                    failure.set("message", ar.message)
                    failure.set("type", "AssertionError")

    def _write(self, xml: bytes) -> None:
        with Path(self.output_path).open("wb") as f:
            f.write(xml)
        print(f"Report saved to {self.output_path}")


REPORTER_DICT: dict[str, type[BaseReporter]] = {
    "console": ConsoleReporter,
    "xlsx": XlsxReporter,
    "txt": TxtReporter,
    "junitxml": JunitXmlReporter,
}
