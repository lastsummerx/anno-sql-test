import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from anno_sql_test.models import SqlTestSuiteResult


class BaseReporter(ABC):
    @abstractmethod
    def report(self, result: SqlTestSuiteResult) -> int:
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
        if tr.skipped:
            report.skipped += 1
            reason = f" ({tr.skip_reason})" if tr.skip_reason else ""
            report.lines.append(f"  SKIP  {tr.case.name}{reason}")
            continue
        if tr.passed:
            report.passed += 1
            report.lines.append(f"  PASS  {tr.case.name}")
        else:
            report.failed += 1
            report.lines.append(f"  FAIL  {tr.case.name}")
            for ar in tr.assertion_results:
                if not ar.passed:
                    report.lines.append(f"         {ar.message}")

    summary_parts = [f"{report.passed} passed"]
    if report.failed:
        summary_parts.append(f"{report.failed} failed")
    if report.skipped:
        summary_parts.append(f"{report.skipped} skipped")
    report.lines.append(f"\n{', '.join(summary_parts)} in {result.suite.path}")

    return report


class ConsoleReporter(BaseReporter):
    def report(self, result: SqlTestSuiteResult) -> int:
        report = _format_text_report(result)
        for line in report.lines:
            print(line)
        return 1 if report.failed else 0


class XlsxReporter(BaseReporter):
    def __init__(self, output_path: str = "test_report.xlsx"):
        self.output_path = output_path

    def report(self, result: SqlTestSuiteResult) -> int:
        try:
            from openpyxl import Workbook
        except ImportError:
            logging.error("openpyxl is required for XLSX output. Install with: pip install openpyxl")
            return 1

        wb = Workbook()
        ws = wb.active
        ws.title = "Test Results"
        ws.append(["Test Name", "Status", "Message"])

        for tr in result.results:
            if tr.skipped:
                ws.append([tr.case.name, "SKIP", tr.skip_reason or ""])
            elif tr.passed:
                ws.append([tr.case.name, "PASS", ""])
            else:
                messages = "; ".join(ar.message for ar in tr.assertion_results if not ar.passed)
                ws.append([tr.case.name, "FAIL", messages])

        wb.save(self.output_path)
        print(f"Report saved to {self.output_path}")

        counts = _count_results(result)
        return 1 if counts.failed else 0


class TxtReporter(BaseReporter):
    def __init__(self, output_path: str = "test_report.txt"):
        self.output_path = output_path

    def report(self, result: SqlTestSuiteResult) -> int:
        report = _format_text_report(result)

        with Path(self.output_path).open("w", encoding="utf-8") as f:
            f.write("\n".join(report.lines) + "\n")

        print(f"Report saved to {self.output_path}")
        return 1 if report.failed else 0


class JunitXmlReporter(BaseReporter):
    XML_HEADER = b'<?xml version="1.0" encoding="UTF-8"?>\n'

    def __init__(self, output_path: str = "test_report.xml"):
        self.output_path = output_path

    def report(self, result: SqlTestSuiteResult) -> int:
        counts = _count_results(result)
        testsuite = Element("testsuite")
        testsuite.set("name", str(result.suite.path))
        testsuite.set("tests", str(len(result.results)))
        testsuite.set("failures", str(counts.failed))
        testsuite.set("errors", "0")
        testsuite.set("skipped", str(counts.skipped))

        for tr in result.results:
            tc = SubElement(testsuite, "testcase")
            tc.set("name", tr.case.name)
            tc.set("classname", str(result.suite.path))

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

        xml = self.XML_HEADER + tostring(testsuite, encoding="unicode").encode("utf-8")

        with Path(self.output_path).open("wb") as f:
            f.write(xml)

        print(f"Report saved to {self.output_path}")
        return 1 if counts.failed else 0


REPORTER_DICT: dict[str, type[BaseReporter]] = {
    "console": ConsoleReporter,
    "xlsx": XlsxReporter,
    "txt": TxtReporter,
    "junitxml": JunitXmlReporter,
}
