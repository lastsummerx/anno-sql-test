import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

from anno_sql_test.discover import discover_sql_files
from anno_sql_test.parser import parse_suite
from anno_sql_test.reporter import (
    ConsoleReporter,
    TxtReporter,
    XlsxReporter,
)


@dataclass
class BaseConfig:
    """Common configuration for all backends"""
    path: str                     # SQL file or directory path
    pattern: str = "*.sql"        # File glob pattern
    report_type: str = "console"  # Report type (comma-separated)

    @property
    def report_types(self) -> list[str]:
        """Parse report type string into a list"""
        return [t.strip().lower() for t in self.report_type.split(",") if t.strip()]


@dataclass
class SparkConfig(BaseConfig):
    """Spark backend specific configuration"""
    master: str | None = None
    conf: list[tuple[str, str]] = field(default_factory=list)   # list of key=value strings

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'SparkConfig':
        conf = args.conf or []
        return cls(
            path=args.path,
            pattern=args.pattern,
            report_type=args.report_type,
            master=args.master,
            conf=[tuple(c.split("=", 1)) for c in conf],
        )


def create_parser():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--pattern", default="*.sql", help="File glob pattern (default: *.sql)")
    parent_parser.add_argument("--report-type", default="console",
                              help="Output report type(s): xlsx,txt,console (comma-separated, default: console)")

    parser = argparse.ArgumentParser(prog="anno-sql-test", description="PySpark SQL unit testing framework")
    subparsers = parser.add_subparsers(dest="backend", required=True, help="Backend to use")

    spark_parser = subparsers.add_parser("spark", parents=[parent_parser])
    spark_parser.add_argument("path", help="SQL file or directory containing .sql files")
    spark_parser.add_argument("--master", default=None, help="Spark master URL")
    spark_parser.add_argument("--conf", action="append", help="key=value")

    return parser


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = create_parser()
    parsed = parser.parse_args(args)

    if parsed.backend == "spark":
        from pyspark.sql import SparkSession

        from anno_sql_test.runner import SparkRunner

        config = SparkConfig.from_args(parsed)
        builder = SparkSession.builder
        if config.master:
            builder = builder.master(config.master)

        for k, v in config.conf:
            builder = builder.config(k, v)

        spark = builder.getOrCreate()
        runner = SparkRunner(spark)
    else:
        print(f"Backend '{parsed.backend}' is not yet implemented")
        return 1

    files = discover_sql_files(Path(parsed.path), parsed.pattern)
    suites = parse_suite(files)

    reporter_dict = {
        "console": ConsoleReporter,
        "xlsx": XlsxReporter,
        "txt": TxtReporter,
    }
    report_types = (t.strip().lower() for t in parsed.report_type.split(","))
    reporters = [
        reporter_dict[rt]()
        for rt in report_types
        if rt in reporter_dict
    ]
    if not reporters:
        reporters.append(ConsoleReporter())

    exit_code = 0
    for suite in suites:
        result = runner.run(suite)
        for reporter in reporters:
            ec = reporter.report(result)
            if ec:
                exit_code = ec
        print()
    return exit_code
