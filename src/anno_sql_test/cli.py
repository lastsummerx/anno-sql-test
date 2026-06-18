import argparse
import logging
import sys
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path

from anno_sql_test.discover import discover_sql_files
from anno_sql_test.log import setup_logging
from anno_sql_test.parser import parse_suite
from anno_sql_test.reporter import REPORTER_DICT, ConsoleReporter


@dataclass
class BaseConfig:
    """Common configuration for all backends"""
    path: str                     # SQL file or directory path
    pattern: str = "*.sql"        # File glob pattern
    report_type: str = "console"  # Report type (comma-separated)
    variables: dict[str, str] = field(default_factory=dict)

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
        variables = {}
        if args.var:
            for v in args.var:
                key, _, val = v.partition("=")
                variables[key.strip()] = val.strip()
        return cls(
            path=args.path,
            pattern=args.pattern,
            report_type=args.report_type,
            master=args.master,
            conf=[tuple(c.split("=", 1)) for c in conf],
            variables=variables,
        )


def create_parser():
    support_reporter = ",".join(REPORTER_DICT.keys())
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--pattern", default="*.sql", help="File glob pattern (default: *.sql)")
    parent_parser.add_argument("--report-type", default="console",
                              help=f"Output report type(s): {support_reporter} (comma-separated, default: console)")
    parent_parser.add_argument("--var", action="append",
                              help="Variable key=value (can be repeated)")
    parent_parser.add_argument("-v", "--verbose", action="count", default=0,
                              help="Increase verbosity (-v: INFO, -vv: DEBUG)")

    parser = argparse.ArgumentParser(prog="anno-sql-test", description="PySpark SQL unit testing framework")
    parser.add_argument("--version", action="version", version=f"anno-sql-test {version('anno_sql_test')}")
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
    setup_logging(parsed.verbose)

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
        logging.error("Backend '%s' is not yet implemented", parsed.backend)
        return 1

    files = discover_sql_files(Path(parsed.path), parsed.pattern)
    suites = parse_suite(files, config.variables)

    report_types = (t.strip().lower() for t in parsed.report_type.split(","))
    reporters = [
        REPORTER_DICT[rt]()
        for rt in report_types
        if rt in REPORTER_DICT
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
