import argparse
import logging
import sys
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path

from anno_sql_test.discover import discover_sql_files
from anno_sql_test.log import setup_logging
from anno_sql_test.parser import parse_suite
from anno_sql_test.reporter import REPORTER_DICT, BaseFileReporter, BaseReporter, ConsoleReporter


@dataclass
class BaseConfig:
    """Common configuration for all backends"""
    path: str                     # SQL file or directory path
    pattern: str = "*.sql"        # File glob pattern
    report_type: str = "console"  # Report type (comma-separated)
    output: str | None = None     # Report filename without extension
    variables: dict[str, str] = field(default_factory=dict)
    sample_count: int = 0

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
            output=args.output,
            master=args.master,
            conf=[tuple(c.split("=", 1)) for c in conf],
            variables=variables,
            sample_count=args.sample_count,
        )


def create_parser():
    support_reporter = ",".join(REPORTER_DICT.keys())
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--pattern", default="*.sql", help="File glob pattern (default: *.sql)")
    parent_parser.add_argument("--report-type", default="console",
                              help=f"Output report type(s): {support_reporter} (comma-separated, default: console)")
    parent_parser.add_argument("-o", "--output",
                              help="Report filename without extension (e.g. --output result => result.txt, result.xml)")
    parent_parser.add_argument("--var", action="append",
                              help="Variable key=value (can be repeated)")
    parent_parser.add_argument("-v", "--verbose", action="count", default=0,
                              help="Increase verbosity (-v: INFO, -vv: DEBUG)")
    parent_parser.add_argument("path", help="SQL file or directory containing .sql files")
    parent_parser.add_argument("--sample-count", type=int, default=5,
                              help="Number of violating rows to sample (0=disabled)")

    parser = argparse.ArgumentParser(prog="anno-sql-test", description="PySpark SQL unit testing framework")
    parser.add_argument("--version", action="version", version=f"anno-sql-test {version('anno_sql_test')}")
    subparsers = parser.add_subparsers(dest="backend", required=True, help="Backend to use")

    spark_parser = subparsers.add_parser("spark", parents=[parent_parser])
    spark_parser.add_argument("--master", default=None, help="Spark master URL")
    spark_parser.add_argument("--conf", action="append", help="key=value")

    return parser


def get_reporters(config: BaseConfig) -> list[BaseReporter]:
    report_types = [t.strip().lower() for t in config.report_type.split(",")]
    reporters = []
    for rt in report_types:
        cls = REPORTER_DICT.get(rt)
        if cls is None:
            continue
        if config.output and issubclass(cls, BaseFileReporter):
            reporters.append(cls(output_path=f"{config.output}{cls.extension()}"))
        else:
            reporters.append(cls())
    if not reporters:
        reporters.append(ConsoleReporter())
    return reporters


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
        runner = SparkRunner(spark, sample_count=config.sample_count)
    else:
        logging.error("Backend '%s' is not yet implemented", parsed.backend)
        return 1

    files = discover_sql_files(Path(parsed.path), parsed.pattern)
    suites = parse_suite(files, config.variables)

    reporters = get_reporters(config)
    results = [runner.run(suite) for suite in suites]

    exit_code = 0
    for reporter in reporters:
        ec = reporter.report(results)
        if ec:
            exit_code = ec
    return exit_code
