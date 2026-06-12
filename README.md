# anno-sql-test

> A PySpark-based SQL unit testing framework — write test cases via SQL comment annotations.

[中文文档](README.zh.md)

---

## Overview

anno-sql-test lets data engineers write unit tests for SQL queries directly inside `.sql` files using annotation hints (SQL comments). Inspired by pytest's discover-and-run model, designed for SQL data testing.

Pipeline:

1. **Discover** — recursively scan `.sql` files under a given path
2. **Parse** — extract test cases, assertions, and dependencies from SQL comments
3. **Execute** — run SQL statements in a PySpark `SparkSession`
4. **Assert** — evaluate user-defined assertions on the resulting DataFrames
5. **Report** — output results (console / TXT / Excel)

---

## Installation

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

For Excel reporting:

```bash
uv sync --extra excel
```

---

## Quick Start

### Writing Tests

Create a `.sql` file with annotation comments:

```sql
-- @test users_active
-- @assert_all status = 'ACTIVE'
-- @assert_not_empty
SELECT id, name, status FROM users WHERE status = 'ACTIVE';

-- @test user_count
-- @assert_agg_equal count *
SELECT * FROM users;
SELECT * FROM users WHERE status = 'ACTIVE';

-- @test compare_revenue
-- @dependency users_active
-- @assert_join_numeric_ratio_approx 0.01 on id values amount
SELECT id, amount FROM orders_2024;
SELECT id, amount FROM orders_2025;
```

### Running Tests

```bash
# Console output
anno-sql-test spark ./sql_tests/

# Single file
anno-sql-test spark example/demo_orders.sql

# Example output
#   PASS  order_stats
#   FAIL  order_total
#          Aggregation mismatch: DF0.sum_amount=250 vs DF1.sum_amount=251
#   PASS  compare_users
#
#   2 passed, 1 failed in example\demo_orders.sql

# Excel report
anno-sql-test spark --report-type xlsx ./sql_tests/

# Multiple report formats
anno-sql-test spark --report-type console,xlsx,txt ./sql_tests/
```

---

## Annotation Reference

| Annotation | Arguments | Description |
| --- | --- | --- |
| `@test` | `<name>` | Start a test case |
| `@non_test` | — | Start a non-test SQL block (setup / teardown, no assertions) |
| `@dependency` | `<name1>[, <name2>]` | Declare dependency on other tests in the same file |
| `@assert_all` | `<predicate>` | All rows must satisfy the predicate |
| `@assert_any` | `<predicate>` | At least one row must satisfy the predicate |
| `@assert_none` | `<predicate>` | No row must satisfy the predicate |
| `@assert_empty` | — | DataFrame must be empty |
| `@assert_not_empty` | — | DataFrame must be non-empty |
| `@assert_unique` | `<field1>[, <field2>]` | Column combination must be unique |
| `@assert_agg_equal` | `<agg> <fields>` | Aggregation results identical across all DataFrames |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields>` | Aggregation approx: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields>` | Aggregation approx: `\|a - b\| <= delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields>` | Aggregation approx: `\|a - b\| <= duration_seconds` (ISO 8601) |
| `@assert_join_equal` | `on <keys> values <vals>` | Join by keys, compare values exactly |
| `@assert_join_numeric_ratio_approx` | `<ratio> on <keys> values <vals>` | Join compare: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_join_numeric_delta_approx` | `<delta> on <keys> values <vals>` | Join compare: `\|a - b\| <= delta` |
| `@assert_join_temporal_approx` | `<duration> on <keys> values <vals>` | Join compare: `\|a - b\| <= duration_seconds` (ISO 8601) |

> **Note**: `<fields>`, `<predicate>`, `<key>`, and `<value>` all support SQL expressions.
>
> **`*` wildcard support**:
> - `*` — all common columns across DataFrames
> - `*_cnt`, `prefix*`, `a*b` — glob pattern matching column names
> - `numeric:*`, `string:*`, `temporal:*` — columns of a specific data type
> - Combined: `numeric:*_cnt` — numeric columns matching `*_cnt`
> - In predicates (e.g. `@assert_all`): `numeric:* is not null`, `*_cnt is not null`, `nvl(*, '@') != ''`
>
> `<duration>` uses ISO 8601 format (e.g. `P1DT12H`).
>
> **Auto SQL**: Any SQL statements before the first `@test` / `@non_test` annotation are automatically treated as a non-test block (equivalent to `@non_test`).

---

## Architecture

```text
src/anno_sql_test/
├── cli.py          # CLI entry & argument parsing (argparse)
├── discover.py     # Recursive SQL file discovery
├── models.py       # Data models (suite, case, assertion, result, non-test block)
├── keywords.py     # Assertion keyword definitions & keyword map
├── log.py          # Logging configuration (optional verbose levels)
├── parser/         # SQL annotation parsing
│   ├── __init__.py # Public API: parse_file, parse_suite
│   ├── _tokenizer.py  # Tokenizer & helpers (ISO duration, smart split, etc.)
│   └── _parser.py     # Parser core: hints, @test / @non_test / auto SQL
├── runner.py       # Test execution with dependency resolution
├── reporter.py     # Report output (console, TXT, Excel)
├── errors.py       # Custom exceptions
└── evaluators/
    ├── base.py           # Abstract assertion evaluator base & stepwise evaluation mixin
    ├── optimizer.py      # Assertion fusion optimizer (group_as_fused)
    ├── _field_parser.py  # Field expression tokenizer (glob, type-prefix)
    └── spark/
        ├── __init__.py
        ├── evaluator.py  # Assertion dispatcher (single & fused)
        ├── _base.py      # Spark-specific evaluator base classes
        ├── _single.py    # Single-DataFrame assertions (all/any/none/empty/unique + fused)
        ├── _multi_agg.py # Multi-DataFrame aggregation assertions
        ├── _dual_join.py # Dual-DataFrame join assertions
        └── _util.py      # Utility functions (field resolution, type checkers)
```

### Assertion Evaluator Pipeline

Assertion evaluation follows a **stepwise pattern**:

1. **validate** — check prerequisites (DataFrame count, column types)
2. **prepare** — transform assertion into execution context
3. **build** — construct the query plan (Spark Column expressions)
4. **execute** — run the plan against DataFrame(s)
5. **finalize** — convert execution results into `AssertionResult` (pass/fail)

This pipeline is defined in `evaluators/base.py` via `StepwiseAssertionMixin`, and implemented by all Spark evaluators. Assertions of the same type are automatically **fused** (batched) by `optimizer.py` for efficiency.

### Assertion Types

- **Single-DataFrame**: predicate check (all/any/none), empty/non-empty, uniqueness
- **Multi-DataFrame Aggregation**: compare aggregated values across multiple queries (supports `*` wildcard)
- **Dual-DataFrame Join**: join by keys and compare value columns (exact / ratio / delta / temporal)

---

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Type check
uv run ty check

# Lint
uv run ruff check
```

---

## Dependencies

- **Runtime**: none (zero extra dependencies)
- **Optional**: `openpyxl` (Excel reports)
- **Dev**: `pyspark`, `pytest`, `ruff`, `ty`

---

## License

[MIT](LICENSE)
