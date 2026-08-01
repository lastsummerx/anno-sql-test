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
-- @var db=prod
-- @var tbl=${db}.users

SELECT 1;

-- @test users_active
-- @assert_all status = 'ACTIVE'
-- @assert_not_empty
SELECT id, name, status FROM ${tbl} WHERE status = 'ACTIVE';

-- @test user_count
-- @assert_agg_equal count columns(*)
SELECT * FROM ${tbl};
SELECT * FROM ${tbl} WHERE status = 'ACTIVE';

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
#   PASS  order_stats (4.055s)                                                    
#   FAIL  order_total (0.354s)
#          Aggregation mismatch: DF0.sum(amount)=250 vs DF1.sum(amount)=251
#   FAIL  compare_users (2.845s)
#          Found 1 row(s) (50.0%) with mismatches: total: 1 row(s) (50.0%)
#          {'user_name': 'alice', 'l.total': 250, 'r.total': 251}
# 
# 1 passed, 2 failed, 8.283s in example\demo_orders.sql

# Excel report
anno-sql-test spark --report-type xlsx ./sql_tests/

# Multiple report formats
anno-sql-test spark --report-type console,xlsx,txt,junitxml ./sql_tests/
```

---

## Annotation Reference

> **DataFrame model**: a test case may contain one or more SQL statements. Each statement produces a result DataFrame (`df0`, `df1`, … in statement order). Single-DataFrame assertions (`@assert_all`, `@assert_any`, `@assert_none`, `@assert_empty`, `@assert_not_empty`, `@assert_unique`, `@assert_set_equal`) inspect **`df0`** (the first statement's result). Dual-DataFrame assertions (`@assert_agg_*`, `@assert_join_*`, `@assert_rows_equal`) compare **`df0` and `df1`**, so the test must contain exactly two SQL statements. See [How Assertions Work](#how-assertions-work-equivalent-sql) for the equivalent SQL of each assertion.

| Annotation | Arguments | Description |
| --- | --- | --- |
| `@test` | `<name>` | Start a test case |
| `@non_test` | — | Start a non-test SQL block (setup / teardown, no assertions) |
| `@var` | `<name>=<value>` | Define a variable (supports `${other_var}` references) |
| `@dependency` | `<name1>[, <name2>]` | Declare dependency on other tests in the same file |
| `@assert_all` | `<predicate>` | All rows must satisfy the predicate |
| `@assert_any` | `<predicate>` | At least one row must satisfy the predicate |
| `@assert_none` | `<predicate>` | No row must satisfy the predicate |
| `@assert_empty` | — | DataFrame must be empty |
| `@assert_not_empty` | — | DataFrame must be non-empty |
| `@assert_unique` | `<field1>[, <field2>]` | Column combination must be unique |
| `@assert_set_equal` | `<col> (<val1>, ...)` | Column distinct values must equal the given set |
| `@assert_agg_equal` | `<agg> <fields> [group by <keys>]` | df0 vs df1: grouped aggregation results must be identical |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields> [group by <keys>]` | df0 vs df1 aggregation approx: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields> [group by <keys>]` | df0 vs df1 aggregation approx: `\|a - b\| <= delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields> [group by <keys>]` | df0 vs df1 aggregation approx: `\|a - b\| <= duration_seconds` (ISO 8601) |
| `@assert_join_equal` | `[row_delta=<n>] [row_ratio=<r>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join by keys, compare values exactly |
| `@assert_join_numeric_approx` | `[row_delta=<n>] [row_ratio=<r>] [val_ratio=<r>] [val_delta=<d>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join numeric approx: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` and/or `\|a - b\| <= delta` |
| `@assert_join_temporal_approx` | `[row_delta=<n>] [row_ratio=<r>] duration=<iso> [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join temporal approx: `\|a - b\| <= duration_seconds` (ISO 8601) |
| `@assert_join_lambda` | `[row_delta=<n>] [row_ratio=<r>] (<lambda>) [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join with custom lambda comparator |
| `@assert_rows_equal` | `[row_delta=<n>] [row_ratio=<r>] [<fields>]` | Group by fields, compare row counts between df0 and df1 (default fields: `columns(*)`) |

> **Note**:
>
> - `<agg>` supports simple aggregation functions (`count`, `sum`, `min`, `max`) and single-parameter lambda expressions: `(x -> count(distinct x))`, `(col -> percentile_approx(col, 0.5))`.
> - `<fields>`, `<predicate>`, `<key>`, and `<value>` all support SQL expressions.
>
> **`columns(*)` wildcard support**:
>
> - `columns(*)` — all common columns across DataFrames
> - `columns(*_cnt)` — suffix glob pattern matching column names
> - `numeric:columns(*)` — columns of a specific data type
> - `numeric:columns(*_cnt)` — combined type filter and name pattern
> - In predicates: `numeric:columns(*) is not null`, `columns(*_cnt) > 0`
> - EXCEPT clause: `columns(* except (col1, col2))` or `columns(* except col1, col2)`
>
> `<duration>` uses ISO 8601 format (e.g. `P1DT12H`).
>
> **Auto SQL**: Any SQL statements before the first `@test` / `@non_test` annotation are automatically treated as a non-test block (equivalent to `@non_test`).
>
> **Variables**:
>
> - Define file-level variables with `@var name=value` (must appear before any `@test` / `@non_test`).
> - Variables can reference each other: `@var db=prod`, `@var tbl=${db}.users`.
> - Use `${var_name}` syntax for substitution in SQL: `SELECT * FROM ${tbl}`.
> - Override variables via CLI: `anno-sql-test spark --var db=staging ./sql_tests/` (can be repeated).
> - CLI variables take precedence over file-level variables.

### How Assertions Work (Equivalent SQL)

Below, `df0` / `df1` stand for the result DataFrames of a test's SQL statements, in statement order. Single-DataFrame assertions inspect `df0`; dual-DataFrame assertions compare `df0` and `df1`. A test passes only when **every** assertion passes.

#### Predicate & Cardinality Checks (`df0`)

| Assertion | Meaning | Passes when |
| --- | --- | --- |
| `@assert_all <pred>` | every row satisfies `<pred>` | `SELECT * FROM df0 WHERE NOT (<pred>)` returns 0 rows |
| `@assert_any <pred>` | at least one row satisfies `<pred>` | `SELECT * FROM df0 WHERE <pred>` returns ≥ 1 row |
| `@assert_none <pred>` | no row satisfies `<pred>` | `SELECT * FROM df0 WHERE <pred>` returns 0 rows |
| `@assert_empty` | `df0` has no rows | `SELECT * FROM df0 LIMIT 1` returns 0 rows |
| `@assert_not_empty` | `df0` has at least one row | `SELECT * FROM df0 LIMIT 1` returns 1 row |

#### Uniqueness & Set Checks (`df0`)

| Assertion | Meaning | Passes when |
| --- | --- | --- |
| `@assert_unique f1, f2` | every `(f1, f2)` combination occurs at most once | `SELECT f1, f2, count(*) c FROM df0 GROUP BY f1, f2 HAVING c > 1` returns 0 rows |
| `@assert_set_equal col (v1, v2)` | the distinct values of `col` are exactly `{v1, v2}` | `SELECT DISTINCT col FROM df0` misses no set value and contains no value outside the set |

#### Aggregation Comparison (`df0` vs `df1`)

`@assert_agg_*` first aggregates both DataFrames with the same key(s) and aggregate expression(s):

```sql
a = SELECT <keys>, <agg>(<fields>) AS agg_val FROM df0 GROUP BY <keys>
b = SELECT <keys>, <agg>(<fields>) AS agg_val FROM df1 GROUP BY <keys>
```

then `FULL OUTER JOIN`s them on the keys. `@assert_agg_equal` passes when the following query returns 0 rows:

```sql
SELECT *
FROM a FULL OUTER JOIN b USING (<keys>)
WHERE a.agg_val IS DISTINCT FROM b.agg_val   -- value mismatch
   OR a.<key> IS NULL OR b.<key> IS NULL;    -- key present on only one side
```

The approximate flavors keep the same shape but replace the value condition with the violation test below:

| Assertion | Value condition (a row is a violation when true) |
| --- | --- |
| `@assert_agg_numeric_ratio_approx <agg> <ratio> …` | `abs(a.agg_val - b.agg_val) > <ratio> * greatest(abs(a.agg_val), abs(b.agg_val))` |
| `@assert_agg_numeric_delta_approx <agg> <delta> …` | `abs(a.agg_val - b.agg_val) > <delta>` |
| `@assert_agg_temporal_approx <agg> <duration> …` | `abs(a.agg_val - b.agg_val) > <duration_seconds>` |

#### Join Comparison (`df0` vs `df1`)

`@assert_join_*` joins the two DataFrames directly on the keys — no aggregation. `@assert_join_equal` passes when the following query returns 0 rows:

```sql
SELECT *
FROM df0 l FULL OUTER JOIN df1 r ON l.<key> = r.<key>  -- default full; or left / right / inner
WHERE l.<val> IS DISTINCT FROM r.<val>                 -- value mismatch
   OR l.<key> IS NULL OR r.<key> IS NULL;              -- key present on only one side
```

| Assertion | Value condition (a row is a violation when true) |
| --- | --- |
| `@assert_join_numeric_approx … val_ratio=<r> …` | `abs(l.val - r.val) > <r> * greatest(abs(l.val), abs(r.val))` |
| `@assert_join_numeric_approx … val_delta=<d> …` | `abs(l.val - r.val) > <d>` |
| `@assert_join_temporal_approx duration=<iso> …` | `abs(cast(l.val as double) - cast(r.val as double)) > <duration_seconds>` |
| `@assert_join_lambda (<lambda>) …` | `NOT (<lambda>)(l.val, r.val)` |

> **NULL handling**: for a matched key, two `NULL` values always count as equal, while `NULL` on only one side is always a violation (regardless of comparator/lambda).

Optional row tolerances relax the whole check: `row_delta=<n>` allows up to `n` violating rows; `row_ratio=<r>` allows up to `r × total_rows` violating rows.

#### Row Count Comparison (`df0` vs `df1`)

`@assert_rows_equal` groups each DataFrame by `<fields>` (default `columns(*)`) and requires matching counts:

```sql
a = SELECT <fields>, count(*) AS c FROM df0 GROUP BY <fields>
b = SELECT <fields>, count(*) AS c FROM df1 GROUP BY <fields>

-- Passes when this returns 0 rows:
SELECT *
FROM a FULL OUTER JOIN b USING (<fields>)
WHERE a.c IS DISTINCT FROM b.c
   OR a.<field> IS NULL OR b.<field> IS NULL;
```

`row_delta` / `row_ratio` tolerances apply as above.

---

## Architecture

```text
src/anno_sql_test/
├── cli.py          # CLI entry & argument parsing (argparse)
├── discover.py     # Recursive SQL file discovery
├── models.py       # Data models (suite, case, assertion, result, non-test block)
├── log.py          # Logging configuration (optional verbose levels)
├── parser/         # SQL annotation parsing
│   ├── __init__.py # Public API: parse_file, parse_suite
│   ├── _parser.py     # Parser core: hints, @test / @non_test / auto SQL
|   ├── keywords.py    # Assertion keyword definitions & keyword map
│   └── _utils.py      # Tokenizer & helpers (ISO duration, glob parsing, etc.)
├── runner.py       # Test execution with dependency resolution
├── reporter.py     # Report output (console, TXT, Excel)
├── errors.py       # Custom exceptions
└── evaluators/
    ├── base.py           # Abstract assertion evaluator base & stepwise evaluation mixin
    ├── optimizer.py      # Assertion fusion optimizer (group_as_fused)
    └── spark/
        ├── __init__.py
        ├── evaluator.py  # Assertion dispatcher (single & fused)
        ├── _base.py      # Spark-specific evaluator base classes
        ├── _single.py    # Single-DataFrame assertions (all/any/none/empty/unique + fused)
        ├── _dual_agg.py  # Dual-DataFrame aggregation assertions
        ├── _dual_join.py # Dual-DataFrame join assertions
        └── _utils.py     # Utility functions (field resolution, type checkers)
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

- **Single-DataFrame**: predicate check (all/any/none), empty/non-empty, uniqueness, set equality — all applied to `df0`
- **Dual-DataFrame Aggregation**: aggregate `df0` and `df1` by keys and compare the aggregated values (exact / ratio / delta / temporal)
- **Dual-DataFrame Join**: join `df0` and `df1` by keys and compare value columns (exact / ratio / delta / temporal / lambda)

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

- **Runtime**: `pyspark`
- **Optional**: `openpyxl` (Excel reports)
- **Dev**: `pytest`, `ruff`, `ty`

---

## License

[MIT](LICENSE)
