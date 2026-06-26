---
name: writing-anno-sql-tests
description: Use when writing SQL unit test cases for data processing logic using the anno-sql-test framework — user provides SQL transformations and asks you to write assertions
---

# Writing Test Cases for anno-sql-test

## Overview

anno-sql-test is a PySpark-based SQL unit testing framework. Users define test cases as SQL comment annotations (e.g. `-- @assert_all`) directly in `.sql` files. Your job: analyze data processing logic and produce correctly annotated `.sql` files with the right assertions.

## When to Use

- User asks you to write tests for SQL data transformations
- User provides SQL logic (filtering, aggregation, joins, CTEs) and expects annotation-based test coverage
- User says "write test cases for this SQL" referencing anno-sql-test

**Do NOT use** for: Python unit tests (use pytest), testing Spark DataFrames directly, or writing tests outside the annotation-based `.sql` file format.

## Critical Rule: DO NOT Copy Transformation Logic

**Never** replicate the production SQL's calculation logic inside test expectations. Doing so makes tests tautological — they only verify that the code is self‑consistent, not that it produces the correct result. If the logic is wrong, the test will pass because it uses the same wrong logic.

**Correct approach:** Use independent test data with known expected outcomes, and validate via:
- Data quality predicates (`@assert_all`, `@assert_none`) that check business rules
- Aggregation or row‑level comparisons against a separate trusted source or a manually crafted baseline
- Approximate tolerances for floating‑point or temporal drift

**Example of what NOT to do:**
```sql
-- WRONG: replicates the production WHERE clause in an assertion
-- @assert_all amount > 100   -- production uses WHERE amount > 100
SELECT * FROM processed WHERE amount > 100;
```
This test passes even if the condition should be `>= 100` — it just checks that the output matches the same condition.

**Correct example:**
```sql
-- GOOD: validates business rule independently
-- @assert_all amount >= 0    -- amounts should never be negative
-- @assert_not_empty
SELECT * FROM processed WHERE amount > 100;
```
The assertion `amount >= 0` is a business invariant, not a copy of the WHERE clause.

## Golden Rule

**You MUST return the complete `.sql` file content with annotations — NO summaries, NO explanations, NO markdown tables, NO descriptions of what you would write.** Any output that is not a valid annotated `.sql` file is wrong. The user needs the file, not a discussion about it.

## Core Pattern: Analyze → Select → Write

```
For each SQL transformation in the file:
  1. Analyze intent: What business rule does this query implement?
  2. Identify risks: What could go wrong? (wrong filter, aggregation errors, data quality issues)
  3. Select assertion type(s) based on transformation shape:
     - Single query output → single-DataFrame assertions
     - Multiple queries compared → multi-agg or dual-join assertions
  4. Write annotation block: annotate the SQL with appropriate assertions
```

### Assertion Selection Decision

```
Transformation has 1 SQL statement?
  → Single-DataFrame assertions
     - Validate data quality:  @assert_all <predicate>, @assert_none <predicate>
     - Validate presence/absence: @assert_not_empty, @assert_empty
     - Validate uniqueness: @assert_unique <cols>
     - Validate "at least one": @assert_any <predicate>

Transformation has 2+ SQL statements, comparing aggregate values?
  → Multi-DataFrame Aggregation assertions
     - Same aggregation across DFs: @assert_agg_equal <agg> <fields>
     - Approx numeric comparison: @assert_agg_numeric_ratio_approx <agg> <ratio> <fields>
     - Approx numeric delta: @assert_agg_numeric_delta_approx <agg> <delta> <fields>
     - Approx temporal delta: @assert_agg_temporal_approx <agg> <duration> <fields>

Transformation has exactly 2 SQL statements, comparing row-level values by key?
  → Dual-DataFrame Join assertions
     - Exact match: @assert_join_equal on <keys> values <vals>
     - Approx ratio: @assert_join_numeric_ratio_approx <ratio> on <keys> values <vals>
     - Approx delta: @assert_join_numeric_delta_approx <delta> on <keys> values <vals>
     - Approx temporal: @assert_join_temporal_approx <duration> on <keys> values <vals>
```

## Quick Reference (All Assertions)

| Annotation | Arguments | Description |
| --- | --- | --- |
| `@assert_all` | `<predicate>` | All rows must satisfy predicate |
| `@assert_any` | `<predicate>` | At least one row must satisfy predicate |
| `@assert_none` | `<predicate>` | No row must satisfy predicate |
| `@assert_empty` | — | DataFrame must be empty |
| `@assert_not_empty` | — | DataFrame must be non-empty |
| `@assert_unique` | `<field>[,<field>]` | Column combination must be unique |
| `@assert_agg_equal` | `<agg> <fields>` | Aggregation identical across all DataFrames |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields>` | `\|a-b\| ≤ ratio * max(\|a\|,\|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields>` | `\|a-b\| ≤ delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields>` | `\|a-b\| ≤ duration_seconds` (ISO 8601) |
| `@assert_join_equal` | `on <keys> values <vals>` | Join by keys, compare values exactly |
| `@assert_join_numeric_ratio_approx` | `<ratio> on <keys> values <vals>` | Join compare: `\|a-b\| ≤ ratio * max(...)` |
| `@assert_join_numeric_delta_approx` | `<delta> on <keys> values <vals>` | Join compare: `\|a-b\| ≤ delta` |
| `@assert_join_temporal_approx` | `<duration> on <keys> values <vals>` | Join compare: `\|a-b\| ≤ duration_seconds` |
| `@assert_rows_equal` | `[<fields>]` | Row‑by‑row comparison (default: all common cols) |
| `@assert_rows_delta_approx` | `<delta> [<fields>]` | Row‑by‑row `Σ\|a-b\| ≤ delta` |
| `@assert_rows_ratio_approx` | `<ratio> [<fields>]` | Row‑by‑row `Σ\|a-b\| ≤ ratio * Σ max(...)` |

### Other Keywords

| Annotation | Arguments | Description |
|---|---|---|
| `@test` | `<name>` | Start a test case (required before assertions) |
| `@non_test` | — | Mark SQL block as setup/teardown (no assertions) |
| `@var` | `<name>=<value>` | Define file‑level variable (BEFORE any @test) |
| `@dependency` | `<name1>[,<name2>]` | Declare dependency on another test in same file |

### Field/Predicate Wildcards

- `columns(*)` — all common columns across DataFrames
- `columns(*_cnt)` — suffix glob pattern matching column names
- `numeric:columns(*)` — columns of a specific data type
- `numeric:columns(*_cnt)` — combined type filter and name pattern
- In predicates: `numeric:columns(*) is not null`, `columns(*_cnt) > 0`
- EXCEPT clause: `columns(* except (col1, col2))` or `columns(* except col1, col2)`

### Duration format

ISO 8601: `P1DT12H` = 1 day 12 hours = 129600 seconds.

## Implementation — Writing Test Annotations

### Step 1: Identify test purpose

Ask: "What business logic does this SQL encode?" Then choose assertions that validate business invariants — never copy the exact computation logic.

| SQL Purpose | Primary Risk | Suggested Assertions |
|---|---|---|
| Filtering (WHERE) | Wrong rows included/excluded | `@assert_all <non‑trivial_rule>` (e.g., `amount >= 0`), `@assert_none <invalid_condition>` |
| Aggregation (GROUP BY) | Wrong aggregation, NULL handling | `@assert_not_empty`, `@assert_unique <group_key>`, `@assert_all <aggregate_range>` |
| Data quality check | Edge cases not caught | `@assert_not_empty`, `@assert_all <quality_check>` |
| Comparing two periods | Data drift, ETL error | `@assert_join_*` or `@assert_agg_*` with explained threshold |
| Uniqueness constraint | Duplicates | `@assert_unique <key_cols>` |

### Step 2: Choose assertion type(s)

Use the decision tree from Core Pattern above.

### Step 3: Write annotations

```sql
-- @test my_test_name
-- @assert_all amount > 0          -- business rule, NOT copied from WHERE clause
-- @assert_not_empty
-- @assert_unique order_id
SELECT * FROM processed_orders;
```

For multi‑query comparison:
```sql
-- @test period_comparison
-- @assert_agg_numeric_equal sum amount
SELECT amount FROM orders_h1;
SELECT amount FROM orders_h2;
```

For dual‑join comparison:
```sql
-- @test user_comparison
-- @assert_join_numeric_ratio_approx 0.01 on user_name values total_amount
SELECT user_name, SUM(amount) total_amount FROM orders_2024 GROUP BY user_name;
SELECT user_name, SUM(amount) total_amount FROM orders_2025 GROUP BY user_name;
```

### Step 4: Return ONLY the annotated `.sql` file

Your entire response must be the annotated `.sql` file content. No introductions, no explanations, no markdown tables, no summaries.

**Red flags — these mean STOP and output the file instead:**
- Any text outside a ```sql block (unless it's the file itself)
- "Here's what I changed", "I added assertions for...", a summary table, "Let me explain my reasoning", a preamble like "Here is the annotated file:"
- "The file already has..." — just output the file.

## Business Rule Patterns (Apply Proactively)

These tests verify invariants independent of how the data was computed. Do **not** derive them from the transformation logic — they come from domain knowledge.

1. **Row integrity** – every row should have at least one non‑null value:
```sql
-- @assert_any columns(*) is not null
```

2. **Upstream/downstream consistency** – totals must match across layers:
```sql
-- @assert_agg_equal count *
-- @assert_agg_equal sum amount
```

3. **No negative/impossible values** – metrics should be in valid ranges:
```sql
-- @assert_all amount >= 0
-- @assert_all amount is not null
```

4. **Date consistency** – start ≤ end, no future dates, etc.:
```sql
-- @assert_all start_date <= end_date
```

5. **Completeness** – every business key exists after a join:
```sql
-- @assert_not_empty
-- @assert_unique user_id
```

6. **Referential integrity** – fact keys exist in dimensions:
```sql
-- @assert_none key is null
```

Apply the rule of thumb: for every table/query, check row integrity, completeness, range, and upstream match. Do **not** copy the ETL logic to create “expected” results — that defeats the purpose of testing.

## Common Mistakes to Avoid

1. **Summarizing instead of producing the annotated file.** Return the complete file, nothing else.
2. **Copying the transformation logic into assertions.** This makes the test tautological — always use independent business rules or compare against a trusted baseline.
3. **Choosing the wrong assertion type.** Single‑query output → single‑DF assertions; multiple queries comparing aggregates → `@assert_agg_*`; two queries comparing row‑level by key → `@assert_join_*`.
4. **Using arbitrary threshold values.** Derive tolerances from business requirements (e.g., 1% for floating‑point drift).
5. **Forgetting the `@test` delimiter.** Every set of assertions must be preceded by `-- @test <name>`.
6. **Ignoring edge cases.** Consider NULLs, empty datasets, duplicates, type mismatches.

## Complete Example

```sql
-- @var src_db=source_db
-- @var tgt_db=target_db


-- Row integrity: no all-null rows from source
-- @test source_row_integrity
-- @assert_any columns(*) is not null
-- @assert_any numeric:columns(*) != 0
SELECT * FROM ${tgt_db}.orders;

-- Filtered results: use a business rule, not the filter condition
-- @test completed_orders
-- @assert_not_empty
-- @assert_unique order_id
SELECT * FROM ${tgt_db}.orders WHERE status = 'completed';

-- Aggregation: totals should be non‑negative, and each user appears once
-- @test user_totals
-- @assert_not_empty
-- @assert_unique user_name
-- @assert_all total_amount >= 0
SELECT user_name, SUM(amount) total_amount
FROM ${tgt_db}.orders
GROUP BY user_name;

-- Upstream/downstream: total amount must be preserved
-- @test tgt_vs_amount_sum
-- @assert_agg_equal sum amount
SELECT * FROM ${src_db}.orders;
SELECT * FROM ${tgt_db}.orders;
```
