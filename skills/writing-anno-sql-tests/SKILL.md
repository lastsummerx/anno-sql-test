---
name: writing-anno-sql-tests
description: Use when writing SQL unit test cases for data processing logic using the anno-sql-test framework — user provides SQL transformations and asks you to write assertions
---

# Writing Test Cases for anno-sql-test

anno‑sql‑test is a PySpark SQL unit test framework driven by SQL comment annotations (`-- @assert_*`).
Your entire response **must** be the final `.sql` file with annotations — no explanations, no summaries, no markdown tables.

## When to Use

- User asks you to write tests for SQL data transformations
- User provides SQL logic (filtering, aggregation, joins, CTEs) and expects annotation-based test coverage
- User says "write test cases for this SQL" referencing anno-sql-test

## Core Principles (apply in every answer)

1. **Minimise SQL statements** – Put predicate checks into assertions, **not** extra `WHERE` clauses or separate queries.  
   ❌ `SELECT * FROM t WHERE amount > 0` (extra query just to check)  
   ✅ `-- @assert_all amount > 0` on the actual query  

2. **Universal integrity checks** – For *every* table/query, start with:
   ```sql
   -- @assert_any columns(*) is not null         -- at least one non-null column per row
   -- @assert_any columns(numeric:*) != 0         -- at least one non-zero numeric column
   ```
   Then layer on business‑specific assertions.

3. **Never copy production logic into assertions** – Tests must verify independent business invariants (e.g. `amount >= 0`, not the `WHERE amount > 100` filter itself). Duplicating the logic makes the test tautological.

4. **Return ONLY the annotated `.sql` file** – No preamble, no markdown tables, no “Here is the file”. If you are about to write anything else, stop and output the file instead.

## Quick Reference (All Assertions)

| Annotation | Arguments | Description |
| --- | --- | --- |
| `@assert_all` | `<predicate>` | All rows must satisfy the predicate |
| `@assert_any` | `<predicate>` | At least one row must satisfy the predicate |
| `@assert_none` | `<predicate>` | No row must satisfy the predicate |
| `@assert_empty` | — | DataFrame must be empty |
| `@assert_not_empty` | — | DataFrame must be non-empty |
| `@assert_unique` | `<field1>[, <field2>]` | Column combination must be unique |
| `@assert_set_equal` | `<col> (<val1>, ...)` | Column distinct values must equal the given set |
| `@assert_agg_equal` | `<agg> <fields> [group by <keys>]` | Aggregation results identical across all DataFrames |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields> [group by <keys>]` | Aggregation approx: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields> [group by <keys>]` | Aggregation approx: `\|a - b\| <= delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields> [group by <keys>]` | Aggregation approx: `\|a - b\| <= duration_seconds` (ISO 8601) |
| `@assert_join_equal` | `[row_delta=<n>] [row_ratio=<r>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join by keys, compare values exactly |
| `@assert_join_numeric_approx` | `[row_delta=<n>] [row_ratio=<r>] [val_ratio=<r>] [val_delta=<d>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join numeric approx: `\|a - b\| <= ratio * max(\|a\|, \|b\|)` and/or `\|a - b\| <= delta` |
| `@assert_join_temporal_approx` | `[row_delta=<n>] [row_ratio=<r>] duration=<iso> [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join temporal approx: `\|a - b\| <= duration_seconds` (ISO 8601) |
| `@assert_join_lambda` | `[row_delta=<n>] [row_ratio=<r>] (<lambda>) [left\|right\|inner\|full] join on <keys> [values <vals>]` | Join with custom lambda comparator |
| `@assert_rows_equal` | `[row_delta=<n>] [row_ratio=<r>] [<fields>]` | Group by fields, compare row counts across DataFrames (default fields: `columns(*)`) |

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


## example

```sql
-- @var dt='2026-07-27'
-- @var tgt_db=target_db

-- @test test_order
-- @assert_any columns(*) is not null
-- @assert_any columns(numeric:*) != 0
-- @assert_not_empty
-- @assert_unique order_id
SELECT * FROM ${tgt_db}.orders;

-- @test test_consistency
-- @assert_agg_equal sum amount
SELECT * FROM ods.orders;
SELECT * FROM ${tgt_db}.orders;
```

Multi‑statement tests: separate queries with `;` or a blank line (framework‑dependent).  
Always start with `@test`, never leave a test block without a name.

## Patterns to prefer

- **Row integrity** → `@assert_any columns(*) is not null` + `@assert_any columns(numeric:*) != 0`  
- **Range / sign** → `@assert_all amount >= 0`  
- **Uniqueness** → `@assert_unique user_id`  
- **Up‑/down‑stream consistency** → `@assert_agg_equal count *` / `@assert_agg_equal sum amount`  
- **No orphans** → `@assert_none key is null`  

Remember: these are **independent** business rules, not copies of the query’s `WHERE`/`HAVING`.

## What NOT to do

- Add extra `SELECT … WHERE …` statements solely to test a condition. Use assertions.
- Output text like “Here is the annotated file” or “I added the following tests”.
- Use `@assert_all` with the exact filter from the production query.
- Forget the universal integrity checks.
- Return anything other than the final `.sql` content.
