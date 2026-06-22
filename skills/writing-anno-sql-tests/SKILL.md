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

## Golden Rule

**You MUST return the complete `.sql` file content with annotations — NO summaries, NO explanations, NO markdown tables, NO descriptions of what you would write.**

Any output that is not a valid annotated `.sql` file is wrong. The user needs the file, not a discussion about it.

## Core Pattern: Analyze → Select → Write

```
For each SQL transformation in the file:
  1. Analyze intent: What does this query do? Filter? Aggregate? Compare groups?
  2. Identify risks: What could go wrong? Wrong filter? Bad aggregation? Data quality issues?
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

## Quick Reference

### All Assertion Keywords

| Annotation | Arguments | Description | Transformation Shape |
|---|---|---|---|
| `@assert_all` | `<predicate>` | All rows must satisfy predicate | 1 query |
| `@assert_any` | `<predicate>` | At least one row satisfies predicate | 1 query |
| `@assert_none` | `<predicate>` | No row satisfies predicate | 1 query |
| `@assert_empty` | — | DataFrame must be empty | 1 query |
| `@assert_not_empty` | — | DataFrame must be non-empty | 1 query |
| `@assert_unique` | `<col1>[,<col2>]` | Column combination unique | 1 query |
| `@assert_agg_equal` | `<agg> <fields>` | Aggregation identical across DFs | 2+ queries |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields>` | `\|a-b\| <= ratio*max(\|a\|,\|b\|)` | 2+ queries |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields>` | `\|a-b\| <= delta` | 2+ queries |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields>` | `\|a-b\| <= duration_seconds` (ISO 8601) | 2+ queries |
| `@assert_join_equal` | `on <keys> values <vals>` | Join by keys, values exact match | 2 queries |
| `@assert_join_numeric_ratio_approx` | `<ratio> on <keys> values <vals>` | Join with ratio tolerance | 2 queries |
| `@assert_join_numeric_delta_approx` | `<delta> on <keys> values <vals>` | Join with delta tolerance | 2 queries |
| `@assert_join_temporal_approx` | `<duration> on <keys> values <vals>` | Join with temporal tolerance | 2 queries |

### Other Keywords

| Annotation | Arguments | Description |
|---|---|---|
| `@test` | `<name>` | Start a test case (required before assertions) |
| `@non_test` | — | Mark SQL block as setup/teardown (no assertions) |
| `@var` | `<name>=<value>` | Define file-level variable (BEFORE any @test) |
| `@dependency` | `<name1>[,<name2>]` | Declare dependency on another test in same file |

### Field/Predicate Wildcards

- `*` — all common columns
- `*_cnt`, `prefix*`, `a*b` — glob pattern matching column names
- `numeric:*`, `string:*`, `temporal:*` — columns of a specific data type
- `numeric:*_cnt` — combined: numeric columns matching `*_cnt`
- In predicates: `numeric:* is not null`, `nvl(*, '@') != ''`

### Duration format

ISO 8601: `P1DT12H` = 1 day 12 hours = 36 hours = 129600 seconds.

## Implementation

### Writing Test Annotations — Step by Step

For each transformation in the `.sql` file:

**Step 1 — Identify the test purpose**
Ask: "What business logic does this SQL encode?"

| SQL Purpose | Primary Risk | Suggested Assertions |
|---|---|---|
| Filtering (WHERE) | Wrong rows included/excluded | `@assert_all <filter_condition>`, `@assert_none <excluded_condition>` |
| Aggregation (GROUP BY) | Wrong aggregation logic, NULL handling | `@assert_not_empty`, `@assert_unique <group_key>` |
| Data quality check | Edge cases not caught | `@assert_not_empty`, `@assert_all <quality_check>` |
| Comparing two periods | Data drift, ETL error | `@assert_join_*` or `@assert_agg_*` with explained threshold |
| Uniqueness constraint | Duplicates | `@assert_unique <key_cols>` |

**Step 2 — Choose assertion type(s)**
Use the decision tree from Core Pattern above.

**Step 3 — Write annotations**

```sql
-- @test my_test_name
-- @assert_all amount > 0
-- @assert_not_empty
-- @assert_unique order_id
SELECT * FROM processed_orders;
```

For multi-query comparison:
```sql
-- @test period_comparison
-- @assert_agg_numeric_delta_approx sum 100.0 amount
SELECT SUM(amount) FROM orders_h1;
SELECT SUM(amount) FROM orders_h2;
```

For dual-join comparison:
```sql
-- @test user_comparison
-- @assert_join_numeric_ratio_approx 0.01 on user_name values total_amount
SELECT user_name, SUM(amount) total_amount FROM orders_2024 GROUP BY user_name;
SELECT user_name, SUM(amount) total_amount FROM orders_2025 GROUP BY user_name;
```

**Step 4 — Return ONLY the annotated `.sql` file**

Your entire response must be the annotated `.sql` file content. No introductions, no explanations, no markdown tables, no summaries.

**Red flags — these mean STOP and output the file instead:**
- "Here's what I changed" → WRONG. Output the complete file.
- "I added assertions for..." → WRONG. Output the complete file.
- A summary table → WRONG. Output the complete file.
- Any text outside a ```sql block → WRONG unless it's the file itself
- "Let me explain my reasoning" → WRONG. The file speaks for itself.
- Even a short preamble like "Here is the annotated file:" → WRONG. That is text before the file.
- "The file already has..." → WRONG. Output the file. No commentary.

## Business Rule Patterns

These are common data testing patterns that apply to most data pipelines. Add them proactively even if the user doesn't explicitly mention them.

### Pattern 1: Row integrity — not all fields can be null (`@assert_any * is not null`)

Every row should have at least one non-null value. A row where ALL columns are null is a sign of upstream corruption or bad join logic.

```sql
-- @test row_integrity
-- @assert_any * is not null
SELECT * FROM processed_orders;
```

Combine with type prefix for selective checking:
```sql
-- Ensure at least one key field survives the join
-- @assert_any numeric:* is not null
```

### Pattern 2: Upstream/downstream consistency (`@assert_agg_equal` or `@assert_join_equal`)

When data flows from source to target (ODS → DWD → DWS → ADS), the totals must match at each layer. This is the most critical test in any ETL pipeline.

```sql
-- Total record count must be preserved downstream
-- @test dwd_vs_ods_count
-- @assert_agg_equal count *
SELECT COUNT(*) FROM ods.orders;
SELECT COUNT(*) FROM dwd.orders;
```

```sql
-- Key metric aggregation must match between layers
-- @test dwd_vs_ods_amount
-- @assert_agg_numeric_ratio_approx sum 0.01 amount
SELECT SUM(amount) FROM ods.orders;
SELECT SUM(amount) FROM dwd.orders;
```

```sql
-- Row-level join comparison between source and target by business key
-- @test dwd_vs_ods_detail
-- @assert_join_equal on order_id values amount, status, user_name
SELECT order_id, amount, status, user_name FROM ods.orders;
SELECT order_id, amount, status, user_name FROM dwd.orders;
```

**Naming convention:** `{target}_vs_{source}_{metric}` makes test names self-documenting.

### Pattern 3: No negative/impossible values

Business fields almost never have negative values. Add range checks proactively.

```sql
-- @test amount_range
-- @assert_all amount >= 0
-- @assert_all amount is not null
SELECT amount FROM order_fact;
```

For numeric columns with wildcards:
```sql
-- All numeric metrics should be non-negative
-- @test metric_range
-- @assert_all numeric:* >= 0
```

### Pattern 4: Date/sequence consistency

Date ranges must be valid: start ≤ end, and sequential dates should not overlap.

```sql
-- @test date_consistency
-- @assert_all start_date <= end_date
SELECT start_date, end_date FROM subscription_periods;
```

### Pattern 5: Completeness — every key has data

After a join or ETL step, verify no business keys were lost.

```sql
-- @test user_coverage
-- @assert_unique user_id
-- @assert_not_empty
SELECT user_id, total_amount FROM user_summary;
```

For multi-table comparison, use `@assert_join_equal` to catch lost keys:
```sql
-- @test no_lost_users
-- @assert_join_equal on user_id values total_amount
SELECT u.user_id, COALESCE(SUM(o.amount), 0) as total_amount
FROM users u LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id;

SELECT user_id, total_amount FROM user_summary;
```

### Pattern 6: Cross-table consistency (star schema)

Fact-to-dimension referential integrity — every fact key must exist in the dimension.

```sql
-- @test fact_dim_ref
-- @assert_none d.key is null
SELECT f.order_id, d.key
FROM fact_orders f LEFT JOIN dim_date d ON f.date_key = d.key;
```

### Pattern 7: Aggregation reconciliation

When the same metric can be computed in different ways, verify they agree.

```sql
-- Total from detail table should match aggregated summary
-- @test reconciliation
-- @assert_agg_equal sum amount
SELECT SUM(amount) FROM order_detail;
SELECT SUM(total_amount) FROM order_summary;
```

### When to apply these patterns

Apply the rule of thumb: for every table/query in the pipeline, check:
1. **Row integrity:** `@assert_any * is not null`
2. **Completeness:** `@assert_not_empty`, `@assert_unique <key>`
3. **Range:** `@assert_all numeric:* >= 0` (or appropriate constraint)
4. **Upstream match:** compare with source via `@assert_agg_equal` or `@assert_join_equal`
5. **No duplicates:** `@assert_unique <business_key>`

## Common Mistakes

### Mistake 1: Summarizing instead of producing annotated SQL
Always return the actual `.sql` file content with annotations, not a description of what you would write.

| Rationalization | Reality |
|---|---|
| "I'll summarize the changes and they can apply them" | WRONG. Output the complete file. |
| "A table is clearer than the full file" | WRONG. Output the complete file. |
| "I'll explain my reasoning first, then the file" | WRONG. Only the file. |
| "The user wants to understand my choices" | WRONG. The assertions in the file are self-documenting. |
| "This is just a quick summary, the file is implied" | WRONG. Output the complete file. |
| "I'll format it nicely with explanations" | WRONG. Output the bare file content. |

### Mistake 2: Missing business-logic assertions
For a data quality check like "find rows with NULL amount", `@assert_not_empty` is not enough. Add `@assert_all amount is null` to validate the query logic itself:
```sql
-- @test null_amount_check
-- @assert_all amount is null    -- validates the WHERE logic
-- @assert_not_empty             -- validates that NULL rows exist
SELECT * FROM raw WHERE amount IS NULL;
```

### Mistake 3: Wrong assertion type for the transformation
- Single query with predicate check → `@assert_all` (not `@assert_agg_equal`)
- Multiple queries comparing aggregates → `@assert_agg_equal` (not `@assert_join_equal`)
- Two queries comparing row-level values by key → `@assert_join_*` (not `@assert_agg_*`)

### Mistake 4: Arbitrary threshold values without rationale
When using ratio/delta/temporal approximations, derive the threshold from business logic:
- Ratio 0.01 = 1% tolerance for floating point drift
- Delta 50 = $50 tolerance for rounding differences
- Duration P1D = 1 day tolerance for timezone/ETL latency

### Mistake 5: Ignoring edge cases
Consider: NULL values, empty datasets, cancelled/deleted records, duplicate keys, type mismatches.

Apply the rule of thumb check for every query:
```
1. Row integrity:  -- @assert_any * is not null
2. Completeness:   -- @assert_not_empty, @assert_unique <key>
3. Range:          -- @assert_all numeric:* >= 0
4. Upstream match: -- @assert_agg_equal / @assert_join_equal vs source
5. No duplicates:  -- @assert_unique <business_key>
```

### Mistake 6: Missing `@test` delimiter
Every set of assertions must be preceded by `-- @test <name>`. Without it, the parser cannot identify the test case.

### Mistake 7: Missing common business rule assertions
Agents often write assertions for the specific transformation but forget universal data quality rules:
- `@assert_any * is not null` — no all-null rows (row integrity)
- `@assert_agg_equal` / `@assert_join_equal` vs source — upstream/downstream reconciliation
- `@assert_all numeric:* >= 0` — no negative metrics
- `@assert_unique <business_key>` — no duplicates on key

Apply the Business Rule Patterns section before finalizing. The user may not ask for these explicitly — add them proactively.

## Example: Complete Annotated File

```sql
-- @var env=prod
-- @var src_db=${env}_source
-- @var tgt_db=${env}_target

CREATE TEMPORARY VIEW source_orders AS
SELECT * FROM VALUES
  (1, 'alice', 100, 'completed', '2025-01-15'),
  (2, 'bob',   250, 'completed', '2025-02-20')
AS t(order_id, user_name, amount, status, order_date);

-- Row integrity: no all-null rows from source
-- @test source_row_integrity
-- @assert_any * is not null
-- @assert_any numeric:* != 0
SELECT * FROM source_orders;

-- @test completed_orders
-- @assert_not_empty
-- @assert_unique order_id
SELECT * FROM source_orders WHERE status = 'completed';

-- @test user_totals
-- @assert_not_empty
-- @assert_unique user_name
-- @assert_all total_amount >= 0
SELECT user_name, SUM(amount) total_amount
FROM source_orders
GROUP BY user_name;

-- Upstream/downstream: record count must match after ETL
-- @test tgt_vs_amount_sum
-- @assert_agg_equal sum amount
SELECT * FROM ${src_db}.orders;
SELECT * FROM ${tgt_db}.orders;
```

## Real-World Impact (from baseline testing)

Without this skill, agents commonly:
- Return summaries of what tests they'd write instead of the annotated file
- Miss critical business-logic assertions (e.g., validating WHERE conditions)
- Choose wrong assertion type for the transformation shape
- Use arbitrary threshold values without reasoning
- Forget the `@test` delimiter entirely
- Miss universal business rules like row integrity (`@assert_any * is not null`) and upstream/downstream reconciliation (`@assert_agg_equal`)
