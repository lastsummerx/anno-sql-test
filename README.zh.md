# anno-sql-test

> 一个基于 PySpark 的 SQL 单元测试框架——通过 SQL 注释注解编写测试用例。

[English](README.md)

---

## 概述

anno-sql-test 允许数据工程师直接在 `.sql` 文件中，通过 SQL 注释（`--`）编写测试注解，实现对 SQL 查询的单元测试。受 pytest 的 discover-and-run 模式启发，专为 SQL 数据测试场景设计。

核心流程：

1. **发现** — 递归扫描指定路径下的 `.sql` 文件
2. **解析** — 从 SQL 注释中提取测试用例、断言和依赖关系
3. **执行** — 在 PySpark `SparkSession` 中顺序执行 SQL 语句
4. **断言** — 对结果 DataFrame 执行用户定义的各类校验
5. **报告** — 输出测试结果（控制台 / TXT / Excel）

---

## 安装

```bash
# 使用 uv（推荐）
uv sync

# 或使用 pip
pip install -e .
```

如需 Excel 报告功能：

```bash
uv sync --extra excel
```

---

## 快速开始

### 编写测试

创建一个 `.sql` 文件，用注释注解定义测试用例：

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

### 运行测试

```bash
# 控制台输出
anno-sql-test spark ./sql_tests/

# 单个文件
anno-sql-test spark example/demo_orders.sql

#   PASS  order_stats (4.055s)                                                    
#   FAIL  order_total (0.354s)
#          Aggregation mismatch: DF0.sum(amount)=250 vs DF1.sum(amount)=251
#   FAIL  compare_users (2.845s)
#          Found 1 row(s) (50.0%) with mismatches: total: 1 row(s) (50.0%)
#          {'user_name': 'alice', 'l.total': 250, 'r.total': 251}
# 
# 1 passed, 2 failed, 8.283s in example\demo_orders.sql

# Excel 报告
anno-sql-test spark --report-type xlsx ./sql_tests/

# 多种报告格式
anno-sql-test spark --report-type console,xlsx,txt,junitxml ./sql_tests/
```

---

## 注解参考

> **DataFrame 模型**：一个测试用例可以包含一条或多条 SQL 语句，每条语句产生一个结果 DataFrame（按下文统一记作 `df0`、`df1`、……）。单 DataFrame 断言（`@assert_all`、`@assert_any`、`@assert_none`、`@assert_empty`、`@assert_not_empty`、`@assert_unique`、`@assert_set_equal`）检查**第一个语句的结果 `df0`**；双 DataFrame 断言（`@assert_agg_*`、`@assert_join_*`、`@assert_rows_equal`）比较 **`df0` 与 `df1`**，因此测试中必须恰好包含两条 SQL 语句。详见[断言是怎么执行的（等效 SQL）](#断言是怎么执行的等效-sql)。

| 注解 | 参数 | 说明 |
| --- | --- | --- |
| `@test` | `<name>` | 标记一个测试用例的开始 |
| `@non_test` | — | 标记一个非测试 SQL 块（setup / teardown，不含断言） |
| `@var` | `<name>=<value>` | 定义变量（支持 `${other_var}` 引用） |
| `@dependency` | `<name1>[, <name2>]` | 声明依赖同一文件中的其他测试 |
| `@assert_all` | `<predicate>` | 所有行必须满足该谓词条件 |
| `@assert_any` | `<predicate>` | 至少有一行满足该谓词条件 |
| `@assert_none` | `<predicate>` | 没有行满足该谓词条件 |
| `@assert_empty` | — | DataFrame 必须为空 |
| `@assert_not_empty` | — | DataFrame 必须非空 |
| `@assert_unique` | `<field1>[, <field2>]` | 指定列组合必须唯一 |
| `@assert_set_equal` | `<col> (<val1>, ...)` | 列的去重值必须与给定集合一致 |
| `@assert_agg_equal` | `<agg> <fields> [group by <keys>]` | df0 与 df1 的分组聚合结果必须完全一致 |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields> [group by <keys>]` | df0 与 df1 聚合结果近似相等：`\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields> [group by <keys>]` | df0 与 df1 聚合结果近似相等：`\|a - b\| <= delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields> [group by <keys>]` | df0 与 df1 聚合结果近似相等：`\|a - b\| <= duration_seconds`（ISO 8601 格式） |
| `@assert_join_equal` | `[row_delta=<n>] [row_ratio=<r>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | 按 key 连接后，值列必须完全一致 |
| `@assert_join_numeric_approx` | `[row_delta=<n>] [row_ratio=<r>] [val_ratio=<r>] [val_delta=<d>] [left\|right\|inner\|full] join on <keys> [values <vals>]` | 连接数值近似：`\|a - b\| <= ratio * max(\|a\|, \|b\|)` 和/或 `\|a - b\| <= delta` |
| `@assert_join_temporal_approx` | `[row_delta=<n>] [row_ratio=<r>] duration=<iso> [left\|right\|inner\|full] join on <keys> [values <vals>]` | 连接时间近似：`\|a - b\| <= duration_seconds`（ISO 8601 格式） |
| `@assert_join_lambda` | `[row_delta=<n>] [row_ratio=<r>] (<lambda>) [left\|right\|inner\|full] join on <keys> [values <vals>]` | 使用自定义 lambda 比较器进行连接比较 |
| `@assert_rows_equal` | `[row_delta=<n>] [row_ratio=<r>] [<fields>]` | 按字段分组后，比较 df0 与 df1 各组的行数是否一致（默认 fields: `columns(*)`） |

> **说明**：
>
> - `<agg>` 支持简单聚合函数（`count`、`sum`、`min`、`max`），也支持单参数 lambda 表达式：`(x -> count(distinct x))`、`(col -> percentile_approx(col, 0.5))`。
> - `<fields>`、`<predicate>`、`<key>`、`<value>` 均支持 SQL 表达式。
>
> **`columns(*)` 通配符支持**：
>
> - `columns(*)` — DataFrame 中的所有公共列
> - `columns(*_cnt)` — 按列名后缀进行通配匹配（glob 模式）
> - `numeric:columns(*)` — 指定数据类型的列
> - `numeric:columns(*_cnt)` — 数据类型过滤与列名模式匹配的组合
> - 在断言条件中使用：`numeric:columns(*) is not null`、`columns(*_cnt) > 0`
> - EXCEPT 子句：`columns(* except (col1, col2))` 或 `columns(* except col1, col2)`
>
> `<duration>` 使用 ISO 8601 格式（如 `P1DT12H`）。
>
> **自动 SQL**：第一个 `@test` / `@non_test` 之前的 SQL 语句会自动视为非测试块（等价于 `@non_test`）。
>
> **变量**：
>
> - 使用 `@var name=value` 在文件级别定义变量（必须出现在所有 `@test` / `@non_test` 之前）。
> - 变量之间可相互引用：`@var db=prod`、`@var tbl=${db}.users`。
> - 在 SQL 中使用 `${var_name}` 进行替换：`SELECT * FROM ${tbl}`。
> - 通过 CLI 覆盖变量：`anno-sql-test spark --var db=staging ./sql_tests/`（可重复使用）。
> - CLI 变量优先级高于文件级变量。

### 断言是怎么执行的（等效 SQL）

下文 `df0` / `df1` 表示一个测试用例中各条 SQL 语句的结果 DataFrame（按语句顺序）。单 DataFrame 断言检查 `df0`；双 DataFrame 断言比较 `df0` 与 `df1`。一个测试用例**只有当所有断言都通过时才视为通过**。

#### 谓词与行数检查（针对 `df0`）

| 断言 | 作用 | 通过条件 |
| --- | --- | --- |
| `@assert_all <pred>` | 每一行都必须满足 `<pred>` | `SELECT * FROM df0 WHERE NOT (<pred>)` 返回 0 行 |
| `@assert_any <pred>` | 至少有一行满足 `<pred>` | `SELECT * FROM df0 WHERE <pred>` 返回 ≥ 1 行 |
| `@assert_none <pred>` | 没有任何行满足 `<pred>` | `SELECT * FROM df0 WHERE <pred>` 返回 0 行 |
| `@assert_empty` | `df0` 必须为空 | `SELECT * FROM df0 LIMIT 1` 返回 0 行 |
| `@assert_not_empty` | `df0` 必须非空 | `SELECT * FROM df0 LIMIT 1` 返回 1 行 |

#### 唯一性与集合检查（针对 `df0`）

| 断言 | 作用 | 通过条件 |
| --- | --- | --- |
| `@assert_unique f1, f2` | `(f1, f2)` 组合必须唯一 | `SELECT f1, f2, count(*) c FROM df0 GROUP BY f1, f2 HAVING c > 1` 返回 0 行 |
| `@assert_set_equal col (v1, v2)` | `col` 列的去重值必须恰好等于 `{v1, v2}` | `SELECT DISTINCT col FROM df0` 既不能缺少集合中的值，也不能包含集合外的值 |

#### 聚合比较（`df0` vs `df1`）

`@assert_agg_*` 会先用相同的分组键和聚合表达式分别对两个 DataFrame 做聚合：

```sql
a = SELECT <keys>, <agg>(<fields>) AS agg_val FROM df0 GROUP BY <keys>
b = SELECT <keys>, <agg>(<fields>) AS agg_val FROM df1 GROUP BY <keys>
```

然后按键做 `FULL OUTER JOIN`。`@assert_agg_equal` 的通过条件等价于下面这条 SQL 返回 0 行：

```sql
SELECT *
FROM a FULL OUTER JOIN b USING (<keys>)
WHERE a.agg_val IS DISTINCT FROM b.agg_val   -- 聚合值不一致
   OR a.<key> IS NULL OR b.<key> IS NULL;    -- 键只出现在一侧
```

近似版本写法相同，只是把值比较条件替换为下面的"违规"判定：

| 断言 | 值比较条件（满足即为违规行） |
| --- | --- |
| `@assert_agg_numeric_ratio_approx <agg> <ratio> …` | `abs(a.agg_val - b.agg_val) > <ratio> * greatest(abs(a.agg_val), abs(b.agg_val))` |
| `@assert_agg_numeric_delta_approx <agg> <delta> …` | `abs(a.agg_val - b.agg_val) > <delta>` |
| `@assert_agg_temporal_approx <agg> <duration> …` | `abs(a.agg_val - b.agg_val) > <duration_seconds>` |

#### 连接比较（`df0` vs `df1`）

`@assert_join_*` 直接按键连接两个 DataFrame，不做聚合。`@assert_join_equal` 的通过条件等价于下面这条 SQL 返回 0 行：

```sql
SELECT *
FROM df0 l FULL OUTER JOIN df1 r ON l.<key> = r.<key>  -- 默认 full，也可为 left / right / inner
WHERE l.<val> IS DISTINCT FROM r.<val>                 -- 值不一致
   OR l.<key> IS NULL OR r.<key> IS NULL;              -- 键只出现在一侧
```

| 断言 | 值比较条件（满足即为违规行） |
| --- | --- |
| `@assert_join_numeric_approx … val_ratio=<r> …` | `abs(l.val - r.val) > <r> * greatest(abs(l.val), abs(r.val))` |
| `@assert_join_numeric_approx … val_delta=<d> …` | `abs(l.val - r.val) > <d>` |
| `@assert_join_temporal_approx duration=<iso> …` | `abs(cast(l.val as double) - cast(r.val as double)) > <duration_seconds>` |
| `@assert_join_lambda (<lambda>) …` | `NOT (<lambda>)(l.val, r.val)` |

> **NULL 处理**：对于匹配到的 key，两侧值都为 `NULL` 时始终视为相等；仅一侧为 `NULL` 时始终视为违规（无论使用何种比较器 / lambda）。

可选的行容差参数可以放宽整条检查：`row_delta=<n>` 允许最多 `n` 行违规；`row_ratio=<r>` 允许最多 `r × 总行数` 行违规。

#### 行数比较（`df0` vs `df1`）

`@assert_rows_equal` 按 `<fields>`（默认为 `columns(*)`）分别对两个 DataFrame 分组统计行数，并要求各组行数一致：

```sql
a = SELECT <fields>, count(*) AS c FROM df0 GROUP BY <fields>
b = SELECT <fields>, count(*) AS c FROM df1 GROUP BY <fields>

-- 等价于下面这条 SQL 返回 0 行：
SELECT *
FROM a FULL OUTER JOIN b USING (<fields>)
WHERE a.c IS DISTINCT FROM b.c
   OR a.<field> IS NULL OR b.<field> IS NULL;
```

同样支持 `row_delta` / `row_ratio` 容差。

---

## 架构

```text
src/anno_sql_test/
├── cli.py          # CLI 入口与参数解析（argparse）
├── discover.py     # 递归发现 SQL 文件
├── models.py       # 数据模型（测试套件 / 用例 / 断言 / 结果 / 非测试块）
├── log.py          # 日志配置（可选 verbose 级别）
├── parser/         # SQL 注解解析
│   ├── __init__.py # 公开 API: parse_file, parse_suite
│   ├── _parser.py     # 解析核心：注解、@test / @non_test / 自动 SQL
|   ├── keywords.py    # 断言关键字定义 & 关键字映射表
│   └── _utils.py      # 分词器 & 辅助函数（ISO 时长解析、通配符解析等）
├── runner.py       # 测试执行与依赖拓扑排序
├── reporter.py     # 报告输出（控制台 / TXT / Excel）
├── errors.py       # 自定义异常
└── evaluators/
    ├── base.py           # 断言求值器抽象基类 & 分步求值 mixin
    ├── optimizer.py      # 断言融合优化器（group_as_fused）
    └── spark/
        ├── __init__.py
        ├── evaluator.py  # 断言派发器（单条 & 融合）
        ├── _base.py      # Spark 求值器基础类
        ├── _single.py    # 单 DataFrame 断言（all/any/none/empty/unique + 融合）
        ├── _dual_agg.py  # 双 DataFrame 聚合断言
        ├── _dual_join.py # 双 DataFrame 连接断言
        └── _utils.py     # 工具函数（字段解析、类型检查器）
```

### 断言求值器流水线

断言求值采用**分步模式**（stepwise pattern）：

1. **validate** — 检查前置条件（DataFrame 数量、列类型）
2. **prepare** — 将断言转换为执行上下文
3. **build** — 构建查询计划（Spark Column 表达式）
4. **execute** — 对 DataFrame 执行计划
5. **finalize** — 将执行结果转换为 `AssertionResult`（通过/失败）

该流水线定义在 `evaluators/base.py` 的 `StepwiseAssertionMixin` 中，所有 Spark 求值器均实现此接口。同类型的断言会被 `optimizer.py` 自动**融合**（fused）为批量执行，提升效率。

### 断言类型

- **单 DataFrame 断言**：谓词检查（全部/任意/无）、空/非空、唯一性、集合相等——均作用于 `df0`
- **双 DataFrame 聚合断言**：将 `df0` 与 `df1` 按键聚合后比较聚合值（精确 / 比例 / 绝对值 / 时间）
- **双 DataFrame 连接断言**：将 `df0` 与 `df1` 按键连接后比较值列（精确 / 比例 / 绝对值 / 时间 / lambda）

---

## 开发

```bash
# 安装开发依赖
uv sync --group dev

# 运行测试
uv run pytest

# 类型检查
uv run ty check

# 代码风格检查
uv run ruff check
```

---

## 依赖

- **运行时**：`pyspark`
- **可选**：`openpyxl`（Excel 报告）
- **开发**：`pytest`、`ruff`、`ty`

---

## 许可证

[MIT](LICENSE)
