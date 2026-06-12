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

### 运行测试

```bash
# 控制台输出
anno-sql-test spark ./sql_tests/

# 单个文件
anno-sql-test spark example/demo_orders.sql

# 示例输出
#   PASS  order_stats
#   FAIL  order_total
#          Aggregation mismatch: DF0.sum_amount=250 vs DF1.sum_amount=251
#   PASS  compare_users
#
#   2 passed, 1 failed in example\demo_orders.sql

# Excel 报告
anno-sql-test spark --report-type xlsx ./sql_tests/

# 多种报告格式
anno-sql-test spark --report-type console,xlsx,txt ./sql_tests/
```

---

## 注解参考

| 注解 | 参数 | 说明 |
| --- | --- | --- |
| `@test` | `<name>` | 标记一个测试用例的开始 |
| `@non_test` | — | 标记一个非测试 SQL 块（setup / teardown，不含断言） |
| `@dependency` | `<name1>[, <name2>]` | 声明依赖同一文件中的其他测试 |
| `@assert_all` | `<predicate>` | 所有行必须满足该谓词条件 |
| `@assert_any` | `<predicate>` | 至少有一行满足该谓词条件 |
| `@assert_none` | `<predicate>` | 没有行满足该谓词条件 |
| `@assert_empty` | — | DataFrame 必须为空 |
| `@assert_not_empty` | — | DataFrame 必须非空 |
| `@assert_unique` | `<field1>[, <field2>]` | 指定列组合必须唯一 |
| `@assert_agg_equal` | `<agg> <fields>` | 多组 DataFrame 的聚合结果必须完全一致 |
| `@assert_agg_numeric_ratio_approx` | `<agg> <ratio> <fields>` | 聚合结果近似相等：`\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_agg_numeric_delta_approx` | `<agg> <delta> <fields>` | 聚合结果近似相等：`\|a - b\| <= delta` |
| `@assert_agg_temporal_approx` | `<agg> <duration> <fields>` | 聚合结果近似相等：`\|a - b\| <= duration_seconds`（ISO 8601 格式） |
| `@assert_join_equal` | `on <keys> values <vals>` | 按 key 连接后，值列必须完全一致 |
| `@assert_join_numeric_ratio_approx` | `<ratio> on <keys> values <vals>` | 连接比较：`\|a - b\| <= ratio * max(\|a\|, \|b\|)` |
| `@assert_join_numeric_delta_approx` | `<delta> on <keys> values <vals>` | 连接比较：`\|a - b\| <= delta` |
| `@assert_join_temporal_approx` | `<duration> on <keys> values <vals>` | 连接比较：`\|a - b\| <= duration_seconds`（ISO 8601 格式） |

> **说明**：`<fields>`、`<predicate>`、`<key>`、`<value>` 均支持 SQL 表达式。
>
> **`*` 通配符支持**：
> - `*` — 所有共同列
> - `*_cnt`、`prefix*`、`a*b` — 通配符模式匹配列名
> - `numeric:*`、`string:*`、`temporal:*` — 指定数据类型的列
> - 组合使用：`numeric:*_cnt` — 数值类型中匹配 `*_cnt` 的列
> - 在谓词中使用（如 `@assert_all`）：`numeric:* is not null`、`*_cnt is not null`、`nvl(*, '@') != ''`
>
> `<duration>` 使用 ISO 8601 格式（如 `P1DT12H`）。
>
> **自动 SQL**：第一个 `@test` / `@non_test` 之前的 SQL 语句会自动视为非测试块（等价于 `@non_test`）。

---

## 架构

```text
src/anno_sql_test/
├── cli.py          # CLI 入口与参数解析
├── discover.py     # 递归发现 SQL 文件
├── models.py       # 数据模型（测试套件 / 用例 / 断言 / 结果 / 非测试块）
├── keywords.py     # 断言关键字定义 & 关键字映射表
├── parser/         # SQL 注解解析（从 parser.py 重构为包）
│   ├── __init__.py # 公开 API: parse_file, parse_suite
│   ├── _tokenizer.py  # 分词器 & 辅助函数（ISO 时长解析、智能分割等）
│   └── _parser.py     # 解析核心：注解、@test / @non_test / 自动 SQL
├── runner.py       # 测试执行与依赖拓扑排序
├── reporter.py     # 报告输出（控制台 / TXT / Excel）
├── errors.py       # 自定义异常
└── evaluators/
    ├── base.py           # 断言求值器抽象基类
    └── spark/
        ├── evaluator.py  # 断言派发器
        ├── _single.py    # 单 DataFrame 断言
        ├── _multi_agg.py # 多 DataFrame 聚合断言
        ├── _dual_join.py # 双 DataFrame 连接断言
        └── _util.py      # 工具函数
```

### 断言类型

- **单 DataFrame 断言**：谓词检查（全部/任意/无）、空/非空、唯一性
- **多 DataFrame 聚合断言**：对多个查询结果的聚合值进行比较（支持 `*` 通配）
- **双 DataFrame 连接断言**：按 key 连接后比较值列（精确 / 比例 / 绝对值 / 时间）

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

- **运行时**：无（零额外依赖）
- **可选**：`openpyxl`（Excel 报告）
- **开发**：`pyspark`、`pytest`、`ruff`、`ty`

---

## 许可证

[MIT](LICENSE)
