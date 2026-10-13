# Day 5 — Spark Internals & Optimization
## Concept 3: Spark Query Planning & Catalyst Optimizer

## Core Flow

```text
PySpark / SQL
      ↓
Unresolved Logical Plan
      ↓
Analyzer
      ↓
Analyzed Logical Plan
      ↓
Catalyst Optimizer
      ↓
Optimized Logical Plan
      ↓
Physical Planning
      ↓
Selected Physical Plan
      ↓
Execution
```

**Mental rule:**

```text
Logical Plan  = WHAT needs to happen
Physical Plan = HOW Spark will execute it
```

## Why Spark Needs Query Planning

For:

```python
result = (
    df
    .filter(col("amount") > 1000)
    .select("customer_id", "amount")
)
```

you specify the required result, but not which files to scan, which columns to read, whether filters can be pushed toward the source, which join algorithm to use, or where data movement is necessary. Spark's planning system makes those decisions.

## Unresolved Logical Plan

Conceptually:

```text
Project [customer_id, amount]
        ↓
Filter [amount > 1000]
        ↓
Relation [transactions]
```

Spark must still resolve questions such as whether columns exist, their data types, and which relation they belong to.

## Analyzer

The analyzer resolves:

- Column names
- Relations/tables
- Data types
- Functions
- References

Example schema:

```text
customer_id    STRING
amount         DOUBLE
status         STRING
```

After resolution, Spark has an **Analyzed Logical Plan**.

A typo such as:

```python
df.select("custmer_id").show()
```

cannot be resolved against a schema containing `customer_id`.

## Catalyst Optimizer

Catalyst is Spark SQL's query optimization framework.

```text
Analyzed Logical Plan
        ↓
     Catalyst
        ↓
Optimization Rules
        ↓
Optimized Logical Plan
```

Catalyst can transform the logical plan into an equivalent but more efficient logical plan without changing the required result.

## Column Pruning

If a source contains 50 columns but the query requires only a few, Spark can potentially avoid unnecessary column reads.

```python
result = (
    df
    .filter(col("status") == "COMPLETED")
    .select("customer_id", "amount")
)
```

The final output uses `customer_id` and `amount`, while `status` is also required to evaluate the filter.

> Column pruning attempts to read only columns required to evaluate the query and produce its result.

This is especially useful with columnar formats such as Parquet and Delta.

## Predicate Pushdown

```python
df.filter(col("amount") > 1000)
```

When supported, compatible filters can be pushed closer to the data source so unnecessary data may be avoided earlier.

Whether this works depends on the source, file format, connector, and filter expression.

## Partition Pruning

If data is partitioned:

```text
/transactions/year=2025/month=12/
/transactions/year=2026/month=01/
/transactions/year=2026/month=02/
/transactions/year=2026/month=03/
```

and the query filters:

```python
df.filter(
    (col("year") == 2026) &
    (col("month") == 3)
)
```

Spark may avoid unrelated partitions.

```text
year=2025/month=12  ❌
year=2026/month=01  ❌
year=2026/month=02  ❌
year=2026/month=03  ✅
```

Remember:

```text
Column pruning
→ avoid unnecessary COLUMNS

Partition pruning
→ avoid unnecessary PARTITIONS

Predicate pushdown
→ push supported FILTERS closer to the source
```

## Expression Simplification

Catalyst has optimization rules including constant folding, boolean simplification, null-related optimization, and expression simplification.

The key interview idea is not memorizing every rule:

> Your code describes the desired result. Catalyst can transform the logical plan into an equivalent, more efficient logical plan.

## Physical Planning

For:

```python
orders_df.join(customers_df, "customer_id")
```

the logical requirement is:

```text
JOIN orders and customers
ON customer_id
```

Possible physical implementations include:

```text
BroadcastHashJoin
SortMergeJoin
ShuffledHashJoin
```

Therefore:

```text
Logical Plan
JOIN A and B
     ↓
Physical Planning
     ↓
Choose concrete execution strategy
```

## Broadcast Example

Suppose:

```text
transactions = 2 TB
countries    = 5 MB
```

The logical requirement remains a join. If appropriate, physical planning may select a `BroadcastHashJoin`.

The logical requirement does **not** change because a different physical join algorithm is selected.

## Physical Plan Operators

A physical plan may contain:

```text
Scan
Filter
Project
HashAggregate
Sort
Exchange
BroadcastExchange
BroadcastHashJoin
SortMergeJoin
```

`Exchange` is often an important clue that data redistribution/shuffle is involved.

## explain()

```python
df.explain()
```

For more detail:

```python
df.explain("extended")
```

Extended output can show:

```text
Parsed Logical Plan
Analyzed Logical Plan
Optimized Logical Plan
Physical Plan
```

When reading a plan, ask:

- What is Spark scanning?
- Which columns are required?
- Which filters are applied?
- Are filters being pushed?
- Are partitions being pruned?
- Which join strategy is selected?
- Is there an Exchange?
- Are sorts or aggregates occurring?

The **Physical Plan** is especially important for understanding how Spark intends to execute the query.

## Full Example

```python
result = (
    transactions_df
    .filter(col("status") == "COMPLETED")
    .select("customer_id", "amount")
    .groupBy("customer_id")
    .agg(sum("amount").alias("total_amount"))
)

result.show()
```

Conceptually:

```text
Transformations
      ↓
Logical Plan
      ↓
Analysis
      ↓
Catalyst Optimization
      ↓
Optimized Logical Plan
      ↓
Physical Planning
      ↓
Scan / Filter / Aggregate / Exchange...
      ↓
Action
      ↓
Driver schedules work
      ↓
Executors execute tasks
```

## Logical vs Physical Plan

| Plan | Main Question |
|---|---|
| Logical Plan | What operations are required? |
| Optimized Logical Plan | Can equivalent logical work be improved? |
| Physical Plan | How will Spark execute those operations? |

Example:

```text
Logical:
Join customers + transactions

Physical:
BroadcastHashJoin
```

or:

```text
Physical:
SortMergeJoin
```

Same logical requirement; different physical implementation.

## Where Catalyst Fits

Do not say Catalyst executes tasks.

```text
Your query
    ↓
Logical plan
    ↓
Catalyst
    ↓
Optimized logical plan
    ↓
Physical planning
    ↓
Physical plan
    ↓
Driver schedules work
    ↓
Executors execute tasks
```

## Production Troubleshooting

If a Spark job becomes slow after a join:

```text
Inspect physical plan
        ↓
Which join strategy?
        ↓
Is there a large Exchange/shuffle?
        ↓
Did Spark choose SortMergeJoin?
        ↓
Could broadcast be appropriate?
        ↓
Is there skew?
        ↓
Are filters being pushed?
        ↓
Are unnecessary columns being read?
```

Do not immediately assume increasing cluster size is the solution.

## Key Rules

1. Logical plan = **WHAT**.
2. Physical plan = **HOW**.
3. Analyzer resolves columns, relations, functions, and data types.
4. Catalyst optimizes the logical plan.
5. Optimizations preserve the logical result.
6. Column pruning reduces unnecessary column reads.
7. Partition pruning avoids irrelevant physical partitions.
8. Predicate pushdown moves supported filtering closer to the source.
9. BroadcastHashJoin and SortMergeJoin are physical strategies.
10. `df.explain("extended")` helps inspect plans.
11. Physical plans are especially useful for performance troubleshooting.
12. Executors execute tasks; Catalyst does not execute worker tasks.

## Interview Mental Model

```text
PySpark / SQL
      ↓
Unresolved Logical Plan
      ↓
Analyzer
      ↓
Analyzed Logical Plan
      ↓
Catalyst Optimizer
      ↓
Optimized Logical Plan
      ↓
Physical Planning
      ↓
Selected Physical Plan
      ↓
Action requires execution
      ↓
Driver schedules work
      ↓
Executors execute tasks
```

```text
Logical  = WHAT
Physical = HOW
```

## Interview Answer

> Spark first represents DataFrame or SQL operations as a logical plan. The analyzer resolves references such as columns and data types, and Catalyst applies logical optimization rules to produce an optimized logical plan. Spark then performs physical planning to choose concrete execution strategies, such as a BroadcastHashJoin or SortMergeJoin. The physical plan therefore describes how Spark intends to execute the logical requirement. I can inspect these stages using `explain("extended")`, and the physical plan is particularly useful when troubleshooting joins, shuffles, scans, and other performance issues.
