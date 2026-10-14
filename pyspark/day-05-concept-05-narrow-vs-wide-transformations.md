# Day 5 — Spark Internals & Optimization
## Concept 5: Narrow vs Wide Transformations

## Core Idea

```text
NARROW
Output partition depends on limited input partitions
→ little/no cross-partition redistribution
→ operations can often remain in the same stage

WIDE
Output partitions need data from multiple input partitions
→ redistribution
→ SHUFFLE
→ usually a stage boundary
```

## Narrow Dependency

Example:

```python
df.filter(col("amount") > 1000)
```

Each partition can generally be processed independently:

```text
P1 → Filter → P1'
P2 → Filter → P2'
P3 → Filter → P3'
P4 → Filter → P4'
```

Typical narrow-style operations:

- `filter()`
- `select()`
- `withColumn()`
- `map()`
- `flatMap()`

Several narrow transformations can often be pipelined within one stage.

Narrow does not automatically mean fast; huge input volumes or expensive computation can still be slow.

## Wide Dependency

Example input:

```text
P1: A 100, B 200
P2: A 300, C 400
P3: B 500, C 600
```

For:

```python
df.groupBy("customer_id").sum("amount")
```

the final result requires values for the same key from different partitions to meet.

```text
A exists in P1 and P2
B exists in P1 and P3
C exists in P2 and P3
```

This creates a wide dependency.

## Shuffle

Wide dependencies commonly require shuffle.

Typical examples:

- `groupBy()`
- `distinct()`
- `orderBy()`
- `repartition()`
- many joins

Shuffle can involve:

```text
partition records
→ serialize
→ write intermediate data
→ network transfer
→ read shuffled data
→ sort/merge where required
→ downstream processing
```

Therefore shuffle can increase network I/O, disk I/O, CPU work, serialization overhead, memory pressure, and execution time.

Correct rule:

> Avoid unnecessary shuffles and make necessary shuffles efficient.

Shuffle itself is not automatically bad.

## Wide Dependency → Stage Boundary

Example:

```python
result = (
    df
    .filter(col("amount") > 1000)
    .select("customer_id", "amount")
    .withColumn("tax", col("amount") * 0.10)
    .groupBy("customer_id")
    .agg(sum("amount"))
)
```

Conceptually:

```text
Stage 1

Read
 ↓
Filter
 ↓
Select
 ↓
withColumn
 ↓
────────────
  SHUFFLE
────────────
 ↓
Stage 2

Aggregate
```

A wide/shuffle dependency generally creates a stage boundary.

## Partial / Local Aggregation

Spark can often partially aggregate before shuffle.

```text
P1 → A:300
P2 → A:900
P3 → A:600
```

But none is the final answer.

```text
A:300 ─┐
A:900 ─┼─ SHUFFLE → A:1800
A:600 ─┘
```

Strong interview explanation:

> Spark can perform local/partial aggregation where possible, but the final aggregate for a key may depend on data originating from multiple partitions, which requires redistribution.

## Filter Before Shuffle

Suppose filtering reduces:

```text
1 TB → 150 GB
```

Then:

```text
1 TB
 ↓
Filter
 ↓
150 GB
 ↓
SHUFFLE
```

Potential benefits:

- less shuffle data
- lower network I/O
- lower shuffle read/write
- lower disk I/O
- lower memory pressure
- less downstream processing

Important nuance: this does not automatically mean the initial source scan is smaller. That depends on optimizations such as predicate pushdown and partition pruning.

## Reduce Columns Before Shuffle

If rows contain 50 columns but downstream processing requires only:

```text
customer_id
amount
```

then column pruning/projection can reduce the data participating in downstream work.

```text
Filter early → fewer ROWS
Required columns only → fewer COLUMNS
```

## orderBy()

Global ordering is wide because independently sorting original partitions does not guarantee global order.

```text
P1: 500, 10
P2: 1000, 20
```

Global descending order must be:

```text
1000
500
20
10
```

Coordination/redistribution is required.

## distinct()

If duplicates occur across partitions:

```text
P1: A, B
P2: A, C
P3: B, D
```

Spark needs cross-partition coordination to produce a global distinct result, commonly requiring shuffle.

## Joins

A shuffle-based join may redistribute both sides by the join key:

```text
Orders ─────┐
            ├─ shuffle by customer_id → JOIN
Customers ──┘
```

But if one side is tiny:

```text
orders    = 2 TB
customers = 5 MB
```

a broadcast strategy can potentially avoid shuffling the large side.

Therefore:

> Many joins are wide/shuffle operations, but physical join strategy matters.

## Narrow vs Wide Is Not Good vs Bad

```text
Narrow ≠ automatically fast
Wide   ≠ automatically bad code
```

A `groupBy()` may necessarily require shuffle to satisfy the business requirement.

A stronger Senior DE answer is:

> `groupBy()` generally introduces a wide dependency and shuffle, so I would reduce unnecessary rows and columns beforehand, verify partitioning, inspect shuffle volume, and check for skew.

## Production Troubleshooting

If the physical plan contains:

```text
Exchange
 ↓
SortMergeJoin
```

`Exchange` is an important clue that redistribution/shuffle is occurring.

Investigate:

- How much data is shuffled?
- Can filters reduce the data first?
- Are unnecessary columns included?
- Can a small join side be broadcast?
- Is there key skew?
- Are shuffle partitions reasonable?

## Practical Classification

| Transformation | Typical Dependency |
|---|---|
| `filter()` | Narrow |
| `select()` | Narrow |
| `withColumn()` | Narrow |
| `map()` | Narrow |
| `groupBy()` | Wide |
| `distinct()` | Wide |
| `orderBy()` | Wide |
| `repartition()` | Wide |
| Many joins | Wide |
| Broadcast join | Can avoid large-side shuffle |

## Connection to Concepts 1–5

```text
Driver
 ↓
Logical Plan
 ↓
Catalyst
 ↓
Physical Plan
 ↓
Action
 ↓
Job
 ↓
DAG
 ↓
Narrow dependencies
can often remain in same stage
 ↓
Wide dependency
 ↓
SHUFFLE
 ↓
new stage
 ↓
Partitions
 ↓
Tasks
 ↓
Executor cores
```

## Completed Knowledge Check

For:

```python
result = (
    transactions_df
    .filter(col("status") == "COMPLETED")
    .select("customer_id", "amount")
    .groupBy("customer_id")
    .agg(sum("amount"))
)
```

**A. Narrow:** `filter()`, `select()`.

**B. Wide:** grouping/aggregation by `customer_id` creates the need to bring matching keys together.

**C. Final aggregation:** Spark can perform local partial aggregation, but final values may require partial results from multiple original partitions.

**D. 1 TB → 150 GB before groupBy:** less data reaches the shuffle, reducing network I/O, shuffle read/write, disk/memory pressure, and downstream processing.

## Key Rules

1. Narrow dependencies generally avoid cross-partition redistribution.
2. Wide dependencies need data originating from multiple upstream partitions.
3. Wide dependencies commonly require shuffle.
4. Shuffle generally creates a stage boundary.
5. Multiple narrow transformations can often execute within one stage.
6. Aggregations can use local/partial aggregation before final shuffled aggregation.
7. Reduce unnecessary rows and columns before expensive wide operations when possible.
8. Unnecessary or inefficient shuffle is the problem—not shuffle itself.
9. `Exchange` in a physical plan is an important redistribution clue.
10. Join strategy can change whether/how data is shuffled.

## Interview Answer

> A narrow dependency means an output partition depends on a limited number of input partitions, so operations such as filter and select can usually be processed locally and pipelined within the same stage. A wide dependency means an output partition needs data originating from multiple input partitions, which generally requires a shuffle and creates a stage boundary. Operations such as groupBy, distinct, global orderBy, repartition, and many joins create wide dependencies. In production, I try to reduce unnecessary rows and columns before wide operations, then inspect shuffle volume, partitioning, join strategy, and skew.
