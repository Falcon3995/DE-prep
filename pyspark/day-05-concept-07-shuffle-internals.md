# Day 5 — Concept 7: Shuffle Internals

## Core Idea

A shuffle occurs when Spark must redistribute data across partitions.

```text
Narrow transformation → partitions usually work independently
Wide transformation   → data redistribution → SHUFFLE → usually a stage boundary
```

Common shuffle-producing operations include `groupBy`, many joins, `distinct`, global `orderBy`, and `repartition()`.

## Shuffle Flow

```text
Stage 1 / upstream tasks
        ↓
partition records by destination
        ↓
SHUFFLE WRITE
        ↓
intermediate shuffle data
        ↓
network / fetch
        ↓
SHUFFLE READ
        ↓
Stage 2 / downstream tasks
```

Upstream tasks produce **shuffle write**. Downstream tasks fetch/consume **shuffle read**.

## groupBy Example

```python
transactions_df.groupBy("customer_id").sum("amount")
```

If customer A exists in several input partitions, its data/partial aggregates must be brought together appropriately for the final aggregation.

Conceptually, key-based redistribution can be thought of as:

```text
destination = hash(key) % number_of_partitions
```

The important idea is that the same key maps consistently to the appropriate downstream partition.

## Why Shuffle Creates a Stage Boundary

```text
Stage 1 tasks
      ↓
produce shuffle output
      ↓
──── stage boundary ────
      ↓
Stage 2 tasks
      ↓
consume shuffle output
```

Wide/shuffle dependencies commonly split execution into stages.

## Shuffle Is More Than Network I/O

Shuffle can involve:

- CPU for partitioning/sorting
- serialization/deserialization
- memory buffers
- intermediate disk I/O
- network transfer/fetch
- sort/merge
- downstream aggregation/join processing

So shuffle can pressure **CPU, memory, disk, and network**.

## Partial Aggregation

For aggregations such as SUM, Spark can often aggregate locally before shuffle.

```text
P1 before:
A 100
A 200
A 300
B 500

Partial aggregation:
A 600
B 500
```

This can reduce how much intermediate data needs to move across the shuffle.

## Shuffle Metrics

If Spark UI shows:

```text
Shuffle Write = 800 GB
Shuffle Read  = 800 GB
```

investigate why so much data is moving:

- Were rows filtered early?
- Are unnecessary columns moving?
- Is the join strategy appropriate?
- Are repartitions necessary?
- Are partition counts reasonable?
- Is there skew?

Do not immediately conclude that the solution is a larger cluster.

## Exchange in the Physical Plan

`df.explain()` may show:

```text
HashAggregate
+- Exchange hashpartitioning(customer_id, 200)
   +- HashAggregate
      +- Scan parquet
```

`Exchange` is an important clue that Spark is redistributing data.

```text
Exchange hashpartitioning(customer_id, 200)
```

roughly means Spark is redistributing by `customer_id` into about 200 shuffle partitions.

Before AQE-related runtime changes:

```text
~200 shuffle partitions → ~200 downstream tasks
```

## Shuffle Partition Count

Too few:

```text
1 TB shuffle / 10 partitions
≈ 100 GB per partition
```

Possible consequences:

- huge tasks
- memory pressure
- spill
- long execution
- OOM

Too many:

```text
1 GB shuffle / 10,000 partitions
≈ 100 KB per partition
```

Possible consequence:

- excessive tiny-task/scheduling overhead

Mental rule:

```text
Too few shuffle partitions → huge tasks
Too many shuffle partitions → tiny-task overhead
```

## Shuffle and Spill

A task may need to sort, aggregate, or join large amounts of shuffled data.

```text
Shuffle Read
    ↓
Task processing
    ↓
Memory pressure
    ↓
SPILL
    ↓
Disk I/O
```

Spill is not automatically a failure, but heavy spill is a performance warning.

Possible causes include large partitions, skew, large shuffles, sorts/aggregations, joins, or insufficient available execution memory.

**Important:** disk spill does not automatically prove that the executor simply needs more memory.

## Shuffle + Data Skew

A hot key can create one disproportionately large shuffle partition.

```text
Normal:
Q1 100 MB
Q2 130 MB
Q3 90 MB

Hot key:
Q4 ███████████████████ HUGE
```

Then:

```text
Q1 → 30 sec
Q2 → 35 sec
Q3 → 28 sec
Q4 → 25 min
```

This is a classic skew/straggler pattern.

## Why More Executors May Not Fix Skew

```text
Huge partition
     ↓
1 task
     ↓
1 task slot
```

Adding executors does not automatically divide that existing partition/task across the new executors.

Possible techniques, depending on the problem, include AQE skew handling, salting, different join strategies, or better partitioning.

## Join Shuffle

If both sides of a join are large, Spark may redistribute both by the join key:

```text
ORDERS                  CUSTOMERS
   ↓                        ↓
shuffle by key          shuffle by key
   ↓                        ↓
Q1 Q2 Q3                Q1 Q2 Q3
```

Then corresponding partitions can join.

This is one reason broadcast joins can be valuable when one side is genuinely small.

## repartition() Causes Shuffle

```python
df2 = df.repartition(500)
```

Conceptually:

```text
Existing partitions
       ↓
SHUFFLE
       ↓
500 partitions
```

`repartition()` is not free. Do not add arbitrary repartitions just because more partitions sound faster.

## Necessary vs Unnecessary Shuffle

Some shuffle is fundamental:

```python
df.groupBy("customer_id").sum("amount")
```

The goal is not to eliminate every shuffle.

Instead:

> Avoid unnecessary shuffles and make necessary shuffles efficient.

Check filtering, selected columns, partition counts, skew, partial aggregation, join strategy, AQE, and unnecessary repartitions.

## Production Troubleshooting

Example:

```text
Stage 1
Shuffle Write: 900 GB

Stage 2
Shuffle Read: 900 GB
Disk Spill: 1.5 TB
Duration: 42 min
```

Investigate:

- partition sizes/count
- skew
- join strategy
- unnecessary rows/columns
- possible broadcast
- unnecessary repartitions
- spill
- task-duration distribution

## Completed Knowledge Check

Given:

```text
Stage 4
200 tasks
Shuffle Write = 1.2 TB

Stage 5
200 tasks
Shuffle Read = 1.2 TB
Disk Spill = 2 TB

199 tasks → ~2 minutes
1 task    → 31 minutes
```

### A — Diagnosis

Strong warning signs:

- Very large 1.2 TB shuffle
- 2 TB disk spill
- One 31-minute straggler while 199 tasks finish in ~2 minutes

The straggler pattern is strong evidence of **data skew**.

### B — Why More Executors Probably Won't Fix It

The skewed partition is processed by one task.

```text
1 skewed partition → 1 task → 1 task slot
```

Adding executors does not automatically split that existing task.

### C — Physical Plan Clue

```text
Exchange
```

For example:

```text
Exchange hashpartitioning(customer_id, 200)
```

indicates redistribution / likely shuffle.

## Interview Rules

```text
1. Wide dependencies commonly cause shuffle.
2. Upstream tasks produce shuffle write.
3. Downstream tasks consume shuffle read.
4. Shuffle can pressure network, disk, memory and CPU.
5. Shuffle dependencies commonly create stage boundaries.
6. Exchange is a redistribution/shuffle clue.
7. Too few shuffle partitions → huge tasks.
8. Too many shuffle partitions → tiny-task overhead.
9. Skew → one/few huge partitions and stragglers.
10. Heavy spill is a warning sign, not automatic proof that more memory is needed.
11. Adding executors does not automatically fix one skewed partition.
12. Necessary shuffle is normal; optimize it rather than blindly trying to eliminate it.
```

## Interview Answer

> A Spark shuffle occurs when data must be redistributed across partitions, typically for wide operations such as groupBy, many joins, distinct, orderBy, and repartition. Upstream tasks generate shuffle write data and downstream tasks fetch it as shuffle read, which commonly creates a stage boundary. Shuffle can pressure network, memory, disk and CPU. In production I inspect Spark UI and the physical plan—especially Exchange operators, shuffle read/write, task-duration distribution, partition counts, skew and spill—to identify the actual bottleneck before changing cluster resources.
