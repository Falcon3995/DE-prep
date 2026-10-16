# Day 5 — Spark Internals & Optimization
## Concept 8: `repartition()` vs `coalesce()`

## Core Difference

```text
repartition()
→ increase or decrease partitions
→ shuffle / redistribution
→ more expensive
→ better opportunity for balanced distribution

coalesce()
→ primarily decrease partitions
→ normally avoids a full shuffle
→ cheaper
→ may preserve uneven distribution
```

## `repartition()`

```python
df2 = df.repartition(8)
```

Conceptually:

```text
P1 P2 P3 P4
     ↓
   SHUFFLE
     ↓
Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8
```

It can increase or decrease partition count. The shuffle costs resources but can be useful when better distribution or more parallelism is required.

### Repartition by Column

```python
df.repartition(200, "customer_id")
```

Conceptually:

```text
Input partitions
      ↓
hash(customer_id)
      ↓
SHUFFLE
      ↓
new partitions
```

Repartitioning by a skewed column does not automatically fix skew.

## `coalesce()`

```python
df2 = df.coalesce(20)
```

Conceptually:

```text
100 existing partitions
        ↓
combine/collapse
        ↓
20 partitions
```

It is primarily useful for reducing partition count without the same full redistribution as `repartition()`.

Tradeoff:

```text
coalesce()
→ cheaper
→ less redistribution
→ potentially uneven partitions

repartition()
→ more expensive
→ shuffle
→ better opportunity for balance
```

## Main Interview Rule

```text
Need MORE partitions?
→ repartition()

Need FEWER partitions cheaply?
→ coalesce()

Need FEWER + redistribution/better balance?
→ repartition()
```

`repartition()` can also decrease partitions, so “repartition increases, coalesce decreases” is incomplete.

## Production Example After Filtering

```text
2 TB input
→ 8,000 partitions
→ FILTER
→ only 20 GB remains
→ thousands of tiny partitions
```

A reasonable option before writing:

```python
filtered_df.coalesce(100).write.parquet(...)
```

This can reduce task overhead and excessive tiny output files.

## Small-File Connection

Useful mental model:

```text
many partitions
      ↓
many write tasks
      ↓
potentially many output files
```

Reducing excessive tiny partitions before writing can help control the small-file problem.

## `coalesce(1)` Trap

```python
df.coalesce(1).write.csv(...)
```

For a large dataset:

```text
large distributed dataset
        ↓
1 partition
        ↓
1 task
        ↓
poor parallelism
```

It can be acceptable for a tiny report, but is generally a poor design for large datasets.

## Connection to Shuffle Internals

```python
df.repartition(500)
```

Conceptually:

```text
Existing partitions
       ↓
Exchange / redistribution
       ↓
Shuffle Write
       ↓
Shuffle Read
       ↓
500 new partitions
```

Repeated unnecessary repartitioning can therefore introduce repeated expensive shuffles.

## More Partitions ≠ Automatically Faster

```text
2 GB data
→ repartition(10000)
→ 10,000 tiny partitions/tasks
→ scheduling overhead
```

`repartition()` is not a generic performance button.

## `coalesce()` Does Not Fix Skew

If partitions are badly uneven:

```text
P1 = 50 GB
P2 = 1 GB
P3 = 1 GB
...
```

`coalesce(10)` does not fundamentally redistribute the hot data.

If redistribution/better balance is the goal, `repartition()` is generally more appropriate.

## Before vs After Heavy Filtering

Potentially poor:

```python
df = spark.read.parquet(...)
df = df.coalesce(100)
df = df.filter(col("status") == "ACTIVE")
```

This can reduce parallelism before processing the large dataset.

Often more sensible:

```python
df = spark.read.parquet(...)
df = df.filter(col("status") == "ACTIVE")
df = df.coalesce(100)
```

Process/filter in parallel first, then reduce partitions after the data becomes much smaller. This is reasoning, not an absolute rule.

## Decision Table

| Requirement | Usually consider |
|---|---|
| 50 → 500 partitions | `repartition(500)` |
| 500 → 50, cheap reduction acceptable | `coalesce(50)` |
| 500 → 50, redistribution needed | `repartition(50)` |
| Redistribute by customer_id | `repartition(..., "customer_id")` |
| Reduce tiny partitions after major filtering | often `coalesce()` |
| Fix badly uneven distribution | generally `repartition()` |
| One file from huge dataset | avoid `coalesce(1)` |

## Completed Knowledge Check

### A — 5 TB, 200 → 2,000

```text
200 partitions → ~200 tasks
Tasks are huge
Cluster has idle cores
```

Correct:

```python
df.repartition(2000)
```

Reason: increase parallelism and redistribute the huge partitions so more available executor cores can participate.

### B — 500 GB filtered to 8 GB

```text
8 GB
4,000 partitions
≈ 2 MB average per partition
```

Correct first consideration:

```python
df.coalesce(80)
```

Reason: dramatically reduce tiny-partition/task overhead without necessarily paying for a full redistribution. It can also reduce small-file problems during the write.

If the resulting partitions are badly uneven, `repartition(80)` may be worth the shuffle.

### C — Fewer and Better-Balanced Partitions

```text
Most partitions ≈ 100 MB
Several partitions ≈ 15 GB
```

Correct:

```python
df.repartition(...)
```

Even though it shuffles, redistribution gives Spark a better opportunity to produce balanced partitions. `coalesce()` may preserve the imbalance.

## Final Mental Model

```text
REPARTITION
-----------
Increase ✓
Decrease ✓
Shuffle ✓
Redistributes ✓
More expensive
Use when distribution/parallelism matters

COALESCE
--------
Primarily decrease ✓
Normally avoids full shuffle ✓
Cheaper
May preserve imbalance
Useful after major data reduction
```

## Interview Answer

> `repartition()` can increase or decrease partitions and performs a shuffle to redistribute data, so I use it when I need more parallelism or better distribution. `coalesce()` is mainly used to reduce partitions while avoiding a full shuffle, making it cheaper but potentially leaving uneven partitions. After a major filter that leaves thousands of tiny partitions, I may coalesce before writing. If the data is badly uneven and redistribution matters, I prefer repartition even when reducing the partition count.
