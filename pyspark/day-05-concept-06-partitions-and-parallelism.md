# Day 5 — Spark Internals & Optimization
## Concept 6: Partitions & Parallelism

## Core Model

```text
DataFrame → Partitions → Tasks → Executor Cores → Parallelism
```

For a given stage, one partition is generally processed by one task.

Example:

```text
200 partitions → ~200 tasks

10 executors × 4 cores
= ~40 concurrent tasks

200 / 40
≈ 5 waves
```

## Too Few Partitions

```text
40 available task slots
4 partitions
→ ~4 tasks
```

Symptoms can include low CPU utilization, idle cores, few tasks, and large/long-running tasks.

Diagnosis:

> Too few partitions can cause insufficient parallelism and cluster underutilization.

Long task duration alone does not prove this. The stronger evidence is few tasks combined with unused capacity and low utilization.

## Too Many Tiny Partitions

Example:

```text
100 GB
→ enormous number of tiny partitions
→ enormous number of tiny tasks
```

Overhead includes:

- task scheduling
- startup
- serialization
- metadata
- coordination

Diagnosis:

> Too many tiny partitions can cause excessive task-management overhead.

Tiny tasks do not automatically mean excessive network I/O.

## Partition Count vs Distribution

These are very different:

```text
100 partitions × ~100 MB each
```

versus:

```text
99 partitions ≈ 10 MB
1 partition   ≈ 20 GB
```

The second can create a straggler.

> Good partitioning is not only about partition count. Distribution across partitions matters.

## Three Production Patterns

```text
Few long tasks + idle cluster
→ TOO FEW PARTITIONS

Huge number of tiny/fast tasks
→ TOO MANY PARTITIONS

Most tasks normal + few extremely slow
→ DATA SKEW
```

Partition-count problem means the work is not divided into a useful number of pieces.

Skew means the pieces exist but are badly uneven.

## Input vs Shuffle Partitions

Before shuffle:

```text
Source → Input partitions → Stage 1 tasks
```

After shuffle:

```text
Stage 1
 ↓
SHUFFLE
 ↓
Shuffle partitions
 ↓
Stage 2 tasks
```

Example:

```text
500 input partitions
→ Stage 1 ~500 tasks
→ SHUFFLE
→ 200 shuffle partitions
→ Stage 2 ~200 tasks
```

The counts do not need to match.

## spark.sql.shuffle.partitions

Important Spark SQL/DataFrame configuration:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

It influences partition count around SQL shuffle operations such as `groupBy`, many joins, `distinct`, and `orderBy`.

A commonly encountered/default value is 200 in many Spark configurations, but:

> 200 is not a universally correct number.

For example:

```text
1 GB / 200  → ~5 MB average
2 TB / 200  → ~10 GB average
```

Same partition count, completely different workload.

AQE can also dynamically optimize/coalesce shuffle partitions.

## Partition Size Heuristics

Ranges such as ~100–200 MB are useful starting heuristics, not laws.

Actual performance depends on data volume, compression, file format, operations, cluster resources, executor memory, skew, and runtime behavior.

Strong interview answer:

> I don't blindly target a fixed partition size. I look at data volume, partition sizes, executor cores, task durations, shuffle metrics and skew, then validate using Spark UI.

## What If I Configure 128 MB at Cluster Level?

A setting such as:

```python
spark.sql.files.maxPartitionBytes
```

can influence file-scan/input partition sizing.

Conceptually:

```text
1 TB Parquet
 ↓
file scan
 ↓
~128 MB input chunks
 ↓
roughly ~8,000 input partitions
```

This is simplified and **does not guarantee every Spark runtime partition will remain exactly 128 MB**.

After:

```python
df.groupBy("customer_id").sum("amount")
```

a shuffle occurs. Downstream partitioning is influenced by:

```text
spark.sql.shuffle.partitions
AQE
data distribution / skew
```

Therefore:

```text
128 MB input target
≠
every shuffle partition = 128 MB
```

### Skew Can Break the Expectation

Conceptually:

```text
Partition 1  ~110 MB
Partition 2  ~125 MB
Partition 3  ~130 MB
...
Partition 57 HUGE ← skew
```

A 128 MB file-scan target does not eliminate skew.

### Important Distinction

```text
spark.sql.files.maxPartitionBytes
        ↓
primarily FILE SCAN / input partition sizing

spark.sql.shuffle.partitions
        ↓
partition count around SQL SHUFFLES

AQE
        ↓
can dynamically optimize/coalesce shuffle partitions at runtime
```

Interview answer:

> I can use roughly 128 MB as a starting target for file-scan partition sizing, but I don't assume all runtime partitions remain 128 MB. Input partitioning, shuffle partitioning, AQE, and data skew can change the distribution. I validate actual task and partition behavior using Spark UI.

## Partitions Do Not Simply Equal Cores

Suppose:

```text
20 executors × 4 cores
= ~80 concurrent task slots
```

Then:

```text
8 partitions   → ~8 tasks → poor utilization likely
80 partitions  → potentially one wave
320 partitions → ~4 waves
```

Having more partitions than cores is normal.

> 80 cores does not mean you must use exactly 80 partitions.

## Large Partitions and Spill

Huge partitions can cause:

- long-running tasks
- heavy spill
- garbage-collection pressure
- executor OOM
- executor loss
- retries

When memory pressure becomes high, Spark may spill intermediate data to disk:

```text
Task
 ↓
Memory pressure
 ↓
Spill
 ↓
Disk I/O
 ↓
Continue processing
```

Large spill metrics can indicate large partitions, skew, large shuffles, expensive sorts/aggregations, or insufficient memory.

## Small Files

Many tiny Parquet/Delta files can cause overhead from file listing, metadata, open/close operations, small reads, and task scheduling.

This connects to the small-file problem covered later with Databricks/Delta optimization.

## Runtime vs Storage Partitioning

Runtime Spark partitions:

```text
DataFrame
├── Spark partition 1
├── Spark partition 2
└── Spark partition 3
```

Storage partitioning:

```text
/transactions/year=2026/month=10/day=14/
```

Storage partitioning can support partition pruning.

Runtime partitions determine distributed execution/task parallelism.

They are related but not identical.

## Inspect Partition Count

```python
df.rdd.getNumPartitions()
```

Partition count can change after shuffle, `repartition()`, `coalesce()`, or AQE.

For production tuning, Spark UI and physical plans are often more useful than looking at one partition count.

## Production Troubleshooting Workflow

For a slow Spark job, check:

1. Spark UI and slow stages
2. Task count
3. Task-duration distribution
4. Partition count
5. Skew
6. Shuffle read/write
7. Spill
8. Executor utilization
9. Physical plan
10. Then decide whether code, partitioning, joins, data layout, or resources should change

## Completed Check

Cluster:

```text
16 executors × 4 cores
≈ 64 concurrent task slots
```

### Job A

```text
8 tasks
~12 minutes each
Low CPU utilization
```

Diagnosis: **too few partitions / insufficient parallelism**.

### Job B

```text
20,000 tasks
Most ~0.2 seconds
```

Diagnosis: **too many tiny partitions/tasks** and excessive scheduling/coordination overhead.

### Job C

```text
200 tasks
199 → ~40 seconds
1 → 18 minutes
```

Diagnosis: **data skew / straggler partition**.

Adding executors may not solve one huge existing partition because that partition is still processed by one task.

## Key Rules

```text
1. Partition ≈ unit of parallel data processing.
2. One partition in a stage ≈ one task.
3. Executor cores determine approximate concurrent task capacity.
4. Too few partitions → insufficient parallelism.
5. Too many tiny partitions → task/scheduling overhead.
6. Uneven partitions → skew / stragglers.
7. Input partitions and shuffle partitions are different.
8. spark.sql.shuffle.partitions is not a magic universal number.
9. A 128 MB file-scan target does not guarantee 128 MB runtime/shuffle partitions.
10. Tune using Spark UI and workload metrics, not fixed rules.
```

## Interview Answer

> Spark partitions determine how data is divided for distributed processing, and for a stage each partition is generally processed by one task. Too few partitions can underutilize executor cores, while too many tiny partitions create scheduling overhead. I also check for uneven partitions because skew creates straggler tasks. I distinguish input partition sizing from shuffle partitioning: `spark.sql.files.maxPartitionBytes` influences file scans, while `spark.sql.shuffle.partitions` influences SQL shuffle partition counts, and AQE can dynamically optimize shuffle partitions. I use partition-size heuristics only as a starting point and validate actual behavior in Spark UI.
