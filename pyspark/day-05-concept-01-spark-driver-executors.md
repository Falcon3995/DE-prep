# Day 5 — Spark Internals & Optimization
## Concept 1: Driver, Executors & Cluster Manager

## 1. Spark Application

A Spark application fundamentally contains:

```text
Spark Application
├── Driver
├── Executor 1
├── Executor 2
└── Executor 3
```

The driver and executors are processes.

## 2. Driver

The **Driver** is the control center of the Spark application. It:

- Runs the main application logic
- Holds the SparkSession
- Builds/plans execution
- Schedules tasks
- Coordinates executors
- Tracks task/executor status
- Receives results explicitly brought back to the driver

The driver does not personally scan every row of a large dataset.

Conceptually:

```text
Logical Plan
     ↓
Optimized Logical Plan
     ↓
Physical Plan
     ↓
Stages
     ↓
Tasks
```

## 3. Executors

An **Executor** is a worker process with CPU cores and memory.

Executors perform distributed work such as:

- Reading partitions
- Filtering rows
- Joins
- Aggregations
- Sorting and shuffling
- Caching partitions
- Writing output

> **Driver coordinates and plans. Executors perform the distributed work.**

## 4. Tasks, Cores and Parallelism

Suppose:

```text
10 Executors
4 cores per Executor
```

A simplified estimate is:

```text
10 × 4 = 40 tasks concurrently
```

If a stage contains 400 tasks:

```text
Stage — 400 tasks

Wave 1  → ~40 tasks
Wave 2  → ~40 tasks
...
Wave 10 → ~40 tasks
```

The remaining tasks wait for task slots.

A later wave is **not automatically a new stage**. Multiple waves can execute within the same stage.

## 5. Tasks and Partitions

Useful mental model:

> For a given stage, one task generally processes one partition.

```text
Partition 1 → Task 1
Partition 2 → Task 2
Partition 3 → Task 3
```

Therefore partitions, task count, executor cores, and parallelism are closely related.

## 6. Executor Memory

Executors also hold distributed data during processing.

For example:

```python
df.cache()
```

Cached partitions are primarily stored across executor memory.

Executor memory matters for caching, joins, aggregations, sorting, shuffle processing, and intermediate computation.

## 7. Driver Memory vs Executor Memory

Suppose:

```text
Driver: 8 GB RAM
DataFrame: 500 GB
```

This can still work:

```python
df.filter(col("status") == "ACTIVE").write.parquet("/output")
```

because the dataset is partitioned and processed across executors. The entire 500 GB does not need to fit in driver memory.

## 8. Why collect() Can Be Dangerous

```python
rows = df.collect()
```

`collect()` returns the DataFrame result rows to the **driver**.

```text
Executor 1 ──┐
Executor 2 ──┼──→ DRIVER
Executor 3 ──┘
```

If the result exceeds available driver memory, the driver can run out of memory.

Important nuance: `collect()` brings the **result** to the driver, not necessarily the original input size. A 500 GB input filtered to 10 MB may be reasonable to collect; a result of hundreds of GB is dangerous.

## 9. Driver OOM vs Executor OOM

### Driver OOM

Often associated with too much data being returned to or stored on the driver, such as a huge:

```python
df.collect()
```

### Executor OOM

Can occur during distributed operations such as:

- Large joins
- Aggregations
- Sorting
- Shuffles
- Poor partitioning
- Data skew

> **Driver OOM and Executor OOM are different problems.**

## 10. Executor Failure

If an executor dies, the entire Spark application does **not necessarily fail**.

Spark can generally reschedule required failed/lost tasks on available executors.

```text
Executor 2 ❌
   ↓
Tasks lost/failed
   ↓
Driver reschedules
   ↓
Another Executor
```

## 11. Driver Failure

Driver failure is more serious because the driver coordinates the application.

```text
Driver ❌
   ↓
Application coordination lost
```

Depending on the environment and recovery configuration, the application may fail or require recovery/restart.

## 12. SparkSession

The object commonly named:

```python
spark
```

is a `SparkSession`.

Examples:

```python
spark.read.parquet(...)
spark.sql(...)
spark.createDataFrame(...)
```

Conceptually:

```text
SparkSession
     │
   Driver
     │
 coordinates
     ↓
 Executors
```

## 13. Cluster / Resource Manager

Spark needs infrastructure that provides/manages compute resources.

Examples include:

- Spark Standalone
- YARN
- Kubernetes
- Databricks-managed infrastructure

Keep the responsibilities separate:

```text
Driver
→ coordinates Spark execution

Cluster / Resource Manager
→ provides/manages compute resources

Executors
→ perform distributed processing
```

## 14. End-to-End Example

```python
transactions = spark.read.parquet("/transactions")

result = (
    transactions
    .filter(col("status") == "COMPLETED")
    .groupBy("customer_id")
    .agg(sum("amount").alias("total"))
)

result.write.format("delta").save("/gold/customer_sales")
```

Conceptually:

```text
PySpark Code
     ↓
DRIVER
     ├── understands requested operations
     ├── builds/plans execution
     ├── determines distributed work
     ├── schedules tasks
     └── tracks execution
     ↓
EXECUTORS
     ├── read partitions
     ├── filter rows
     ├── perform distributed grouping/shuffle
     ├── calculate SUM
     └── write output
```

## 15. Interview Scenario — Large DataFrame

Cluster:

```text
Driver:
8 GB memory
4 cores

10 Executors:
16 GB memory each
4 cores each
```

DataFrame: **500 GB**

This works:

```python
df.filter(col("status") == "ACTIVE").write.parquet("/output")
```

But this may crash:

```python
rows = df.collect()
```

### Explanation

1. The 500 GB is distributed into partitions and processed across executors.
2. `collect()` brings the DataFrame result back to the driver.
3. A large result can exceed driver memory.
4. This is primarily a **driver-memory problem**.

## 16. Interview Scenario — Task Concurrency

```text
10 Executors
4 cores per Executor
400 partitions/tasks
```

Approximate concurrent capacity:

```text
10 × 4 = 40 tasks
```

Remaining tasks execute in later **waves within the same stage** as slots become available.

If an executor dies while processing tasks, Spark can generally reschedule the required tasks elsewhere rather than automatically failing the entire application.

## Key Rules to Memorize

1. Driver coordinates; executors process data.
2. A Spark application has a driver and executors.
3. Executors have CPU cores and memory.
4. Tasks execute against partitions.
5. For a stage, one task generally processes one partition.
6. More executor cores allow more concurrent tasks.
7. Later task waves do not mean a new stage.
8. Cached distributed data lives primarily on executors.
9. `collect()` returns result data to the driver.
10. Huge `collect()` operations can cause driver OOM.
11. Heavy distributed processing can cause executor-memory pressure.
12. Executor failure does not automatically mean application failure.
13. Driver failure is more serious because it coordinates the application.
14. SparkSession is the main Spark entry point on the driver side.
15. The cluster/resource manager manages compute resources.

## One-Line Interview Answer

> A Spark application has a driver that plans and coordinates distributed execution and executors that use CPU and memory to run tasks over data partitions; the cluster/resource manager provides the compute resources on which those executors run.
