# Day 5 — Concept 9: `cache()` vs `persist()` + Spark Memory Behavior

## 1. Why Cache?

Spark transformations are lazily evaluated. Without caching, separate actions on the same expensive intermediate DataFrame may recompute its lineage.

```python
result = (
    spark.read.parquet('/transactions')
    .filter('status = "ACTIVE"')
    .groupBy('customer_id')
    .sum('amount')
)
result.count()
result.show()
```

The two actions may each require reading, filtering, shuffling, and aggregating. Caching can let subsequent actions reuse the intermediate result rather than repeating that work.

## 2. `cache()` Is Lazy

```python
result.cache()   # Mark for caching; does not necessarily compute the whole DF
result.count()   # Action: computes result and can populate cache
result.show()    # Can reuse cached partitions
```

**Rule:** Calling `cache()` alone does not necessarily materialize the DataFrame. An action initiates distributed computation and can populate the cache.

## 3. Where Is Cached Data Stored?

Cached DataFrame partitions are distributed among **executors**, not collected as one DataFrame into driver memory.

```text
Executor 1: cached partitions 1, 2
Executor 2: cached partitions 3, 4
Executor 3: cached partitions 5, 6
```

Caching 60 GB does **not** mean the driver needs 60 GB of RAM.

## 4. `cache()` vs `persist()`

```python
result.cache()
```

Convenient default persistence behavior.

```python
from pyspark import StorageLevel
result.persist(StorageLevel.MEMORY_AND_DISK)
```

`persist()` lets you explicitly choose the storage level. The precise default storage level depends on Spark API/version; focus on the conceptual distinction.

Depending on storage level and runtime conditions, partitions can be retained in memory, stored on disk, evicted, or recomputed from lineage when needed. A DataFrame larger than executor RAM does **not** automatically imply caching will OOM.

## 5. Executor Memory: Storage vs Execution

Spark executor memory supports both:

- **Storage:** cached/persisted partitions.
- **Execution:** joins, aggregations, sorts, shuffle processing.

Modern Spark uses unified memory management rather than permanently fixed independent boxes. Excessive caching can compete with execution needs, leading to eviction, spill, garbage-collection pressure, or slowdowns.

**Important:** Disk spill is not automatically proof that executor memory must be increased. Check partition sizes, skew, shuffle volume, and unnecessary caching first.

## 6. When Caching Helps

Best candidate: a **costly intermediate result reused several times**.

```text
Raw input
  ↓
Filter
  ↓
Expensive join + aggregation
  ↓
Intermediate result ← CACHE
  ├── Report A
  ├── Metrics B
  ├── Metrics C
  └── Validation D
```

Benefits can include avoiding repeated scans, joins, shuffles, and aggregations.

### Where to Cache

```text
2 TB raw → filter to 100 GB → expensive join → 80 GB reused result
```

The 80 GB reused result is often a better cache candidate than the 2 TB raw input: it is smaller and saves expensive upstream work.

## 7. When Caching Hurts

Avoid caching automatically when:

- A DataFrame is used only once.
- Recomputing it is cheap.
- The intermediate result is enormous relative to available resources.
- Cache storage competes with expensive joins/aggregations.
- A notebook caches many DataFrames without releasing them.

**Rule:** Reuse alone is not sufficient. Compare recomputation cost with cache storage and management cost.

## 8. `unpersist()`

When downstream consumers are finished:

```python
result.unpersist()
```

This releases persisted data that is no longer needed and helps reduce memory/storage pressure in long-running notebooks and pipelines.

## 9. Fault Tolerance and Durability

If an executor containing cached partitions fails, Spark can generally recompute missing partitions from lineage.

**Cache/persist is temporary runtime optimization, not durable business storage.** Parquet/Delta files are durable datasets; a Spark cache is not their replacement.

## 10. How to Inspect Cache Usage

Use the Spark UI Storage tab and execution plans. Cached scans may appear with operators such as:

```text
InMemoryTableScan
```

Look for storage consumption, cached partition coverage, eviction, recomputation, and whether later actions actually benefit.

## 11. Production Troubleshooting

Scenario:

```text
High memory pressure
Heavy disk spill
Frequent GC
Large cached intermediates
```

Ask whether the cached DataFrames are reused and expensive enough to justify their cost before simply increasing executor memory.

## 12. Completed Knowledge Check — 10/10

**Scenario:**

```text
Raw transactions: 3 TB
Filter result: 400 GB
Expensive join + aggregation: 60 GB

60 GB result feeds:
A. Customer summary
B. Fraud metrics
C. Monthly metrics
D. Validation count
```

Cluster has sufficient resources to reasonably retain the 60 GB intermediate result.

**A. What should be cached?**

**Answer:** The **60 GB intermediate result**, because it is reused by multiple downstream operations and avoids repeating the expensive join and aggregation. Do not automatically cache the 3 TB raw dataset.

**B. Does `result.cache()` immediately compute the whole result?**

**Answer:** No. Cache registration is lazy; an action triggers computation and can populate the cache.

**C. What should happen after A/B/C/D finish?**

**Answer:** Call `result.unpersist()` to release resources.

## 13. Ten Interview Rules

1. Cache/persist can prevent repeated recomputation of reused DataFrames.
2. `cache()` does not necessarily materialize data immediately.
3. An action can populate the cache.
4. Cached partitions primarily reside on executors, not the driver.
5. `persist()` allows explicit storage-level selection.
6. Cache is temporary; it is not durable Parquet/Delta storage.
7. Cache selectively based on reuse and recomputation cost.
8. Over-caching can create memory/storage pressure and slow jobs.
9. Call `unpersist()` when cached data is no longer needed.
10. Lost cached partitions can generally be recomputed from lineage.

## Interview Answer

> I use caching when an expensive intermediate DataFrame is consumed repeatedly, so Spark can reuse computed partitions rather than rerunning upstream scans, joins, aggregations, and shuffles. `cache()` uses default persistence behavior, while `persist()` lets me specify a storage level. Both are lazy in the sense that marking a DataFrame for caching does not necessarily materialize it until an action executes. Cached partitions are distributed across executors. I avoid caching one-time or oversized intermediates because storage can compete with execution memory, and I unpersist results after their last use.
