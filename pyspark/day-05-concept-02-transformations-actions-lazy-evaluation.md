# Day 5 — Spark Internals & Optimization
## Concept 2: Transformations, Actions & Lazy Evaluation

## 1. Core Idea

Spark operations broadly fall into two categories:

- **Transformations** describe how data should be transformed.
- **Actions** require a result and trigger execution of the required work.

```text
Transformation = describe/build the work
Action         = trigger the work
```

## 2. Transformations

Common transformations:

```python
df.filter(...)
df.select(...)
df.withColumn(...)
df.groupBy(...)
df.join(...)
df.orderBy(...)
df.drop(...)
df.dropDuplicates(...)
```

Example:

```python
df2 = df.filter(col("amount") > 1000)
df3 = df2.select("customer_id", "amount")
```

Conceptually:

```text
df
 ↓
FILTER amount > 1000
 ↓
df2
 ↓
SELECT customer_id, amount
 ↓
df3
```

Spark builds the required transformation chain rather than immediately materializing every intermediate DataFrame.

## 3. Actions

Common actions include:

```python
df.show()
df.count()
df.collect()
df.take(10)
df.first()
```

Writing data also triggers execution:

```python
df.write.parquet("/output")
```

An action causes Spark to execute the work required to produce that result.

## 4. Lazy Evaluation

```python
df1 = spark.read.parquet("/transactions")
df2 = df1.filter(col("amount") > 500)
df3 = df2.select("customer_id", "amount")
df4 = df3.withColumn("tax", col("amount") * 0.10)
```

Without an action requiring a result, Spark has not yet needed to fully execute the distributed transformation chain.

A precise interview explanation:

> Transformations are lazily evaluated. Spark builds a logical plan/lineage describing the required computation, while distributed computation is generally triggered when an action requires a result.

Avoid simply saying "Spark does nothing." Spark is still creating DataFrame objects and building/analyzing plans.

## 5. Why Lazy Evaluation Matters

```text
PySpark Code
     ↓
Transformations
     ↓
Logical Plan
     ↓
Optimization / Planning
     ↓
Physical Execution Strategy
     ↓
Action triggers execution
     ↓
Jobs / Stages / Tasks
     ↓
Executors
```

Lazy evaluation lets Spark inspect the broader computation before executing it.

When applicable, this can enable optimizations such as:

- Predicate pushdown
- Column pruning
- Partition pruning
- Other logical and physical plan optimizations

Do not assume every optimization is always possible.

## 6. Lineage

```python
df1 = spark.read.parquet("/transactions")
df2 = df1.filter(col("amount") > 1000)
df3 = df2.select("customer_id", "amount")
df4 = df3.withColumn("tax", col("amount") * 0.10)
```

Conceptually:

```text
df1
 ↓
filter
 ↓
df2
 ↓
select
 ↓
df3
 ↓
withColumn
 ↓
df4
```

This chain describes how later DataFrames are derived from earlier ones.

Lineage matters for execution planning, recomputation, fault tolerance, caching/persistence, and DAGs.

## 7. One Action Can Trigger Many Transformations

```python
df2 = df.filter(col("amount") > 1000)
df3 = df2.select("customer_id", "amount")
df4 = df3.withColumn("tax", col("amount") * 0.10)
df4.show()
```

There are three transformations but only one action.

The transformations form the required plan/lineage, and `show()` triggers the required execution.

## 8. Multiple Actions

```python
df2 = df.filter(col("amount") > 1000)

df2.count()
df2.show()
```

There are two actions.

Without explicit persistence/materialization, do not assume the result computed for the first action is automatically reused by the second.

```text
Action 1: count()
    ↓
Read → Filter → Count

Action 2: show()
    ↓
Read → Filter → Show
```

## 9. Identical Actions Are Not Automatically Reused

```python
df2 = df.filter(col("amount") > 1000)

df2.count()
df2.count()
```

The second `count()` should not be assumed to reuse the first action's computed DataFrame.

Lazy evaluation does not mean Spark scans ahead through the Python program, finds duplicate future actions, executes them once, and automatically saves all intermediate results.

Interview rule:

> Unless data has been explicitly persisted/materialized appropriately, do not assume separate actions reuse a previously computed DataFrame result.

## 10. Catalyst Optimization vs Cache/Persist

These are different concepts:

```text
Catalyst / query optimization
→ makes an execution plan smarter

cache() / persist()
→ allows computed data to be reused across actions
```

Example:

```python
df2 = df.filter(col("amount") > 1000)

df2.cache()

df2.count()
df2.show()
```

`cache()` marks the DataFrame for caching. The first action triggers computation and can populate cached partitions. A later action can potentially reuse them.

## 11. Optimization Example

```python
df = spark.read.parquet("/transactions")

df2 = df.select(
    "customer_id",
    "amount",
    "status",
    "transaction_date",
    "description",
    "merchant",
    "location"
)

df3 = df2.select("customer_id", "amount")
df3.show()
```

The final result only needs `customer_id` and `amount`.

Because Spark can inspect the broader plan before execution, it can potentially eliminate unnecessary work through **column pruning**.

Filters and partition conditions may similarly allow predicate pushdown or partition pruning when supported.

## 12. Connecting Concept 1 and Concept 2

Before an action:

```text
DRIVER

Builds / analyzes plan
       ↓
filter
       ↓
select
       ↓
withColumn
```

After an action:

```text
Action
   ↓
Driver examines/plans required computation
   ↓
Spark determines distributed execution
   ↓
Tasks are scheduled
   ↓
Executors process partitions
   ↓
Result is produced
```

> The driver builds/plans/co-ordinates the computation. An action triggers the required distributed execution. Executors perform the tasks over partitions.

## 13. Normal Python print() Is Not a Spark Action

```python
df = spark.read.parquet("/transactions")
df2 = df.filter(col("amount") > 1000)
df3 = df2.select("customer_id", "amount")

print("Finished")
```

`print("Finished")` is normal Python code and does not require the result of `df3`.

Therefore Spark does not need to process every transaction through the DataFrame transformation chain just to print that string.

## 14. count() Triggers Execution

```python
df3.count()
```

Conceptually:

```text
count() ACTION
      ↓
Driver knows a result is required
      ↓
Spark examines plan / lineage
      ↓
Spark optimizes and plans execution
      ↓
Distributed work is created
      ↓
Tasks are scheduled
      ↓
Executors process required partitions
      ↓
Count result is produced
      ↓
Result returned to Driver
```

Because the final action is `count()`, Spark only needs to perform work required to calculate the count. It does not necessarily need to materialize every selected value as an output dataset.

## 15. Interview Scenario

```python
df1 = spark.read.parquet("/transactions")
df2 = df1.filter(col("amount") > 1000)
df3 = df2.select("customer_id", "amount")

df3.count()
df3.show()
```

### A. How many actions?

Two: `count()` and `show()`.

### B. Does filter() immediately process the dataset?

No. `filter()` is a transformation. Spark adds it to the logical plan/lineage.

### C. Can show() require recomputation?

Yes. Without `cache()`, `persist()`, or another appropriate reuse mechanism, the second action may require Spark to recompute the necessary lineage.

### D. Why is lazy evaluation useful?

It lets Spark analyze the broader transformation plan before execution and determine a more efficient execution strategy.

Potential optimizations include predicate pushdown, column pruning, partition pruning, and other Catalyst optimizations.

## 16. Interview Traps

1. **"Every transformation executes immediately."**  
   Wrong. Transformations are lazily evaluated.

2. **"Spark does absolutely nothing before an action."**  
   Too simplistic. Spark can build/analyze plans before distributed execution.

3. **"Once Spark calculated a DataFrame once, future actions automatically reuse it."**  
   Wrong assumption. Separate actions may recompute lineage without persistence/materialization.

4. **"Lazy evaluation automatically combines duplicate Python actions."**  
   Wrong. Two separate `count()` calls are still separate actions.

5. **"cache() itself necessarily calculates the whole DataFrame immediately."**  
   Generally think of `cache()` as marking the DataFrame for caching; an action triggers computation/population.

## 17. Key Rules to Memorize

1. Transformations describe computation.
2. Actions require a result and trigger execution.
3. Spark transformations use lazy evaluation.
4. Transformations build a logical plan/lineage.
5. One action can trigger many transformations.
6. Multiple actions are separate execution requests.
7. Do not assume separate actions automatically reuse previous computed results.
8. `cache()` / `persist()` help reuse computed data across actions.
9. Lazy evaluation lets Spark optimize the broader query before execution.
10. Predicate pushdown, column pruning, and partition pruning may reduce unnecessary work when applicable.
11. The driver plans/co-ordinates execution.
12. Executors perform distributed tasks over partitions.

## Interview Answer

> Spark uses lazy evaluation, so DataFrame transformations build a logical plan rather than immediately materializing their results. When an action such as `count()`, `show()`, or a write requires a result, Spark analyzes and optimizes the plan, determines a physical execution strategy, schedules distributed tasks, and executors process the required partitions. Lazy evaluation allows optimizations such as predicate pushdown, column pruning, and partition pruning when applicable.

## Quick Mental Model

```text
PySpark transformations
        ↓
Logical plan / lineage
        ↓
Action
        ↓
Optimization + physical planning
        ↓
Distributed execution
        ↓
Tasks
        ↓
Executors
```

Repeated actions:

```text
Action 1 → computation

Action 2 → may recompute
              ↑
       unless cached/persisted
```
