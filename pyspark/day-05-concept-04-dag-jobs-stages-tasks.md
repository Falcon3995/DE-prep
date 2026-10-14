# Day 5 — Spark Internals & Optimization
## Concept 4: DAG → Jobs → Stages → Tasks

## Core Hierarchy
```text
Spark Application → Action → Job → DAG → Stages → Tasks → Executors
```

## Job
An action triggers execution and typically results in one or more Spark jobs.

```python
df.count()
df.show()
df.collect()
df.write.parquet("/output")
```

Useful mental model: `Action → Job`, while remembering the exact action/job relationship can be more complex internally.

## Stage
A job is divided into stages based on execution dependencies. A **shuffle/wide dependency commonly creates a stage boundary**.

```text
Stage 1
Read → Filter → pre-shuffle work
             ↓
          SHUFFLE
             ↓
Stage 2
Aggregate shuffled data
```

Five transformations do not mean five stages. Several transformations can be pipelined in the same stage when no shuffle boundary separates them.

## Task
A task is the smallest unit of work Spark sends to an executor for a stage.

Useful mental model:

```text
one partition in a stage → approximately one task
```

Example:

```text
100 partitions → ~100 tasks
```

## Task Waves
Suppose:

```text
10 executors × 4 cores = ~40 concurrent tasks
Stage = 100 tasks
```

Execution can occur approximately as:

```text
Wave 1 → 40
Wave 2 → 40
Wave 3 → 20
```

These are **three waves inside one stage**, not three stages.

## DAG
DAG = Directed Acyclic Graph.

For interview purposes:

> The DAG represents dependencies between transformations so Spark can organize execution into stages and tasks.

Example:

```text
Read
 ↓
Filter
 ↓
Select
 ↓
────────────
  SHUFFLE
────────────
 ↓
Aggregate
```

The DAG is not itself a stage. Spark examines the dependencies and determines stage boundaries.

## Different Stages Can Have Different Task Counts
Suppose:

```text
Input partitions = 200
Shuffle partitions = 100
```

A simplified execution could be:

```text
Stage 1
200 partitions
≈ 200 tasks
     ↓
  SHUFFLE
     ↓
Stage 2
100 partitions
≈ 100 tasks
```

Each stage can operate on a different partition structure.

## Why Partitions Matter
```text
Partitions → Tasks → Available parallelism
```

Too few partitions can leave executor cores idle.

Too many tiny partitions can create excessive scheduling, serialization, startup, metadata, and coordination overhead.

There is no universal perfect partition count.

## Narrow vs Wide Preview
A filter can generally process partitions independently:

```text
P1 → P1
P2 → P2
P3 → P3
```

This is broadly a narrow dependency.

A `groupBy()` may require records from many input partitions to be redistributed:

```text
P1 ─┐
P2 ─┼→ output partition
P3 ─┤
P4 ─┘
```

This is a wide dependency and commonly requires shuffle. Concept 5 covers this deeply.

## End-to-End Flow
```text
Transformations
      ↓
Logical Plan
      ↓
Catalyst Optimization
      ↓
Physical Plan
      ↓
Action
      ↓
Job
      ↓
DAG / dependencies
      ↓
Stages
      ↓
Tasks
      ↓
Executor cores
```

## Completed Check
Given:

```text
8 executors
4 cores each
Stage 1 = 320 partitions
```

Answers:

```text
A. ~320 tasks
B. ~32 concurrent tasks (8 × 4)
C. ~10 waves (320 / 32)
```

Therefore:

```text
ONE stage
320 tasks
~32 concurrent tasks
~10 waves
```

The 10 waves are **not** 10 stages.

## Production Troubleshooting
If Spark UI shows one stage taking much longer than another, inspect its tasks.

Examples:

```text
Most tasks = 2 minutes
One task   = 40 minutes
```

This can suggest data skew.

```text
20,000 tiny tasks
```

This can suggest partition/task overhead.

```text
80 available task slots
8 tasks
```

This can indicate insufficient parallelism.

Thinking in terms of jobs, stages, tasks, and partitions makes Spark troubleshooting systematic.

## Key Mental Model
```text
ACTION
  ↓
JOB
  ↓
DAG describes dependencies
  ↓
SHUFFLE / WIDE DEPENDENCY
commonly creates stage boundary
  ↓
STAGES
  ↓
each stage processes partitions
  ↓
~one TASK per partition
  ↓
EXECUTOR CORES run tasks
  ↓
tasks > available slots
  ↓
multiple WAVES
```

## Definitions
```text
Job
= execution work triggered by an action

Stage
= section of a job separated by dependency/shuffle boundaries

Task
= work on a partition within a stage

Wave
= scheduling consequence when tasks exceed available execution slots

DAG
= dependency graph describing the computation
```

## Interview Answer
> When a Spark action requires execution, Spark creates execution work as a job. Spark uses the DAG of dependencies to divide that job into stages, with shuffle or wide-dependency boundaries commonly creating new stages. Within each stage, partitions are processed by tasks, generally one task per partition. Executor cores determine how many tasks can run concurrently, so when a stage has more tasks than available execution slots, those tasks run in multiple waves. Multiple waves do not mean multiple stages.
