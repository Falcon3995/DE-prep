# PySpark Day 4: Core Cheatsheet

Ten concepts to revise before a data engineering interview. The examples assume an existing `SparkSession` named `spark` and DataFrames with the columns shown.

```python
from pyspark.sql.functions import avg, col, count, row_number, sum, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window
```

## 1. Read data and inspect a DataFrame

```python
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/data/file.csv")
df.show()         # Display rows
df.printSchema()  # Display column names and types
```

For predictable production types, supply a schema:

```python
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
])
df = spark.read.schema(schema).csv("/data/file.csv", header=True)
```

## 2. Select and alias columns

SQL: `SELECT employee_id, employee_name AS name FROM employees`

```python
df.select(col("employee_id"), col("employee_name").alias("name"))
```

Use `select()` for the columns you want to keep and `alias()` to rename a result column.

## 3. Filter rows

`filter()` and `where()` are equivalent. Use `&` for AND, `|` for OR, and `~` for NOT; wrap each condition in parentheses.

```python
df.filter(col("salary") > 80000)
df.filter((col("department") == "IT") & (col("salary") > 80000))
df.filter((col("department") == "IT") | (col("department") == "Finance"))
```

## 4. Add or replace a column

`withColumn()` adds a column when its name is new, or replaces the existing column with that name.

```python
df.withColumn("monthly_salary", col("salary") / 12)
df.withColumn("salary", col("salary") / 12)
```

## 5. Express CASE WHEN logic

`when().otherwise()` is the PySpark equivalent of SQL `CASE WHEN`. Conditions are checked from top to bottom.

```python
df.withColumn(
    "salary_category",
    when(col("salary") >= 100000, "HIGH")
    .when(col("salary") >= 80000, "MEDIUM")
    .otherwise("LOW"),
)
```

## 6. Group and aggregate

```python
df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("employee_id").alias("employee_count"),
)
```

Keep columns needed by later aggregations. For example, `df.select("account_id").groupBy("account_id").agg(sum("amount"))` fails because `select()` already removed `amount`.

## 7. Join DataFrames

When both DataFrames use the same key name, pass it directly:

```python
customers_df.join(orders_df, "customer_id", "left")
```

For different key names, pass a join condition:

```python
customers_df.join(
    orders_df,
    customers_df.customer_id == orders_df.customer_id,
    "left",
)
```

| Join type | Result |
| --- | --- |
| `inner` | Matching rows from both sides |
| `left` | All left rows, with matching right rows where present |
| `left_semi` | Left rows that have a match; no right columns |
| `left_anti` | Left rows that have no match; no right columns |

Interview pattern — customers without orders:

```python
customers_df.join(orders_df, "customer_id", "left_anti")
```

## 8. Handle NULL values

```python
df.filter(col("city").isNull())
df.filter(col("city").isNotNull())
df.fillna({"city": "UNKNOWN", "salary": 0})
```

`isNull()` finds missing values, `isNotNull()` finds present values, and `fillna()` replaces missing values in specified columns.

## 9. Use window functions

Partition by a key, then order rows within each partition:

```python
window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
numbered_df = df.withColumn("rn", row_number().over(window_spec))
```

This maps to SQL `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC)`. Other useful window functions include `rank`, `dense_rank`, `lag`, and `lead`.

## 10. Deduplicate and keep the latest record

```python
df.dropDuplicates()                 # Remove exact duplicate rows
df.dropDuplicates(["customer_id"])  # Keep one arbitrary row per customer
```

To keep the latest row for each customer, order within each customer partition and keep row 1:

```python
window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
result_df = (
    df.withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)
```

`dropDuplicates(["customer_id"])` does not specify *which* row survives. If timestamps can tie, add a stable second sort key when the latest row must be deterministic.

## SQL to PySpark quick reference

| SQL | PySpark |
| --- | --- |
| `SELECT`, `AS` | `select()`, `alias()` |
| `WHERE` | `filter()` / `where()` |
| `CASE WHEN` | `when().otherwise()` |
| `GROUP BY`, `SUM`, `COUNT`, `AVG` | `groupBy()`, `sum()`, `count()`, `avg()` |
| `JOIN`, `LEFT JOIN` | `join()`, `join(..., "left")` |
| `IS NULL`, `IS NOT NULL` | `isNull()`, `isNotNull()` |
| Default for NULL | `fillna()` for replacement use cases |
| `ORDER BY ... DESC` | `orderBy(col(...).desc())` |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD` | `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()` with `over(window_spec)` |

**Day 4 reminder:** transformations return DataFrames. `show()` displays rows and returns `None`, so assign the transformed DataFrame first:

```python
result_df = df.filter(col("salary") > 80000)
result_df.show()
```
