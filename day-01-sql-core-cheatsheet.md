# SQL Day 1 — Core Cheat Sheet

## 1. GROUP BY + SUM

Use when the requirement says:
- total per customer
- total per department
- sales per account

```sql
SELECT customer_id,
       SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id;
```

Mental rule:

WHAT calculation? → `SUM(amount)`  
PER WHAT? → `GROUP BY customer_id`

## 2. HAVING

`WHERE` filters individual rows. `HAVING` filters groups after aggregation.

```sql
SELECT customer_id,
       SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 1000;
```

Remember: `WHERE` → rows; `HAVING` → aggregated groups.

## 3. COUNT

```sql
SELECT department,
       COUNT(*) AS employee_count
FROM employees
GROUP BY department;
```

`COUNT(*)` counts rows. `COUNT(column)` counts non-NULL values.

## 4. ROW_NUMBER

Creates a unique sequence.

```sql
SELECT employee_id,
       department,
       salary,
       ROW_NUMBER() OVER (
           PARTITION BY department
           ORDER BY salary DESC
       ) AS rn
FROM employees;
```

Use for latest record, top 1 per group, deduplication, and exactly N rows per group.

## 5. RANK

Ties receive the same rank and gaps appear afterward.

```sql
RANK() OVER (ORDER BY salary DESC)
```

Example: 100000 → 1, 100000 → 1, 90000 → 3.

## 6. DENSE_RANK

Ties receive the same rank but there are no gaps.

```sql
DENSE_RANK() OVER (ORDER BY salary DESC)
```

Example: 100000 → 1, 100000 → 1, 90000 → 2.

For second-highest distinct salary:

```sql
WITH ranked AS (
    SELECT employee_name,
           department,
           salary,
           DENSE_RANK() OVER (
               PARTITION BY department
               ORDER BY salary DESC
           ) AS salary_rank
    FROM employees
)
SELECT *
FROM ranked
WHERE salary_rank = 2;
```

## 7. LAG

Gets a value from the previous row.

```sql
SELECT account_id,
       transaction_date,
       amount,
       LAG(amount) OVER (
           PARTITION BY account_id
           ORDER BY transaction_date
       ) AS previous_amount
FROM transactions;
```

Use for previous status, balance, transaction, and change detection.

## 8. CTE

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM customers
)
SELECT *
FROM ranked
WHERE rn = 1;
```

## 9. Latest Record Per Key

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM customer_history
)
SELECT *
FROM ranked
WHERE rn = 1;
```

Mental pattern: business key → timestamp DESC → ROW_NUMBER → `rn = 1`.

## 10. Window Function Decision

| Requirement | Function |
|---|---|
| Latest record | ROW_NUMBER |
| Exactly top 3 rows | ROW_NUMBER |
| Ranking with gaps | RANK |
| Nth distinct value | DENSE_RANK |
| Previous row | LAG |

## Interview Traps

- Use `HAVING` for aggregate filtering.
- Don't forget `GROUP BY` for non-aggregated selected columns.
- `ROW_NUMBER` never gives ties the same number.
- `RANK` creates gaps.
- `DENSE_RANK` does not create gaps.
- Latest-record problems usually require `ORDER BY date DESC`.
