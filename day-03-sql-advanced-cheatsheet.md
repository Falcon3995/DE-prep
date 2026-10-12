# SQL Day 3 — Advanced / Data Engineering Cheat Sheet

## 1. Scalar Subquery

```sql
SELECT employee_name,
       salary
FROM employees
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
);
```

The inner query returns one value.

## 2. Correlated Subquery

```sql
SELECT e.employee_name,
       e.department,
       e.salary
FROM employees e
WHERE e.salary > (
    SELECT AVG(e2.salary)
    FROM employees e2
    WHERE e2.department = e.department
);
```

The inner query depends on the current outer row.

## 3. CTE vs Subquery

Subquery:

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM customers
) x
WHERE rn = 1;
```

CTE:

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

## 4. UNION vs UNION ALL

`UNION` removes duplicates:

```sql
SELECT customer_id FROM current_customers
UNION
SELECT customer_id FROM old_customers;
```

`UNION ALL` keeps duplicates:

```sql
SELECT customer_id FROM current_customers
UNION ALL
SELECT customer_id FROM old_customers;
```

## 5. Date Difference

SQL Server / Azure SQL:

```sql
DATEDIFF(DAY, start_date, end_date)
```

Example:

```sql
SELECT order_id,
       DATEDIFF(DAY, order_date, delivery_date) AS delivery_days
FROM orders
WHERE DATEDIFF(DAY, order_date, delivery_date) > 5;
```

## 6. Top N Per Group

```sql
WITH ranked AS (
    SELECT order_id,
           customer_id,
           amount,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY amount DESC
           ) AS rn
    FROM orders
)
SELECT *
FROM ranked
WHERE rn <= 2;
```

## 7. Latest Record Deduplication

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM customer_updates
)
SELECT *
FROM ranked
WHERE rn = 1;
```

For deterministic ordering when timestamps can tie:

```sql
ORDER BY updated_at DESC, update_id DESC
```

## 8. Nth Highest Distinct Value

```sql
WITH ranked AS (
    SELECT employee_name,
           salary,
           DENSE_RANK() OVER (
               ORDER BY salary DESC
           ) AS salary_rank
    FROM employees
)
SELECT *
FROM ranked
WHERE salary_rank = 3;
```

Third-highest DISTINCT value → `DENSE_RANK`. Exactly third row → `ROW_NUMBER`. Ranking with gaps → `RANK`.

## 9. Change Detection with LAG

```sql
WITH history AS (
    SELECT account_id,
           effective_date,
           status,
           LAG(status) OVER (
               PARTITION BY account_id
               ORDER BY effective_date
           ) AS previous_status
    FROM account_history
)
SELECT *
FROM history
WHERE status <> previous_status;
```

Useful for CDC, status changes, price changes, and balance changes.

## 10. NULL Comparisons

Correct:

```sql
WHERE email IS NULL
```

```sql
WHERE email IS NOT NULL
```

Wrong:

```sql
WHERE email = NULL
```

## 11. EXISTS vs IN

```sql
SELECT *
FROM customers
WHERE customer_id IN (
    SELECT customer_id
    FROM orders
);
```

```sql
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
);
```

`IN` asks whether a value is in a set. `EXISTS` asks whether at least one matching row exists.

## 12. NOT EXISTS vs NOT IN

Prefer:

```sql
SELECT *
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
);
```

`NOT IN` can behave unexpectedly if the subquery contains NULL.

## Advanced Mixed Pattern

Every customer + latest order, including customers with no orders:

```sql
WITH ranked AS (
    SELECT c.customer_id,
           c.customer_name,
           o.order_id,
           o.order_date,
           o.amount,
           ROW_NUMBER() OVER (
               PARTITION BY c.customer_id
               ORDER BY o.order_date DESC, o.order_id DESC
           ) AS rn
    FROM customers c
    LEFT JOIN orders o
        ON c.customer_id = o.customer_id
)
SELECT customer_id,
       customer_name,
       order_id,
       order_date,
       amount
FROM ranked
WHERE rn = 1;
```

## Interview Decision Table

| Requirement | Pattern |
|---|---|
| Above overall average | Scalar subquery |
| Above own group's average | Correlated subquery |
| Combine + remove duplicates | UNION |
| Combine + keep everything | UNION ALL |
| Top N rows/group | ROW_NUMBER |
| Nth distinct value | DENSE_RANK |
| Previous value | LAG |
| Detect change | LAG + comparison |
| Missing value | IS NULL |
| Matching row exists | EXISTS |
| Matching row doesn't exist | NOT EXISTS |
| Latest row/key | ROW_NUMBER + DESC |
| Keep unmatched left rows | LEFT JOIN |

## Data Engineer Patterns to Memorize

1. Latest record per business key
2. Top N per group
3. Change detection with LAG
4. Anti-join with NOT EXISTS
5. LEFT JOIN while preserving unmatched records
6. NULL-safe thinking
7. Deterministic window ordering
