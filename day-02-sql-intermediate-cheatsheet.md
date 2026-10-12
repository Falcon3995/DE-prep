# SQL Day 2 — Intermediate Cheat Sheet

## 1. CASE WHEN

```sql
SELECT employee_name,
       salary,
       CASE
           WHEN salary >= 100000 THEN 'HIGH'
           WHEN salary >= 80000 THEN 'MEDIUM'
           ELSE 'LOW'
       END AS salary_category
FROM employees;
```

Conditions are evaluated top to bottom.

## 2. Conditional Aggregation

```sql
SELECT customer_id,
       SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders
FROM orders
GROUP BY customer_id;
```

Conditional amount:

```sql
SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END)
```

Trap:

```sql
COUNT(CASE WHEN condition THEN 1 ELSE 0 END)
```

This counts both 1 and 0 because both are non-NULL. Prefer:

```sql
COUNT(CASE WHEN condition THEN 1 END)
```

or:

```sql
SUM(CASE WHEN condition THEN 1 ELSE 0 END)
```

## 3. LEFT JOIN

```sql
SELECT c.customer_id,
       c.customer_name,
       o.order_id
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id;
```

LEFT JOIN keeps every left-side row.

## 4. ON vs WHERE with LEFT JOIN

To keep every customer but only match orders from 2026:

```sql
SELECT c.customer_id,
       COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
   AND o.order_date >= '2026-01-01'
GROUP BY c.customer_id;
```

Mental rule: `ON` → which rows match; `WHERE` → which result rows survive; `HAVING` → which groups survive.

## 5. COUNT(*) vs COUNT(column)

After a LEFT JOIN, `COUNT(*)` counts the NULL-extended left-side row. `COUNT(o.order_id)` ignores NULL order IDs.

## 6. Anti-Join

```sql
SELECT c.customer_id,
       c.customer_name
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;
```

## 7. NOT EXISTS

```sql
SELECT c.customer_id,
       c.customer_name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
);
```

## 8. Running Total

```sql
SELECT account_id,
       transaction_date,
       amount,
       SUM(amount) OVER (
           PARTITION BY account_id
           ORDER BY transaction_date
       ) AS running_total
FROM transactions;
```

## 9. LEAD

```sql
SELECT account_id,
       transaction_date,
       amount,
       LEAD(amount) OVER (
           PARTITION BY account_id
           ORDER BY transaction_date
       ) AS next_amount
FROM transactions;
```

`LAG` → previous row. `LEAD` → next row.

## 10. COALESCE

```sql
SELECT customer_id,
       COALESCE(city, 'UNKNOWN') AS city
FROM customers;
```

Useful after LEFT JOIN:

```sql
COALESCE(SUM(o.amount), 0)
```

## Important Combined Pattern

Every customer, total order amount, zero when none:

```sql
SELECT c.customer_id,
       c.customer_name,
       COALESCE(SUM(o.amount), 0) AS total_order_amount
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;
```

Keep customers with total >= 1000 OR no orders:

```sql
SELECT c.customer_id,
       c.customer_name,
       COALESCE(SUM(o.amount), 0) AS total_order_amount
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING SUM(o.amount) >= 1000
    OR COUNT(o.order_id) = 0;
```

## Interview Traps

- A right-table condition in `WHERE` can remove unmatched LEFT JOIN rows.
- `COUNT(*)` and `COUNT(column)` behave differently with NULLs.
- `COUNT(CASE ... ELSE 0 END)` counts zero too.
- Use `HAVING` for aggregate filtering.
- `NOT EXISTS` is generally safer than `NOT IN` when NULLs are possible.
