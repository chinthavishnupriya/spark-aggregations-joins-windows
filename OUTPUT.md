# Verified Output

This file contains the output produced by running the project in Ubuntu/WSL2 with `sbt run`.

## Environment

- Scala: 2.12.18
- Spark: 3.5.6
- Java: 17.0.20
- OS: Ubuntu on WSL2
- Command: `sbt run`

## Compilation

```text
[success] elapsed time: 8 s
```

## Example 1 — Sales Analysis

### Simple Aggregation

```text
+-----------+-------------+------------+------------+----------------+
|total_sales|average_sales|minimum_sale|maximum_sale|number_of_orders|
+-----------+-------------+------------+------------+----------------+
|300000     |37500.0      |15000       |60000       |8               |
+-----------+-------------+------------+------------+----------------+
```

### Grouping Aggregation by Product

```text
+-------+-----------+-------------+----------------+
|product|total_sales|average_sales|number_of_orders|
+-------+-----------+-------------+----------------+
|Laptop |210000     |52500.0      |4               |
|Mobile |90000      |22500.0      |4               |
+-------+-----------+-------------+----------------+
```

### Customer Aggregation

```text
+--------+--------------+----------------+
|customer|total_spending|number_of_orders|
+--------+--------------+----------------+
|Amit    |60000         |2               |
|Chintan |75000         |2               |
|Priya   |90000         |2               |
|Rahul   |75000         |2               |
+--------+--------------+----------------+
```

### Window Aggregation — Running Total

```text
+--------+--------+-------+------+-------------+
|order_id|customer|product|amount|running_total|
+--------+--------+-------+------+-------------+
|4       |Amit    |Mobile |15000 |15000        |
|8       |Amit    |Laptop |45000 |60000        |
|1       |Chintan |Laptop |50000 |50000        |
|5       |Chintan |Mobile |25000 |75000        |
|3       |Priya   |Laptop |60000 |60000        |
|7       |Priya   |Mobile |30000 |90000        |
|2       |Rahul   |Mobile |20000 |20000        |
|6       |Rahul   |Laptop |55000 |75000        |
+--------+--------+-------+------+-------------+
```

### Window Ranking

```text
+--------+--------+-------+------+----------+
|order_id|customer|product|amount|order_rank|
+--------+--------+-------+------+----------+
|8       |Amit    |Laptop |45000 |1         |
|4       |Amit    |Mobile |15000 |2         |
|1       |Chintan |Laptop |50000 |1         |
|5       |Chintan |Mobile |25000 |2         |
|3       |Priya   |Laptop |60000 |1         |
|7       |Priya   |Mobile |30000 |2         |
|6       |Rahul   |Laptop |55000 |1         |
|2       |Rahul   |Mobile |20000 |2         |
+--------+--------+-------+------+----------+
```

## Example 2 — Customer / Orders JOIN

### Inner Join

```text
+-----------+-------------+---------+--------+------+
|customer_id|customer_name|city     |order_id|amount|
+-----------+-------------+---------+--------+------+
|1          |Chintan      |Hyderabad|101     |50000 |
|1          |Chintan      |Hyderabad|103     |25000 |
|2          |Rahul        |Mumbai   |102     |20000 |
|2          |Rahul        |Mumbai   |106     |40000 |
|3          |Priya        |Bangalore|104     |30000 |
|4          |Amit         |Delhi    |105     |15000 |
+-----------+-------------+---------+--------+------+
```

### Aggregation After JOIN

```text
+-----------+-------------+--------------+----------------+
|customer_id|customer_name|total_spending|number_of_orders|
+-----------+-------------+--------------+----------------+
|1          |Chintan      |75000         |2               |
|2          |Rahul        |60000         |2               |
|3          |Priya        |30000         |1               |
|4          |Amit         |15000         |1               |
+-----------+-------------+--------------+----------------+
```

## Final Exercise — JOIN + GROUPING + WINDOW

```text
+-----------+-------------+---------+--------+------+--------------+----------+
|customer_id|customer_name|city     |order_id|amount|total_spending|order_rank|
+-----------+-------------+---------+--------+------+--------------+----------+
|1          |Chintan      |Hyderabad|101     |50000 |75000         |1         |
|1          |Chintan      |Hyderabad|103     |25000 |75000         |2         |
|2          |Rahul        |Mumbai   |106     |40000 |60000         |1         |
|2          |Rahul        |Mumbai   |102     |20000 |60000         |2         |
|3          |Priya        |Bangalore|104     |30000 |30000         |1         |
|4          |Amit         |Delhi    |105     |15000 |15000         |1         |
+-----------+-------------+---------+--------+------+--------------+----------+
```

## Execution Plan Verification

The final execution plan showed Spark performing the required distribution and sorting stages, including:

```text
Exchange hashpartitioning(customer_id, 200)
Sort [customer_id]
Window [sum(amount) ...]
Sort [customer_id, amount DESC, order_id]
Window [row_number() ...]
Exchange rangepartitioning(...)
Sort
```

For the small Example 2 DataFrames, Spark selected `BroadcastHashJoin` rather than `SortMergeJoin`. This is a valid Spark physical-plan choice for the small in-memory input. The final exercise still demonstrates `Exchange` and `Sort` stages in its physical plan.

## Run Result

```text
[success] elapsed time: 13 s
```

The project was successfully compiled and executed in Ubuntu/WSL2.