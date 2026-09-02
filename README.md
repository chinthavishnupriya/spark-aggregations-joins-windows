# Spark Aggregations, Joins and Windows

A practical **Apache Spark + Scala** project implementing the concepts from the supplied project specification: aggregations, grouping, customer analysis, window functions, joins, execution plans, shuffle/sort concepts, and a final combined exercise.

## Project Status

**Completed and verified successfully in Ubuntu/WSL2.**

The project was compiled and executed with:

```text
Scala 2.12.18
Apache Spark 3.5.6
Java 17.0.20
sbt 2.0.7
Ubuntu / WSL2
```

Verification commands:

```bash
sbt compile
sbt run
```

Both commands completed successfully.

## Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.6
- Spark Core 3.5.6
- Spark SQL 3.5.6
- sbt
- Java 17+

## Project Structure

```text
spark-aggregations-joins-windows/
├── build.sbt
├── README.md
├── OUTPUT.md
└── src/
    └── main/
        └── scala/
            └── AggregationsJoinsWindows.scala
```

## Project Objectives

This project demonstrates how Spark DataFrames can be used for:

- Simple aggregations
- Grouping and grouped aggregations
- Customer-level analysis
- Running totals using window functions
- Ranking rows within each customer
- Joining customer and order DataFrames
- Aggregating data after a join
- Inspecting Spark execution plans
- Understanding shuffle and sort stages
- Combining JOIN, customer-level totals, and WINDOW ranking in one exercise

## Example 1 - Sales Analysis

### 1. Simple Aggregation

Calculates:

- Total sales
- Average sale
- Minimum sale
- Maximum sale
- Number of orders

Expected result:

```text
Total sales:       300000
Average sale:      37500
Minimum sale:      15000
Maximum sale:      60000
Number of orders:  8
```

### 2. Grouping Aggregation by Product

Groups sales by product and calculates total sales, average sales, and order count.

```text
Laptop -> Total: 210000, Average: 52500, Orders: 4
Mobile -> Total:  90000, Average: 22500, Orders: 4
```

### 3. Customer Aggregation

Groups sales by customer and calculates total spending and number of orders.

```text
Amit    -> 60000, 2 orders
Chintan -> 75000, 2 orders
Priya   -> 90000, 2 orders
Rahul   -> 75000, 2 orders
```

### 4. Window Aggregation

Uses a customer-partitioned window ordered by `order_id` to calculate a running total while retaining every order row.

### 5. Window Ranking

Uses `row_number()` with a customer-partitioned window ordered by amount descending to rank each customer's orders.

## Example 2 - Customer and Orders JOIN

The project creates separate customer and order DataFrames.

### Customer Data

| customer_id | customer_name | city |
|---:|---|---|
| 1 | Chintan | Hyderabad |
| 2 | Rahul | Mumbai |
| 3 | Priya | Bangalore |
| 4 | Amit | Delhi |

### Order Data

| order_id | customer_id | amount |
|---:|---:|---:|
| 101 | 1 | 50000 |
| 102 | 2 | 20000 |
| 103 | 1 | 25000 |
| 104 | 3 | 30000 |
| 105 | 4 | 15000 |
| 106 | 2 | 40000 |

### Inner JOIN

The DataFrames are joined using `customer_id`.

Expected customer totals after the join:

```text
Chintan -> 75000 from 2 orders
Rahul   -> 60000 from 2 orders
Priya   -> 30000 from 1 order
Amit    -> 15000 from 1 order
```

### Execution Plan

The code uses:

```scala
joinedData.explain(true)
```

This prints the extended logical and physical execution plan.

For the small example DataFrames, Spark selected a `BroadcastHashJoin` in the physical plan. This is an optimization chosen for the small input size. The final exercise also shows `Exchange` and `Sort` stages associated with data redistribution and window processing.

## Final Exercise - JOIN + GROUPING + WINDOW

The final exercise combines customer and order information and produces:

- Customer ID
- Customer name
- City
- Order ID
- Order amount
- Total spending for the customer
- Order rank within the customer

The customer total is calculated with a customer-partitioned window so that order-level detail is retained. The ranking uses `row_number()` ordered by amount descending.

Expected final result:

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

## Important Spark Concepts

### Aggregation

Aggregation reduces multiple rows into summary information using functions such as `sum`, `avg`, `min`, `max`, and `count`.

### Grouping

`groupBy` creates groups based on one or more columns and then applies aggregation functions to each group.

### Window Function

A window function calculates values across related rows without collapsing those rows into one row. This is useful for running totals and rankings.

### JOIN

A join combines rows from different DataFrames using a common key. This project uses `customer_id` to connect customers with orders.

### Shuffle

A shuffle redistributes data between partitions. It can occur for operations that require rows with the same key to be brought together, including grouping and certain window operations.

### Sort

Spark may sort data to satisfy ordering requirements for joins or window operations. The physical plan can expose these stages.

### Execution Plan

`explain(true)` prints the extended plan, allowing the logical and physical processing stages to be inspected.

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/chinthavishnupriya/spark-aggregations-joins-windows.git
cd spark-aggregations-joins-windows
```

### 2. Compile

```bash
sbt compile
```

Expected result:

```text
[success] elapsed time: ...
```

### 3. Run

```bash
sbt run
```

The application runs Spark locally with `local[*]` and prints all example results and execution plans to the console.

## Output Documentation

The complete verified console output is stored in:

**[`OUTPUT.md`](OUTPUT.md)**

It includes the results of `sbt compile`, `sbt run`, all major DataFrame outputs, and execution-plan verification.

## Source File

Main Scala implementation:

```text
src/main/scala/AggregationsJoinsWindows.scala
```

## Verification Result

```text
sbt compile -> SUCCESS
sbt run     -> SUCCESS
Spark       -> 3.5.6
Scala       -> 2.12.18
Java        -> 17.0.20
```

The project is ready as a standalone Spark/Scala implementation of the supplied specification.
