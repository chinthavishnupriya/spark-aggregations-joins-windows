# Spark Aggregations, Joins and Windows

A practical Apache Spark + Scala project covering the concepts from the supplied project specification: aggregations, grouping, customer analysis, window functions, joins, execution plans, shuffle/sort concepts, and a final combined exercise.

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
└── src/
    └── main/
        └── scala/
            └── AggregationsJoinsWindows.scala
```

## Covered Topics

### Example 1 - Sales Analysis

1. Simple aggregation: total, average, minimum, maximum, and order count.
2. Grouping aggregation by product.
3. Customer-level aggregation.
4. Window aggregation with a running total per customer.
5. Window ranking of each customer's orders by amount.

### Example 2 - Customer and Orders JOIN

1. Customer and order DataFrames.
2. Inner join using `customer_id`.
3. Selecting useful columns after the join.
4. Aggregation after the join to calculate customer spending and order count.
5. Extended execution-plan inspection using `explain(true)`.
6. The execution-plan output can be used to observe Spark's logical and physical operations, including shuffle-related `Exchange` stages and join/sort operations chosen by Spark.

### Final Exercise - JOIN + GROUPING + WINDOW

The final result combines customer and order information and displays:

- customer ID
- customer name
- city
- order ID
- order amount
- total spending for that customer
- order rank within that customer

The total spending is calculated with a customer-partitioned window so that order-level detail is retained. The rank uses `row_number()` ordered by amount descending.

## Input Data

The project creates the example DataFrames directly in Scala.

### Sales data

| order_id | customer | product | amount |
|---:|---|---|---:|
| 1 | Chintan | Laptop | 50000 |
| 2 | Rahul | Mobile | 20000 |
| 3 | Priya | Laptop | 60000 |
| 4 | Amit | Mobile | 15000 |
| 5 | Chintan | Mobile | 25000 |
| 6 | Rahul | Laptop | 55000 |
| 7 | Priya | Mobile | 30000 |
| 8 | Amit | Laptop | 45000 |

Simple aggregation result:

- Total sales: 300000
- Average sale: 37500
- Minimum sale: 15000
- Maximum sale: 60000
- Number of orders: 8

### Customer and order data

Customers:

| customer_id | customer_name | city |
|---:|---|---|
| 1 | Chintan | Hyderabad |
| 2 | Rahul | Mumbai |
| 3 | Priya | Bangalore |
| 4 | Amit | Delhi |

Orders:

| order_id | customer_id | amount |
|---:|---:|---:|
| 101 | 1 | 50000 |
| 102 | 2 | 20000 |
| 103 | 1 | 25000 |
| 104 | 3 | 30000 |
| 105 | 4 | 15000 |
| 106 | 2 | 40000 |

Customer totals:

- Chintan: 75000 from 2 orders
- Rahul: 60000 from 2 orders
- Priya: 30000 from 1 order
- Amit: 15000 from 1 order

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/chinthavishnupriya/spark-aggregations-joins-windows.git
cd spark-aggregations-joins-windows
```

### 2. Run with sbt

```bash
sbt run
```

The program runs locally with `local[*]` and prints the intermediate results and execution plans to the console.

## Important Spark Concepts

### Aggregation

Aggregation reduces multiple rows into summary information using functions such as `sum`, `avg`, `min`, `max`, and `count`.

### Grouping

`groupBy` creates groups based on one or more columns and then applies aggregation functions to each group.

### Window Function

A window function calculates a value across related rows without collapsing those rows into one row. This is useful for running totals and rankings.

### JOIN

A join combines rows from different DataFrames using a common key. This project uses `customer_id` to connect customers with orders.

### Shuffle and Sort

For operations such as joins, grouping, and certain window operations, Spark may redistribute data between partitions. The execution plan can expose this through physical-plan operations such as `Exchange` and sorting operations.

### Execution Plan

`explain(true)` prints the extended plan, allowing the logical and physical processing stages to be inspected rather than treating Spark SQL as a black box.

## Expected Final Output

The final exercise produces rows equivalent to:

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

## Purpose

This repository is a standalone implementation of the supplied Spark project specification and is intentionally kept separate from the other Spark projects in the GitHub account.
