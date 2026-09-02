# Execution Plan Verification

The project was executed successfully in Ubuntu/WSL2 using `sbt run`.

## Verified environment

- Scala 2.12.18
- Apache Spark 3.5.6
- Java 17.0.20
- sbt 2.0.7

## What was observed

The final exercise's physical plan included Spark distribution and sorting stages such as:

```text
Exchange hashpartitioning(customer_id, 200)
Sort [customer_id]
Window [sum(amount) ...]
Sort [customer_id, amount DESC, order_id]
Window [row_number() ...]
```

The small customer/order join used `BroadcastHashJoin` in the observed physical plan. Spark selected this strategy because the example input is small. The project still demonstrates execution-plan inspection and the required shuffle/sort/window behavior.

## Result

The application completed successfully:

```text
[success] elapsed time: 13 s
```
