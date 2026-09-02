import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AggregationsJoinsWindows {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Aggregations Joins Windows")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      example1SalesAnalysis(spark)
      example2JoinAndExecutionPlan(spark)
      finalExercise(spark)
    } finally {
      spark.stop()
    }
  }

  // --------------------------------------------------------------------------
  // Example 1: Sales Analysis
  // --------------------------------------------------------------------------
  def example1SalesAnalysis(spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n================ EXAMPLE 1: SALES ANALYSIS ================")

    val salesData = Seq(
      (1, "Chintan", "Laptop", 50000),
      (2, "Rahul", "Mobile", 20000),
      (3, "Priya", "Laptop", 60000),
      (4, "Amit", "Mobile", 15000),
      (5, "Chintan", "Mobile", 25000),
      (6, "Rahul", "Laptop", 55000),
      (7, "Priya", "Mobile", 30000),
      (8, "Amit", "Laptop", 45000)
    ).toDF("order_id", "customer", "product", "amount")

    println("\nSales Data:")
    salesData.show(false)

    // 1. Simple aggregation
    println("\n1. Simple Aggregation:")
    salesData.agg(
      sum("amount").alias("total_sales"),
      avg("amount").alias("average_sales"),
      min("amount").alias("minimum_sale"),
      max("amount").alias("maximum_sale"),
      count("order_id").alias("number_of_orders")
    ).show(false)

    // 2. Grouping aggregation by product
    println("\n2. Grouping Aggregation by Product:")
    salesData.groupBy("product")
      .agg(
        sum("amount").alias("total_sales"),
        avg("amount").alias("average_sales"),
        count("order_id").alias("number_of_orders")
      )
      .orderBy("product")
      .show(false)

    // 3. Customer aggregation
    println("\n3. Customer Aggregation:")
    salesData.groupBy("customer")
      .agg(
        sum("amount").alias("total_spending"),
        count("order_id").alias("number_of_orders")
      )
      .orderBy("customer")
      .show(false)

    // 4. Window aggregation / running total
    val runningWindow = Window
      .partitionBy("customer")
      .orderBy("order_id")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    println("\n4. Window Aggregation - Running Total per Customer:")
    salesData
      .withColumn("running_total", sum("amount").over(runningWindow))
      .orderBy("customer", "order_id")
      .show(false)

    // 5. Window ranking
    val rankingWindow = Window
      .partitionBy("customer")
      .orderBy(desc("amount"))

    println("\n5. Window Ranking - Orders Ranked by Amount per Customer:")
    salesData
      .withColumn("order_rank", row_number().over(rankingWindow))
      .orderBy("customer", "order_rank")
      .show(false)
  }

  // --------------------------------------------------------------------------
  // Example 2: Customer / Orders JOIN and execution plan
  // --------------------------------------------------------------------------
  def example2JoinAndExecutionPlan(spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n================ EXAMPLE 2: JOIN + EXECUTION PLAN ================")

    val customers = Seq(
      (1, "Chintan", "Hyderabad"),
      (2, "Rahul", "Mumbai"),
      (3, "Priya", "Bangalore"),
      (4, "Amit", "Delhi")
    ).toDF("customer_id", "customer_name", "city")

    val orders = Seq(
      (101, 1, 50000),
      (102, 2, 20000),
      (103, 1, 25000),
      (104, 3, 30000),
      (105, 4, 15000),
      (106, 2, 40000)
    ).toDF("order_id", "customer_id", "amount")

    println("\nCustomers:")
    customers.show(false)

    println("\nOrders:")
    orders.show(false)

    // 1. Inner join
    val joinedData = customers.join(orders, Seq("customer_id"), "inner")

    println("\n1. Inner Join:")
    joinedData
      .select("customer_id", "customer_name", "city", "order_id", "amount")
      .orderBy("customer_id", "order_id")
      .show(false)

    // 2. Aggregation after join
    println("\n2. Aggregation after Join - Customer Totals:")
    joinedData
      .groupBy("customer_id", "customer_name")
      .agg(
        sum("amount").alias("total_spending"),
        count("order_id").alias("number_of_orders")
      )
      .orderBy("customer_id")
      .show(false)

    // 3. Execution plan: demonstrates logical/physical plan stages,
    // including Exchange and join/sort operations selected by Spark.
    println("\n3. Extended Execution Plan:")
    joinedData.explain(true)
  }

  // --------------------------------------------------------------------------
  // Final Exercise: JOIN + GROUPING + WINDOW
  // --------------------------------------------------------------------------
  def finalExercise(spark: SparkSession): Unit = {
    import spark.implicits._

    println("\n================ FINAL EXERCISE ================")
    println("JOIN + GROUPING + WINDOW")

    val customers = Seq(
      (1, "Chintan", "Hyderabad"),
      (2, "Rahul", "Mumbai"),
      (3, "Priya", "Bangalore"),
      (4, "Amit", "Delhi")
    ).toDF("customer_id", "customer_name", "city")

    val orders = Seq(
      (101, 1, 50000),
      (102, 2, 20000),
      (103, 1, 25000),
      (104, 3, 30000),
      (105, 4, 15000),
      (106, 2, 40000)
    ).toDF("order_id", "customer_id", "amount")

    val customerOrders = customers
      .join(orders, Seq("customer_id"), "inner")

    // GROUPING concept implemented as a customer-level window aggregation so
    // each order row retains its order-level detail while showing total spend.
    val totalSpendingWindow = Window.partitionBy("customer_id")

    val orderRankingWindow = Window
      .partitionBy("customer_id")
      .orderBy(desc("amount"), asc("order_id"))

    val finalResult = customerOrders
      .withColumn("total_spending", sum("amount").over(totalSpendingWindow))
      .withColumn("order_rank", row_number().over(orderRankingWindow))
      .select(
        "customer_id",
        "customer_name",
        "city",
        "order_id",
        "amount",
        "total_spending",
        "order_rank"
      )
      .orderBy("customer_id", "order_rank")

    println("\nFinal Result:")
    finalResult.show(false)

    println("\nFinal Result Execution Plan:")
    finalResult.explain(true)

    println("\nExpected customer totals:")
    println("Chintan = 75000, Rahul = 60000, Priya = 30000, Amit = 15000")
  }
}
