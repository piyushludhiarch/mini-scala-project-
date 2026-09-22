"""
Piyush - Customer Revenue Analysis
Business question: How much completed revenue comes from each city and
segment, and who is the top customer?
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count, round as _round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# ---------------------------------------------------------------------------
# 1. SparkSession + read both files with schemas
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("PiyushCustomerRevenueAnalysis").getOrCreate()

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("city", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("amount", IntegerType(), True),
    StructField("status", StringType(), True),
])

customers_df = spark.read.csv("customers.csv", header=True, schema=customers_schema)
orders_df = spark.read.csv("orders.csv", header=True, schema=orders_schema)

print("=== Customers schema ===")
customers_df.printSchema()
print(f"Customer record count: {customers_df.count()}")

print("=== Orders schema ===")
orders_df.printSchema()
print(f"Order record count: {orders_df.count()}")

# ---------------------------------------------------------------------------
# 2. Filter to completed orders only
# ---------------------------------------------------------------------------
completed_orders_df = orders_df.filter(col("status") == "complete")
print(f"Completed order count: {completed_orders_df.count()}")

# ---------------------------------------------------------------------------
# 3. Join customers + completed orders on customer_id (inner join)
# ---------------------------------------------------------------------------
joined_df = completed_orders_df.join(customers_df, on="customer_id", how="inner")
print("=== Joined sample rows ===")
joined_df.show(5)

# ---------------------------------------------------------------------------
# 4. Total revenue by city
# ---------------------------------------------------------------------------
revenue_by_city_df = (
    joined_df.groupBy("city")
    .agg(_sum("amount").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)
print("=== Revenue by city ===")
revenue_by_city_df.show()

# ---------------------------------------------------------------------------
# 5. Total revenue and average order value by segment
# ---------------------------------------------------------------------------
revenue_by_segment_df = (
    joined_df.groupBy("segment")
    .agg(
        _sum("amount").alias("total_revenue"),
        _round(_avg("amount"), 2).alias("avg_order_value"),
    )
    .orderBy(col("total_revenue").desc())
)
print("=== Revenue by segment ===")
revenue_by_segment_df.show()

# ---------------------------------------------------------------------------
# 6. Top customer by completed revenue
# ---------------------------------------------------------------------------
revenue_by_customer_df = (
    joined_df.groupBy("customer_id", "customer_name")
    .agg(_sum("amount").alias("total_revenue"))
    .orderBy(col("total_revenue").desc())
)
print("=== Top customer ===")
revenue_by_customer_df.show(1)

# ---------------------------------------------------------------------------
# 7. Unmatched customer IDs (orders with no matching customer, e.g. c99)
# ---------------------------------------------------------------------------
unmatched_df = completed_orders_df.join(customers_df, on="customer_id", how="left_anti")
print("=== Unmatched customer IDs (in orders, not in customers) ===")
unmatched_df.select("order_id", "customer_id").show()

# ---------------------------------------------------------------------------
# 8. Compare inner join vs left join counts
# ---------------------------------------------------------------------------
inner_join_df = completed_orders_df.join(customers_df, on="customer_id", how="inner")
left_join_df = completed_orders_df.join(customers_df, on="customer_id", how="left")

inner_count = inner_join_df.count()
left_count = left_join_df.count()

print(f"Inner join count: {inner_count}")
print(f"Left join count: {left_count}")
print(f"Difference (rows lost with inner join): {left_count - inner_count}")

# ---------------------------------------------------------------------------
# 9. explain() to find where shuffle occurs
# ---------------------------------------------------------------------------
print("=== explain() for revenue_by_city_df ===")
revenue_by_city_df.explain()
# The groupBy("city").agg(sum(...)) triggers a shuffle because Spark must
# redistribute rows so all records for the same city land on the same
# partition before the sum can be computed. Look for "Exchange hashpartitioning"
# in the physical plan below the HashAggregate steps - that Exchange step is
# the shuffle.

# ---------------------------------------------------------------------------
# 10. Optional stretch: broadcast join (customers is the smaller table)
# ---------------------------------------------------------------------------
from pyspark.sql.functions import broadcast

broadcast_join_df = completed_orders_df.join(broadcast(customers_df), on="customer_id", how="inner")
print("=== explain() for broadcast join ===")
broadcast_join_df.explain()
# With broadcast(customers_df), Spark sends the small customers DataFrame to
# every executor instead of shuffling both DataFrames across the cluster.
# The physical plan shows "BroadcastHashJoin" and "BroadcastExchange" instead
# of the "SortMergeJoin" + "Exchange hashpartitioning" pattern seen in a
# regular (non-broadcast) join. This avoids a shuffle on the larger orders
# table, which is faster when one side of the join is small.

# ---------------------------------------------------------------------------
# Save outputs (Windows-safe export)
# ---------------------------------------------------------------------------
output_dir = os.path.join(os.getcwd(), "output")
os.makedirs(output_dir, exist_ok=True)

for name, df in [
    ("revenue_by_city", revenue_by_city_df),
    ("revenue_by_segment", revenue_by_segment_df),
    ("revenue_by_customer", revenue_by_customer_df),
]:
    output_path = os.path.join(output_dir, f"{name}.csv")
    df.toPandas().to_csv(output_path, index=False)
    print(f"Saved summary CSV: {output_path}")

spark.stop()
