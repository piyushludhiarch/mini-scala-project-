"""
Customer Revenue Analysis

This script answers a few business questions:
- Which cities generate the most completed revenue?
- Which customer segments are driving the most value?
- Who is the top customer by total completed revenue?
- Are there any orders that do not match a valid customer record?
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg as _avg,
    broadcast,
    col,
    round as _round,
    sum as _sum,
)
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def build_spark_session():
    """Create a Spark session for the data analysis."""
    return SparkSession.builder.appName("CustomerRevenueAnalysis").getOrCreate()


def load_customer_data(spark):
    """Load the customer file with a clear schema."""
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
    ])

    return spark.read.csv("customers.csv", header=True, schema=customers_schema)


def load_order_data(spark):
    """Load the orders file with a clear schema."""
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("amount", IntegerType(), True),
        StructField("status", StringType(), True),
    ])

    return spark.read.csv("orders.csv", header=True, schema=orders_schema)


def save_summary_csvs(summaries):
    """Write the output tables to the output folder."""
    output_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(output_dir, exist_ok=True)

    for file_name, dataframe in summaries.items():
        output_path = os.path.join(output_dir, f"{file_name}.csv")
        dataframe.toPandas().to_csv(output_path, index=False)
        print(f"Saved summary CSV: {output_path}")


def main():
    spark = build_spark_session()

    try:
        customers_df = load_customer_data(spark)
        orders_df = load_order_data(spark)

        print("=== Customers schema ===")
        customers_df.printSchema()
        print(f"Customer record count: {customers_df.count()}")

        print("=== Orders schema ===")
        orders_df.printSchema()
        print(f"Order record count: {orders_df.count()}")

        completed_orders_df = orders_df.filter(col("status") == "complete")
        print(f"Completed order count: {completed_orders_df.count()}")

        joined_df = completed_orders_df.join(customers_df, on="customer_id", how="inner")
        print("=== Sample joined rows ===")
        joined_df.show(5)

        revenue_by_city_df = (
            joined_df.groupBy("city")
            .agg(_sum("amount").alias("total_revenue"))
            .orderBy(col("total_revenue").desc())
        )
        print("=== Revenue by city ===")
        revenue_by_city_df.show()

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

        revenue_by_customer_df = (
            joined_df.groupBy("customer_id", "customer_name")
            .agg(_sum("amount").alias("total_revenue"))
            .orderBy(col("total_revenue").desc())
        )
        print("=== Top customer by completed revenue ===")
        revenue_by_customer_df.show(5)

        unmatched_customer_ids_df = completed_orders_df.join(
            customers_df, on="customer_id", how="left_anti"
        )
        print("=== Orders without a matching customer ===")
        unmatched_customer_ids_df.select("order_id", "customer_id").show()

        inner_join_count = completed_orders_df.join(
            customers_df, on="customer_id", how="inner"
        ).count()
        left_join_count = completed_orders_df.join(
            customers_df, on="customer_id", how="left"
        ).count()

        print(f"Inner join count: {inner_join_count}")
        print(f"Left join count: {left_join_count}")
        print(f"Rows lost with inner join: {left_join_count - inner_join_count}")

        print("=== Spark explain plan for revenue by city ===")
        revenue_by_city_df.explain()

        broadcast_join_df = completed_orders_df.join(
            broadcast(customers_df), on="customer_id", how="inner"
        )
        print("=== Spark explain plan for broadcast join ===")
        broadcast_join_df.explain()

        save_summary_csvs(
            {
                "revenue_by_city": revenue_by_city_df,
                "revenue_by_segment": revenue_by_segment_df,
                "revenue_by_customer": revenue_by_customer_df,
            }
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
