# Piyush - Customer Revenue Analysis

## Project overview

This project is a small Apache Spark data-analysis pipeline. It combines
customer master data with order transaction data to answer three business
questions:

1. How much completed revenue comes from each city?
2. How much completed revenue comes from each customer segment, and what is
   the average order value for each segment?
3. Which customer generated the most completed revenue?

The project also demonstrates important Spark and data-engineering concepts:

- Reading CSV files with explicit schemas
- Filtering cancelled orders before analysis
- Joining transaction data with reference data
- Grouping and aggregating data
- Finding unmatched keys with a left anti join
- Comparing inner and left joins
- Understanding Spark lazy evaluation and actions
- Identifying shuffles with `explain()`
- Improving a small-table join with a broadcast join
- Exporting Spark results as simple CSV reports

## Project files

```text
customers.csv                         Customer master data
orders.csv                            Order transaction data
piyush_customer_revenue_analysis.py  Spark analysis script
output/                               Generated report files
```

The main script creates a Spark session, reads both input files, cleans and
joins the data, calculates the reports, explains Spark execution plans, and
writes the results to the `output/` directory.

## How the files match the assignment

The assignment first provides a starter dataset containing 5 customers and 8
orders. It then asks for a few additional rows so the final files contain
approximately 8-10 customers and 12-15 orders, while keeping `c99` unmatched.

The current project files satisfy that requirement:

```text
customers.csv: 9 customers
orders.csv:    14 orders
```

The original starter records are still present:

- Customers `c01` through `c05`
- Orders 1 through 8
- The unmatched completed order for `c99`

The project adds customers `c06` through `c09` and orders 9 through 14. These
additional rows bring the dataset into the requested size range and provide
more realistic group-by results across cities, segments, and customers. The
added data also includes a second cancelled order, so the completed-order
filter can be demonstrated with 12 completed orders out of 14 total orders.

There is therefore no conflict between the shorter dataset shown in the
assignment description and the larger dataset used by this project. The
assignment describes the starting rows and explicitly requests additional
rows; the CSV files contain the completed version.

## Input data

### customers.csv

This file contains 9 customers and four columns:

| Column          | Type   | Description                                       |
| --------------- | ------ | ------------------------------------------------- |
| `customer_id`   | String | Unique customer identifier, such as `c01`         |
| `customer_name` | String | Customer's name                                   |
| `segment`       | String | Customer segment, such as `student` or `business` |
| `city`          | String | Customer city                                     |

This is reference or master data. It provides descriptive information that is
not present in the order transactions.

### orders.csv

This file contains 14 orders and five columns:

| Column        | Type    | Description                                     |
| ------------- | ------- | ----------------------------------------------- |
| `order_id`    | Integer | Unique order number                             |
| `customer_id` | String  | Customer who placed the order                   |
| `order_date`  | Date    | Date on which the order was placed              |
| `amount`      | Integer | Value of the order                              |
| `status`      | String  | Order status, such as `complete` or `cancelled` |

The input intentionally includes two cases that the pipeline must handle:

- Two cancelled orders, which must be filtered out and not counted as revenue.
- One completed order for `c99`, a customer ID that does not exist in
  `customers.csv`. This demonstrates how missing join keys are handled.

## End-to-end data flow

```text
customers.csv + orders.csv
           |
           v
    Read with explicit schemas
           |
           v
    Inspect schemas and row counts
           |
           v
    Keep only status == "complete"
           |
           v
    Inner join on customer_id
           |
           v
    Calculate city, segment, and customer summaries
           |
           v
    Find unmatched customer IDs
           |
           v
    Explain shuffle and broadcast plans
           |
           v
    Export three CSV reports
```

## Detailed processing steps

### 1. Create the Spark session

The script starts Spark with:

```python
spark = SparkSession.builder \\
   .appName("PiyushCustomerRevenueAnalysis") \\
   .getOrCreate()
```

`SparkSession` is the main entry point for Spark SQL and DataFrame operations.
It manages the Spark application and allows the script to read files, build
DataFrames, execute transformations, and run actions.

### 2. Define explicit schemas

The script defines a `StructType` schema for each CSV file before reading it.
This avoids unreliable type inference and guarantees that values are read in
the intended types.

The customer schema is:

```text
customer_id    StringType
customer_name  StringType
segment        StringType
city           StringType
```

The order schema is:

```text
order_id       IntegerType
customer_id    StringType
order_date     DateType
amount         IntegerType
status         StringType
```

Using `DateType` for `order_date` is better than leaving it as a string because
Spark can then use date operations correctly. Using `IntegerType` for
`amount` also makes numeric aggregation safe and explicit.

The files are read with `header=True`, so Spark uses the first row as the
column names:

```python
customers_df = spark.read.csv(
   "customers.csv",
   header=True,
   schema=customers_schema
)

orders_df = spark.read.csv(
   "orders.csv",
   header=True,
   schema=orders_schema
)
```

### 3. Inspect schemas and row counts

The script uses `printSchema()` and `count()` to validate the inputs:

```python
customers_df.printSchema()
customers_df.count()

orders_df.printSchema()
orders_df.count()
```

Expected input counts are:

```text
Customers: 9
Orders: 14
```

`printSchema()` is useful for checking data types. `count()` is a Spark action,
so it causes Spark to execute the required read and count operation.

### 4. Filter completed orders

Cancelled orders should not contribute to revenue. The script filters them out
before joining or aggregating:

```python
completed_orders_df = orders_df.filter(
   col("status") == "complete"
)
```

The result is:

```text
Total orders:       14
Completed orders:   12
Cancelled orders:    2
```

The cancelled orders have amounts of 500 and 900. They are excluded from all
revenue reports.

Filtering early is a good data-processing practice because later operations
work with fewer rows and cannot accidentally include cancelled revenue.

### 5. Join orders to customers

Completed orders are joined to customer data using `customer_id`:

```python
joined_df = completed_orders_df.join(
   customers_df,
   on="customer_id",
   how="inner"
)
```

The order table contains transaction information such as amount and date. The
customer table contains descriptive information such as name, segment, and
city. The join combines these two types of information into one DataFrame.

An inner join keeps only rows where `customer_id` exists in both DataFrames.
The completed order for `c99` therefore does not appear in `joined_df`. The
record is not removed from the source file; it is identified separately with
the left anti join and is excluded from the matched revenue reports because it
has no customer, city, or segment information.

Expected counts:

```text
Completed orders:           12
Matched completed orders:   11
Unmatched completed orders:  1
```

The matched completed revenue is 3,950. The unmatched `c99` order has revenue
of 600, but it cannot be assigned to a known customer, city, or segment.

### 6. Calculate revenue by city

The city report groups joined rows by city and sums the order amount:

```python
revenue_by_city_df = (
   joined_df
   .groupBy("city")
   .agg(_sum("amount").alias("total_revenue"))
   .orderBy(col("total_revenue").desc())
)
```

The output is sorted from highest to lowest revenue:

| City    | Total revenue |
| ------- | ------------: |
| Pune    |         1,420 |
| Mumbai  |           980 |
| Delhi   |           850 |
| Chennai |           700 |

Pune is the highest-revenue city.

### 7. Calculate revenue by segment

The segment report calculates both total revenue and average order value:

```python
revenue_by_segment_df = (
   joined_df
   .groupBy("segment")
   .agg(
      _sum("amount").alias("total_revenue"),
      _round(_avg("amount"), 2).alias("avg_order_value")
   )
   .orderBy(col("total_revenue").desc())
)
```

The output is:

| Segment      | Total revenue | Average order value |
| ------------ | ------------: | ------------------: |
| student      |         1,780 |              296.67 |
| business     |         1,170 |              390.00 |
| professional |         1,000 |              500.00 |

The student segment has the highest total revenue. The professional segment
has the highest average order value.

### 8. Find the top customer

The customer report groups by both customer ID and customer name:

```python
revenue_by_customer_df = (
   joined_df
   .groupBy("customer_id", "customer_name")
   .agg(_sum("amount").alias("total_revenue"))
   .orderBy(col("total_revenue").desc())
)
```

The first row after descending sort is the top customer:

| Customer ID | Name | Total revenue |
| ----------- | ---- | ------------: |
| c03         | Neha |           980 |

The complete customer ranking is written to
`output/revenue_by_customer.csv`. The script displays only the first row with
`show(1)` because the business question asks for the top customer.

### 9. Find unmatched customer IDs

The script uses a left anti join to find completed orders whose customer IDs
are missing from the customer table:

```python
unmatched_df = completed_orders_df.join(
   customers_df,
   on="customer_id",
   how="left_anti"
)
```

A left anti join returns rows from the left DataFrame that have no matching
row on the right. It returns:

```text
order_id  customer_id
8         c99
```

This is a useful data-quality check. In a production pipeline, such rows might
be sent to an error report, corrected, or held for investigation.

### 10. Compare inner and left joins

The script compares the row counts produced by inner and left joins:

```python
inner_join_df = completed_orders_df.join(
   customers_df,
   on="customer_id",
   how="inner"
)

left_join_df = completed_orders_df.join(
   customers_df,
   on="customer_id",
   how="left"
)
```

Expected results:

```text
Inner join count:                 11
Left join count:                  12
Rows lost with inner join:         1
```

The inner join removes `c99`. The left join keeps the order and fills the
customer-side columns with null values. This demonstrates that join type is a
business decision, not just a technical detail.

### 11. Understand Spark lazy evaluation

Spark DataFrame operations are divided into transformations and actions.

Transformations describe a computation:

- `filter`
- `join`
- `groupBy`
- `agg`
- `orderBy`

Actions cause Spark to execute a plan:

- `count`
- `show`
- `toPandas`
- `explain`

For example, this creates a logical plan but does not immediately calculate
the final data:

```python
completed_orders_df = orders_df.filter(
   col("status") == "complete"
)
```

The following action causes Spark to execute the plan:

```python
completed_orders_df.count()
```

This lazy approach lets Spark optimize the complete computation before running
it.

### 12. Understand shuffle during aggregation

The script calls:

```python
revenue_by_city_df.explain()
```

The `groupBy("city")` operation normally requires a shuffle. Initially, rows
for the same city may be located in different Spark partitions. Spark must
redistribute them so all rows for one city are available together before
calculating the sum.

The physical plan usually contains entries such as:

```text
Exchange hashpartitioning
HashAggregate
```

Conceptually, the work is:

```text
Read data
  -> filter completed orders
  -> join customer data
  -> shuffle rows by city
  -> aggregate revenue
  -> sort results
```

Shuffles can be expensive for large datasets because they require network data
movement. Operations that commonly cause shuffles include `groupBy`,
`orderBy`, `distinct`, and many joins.

### 13. Understand broadcast joins

The customers table has only 9 rows, so the script also demonstrates a
broadcast join:

```python
from pyspark.sql.functions import broadcast

broadcast_join_df = completed_orders_df.join(
   broadcast(customers_df),
   on="customer_id",
   how="inner"
)
```

Broadcasting sends the small customers DataFrame to each executor. This can
avoid shuffling both sides of the join.

The physical plan may contain:

```text
BroadcastHashJoin
BroadcastExchange
```

For comparison, a normal large-table join may use a shuffle-based
`SortMergeJoin`.

Broadcast joins are useful when one side is small enough to fit in executor
memory. They should not be used blindly for large tables because broadcasting
an oversized table can cause memory problems.

## Generated output files

The script creates the `output/` directory if it does not already exist and
writes three reports:

### revenue_by_city.csv

Contains one row per city and the sum of completed revenue for that city.

### revenue_by_segment.csv

Contains one row per segment with:

- `total_revenue`
- `avg_order_value`

### revenue_by_customer.csv

Contains one row per matched customer with their total completed revenue,
sorted from highest to lowest.

The reports are exported with:

```python
df.toPandas().to_csv(output_path, index=False)
```

This creates ordinary single-file CSV reports that are convenient to open on
Windows. It is suitable for this small dataset. For large data, converting a
large Spark DataFrame to Pandas can exhaust driver memory; a production
pipeline should use Spark's distributed `DataFrame.write.csv()` instead.

## Final verified results

```text
Customers:                   9
Orders:                     14
Completed orders:            12
Cancelled orders:             2
Matched completed orders:    11
Unmatched completed orders:   1
Matched completed revenue: 3950
Unmatched c99 revenue:       600
```

Business conclusions:

- Pune generated the most revenue: 1,420.
- The student segment generated the most total revenue: 1,780.
- The professional segment had the highest average order value: 500.00.
- Neha (`c03`) was the top customer: 980.
- Order 8 belongs to missing customer ID `c99` and is excluded from the
  matched customer, city, and segment reports.
- The two cancelled orders are excluded from every revenue report.

## How to run

Install PySpark:

```powershell
pip install pyspark
```

Run the script from the project directory:

```powershell
python .\piyush_customer_revenue_analysis.py
```

The script expects `customers.csv` and `orders.csv` to be in the current
working directory. It writes the reports under `output/`.

## Java compatibility note

Spark requires a compatible Java runtime. During validation of this project,
the local machine had:

```text
Python: 3.12.10
PySpark: 4.2.0
Java: 25.0.4
```

Spark failed during startup with a missing internal Java class:

```text
ClassNotFoundException: jdk.internal.ref.Cleaner
```

This happens before the project reads any CSV data, so it is an environment
compatibility issue rather than a problem with the transformations or input
files. If Spark shows this error, install and select a supported JDK, commonly
Java 17 or Java 21, then configure PowerShell before running the script:

```powershell
$env:JAVA_HOME = "C:\Path\To\jdk-21"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

python .\piyush_customer_revenue_analysis.py
```

## Project assessment

This mini-project is designed as both a business report and a Spark learning
exercise. The business result comes from filtering cancelled orders, matching
valid customers, and aggregating completed order amounts. The Spark learning
result comes from observing schemas, actions, joins, shuffles, physical plans,
and broadcast optimization in one small, understandable example.
