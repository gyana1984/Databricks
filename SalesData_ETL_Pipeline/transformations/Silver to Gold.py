from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Table 1: Daily Sales Summary
@dp.materialized_view(
    name="salesdata_daily_summary_gold",
    comment="Gold layer: Daily sales metrics aggregated by date, region, product line, and channel"
)
def salesdata_daily_summary_gold():
    return (
        spark.read.table("salesdata_silver")
        .groupBy("sale_date", "region", "product_line", "channel")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price_usd")).alias("total_revenue"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.count("sale_id").alias("total_transactions"),
            F.avg(F.col("quantity") * F.col("unit_price_usd")).alias("average_order_value"),
            F.countDistinct("customer_name").alias("unique_customers")
        )
    )

# Gold Table 2: Product Performance
@dp.materialized_view(
    name="salesdata_product_performance_gold",
    comment="Gold layer: Product-level performance metrics by product line, SKU, and region"
)
def salesdata_product_performance_gold():
    return (
        spark.read.table("salesdata_silver")
        .groupBy("product_line", "product_sku", "region")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price_usd")).alias("total_revenue_by_product"),
            F.sum("quantity").alias("total_units_sold"),
            F.count("sale_id").alias("number_of_transactions"),
            F.avg("unit_price_usd").alias("average_unit_price")
        )
    )

# Gold Table 3: Sales Rep Performance
@dp.materialized_view(
    name="salesdata_rep_performance_gold",
    comment="Gold layer: Sales representative performance metrics by rep, region, and channel"
)
def salesdata_rep_performance_gold():
    return (
        spark.read.table("salesdata_silver")
        .groupBy("sales_rep", "region", "channel")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price_usd")).alias("total_revenue"),
            F.count("sale_id").alias("total_sales_count"),
            F.countDistinct("customer_name").alias("unique_customers_served"),
            F.avg(F.col("quantity") * F.col("unit_price_usd")).alias("average_deal_size")
        )
    )

# Gold Table 4: Regional Summary
@dp.materialized_view(
    name="salesdata_regional_summary_gold",
    comment="Gold layer: High-level regional insights with year and month breakdowns"
)
def salesdata_regional_summary_gold():
    return (
        spark.read.table("salesdata_silver")
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .groupBy("region", "year", "month")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price_usd")).alias("total_revenue"),
            F.count("sale_id").alias("total_transactions"),
            F.avg(F.col("quantity") * F.col("unit_price_usd")).alias("average_transaction_value"),
            F.first("product_line").alias("top_product_line")
        )
    )
