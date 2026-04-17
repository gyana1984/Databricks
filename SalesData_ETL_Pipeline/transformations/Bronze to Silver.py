from pyspark import pipelines as dp
from pyspark.sql.functions import to_timestamp


@dp.table(
    comment="Silver layer: Cleaned sales data with deduplication and proper date types",
    schema="""
        sale_id STRING COMMENT 'Unique sales transaction identifier',
        sale_date TIMESTAMP COMMENT 'Date and time of the sale transaction',
        region STRING COMMENT 'Geographic region of the sale',
        customer_name STRING COMMENT 'Name of the customer',
        product_line STRING COMMENT 'Product line category',
        product_sku STRING COMMENT 'Product SKU identifier',
        quantity INT COMMENT 'Number of units sold',
        unit_price_usd DOUBLE COMMENT 'Unit price in USD',
        currency STRING COMMENT 'Currency code for the transaction',
        sales_rep STRING COMMENT 'Name of the sales representative',
        channel STRING COMMENT 'Sales channel (Direct, Partner, etc.)',
        status STRING COMMENT 'Transaction status (Closed-Won, Closed-Lost, etc.)'
    """
)
def salesdata_silver():
    return (
        spark.readStream.table("applied_sales_data.default.salesdata_brone")
        .dropDuplicates(["sale_id"])  # Remove duplicate sales by sale_id
        .withColumn("sale_date", to_timestamp("sale_date"))  # Convert STRING to TIMESTAMP
        .select(
            "sale_id", "sale_date", "region", "customer_name", 
            "product_line", "product_sku", "quantity", "unit_price_usd",
            "currency", "sales_rep", "channel", "status"
        )
    )
