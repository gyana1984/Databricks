from pyspark import pipelines as dp
from pyspark.sql import types as T

# Define sales data schema
sales_schema = T.StructType([
    T.StructField("sale_id", T.StringType(), True),
    T.StructField("sale_date", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("product_line", T.StringType(), True),
    T.StructField("product_sku", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price_usd", T.DoubleType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("sales_rep", T.StringType(), True),
    T.StructField("channel", T.StringType(), True),
    T.StructField("status", T.StringType(), True)
])

@dp.table(
    name="SalesData_Brone",
    comment="Bronze layer: Raw sales data ingested from JSON files using Auto Loader"
)
def salesdata_brone():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(sales_schema)
        .load("/Volumes/applied_sales_data/default/salesdatafiles")
    )
