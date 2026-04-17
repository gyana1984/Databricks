Applied SalesData ETL Pipeline
Databricks Python PySpark

A production-ready Databricks Lakeflow Spark Declarative Pipeline implementing the medallion architecture (Bronze → Silver → Gold) for real-time sales data analytics and business intelligence.

🏗️ Architecture Overview
This pipeline provides end-to-end ETL capabilities for sales data processing:

Pipeline Type: Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables)
Catalog: applied_sales_data
Schema: default
Compute: Serverless with Photon acceleration
Ingestion: Auto Loader for incremental JSON processing
Processing Pattern: Streaming tables (Bronze/Silver) + Materialized views (Gold)
📊 Data Flow Architecture
┌─────────────────────────────────────────────────────────────┐
│                    Source JSON Files                        │
│         /Volumes/applied_sales_data/default/                │
│                    salesdatafiles/                          │
└───────────────────────┬─────────────────────────────────────┘
                        │ Auto Loader (cloudFiles)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER                              │
│              salesdata_brone (Streaming Table)              │
│                                                             │
│  • Raw JSON ingestion with schema enforcement               │
│  • Incremental processing of new files                      │
│  • 20 rows ingested                                         │
└───────────────────────┬─────────────────────────────────────┘
                        │ Deduplication + Type Conversion
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER                              │
│             salesdata_silver (Streaming Table)              │
│                                                             │
│  • Deduplication by sale_id                                 │
│  • Type conversion (STRING → TIMESTAMP)                     │
│  • Data quality checks                                      │
│  • 20 unique transactions                                   │
└───────────────────────┬─────────────────────────────────────┘
                        │ Business Aggregations
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER                               │
│              (4 Materialized Views)                         │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  1. salesdata_daily_summary_gold (28 rows)         │   │
│  │     Daily metrics by date/region/product/channel   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  2. salesdata_product_performance_gold (10 rows)   │   │
│  │     Product performance by SKU/region              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  3. salesdata_rep_performance_gold (6 rows)        │   │
│  │     Sales rep performance metrics                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  4. salesdata_regional_summary_gold (8 rows)       │   │
│  │     Regional insights by year/month                │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
📁 Project Structure
Applied-SalesData-ETL-Pipeline/
├── README.md                           # This file
├── transformations/                    # Pipeline transformation code
│   ├── JSON to Bronze.py              # Auto Loader ingestion layer
│   ├── Bronze to Silver.py            # Data cleansing & deduplication
│   └── Silver to Gold.py              # Business analytics aggregations
└── Applied_SalesData_ETL_Pipeline.tar.gz  # Complete archive
🚀 Quick Start
Prerequisites
Before deploying this pipeline, ensure you have:

Databricks Workspace with Unity Catalog enabled
Catalog: applied_sales_data (create if it doesn't exist)
Volume: /Volumes/applied_sales_data/default/salesdatafiles/ containing source JSON files
Permissions:
USE CATALOG on applied_sales_data
USE SCHEMA on applied_sales_data.default
CREATE TABLE on applied_sales_data.default
READ FILES on the source volume
Deployment Steps
Option 1: Using Databricks UI
Clone this repository to your Databricks workspace:

git clone https://github.com/yourusername/applied-salesdata-etl-pipeline.git
Upload to Workspace:

Navigate to your Databricks workspace
Upload the transformations/ folder
Create Pipeline:

Go to Workflows → Lakeflow Pipelines
Click Create Pipeline
Configure settings:
Name: Applied SalesData ETL
Catalog: applied_sales_data
Target Schema: default
Source Code: Browse to transformations/**
Compute: Enable Serverless and Photon
Channel: Current
Start Pipeline:

Click Start to begin processing
Monitor progress in the pipeline monitoring view
Option 2: Using Databricks CLI
# Create pipeline using CLI
databricks pipelines create \
  --name "Applied SalesData ETL" \
  --catalog applied_sales_data \
  --target default \
  --libraries '{"glob":{"include":"/Workspace/path/to/transformations/**"}}' \
  --photon true \
  --serverless true \
  --channel CURRENT

# Start the pipeline
databricks pipelines start --pipeline-id <pipeline_id>
📋 Data Schema
Source Schema (JSON)
{
  "sale_id": "string",           // Unique sales transaction identifier
  "sale_date": "string",          // Sale date in ISO format
  "region": "string",             // Geographic region (North, South, East, West)
  "customer_name": "string",      // Customer name
  "product_line": "string",       // Product line category
  "product_sku": "string",        // Product SKU identifier
  "quantity": "integer",          // Number of units sold
  "unit_price_usd": "double",     // Unit price in USD
  "currency": "string",           // Currency code
  "sales_rep": "string",          // Sales representative name
  "channel": "string",            // Sales channel (Direct, Partner, etc.)
  "status": "string"              // Transaction status (Closed-Won, etc.)
}
Transformation Logic
Bronze → Silver
Deduplication: Remove duplicates by sale_id
Type Conversion: Convert sale_date from STRING to TIMESTAMP
Column Selection: Ensure consistent schema
Data Quality: Implicit null handling
Silver → Gold
Multiple business-focused aggregations:

Daily Summary: Group by date, region, product_line, channel

Total revenue, quantity, transactions
Average order value
Unique customer count
Product Performance: Group by product_line, product_sku, region

Revenue per product
Units sold
Average unit price
Sales Rep Performance: Group by sales_rep, region, channel

Total revenue per rep
Sales count
Unique customers served
Average deal size
Regional Summary: Group by region, year, month

Monthly revenue by region
Transaction volume
Average transaction value
📊 Output Tables
All tables are created in catalog applied_sales_data, schema default:

Layer	Table Name	Type	Description	Rows
🥉 Bronze	salesdata_brone	Streaming Table	Raw JSON ingestion with Auto Loader	20
🥈 Silver	salesdata_silver	Streaming Table	Cleaned, deduplicated, typed data	20
🥇 Gold	salesdata_daily_summary_gold	Materialized View	Daily metrics by date/region/product/channel	28
🥇 Gold	salesdata_product_performance_gold	Materialized View	Product-level performance by SKU/region	10
🥇 Gold	salesdata_rep_performance_gold	Materialized View	Sales rep performance metrics	6
🥇 Gold	salesdata_regional_summary_gold	Materialized View	Regional insights with year/month	8
💻 Code Examples
1. Bronze Layer: JSON to Bronze.py
Ingests raw JSON files using Auto Loader with schema enforcement:

from pyspark import pipelines as dp
from pyspark.sql import types as T

# Define sales data schema for validation
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
2. Silver Layer: Bronze to Silver.py
Cleanses and deduplicates data with type conversions:

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
        .dropDuplicates(["sale_id"])
        .withColumn("sale_date", to_timestamp("sale_date"))
        .select(
            "sale_id", "sale_date", "region", "customer_name", 
            "product_line", "product_sku", "quantity", "unit_price_usd",
            "currency", "sales_rep", "channel", "status"
        )
    )
3. Gold Layer: Silver to Gold.py (Sample)
Creates business-focused analytics tables:

from pyspark import pipelines as dp
from pyspark.sql import functions as F

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
See the full transformations/Silver to Gold.py file for all four gold table definitions.

🛠️ Technology Stack
Platform: Databricks on AWS
Runtime: Databricks Runtime (Current Channel)
Compute: Serverless with Photon Engine
Framework: PySpark 3.5+ with Spark Declarative Pipelines
Ingestion: Auto Loader (cloudFiles API)
Storage: Delta Lake format
Catalog: Unity Catalog
Processing Types:
Streaming Tables (Bronze & Silver)
Materialized Views (Gold)
📈 Monitoring & Operations
Pipeline Status Checks
-- Check Bronze layer ingestion
SELECT COUNT(*) as total_records, 
       MAX(sale_date) as latest_sale
FROM applied_sales_data.default.salesdata_brone;

-- Check Silver layer data quality
SELECT region, COUNT(*) as transaction_count
FROM applied_sales_data.default.salesdata_silver
GROUP BY region;

-- Check Gold layer metrics
SELECT region, year, month, total_revenue
FROM applied_sales_data.default.salesdata_regional_summary_gold
ORDER BY year DESC, month DESC, total_revenue DESC;
Common Operations
View Pipeline Status:

databricks pipelines get --pipeline-id <pipeline_id>
Trigger Manual Update:

databricks pipelines start --pipeline-id <pipeline_id> --full-refresh false
Full Refresh (Reprocess All Data):

databricks pipelines start --pipeline-id <pipeline_id> --full-refresh true
🔧 Configuration
Pipeline Settings (JSON)
{
  "id": "f85da41b-aa56-41c9-8a3a-642d1bd738b7",
  "name": "Applied SalesData ETL",
  "catalog": "applied_sales_data",
  "schema": "default",
  "libraries": [
    {
      "glob": {
        "include": "/Workspace/Users/gyana.gniit@gmail.com/New Pipeline 2026-04-16 10:40/transformations/**"
      }
    }
  ],
  "continuous": false,
  "development": false,
  "photon": true,
  "serverless": true,
  "channel": "CURRENT"
}
Environment Variables
You can parameterize the pipeline using environment variables:

# Example: Use environment-specific catalogs
catalog = spark.conf.get("pipeline.catalog", "applied_sales_data")
source_volume = spark.conf.get("pipeline.source_volume", 
                                "/Volumes/applied_sales_data/default/salesdatafiles")
🧪 Testing
Sample Data Generation
# Generate sample JSON files for testing
import json
from datetime import datetime, timedelta

sample_data = [
    {
        "sale_id": f"SALE-{i:04d}",
        "sale_date": (datetime.now() - timedelta(days=i)).isoformat(),
        "region": ["North", "South", "East", "West"][i % 4],
        "customer_name": f"Customer-{i}",
        "product_line": ["Electronics", "Furniture", "Clothing"][i % 3],
        "product_sku": f"SKU-{i:03d}",
        "quantity": (i % 10) + 1,
        "unit_price_usd": 100.0 + (i * 10),
        "currency": "USD",
        "sales_rep": f"Rep-{i % 5}",
        "channel": ["Direct", "Partner", "Online"][i % 3],
        "status": "Closed-Won"
    }
    for i in range(1, 21)
]

# Write to volume
with open("/dbfs/Volumes/applied_sales_data/default/salesdatafiles/sample.json", "w") as f:
    for record in sample_data:
        f.write(json.dumps(record) + "\n")
🐛 Troubleshooting
Common Issues
Issue: Pipeline fails with "Table not found"

Solution: Ensure the catalog and schema exist. Create them using:
CREATE CATALOG IF NOT EXISTS applied_sales_data;
CREATE SCHEMA IF NOT EXISTS applied_sales_data.default;
Issue: "Permission denied" on source volume

Solution: Grant read permissions:
GRANT READ FILES ON VOLUME applied_sales_data.default.salesdatafiles 
TO `gyana.gniit@gmail.com`;
Issue: Duplicate records in Silver layer

Solution: This is expected if running full refresh. The deduplication logic only applies within each micro-batch.
📚 Additional Resources
Databricks Lakeflow Pipelines Documentation
Auto Loader Documentation
Unity Catalog Documentation
Medallion Architecture Best Practices
📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

👤 Contact
Author: Your Name
Email: gyana.gniit@gmail.com
GitHub: yourusername

🙏 Acknowledgments
Built with Databricks Lakeflow (Spark Declarative Pipelines)
Implements medallion architecture design pattern
Uses Unity Catalog for data governance
Last Updated: April 2026
Pipeline Version: 1.0.0
Status: ✅ Production Ready
