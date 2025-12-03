# Databricks notebook source
"""
Upload Customer ML Attributes to Unity Catalog

This notebook uploads the customer_ml_attributes.csv file to Unity Catalog.

Author: Matthew Molony
"""

# COMMAND ----------

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
import os

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])  # Go up two levels from notebooks/
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# COMMAND ----------

# Configuration
TABLE = "customer_ml_attributes"

# Use DBFS for the CSV file since bundle Workspace files can't be accessed from jobs
# The CSV file should be uploaded to DBFS before running this job
CSV_PATH = "dbfs:/FileStore/customer-360/customer_ml_attributes.csv"

print("="*80)
print("CSV DATA LOCATION")
print("="*80)
print(f"CSV Path: {CSV_PATH}")
print()
print("NOTE: The CSV file must be uploaded to DBFS before running this job.")
print("Upload using one of these methods:")
print("  1. Databricks CLI:")
print("     databricks fs cp resources/synthetic-data/customer_ml_attributes.csv \\")
print(f"       {CSV_PATH.replace('dbfs:', 'dbfs:/')}")
print("  2. Databricks UI: Data > Browse DBFS > Upload")
print("="*80)

# COMMAND ----------

# Define schema for the CSV file
csv_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("customer_lifetime_value", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("assigned_city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("market_segment", StringType(), True),
    StructField("vip_customer_probability", DoubleType(), True)
])

# COMMAND ----------

print(f"Starting upload at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Configuration loaded from src/config.py:")
print(f"  - Catalog: {DB_CATALOG}")
print(f"  - Schema: {DB_SCHEMA}")
print(f"Target table: {DB_CATALOG}.{DB_SCHEMA}.{TABLE}")
print(f"Source CSV: {CSV_PATH}")

# Check if file exists in DBFS
try:
    file_info = dbutils.fs.ls(CSV_PATH)
    print(f"\n✓ CSV file found!")
    print(f"  Path: {CSV_PATH}")
    print(f"  File size: {file_info[0].size / (1024*1024):.2f} MB")
except Exception as e:
    print(f"\n✗ CSV file not found!")
    print(f"  Error: {e}")
    print("\nPlease upload the CSV file first using:")
    print(f"  databricks fs cp resources/synthetic-data/customer_ml_attributes.csv {CSV_PATH.replace('dbfs:', 'dbfs:/')}")
    raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")

print("-" * 80)

# COMMAND ----------

# Read CSV file
print("Reading CSV file...")
# Spark can read directly from Workspace paths
df = spark.read \
    .option("header", "true") \
    .schema(csv_schema) \
    .csv(CSV_PATH)

row_count = df.count()
print(f"✓ Successfully read {row_count:,} rows from CSV")

# COMMAND ----------

# Display sample of data
print("\nSample data (first 5 rows):")
df.show(5, truncate=False)

# COMMAND ----------

# Create catalog and schema if they don't exist
print(f"\nEnsuring catalog and schema exist...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DB_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB_CATALOG}.{DB_SCHEMA}")
print(f"✓ Catalog and schema ready")

# COMMAND ----------

# Write to Unity Catalog
print(f"\nWriting data to Unity Catalog table...")
full_table_name = f"{DB_CATALOG}.{DB_SCHEMA}.{TABLE}"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_name)

print(f"✓ Successfully wrote {row_count:,} rows to {full_table_name}")

# COMMAND ----------

# Verify the data was written correctly
print("\nVerifying data in Unity Catalog...")
verification_df = spark.table(full_table_name)
verification_count = verification_df.count()

print(f"✓ Verified {verification_count:,} rows in {full_table_name}")
print("\nSample from Unity Catalog table:")
verification_df.show(5, truncate=False)

# COMMAND ----------

# Summary
print("\n" + "=" * 80)
print("UPLOAD COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"Table: {full_table_name}")
print(f"Rows: {verification_count:,}")
print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
