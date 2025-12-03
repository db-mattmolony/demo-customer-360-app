# Databricks notebook source
# DBTITLE 1,Generate Synthetic Email Addresses Using AI
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# This notebook uses Databricks AI functions to generate realistic email addresses
# based on customer first and last names

# COMMAND ----------

# DBTITLE 1,Create Table with AI-Generated Emails
spark.sql(f"""
CREATE OR REPLACE TABLE {DB_CATALOG}.default.temp_customer_emails AS 
SELECT 
    user_id,
    email, 
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct', 
        CONCAT(
            'Create a new email address for this person, only return the email string ', 
            firstname, 
            ' ', 
            lastname
        )
    ) AS updated_email 
FROM {DB_CATALOG}.{DB_SCHEMA}.churn_users
""")

# COMMAND ----------

# DBTITLE 1,Merge AI-Generated Emails into Churn Features
spark.sql(f"""
MERGE INTO {DB_CATALOG}.{DB_SCHEMA}.churn_features AS target
USING {DB_CATALOG}.default.temp_customer_emails AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
  UPDATE SET target.email = source.updated_email
""")

# COMMAND ----------

# DBTITLE 1,Clean Up Temporary Table
spark.sql(f"""
DROP TABLE IF EXISTS {DB_CATALOG}.default.temp_customer_emails
""")

# COMMAND ----------

# DBTITLE 1,Clean Up Intermediate Tables
# Drop the intermediate tables created by _*.py files
# These are no longer needed after creating the final customer_ml_attributes table
spark.sql(f"""
DROP TABLE IF EXISTS {DB_CATALOG}.default.customer_360_clv
""")

spark.sql(f"""
DROP TABLE IF EXISTS {DB_CATALOG}.default.customer_360_locations
""")

spark.sql(f"""
DROP TABLE IF EXISTS {DB_CATALOG}.default.customer_360_customer_segments
""")

spark.sql(f"""
DROP TABLE IF EXISTS {DB_CATALOG}.default.customer_360_vip
""")

