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
CREATE OR REPLACE TABLE {DB_CATALOG}.{DB_SCHEMA}._customer_emails AS 
SELECT 
    user_id,
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct', 
        CONCAT(
            'Create a new email address for this person, only return the email string ', 
            firstname, 
            ' ', 
            lastname
        )
    ) AS email 
FROM {DB_CATALOG}.{DB_SCHEMA}.churn_users
""")
