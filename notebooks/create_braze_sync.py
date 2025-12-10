# Databricks notebook source
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# COMMAND ----------

# Create the main braze_target_segment_sync table
spark.sql(f"""
CREATE OR REPLACE TABLE {DB_CATALOG}.{DB_SCHEMA}.braze_target_segment_sync
(
  updated_at TIMESTAMP DEFAULT current_timestamp(),
  user_id STRING,
  email STRING,
  firstname STRING,
  lastname STRING,
  address STRING,
  payload STRUCT<churn INT, customer_lifetime_value STRING, country STRING, assigned_city STRING, market_segment STRING, vip_customer_probability STRING>
)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
)
""")

# COMMAND ----------

# Insert data into braze_target_segment_sync
spark.sql(f"""
INSERT INTO {DB_CATALOG}.{DB_SCHEMA}.braze_target_segment_sync (
  user_id,
  email,
  firstname,
  lastname,
  address,
  payload
)
SELECT
  COALESCE(f.user_id, u.user_id, a.user_id) AS user_id,
  a.email, 
  u.firstname,
  u.lastname,
  u.address,
  struct(
    f.churn,
    a.customer_lifetime_value,
    f.country,
    a.assigned_city,
    a.market_segment,
    a.vip_customer_probability
  ) AS payload
FROM {DB_CATALOG}.{DB_SCHEMA}.churn_features f
FULL OUTER JOIN {DB_CATALOG}.{DB_SCHEMA}.churn_users u
  ON f.user_id = u.user_id
FULL OUTER JOIN {DB_CATALOG}.{DB_SCHEMA}.customer_ml_attributes a
  ON COALESCE(f.user_id, u.user_id) = a.user_id
""")

# COMMAND -------