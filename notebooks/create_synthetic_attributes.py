# Databricks notebook source
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

df = spark.sql(f"""
CREATE OR REPLACE TABLE {DB_CATALOG}.{DB_SCHEMA}.customer_ml_attributes as 
SELECT 
  coalesce(clv.user_id, loc.user_id, seg.user_id, vip.user_id) as user_id,
  clv.* EXCEPT(user_id),
  loc.* EXCEPT(user_id),
  seg.* EXCEPT(user_id),
  vip.* EXCEPT(user_id)
FROM {DB_CATALOG}.default.customer_360_clv AS clv
FULL JOIN {DB_CATALOG}.default.customer_360_locations AS loc
  ON clv.user_id = loc.user_id
FULL JOIN {DB_CATALOG}.default.customer_360_customer_segments AS seg
  ON clv.user_id = seg.user_id
FULL JOIN {DB_CATALOG}.default.customer_360_vip AS vip
  ON clv.user_id = vip.user_id
""")

