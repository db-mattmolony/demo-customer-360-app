# Databricks notebook source
# DBTITLE 1,Load Customer Data and Create Synthetic CLV
from pyspark.sql.functions import col, when, greatest, least, log, sqrt, exp, lit, rand
from pyspark.sql.types import DoubleType
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# Load the customer features data
df = spark.sql(f"SELECT * FROM {DB_CATALOG}.{DB_SCHEMA}.churn_user_features")

# COMMAND ----------

# DBTITLE 1,Engineer CLV Components and Calculate Synthetic CLV
# Create CLV based on multiple correlated factors
# CLV Formula considers: Historical Value + Future Potential - Churn Risk

clv_df = df.withColumn(
    # Base spending velocity (annual spending rate)
    "annual_spending_rate",
    when(col("days_since_creation") > 0, 
         (col("total_amount") * 365.0) / col("days_since_creation")).otherwise(0.0)
).withColumn(
    # Order frequency score (orders per month)
    "monthly_order_frequency", 
    when(col("days_since_creation") > 0,
         (col("order_count") * 30.0) / col("days_since_creation")).otherwise(0.0)
).withColumn(
    # Engagement multiplier (session activity indicates future engagement)
    "engagement_multiplier",
    1.0 + (col("session_count") / 10.0)  # Higher sessions = higher future value
).withColumn(
    # Loyalty factor (longer tenure = higher future value)
    "loyalty_factor",
    when(col("days_since_creation") >= 730, 1.5)  # 2+ years = 50% bonus
    .when(col("days_since_creation") >= 365, 1.3)  # 1+ year = 30% bonus
    .when(col("days_since_creation") >= 180, 1.1)  # 6+ months = 10% bonus
    .otherwise(1.0)
).withColumn(
    # Churn risk penalty (recent activity and churn status)
    "churn_risk_factor",
    when(col("churn") == 1, 0.3)  # Churned customers have 70% value reduction
    .when(col("days_since_last_activity") > 90, 0.6)  # Inactive >90 days = 40% reduction
    .when(col("days_since_last_activity") > 30, 0.8)   # Inactive >30 days = 20% reduction
    .otherwise(1.0)  # Active customers = no penalty
).withColumn(
    # Platform sophistication bonus (tech-savvy customers tend to spend more)
    "platform_bonus",
    when(col("platform") == "ios", 1.1)      # iOS users typically higher value
    .when(col("platform") == "android", 1.05) # Android users moderate bonus
    .otherwise(1.0)
).withColumn(
    # Add some realistic noise/variance (±15%)
    "random_variance",
    0.85 + (rand() * 0.3)  # Random factor between 0.85 and 1.15
)

print("⚙️ Calculated CLV component factors")

# Calculate final synthetic CLV
clv_df = clv_df.withColumn(
    "customer_lifetime_value",
    # Base CLV = (Annual Spending * Projected Years) + (Order Frequency Bonus) 
    # Modified by engagement, loyalty, churn risk, platform, and variance
    (
        # Historical spending projected forward (2-3 years average)
        (col("annual_spending_rate") * 2.5) +
        
        # Order frequency value (frequent buyers worth more)
        (col("monthly_order_frequency") * 50.0) +
        
        # Base engagement value
        (col("session_count") * 25.0)
        
    ) * col("engagement_multiplier") 
      * col("loyalty_factor") 
      * col("churn_risk_factor") 
      * col("platform_bonus")
      * col("random_variance")
).withColumn(
    # Ensure CLV is reasonable (minimum $50, maximum $10,000)
    "customer_lifetime_value",
    greatest(least(col("customer_lifetime_value"), lit(10000.0)), lit(50.0))
).withColumn(
    # Round to 2 decimal places for currency
    "customer_lifetime_value",
    (col("customer_lifetime_value") * 100).cast("long") / 100.0
)

# COMMAND ----------

# DBTITLE 1,Save CLV Table
clv_df.select("user_id", "customer_lifetime_value").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB_CATALOG}.default.customer_360_clv")
