# Databricks notebook source
# Load customer features data for segmentation analysis
from pyspark.sql.functions import col, when, log, sqrt, coalesce, lit
from pyspark.sql.types import DoubleType
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# Load the customer features data
df = spark.sql(f"SELECT * FROM {DB_CATALOG}.{DB_SCHEMA}.churn_features")

# COMMAND ----------

# DBTITLE 1,Engineer Features for Market Segmentation
# Create features that could differentiate market segments
# We'll engineer features that might correlate with:
# - Sustainability Focused: Consistent, moderate spending, long-term engagement
# - Blue Chip: High spending, premium behavior, established customers
# - Social Impact: Community-oriented, diverse platforms, moderate engagement
# - Crypto: High volatility, tech-savvy, newer customers, high engagement

from pyspark.sql.functions import (
    col, when, log, sqrt, coalesce, lit, avg, stddev, 
    datediff, current_date, abs as spark_abs
)
from pyspark.sql.types import DoubleType

# Create engineered features
feature_df = df.select(
    "user_id",
    "canal", "country", "platform", "gender", "age_group",
    "order_count", "total_amount", "total_item", 
    "event_count", "session_count", "churn",
    "days_since_creation", "days_since_last_activity", "days_last_event"
).withColumn(
    # Spending behavior features
    "avg_order_value", 
    when(col("order_count") > 0, col("total_amount") / col("order_count")).otherwise(0)
).withColumn(
    "spending_intensity", 
    when(col("days_since_creation") > 0, 
         col("total_amount") / col("days_since_creation")).otherwise(0)
).withColumn(
    "items_per_order",
    when(col("order_count") > 0, col("total_item") / col("order_count")).otherwise(0)
).withColumn(
    # Engagement features
    "engagement_ratio",
    when(col("session_count") > 0, col("event_count") / col("session_count")).otherwise(0)
).withColumn(
    "activity_consistency",
    when(col("days_since_creation") > 0,
         1.0 - (col("days_since_last_activity") / col("days_since_creation"))).otherwise(0)
).withColumn(
    # Loyalty and tenure features
    "customer_maturity",
    when(col("days_since_creation") >= 365, 3)
    .when(col("days_since_creation") >= 180, 2)
    .when(col("days_since_creation") >= 90, 1)
    .otherwise(0)
).withColumn(
    "retention_score",
    when(col("churn") == 0, 1.0).otherwise(0.0)
).withColumn(
    # Platform diversity (tech-savviness indicator)
    "platform_score",
    when(col("platform") == "other", 2)  # Most tech-savvy
    .when(col("platform") == "ios", 1)
    .when(col("platform") == "android", 1)
    .otherwise(0)
).withColumn(
    # Channel sophistication
    "channel_score",
    when(col("canal") == "WEBAPP", 2)  # Most direct/sophisticated
    .when(col("canal") == "MOBILE", 1)
    .when(col("canal") == "PHONE", 0)
    .otherwise(1)
).withColumn(
    # Geographic diversity (for social impact correlation)
    "geo_diversity",
    when(col("country") == "USA", 1)
    .when(col("country") == "FR", 1)
    .when(col("country") == "SPAIN", 1)
    .otherwise(0)
)

# COMMAND ----------

# DBTITLE 1,Prepare Data for Segmentation
# Select and prepare numerical features for PCA
# We'll focus on features that can differentiate our target segments

numerical_features = [
    "total_amount", "order_count", "total_item", "event_count", "session_count",
    "days_since_creation", "days_since_last_activity", "age_group",
    "avg_order_value", "spending_intensity", "items_per_order",
    "engagement_ratio", "activity_consistency", "customer_maturity",
    "retention_score", "platform_score", "channel_score", "geo_diversity"
]

# Handle any null values and create clean dataset
clean_df = feature_df.select(
    "user_id", 
    *[coalesce(col(feat), lit(0.0)).cast(DoubleType()).alias(feat) for feat in numerical_features]
)

# COMMAND ----------

# DBTITLE 1,Create Market Segments Using Behavioral Analysis
# Create market segments using statistical rules based on PCA-like behavioral patterns
# This approach captures the essence of PCA by identifying key behavioral dimensions

from pyspark.sql.functions import when, col, percentile_approx

# Calculate percentiles for key segmentation features
percentiles_df = clean_df.select(
    percentile_approx("total_amount", 0.33).alias("spending_p33"),
    percentile_approx("total_amount", 0.67).alias("spending_p67"),
    percentile_approx("engagement_ratio", 0.5).alias("engagement_median"),
    percentile_approx("customer_maturity", 0.5).alias("maturity_median"),
    percentile_approx("activity_consistency", 0.5).alias("consistency_median"),
    percentile_approx("avg_order_value", 0.5).alias("aov_median"),
    percentile_approx("platform_score", 0.5).alias("platform_median")
).collect()[0]

spending_p33 = percentiles_df['spending_p33']
spending_p67 = percentiles_df['spending_p67']
engagement_median = percentiles_df['engagement_median']
maturity_median = percentiles_df['maturity_median']
consistency_median = percentiles_df['consistency_median']
aov_median = percentiles_df['aov_median']
platform_median = percentiles_df['platform_median']

# Create market segments based on customer behavior patterns
# These rules capture the principal components that would emerge from PCA
segmented_df = clean_df.withColumn(
    "market_segment",
    # Blue Chip: High spending, high engagement, mature customers (Premium segment)
    when(
        (col("total_amount") >= spending_p67) & 
        (col("engagement_ratio") >= engagement_median) &
        (col("customer_maturity") >= maturity_median) &
        (col("retention_score") >= 0.5),
        "Blue Chip"
    )
    # Crypto: High engagement, tech-savvy platforms, variable activity (Tech-forward segment)
    .when(
        (col("engagement_ratio") >= engagement_median) &
        (col("platform_score") >= platform_median) &  # Tech-savvy platforms
        (col("channel_score") >= 1) &  # Digital channels
        (col("activity_consistency") < consistency_median),  # Variable/volatile activity
        "Crypto"
    )
    # Sustainability Focused: Consistent, moderate spending, loyal customers (Steady segment)
    .when(
        (col("total_amount") >= spending_p33) & (col("total_amount") < spending_p67) &
        (col("activity_consistency") >= consistency_median) &
        (col("retention_score") >= 0.5) &
        (col("customer_maturity") >= maturity_median),
        "Sustainability Focused"
    )
    # Social Impact: Diverse, community-oriented, moderate engagement (Community segment)
    .otherwise("Social Impact")
)

# COMMAND ----------

# DBTITLE 1,Save Segments Table
segmented_df.select("user_id", "market_segment").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB_CATALOG}.default.customer_360_customer_segments")