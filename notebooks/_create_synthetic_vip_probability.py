# Databricks notebook source
# DBTITLE 1,Create VIP Probability Table
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# This approach combines multiple customer behavior indicators into a single probability score
vip_query = f"""
SELECT 
    user_id,
    canal,
    country,
    gender,
    age_group,
    churn,
    order_count,
    total_amount,
    total_item,
    last_transaction,
    platform,
    event_count,
    session_count,
    days_since_creation,
    days_since_last_activity,
    days_last_event,
    
    -- Create VIP probability score based on key indicators:
    -- 1. Spending behavior (35% weight)
    -- 2. Order frequency (25% weight) 
    -- 3. Engagement levels (20% weight)
    -- 4. Recency bonuses (10% weight)
    -- 5. Loyalty bonuses (10% weight)
    
    LEAST(1.0,
        -- Spending component: normalize total_amount (higher spending = higher score)
        (LEAST(total_amount / 500.0, 1.0) * 0.35) +
        
        -- Frequency component: normalize order_count (more orders = higher score)
        (LEAST(order_count / 10.0, 1.0) * 0.25) +
        
        -- Engagement component: normalize combined session and event activity
        (LEAST((session_count + event_count) / 20.0, 1.0) * 0.20) +
        
        -- Recency bonus: recent activity increases VIP probability
        CASE 
            WHEN days_since_last_activity <= 7 THEN 0.10
            WHEN days_since_last_activity <= 30 THEN 0.05
            WHEN days_since_last_activity <= 90 THEN 0.02
            ELSE 0.0
        END +
        
        -- Tenure bonus: long-term customers get slight boost
        CASE 
            WHEN days_since_creation >= 365 THEN 0.05
            WHEN days_since_creation >= 180 THEN 0.025
            ELSE 0.0
        END +
        
        -- Loyalty bonus: non-churned customers get bonus
        CASE WHEN churn = 0 THEN 0.05 ELSE 0.0 END
        
    ) AS vip_customer_probability
    
FROM {DB_CATALOG}.{DB_SCHEMA}.churn_user_features
ORDER BY vip_customer_probability DESC
"""

# Execute the query and save table
vip_results = spark.sql(vip_query)
vip_results.select("user_id", "vip_customer_probability").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB_CATALOG}.default.customer_360_vip")