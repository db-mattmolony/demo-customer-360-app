"""
Customer 360 Service for customer analytics operations.

This service handles:
- Customer churn analysis
- Customer segmentation
- Risk assessment
- Platform metrics
"""

import pandas as pd
from typing import Optional
from .SQLService import get_sql_service


class C360Service:
    """Service for Customer 360 analytics queries."""
    
    def __init__(self, sql_service=None):
        """Initialize the C360 service with SQL service dependency."""
        self.sql_service = sql_service or get_sql_service()
    
    def get_churn_features(self, limit: int = 100) -> pd.DataFrame:
        """
        Fetch customer churn features data.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            pd.DataFrame: Churn features data
        """
        query = f"""
            SELECT * 
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features 
            LIMIT {limit}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_all_churn_features(self) -> pd.DataFrame:
        """
        Fetch ALL customer churn features data.
        
        Returns:
            pd.DataFrame: Complete churn features data
        """
        query = """
            SELECT * 
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_summary(self) -> pd.DataFrame:
        """
        Get summary statistics for churn analysis.
        
        Returns:
            pd.DataFrame: Summary statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_customers,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned_customers,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate,
                AVG(order_count) as avg_orders,
                AVG(total_amount) as avg_revenue,
                AVG(days_since_last_activity) as avg_days_inactive
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_segment(self) -> pd.DataFrame:
        """
        Get churn rate by customer segment.
        
        Returns:
            pd.DataFrame: Churn statistics by segment
        """
        query = """
            SELECT 
                age_group,
                gender,
                COUNT(*) as customer_count,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
            GROUP BY age_group, gender
            ORDER BY churn_rate DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_customer_metrics_by_platform(self) -> pd.DataFrame:
        """
        Get customer engagement metrics by platform.
        
        Returns:
            pd.DataFrame: Metrics grouped by platform
        """
        query = """
            SELECT 
                platform,
                COUNT(*) as customer_count,
                AVG(session_count) as avg_sessions,
                AVG(event_count) as avg_events,
                AVG(order_count) as avg_orders,
                AVG(total_amount) as avg_revenue
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
            WHERE platform IS NOT NULL
            GROUP BY platform
            ORDER BY customer_count DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_high_risk_customers(self, risk_threshold: int = 30) -> pd.DataFrame:
        """
        Get customers at high risk of churning.
        
        Args:
            risk_threshold: Days since last activity to consider high risk
            
        Returns:
            pd.DataFrame: High risk customers
        """
        query = f"""
            SELECT 
                user_id,
                email,
                firstname,
                lastname,
                days_since_last_activity,
                order_count,
                total_amount,
                last_activity_date,
                churn
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
            WHERE days_since_last_activity > {risk_threshold}
                AND churn = 0
            ORDER BY days_since_last_activity DESC
            LIMIT 50
        """
        return self.sql_service.execute_query_as_dataframe(query)


# Default singleton instance
_c360_service = None


def get_c360_service() -> C360Service:
    """Get or create the default C360Service instance."""
    global _c360_service
    if _c360_service is None:
        _c360_service = C360Service()
    return _c360_service

