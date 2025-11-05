"""
Customer Lifetime Value (CLV) Service

This service handles:
- CLV distribution analysis
- Customer segmentation by CLV
- Correlation analysis with behavioral metrics
- Geographic and market segment CLV analysis
"""

from typing import Optional
import pandas as pd
from .SQLService import SQLService, get_sql_service
from config import get_table_name


class CLVService:
    """Service for fetching and analyzing Customer Lifetime Value data."""
    
    def __init__(self, sql_service: Optional[SQLService] = None):
        """Initialize the CLV service with SQL service dependency."""
        self.sql_service = sql_service or get_sql_service()
    
    def get_clv_statistics(self) -> pd.DataFrame:
        """
        Get overall CLV distribution statistics.
        
        Returns:
            pd.DataFrame: Min, max, avg, stddev of CLV
        """
        query = f"""
            SELECT 
                COUNT(*) as total_customers,
                ROUND(MIN(customer_lifetime_value), 2) as min_clv,
                ROUND(MAX(customer_lifetime_value), 2) as max_clv,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(STDDEV(customer_lifetime_value), 2) as stddev_clv,
                ROUND(SUM(customer_lifetime_value), 2) as total_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE customer_lifetime_value IS NOT NULL
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_clv_segments(self) -> pd.DataFrame:
        """
        Get customer distribution by CLV segments.
        
        Segments:
        - High Value (≥$2000)
        - Medium-High ($1000-$2000)
        - Medium ($500-$1000)
        - Low-Medium ($200-$500)
        - Low (<$200)
        
        Returns:
            pd.DataFrame: Customer counts and metrics by CLV segment
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN customer_lifetime_value >= 2000 THEN 'High Value (≥$2000)'
                    WHEN customer_lifetime_value >= 1000 THEN 'Medium-High ($1000-$2000)'
                    WHEN customer_lifetime_value >= 500 THEN 'Medium ($500-$1000)'
                    WHEN customer_lifetime_value >= 200 THEN 'Low-Medium ($200-$500)'
                    ELSE 'Low (<$200)'
                END as clv_segment,
                COUNT(*) as customer_count,
                ROUND(AVG(ml.customer_lifetime_value), 2) as avg_clv,
                ROUND(SUM(ml.customer_lifetime_value), 2) as total_clv_value
            FROM {get_table_name('customer_ml_attributes')} ml
            WHERE ml.customer_lifetime_value IS NOT NULL
            GROUP BY clv_segment
            ORDER BY avg_clv DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_clv_by_market_segment(self) -> pd.DataFrame:
        """
        Get CLV distribution by market segment.
        
        Returns:
            pd.DataFrame: CLV metrics by market segment
        """
        query = f"""
            SELECT 
                market_segment,
                COUNT(*) as customer_count,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(MIN(customer_lifetime_value), 2) as min_clv,
                ROUND(MAX(customer_lifetime_value), 2) as max_clv,
                ROUND(SUM(customer_lifetime_value), 2) as total_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE market_segment IS NOT NULL AND customer_lifetime_value IS NOT NULL
            GROUP BY market_segment
            ORDER BY avg_clv DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_clv_by_country(self) -> pd.DataFrame:
        """
        Get CLV distribution by country.
        
        Returns:
            pd.DataFrame: CLV metrics by country
        """
        query = f"""
            SELECT 
                country,
                COUNT(*) as customer_count,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(SUM(customer_lifetime_value), 2) as total_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE country IS NOT NULL AND customer_lifetime_value IS NOT NULL
            GROUP BY country
            ORDER BY total_clv DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_clv_with_behavioral_metrics(self) -> pd.DataFrame:
        """
        Get CLV joined with behavioral metrics for correlation analysis.
        
        Returns:
            pd.DataFrame: CLV with total_amount, order_count, session_count, etc.
        """
        query = f"""
            SELECT 
                ml.customer_lifetime_value,
                cf.total_amount,
                cf.order_count,
                cf.session_count,
                cf.event_count,
                cf.days_since_creation,
                cf.days_since_last_activity,
                ml.vip_customer_probability
            FROM {get_table_name('customer_ml_attributes')} ml
            INNER JOIN {get_table_name('churn_features')} cf
                ON ml.user_id = cf.user_id
            WHERE ml.customer_lifetime_value IS NOT NULL
                AND cf.total_amount IS NOT NULL
                AND cf.order_count IS NOT NULL
                AND cf.session_count IS NOT NULL
            LIMIT 10000
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_segments(self) -> pd.DataFrame:
        """
        Get customer distribution by VIP probability segments.
        
        Returns:
            pd.DataFrame: Customer counts and CLV by VIP probability
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN vip_customer_probability >= 0.7 THEN 'High VIP (≥0.7)'
                    WHEN vip_customer_probability >= 0.5 THEN 'Medium VIP (0.5-0.7)'
                    WHEN vip_customer_probability >= 0.3 THEN 'Low VIP (0.3-0.5)'
                    ELSE 'Very Low VIP (<0.3)'
                END as vip_segment,
                COUNT(*) as customer_count,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(AVG(vip_customer_probability), 4) as avg_vip_probability,
                ROUND(SUM(customer_lifetime_value), 2) as total_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE vip_customer_probability IS NOT NULL AND customer_lifetime_value IS NOT NULL
            GROUP BY vip_segment
            ORDER BY avg_vip_probability DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_top_clv_customers(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top customers by CLV.
        
        Args:
            limit: Number of top customers to return
            
        Returns:
            pd.DataFrame: Top customers with their CLV and attributes
        """
        query = f"""
            SELECT 
                ml.user_id,
                ROUND(ml.customer_lifetime_value, 2) as clv,
                ml.country,
                ml.market_segment,
                ROUND(ml.vip_customer_probability, 4) as vip_probability,
                cf.order_count,
                ROUND(cf.total_amount, 2) as total_spent,
                cf.session_count
            FROM {get_table_name('customer_ml_attributes')} ml
            LEFT JOIN {get_table_name('churn_features')} cf
                ON ml.user_id = cf.user_id
            WHERE ml.customer_lifetime_value IS NOT NULL
            ORDER BY ml.customer_lifetime_value DESC
            LIMIT {limit}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_clv_segments_with_behavior(self) -> pd.DataFrame:
        """
        Get CLV segments enriched with behavioral metrics.
        
        Returns:
            pd.DataFrame: CLV segments with avg spending, orders, sessions
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN ml.customer_lifetime_value >= 2000 THEN 'High Value (≥$2000)'
                    WHEN ml.customer_lifetime_value >= 1000 THEN 'Medium-High ($1000-$2000)'
                    WHEN ml.customer_lifetime_value >= 500 THEN 'Medium ($500-$1000)'
                    WHEN ml.customer_lifetime_value >= 200 THEN 'Low-Medium ($200-$500)'
                    ELSE 'Low (<$200)'
                END as clv_segment,
                COUNT(*) as customer_count,
                ROUND(AVG(ml.customer_lifetime_value), 2) as avg_clv,
                ROUND(AVG(cf.total_amount), 2) as avg_historical_spending,
                ROUND(AVG(cf.order_count), 2) as avg_orders,
                ROUND(AVG(cf.session_count), 2) as avg_sessions,
                ROUND(AVG(cf.event_count), 2) as avg_events
            FROM {get_table_name('customer_ml_attributes')} ml
            INNER JOIN {get_table_name('churn_features')} cf
                ON ml.user_id = cf.user_id
            WHERE ml.customer_lifetime_value IS NOT NULL
            GROUP BY clv_segment
            ORDER BY avg_clv DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)


# Default singleton instance
_clv_service = None


def get_clv_service(sql_service: Optional[SQLService] = None) -> CLVService:
    """Get or create the default CLVService instance."""
    global _clv_service
    if _clv_service is None:
        _clv_service = CLVService(sql_service)
    return _clv_service

