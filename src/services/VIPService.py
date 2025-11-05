"""
VIP Customer Service

This service handles:
- VIP customer identification and analysis
- VIP segmentation by probability tiers
- VIP performance metrics
- VIP geographic and segment distribution
"""

from typing import Optional
import pandas as pd
from .SQLService import SQLService, get_sql_service
from config import get_table_name


class VIPService:
    """Service for fetching and analyzing VIP customer data."""
    
    def __init__(self, sql_service: Optional[SQLService] = None):
        """Initialize the VIP service with SQL service dependency."""
        self.sql_service = sql_service or get_sql_service()
    
    def get_vip_overview(self) -> pd.DataFrame:
        """
        Get overall VIP customer statistics.
        
        Returns:
            pd.DataFrame: VIP counts, avg CLV, and key metrics
        """
        query = f"""
            SELECT 
                COUNT(*) as total_customers,
                COUNT(CASE WHEN vip_customer_probability >= 0.7 THEN 1 END) as high_vip_count,
                COUNT(CASE WHEN vip_customer_probability >= 0.5 THEN 1 END) as medium_high_vip_count,
                COUNT(CASE WHEN vip_customer_probability >= 0.3 THEN 1 END) as low_vip_count,
                ROUND(AVG(vip_customer_probability), 4) as avg_vip_probability,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(AVG(CASE WHEN vip_customer_probability >= 0.7 THEN customer_lifetime_value END), 2) as avg_clv_high_vip,
                ROUND(SUM(CASE WHEN vip_customer_probability >= 0.5 THEN customer_lifetime_value END), 2) as total_vip_value
            FROM {get_table_name('customer_ml_attributes')}
            WHERE vip_customer_probability IS NOT NULL
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_tiers(self) -> pd.DataFrame:
        """
        Get customer distribution by VIP probability tiers.
        
        Returns:
            pd.DataFrame: Customer counts and metrics by VIP tier
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN vip_customer_probability >= 0.9 THEN 'Platinum VIP (0.9+)'
                    WHEN vip_customer_probability >= 0.7 THEN 'Gold VIP (0.7-0.9)'
                    WHEN vip_customer_probability >= 0.5 THEN 'Silver VIP (0.5-0.7)'
                    WHEN vip_customer_probability >= 0.3 THEN 'Bronze VIP (0.3-0.5)'
                    ELSE 'Standard (<0.3)'
                END as vip_tier,
                COUNT(*) as customer_count,
                ROUND(AVG(vip_customer_probability), 4) as avg_vip_probability,
                ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
                ROUND(MIN(customer_lifetime_value), 2) as min_clv,
                ROUND(MAX(customer_lifetime_value), 2) as max_clv,
                ROUND(SUM(customer_lifetime_value), 2) as total_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE vip_customer_probability IS NOT NULL
            GROUP BY vip_tier
            ORDER BY avg_vip_probability DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_by_market_segment(self) -> pd.DataFrame:
        """
        Get VIP distribution across market segments.
        
        Returns:
            pd.DataFrame: VIP rates and counts by market segment
        """
        query = f"""
            SELECT 
                market_segment,
                COUNT(*) as total_customers,
                COUNT(CASE WHEN vip_customer_probability >= 0.5 THEN 1 END) as vip_customers,
                ROUND(100.0 * COUNT(CASE WHEN vip_customer_probability >= 0.5 THEN 1 END) / COUNT(*), 1) as vip_rate_pct,
                ROUND(AVG(CASE WHEN vip_customer_probability >= 0.5 THEN vip_customer_probability END), 4) as avg_vip_probability,
                ROUND(AVG(CASE WHEN vip_customer_probability >= 0.5 THEN customer_lifetime_value END), 2) as avg_vip_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE market_segment IS NOT NULL
            GROUP BY market_segment
            ORDER BY vip_rate_pct DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_by_country(self) -> pd.DataFrame:
        """
        Get VIP customer distribution by country.
        
        Returns:
            pd.DataFrame: VIP counts and rates by country
        """
        query = f"""
            SELECT 
                country,
                COUNT(*) as total_customers,
                COUNT(CASE WHEN vip_customer_probability >= 0.5 THEN 1 END) as vip_customers,
                ROUND(100.0 * COUNT(CASE WHEN vip_customer_probability >= 0.5 THEN 1 END) / COUNT(*), 1) as vip_rate_pct,
                ROUND(AVG(CASE WHEN vip_customer_probability >= 0.5 THEN customer_lifetime_value END), 2) as avg_vip_clv
            FROM {get_table_name('customer_ml_attributes')}
            WHERE country IS NOT NULL
            GROUP BY country
            ORDER BY vip_customers DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_top_vip_customers(self, limit: int = 20) -> pd.DataFrame:
        """
        Get top VIP customers by probability.
        
        Args:
            limit: Number of top VIP customers to return
            
        Returns:
            pd.DataFrame: Top VIP customers with their metrics
        """
        query = f"""
            SELECT 
                user_id,
                ROUND(vip_customer_probability, 4) as vip_probability,
                ROUND(customer_lifetime_value, 2) as clv,
                country,
                market_segment,
                assigned_city
            FROM {get_table_name('customer_ml_attributes')}
            WHERE vip_customer_probability >= 0.5
            ORDER BY vip_customer_probability DESC, customer_lifetime_value DESC
            LIMIT {limit}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_clv_correlation(self) -> pd.DataFrame:
        """
        Get VIP probability vs CLV correlation data for visualization.
        
        Returns:
            pd.DataFrame: VIP probability and CLV for scatter plot
        """
        query = f"""
            SELECT 
                vip_customer_probability,
                customer_lifetime_value,
                market_segment
            FROM {get_table_name('customer_ml_attributes')}
            WHERE vip_customer_probability IS NOT NULL 
                AND customer_lifetime_value IS NOT NULL
                AND vip_customer_probability >= 0.3
            LIMIT 5000
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_vip_behavioral_metrics(self) -> pd.DataFrame:
        """
        Get behavioral metrics for VIP vs non-VIP customers.
        
        Returns:
            pd.DataFrame: Engagement and spending metrics by VIP status
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN ml.vip_customer_probability >= 0.5 THEN 'VIP (≥0.5)'
                    ELSE 'Non-VIP (<0.5)'
                END as vip_status,
                COUNT(*) as customer_count,
                ROUND(AVG(cf.total_amount), 2) as avg_spending,
                ROUND(AVG(cf.order_count), 2) as avg_orders,
                ROUND(AVG(cf.session_count), 2) as avg_sessions,
                ROUND(AVG(cf.event_count), 2) as avg_events,
                ROUND(AVG(cf.days_since_creation), 1) as avg_tenure_days,
                ROUND(100.0 * SUM(CASE WHEN cf.churn = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as retention_rate_pct,
                ROUND(AVG(ml.customer_lifetime_value), 2) as avg_clv
            FROM {get_table_name('customer_ml_attributes')} ml
            INNER JOIN {get_table_name('churn_features')} cf
                ON ml.user_id = cf.user_id
            WHERE ml.vip_customer_probability IS NOT NULL
            GROUP BY vip_status
            ORDER BY vip_status DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)


# Default singleton instance
_vip_service = None


def get_vip_service(sql_service: Optional[SQLService] = None) -> VIPService:
    """Get or create the default VIPService instance."""
    global _vip_service
    if _vip_service is None:
        _vip_service = VIPService(sql_service)
    return _vip_service

