"""
Customer Churn Service for churn prediction and analysis.

This service handles:
- Churn analysis by demographics
- Risk assessment
- Customer segmentation by churn likelihood
- Churn prediction insights
"""

import pandas as pd
from typing import Optional
from .SQLService import get_sql_service
from config import get_table_name


class CustomerChurnService:
    """Service for customer churn analysis and predictions."""
    
    def __init__(self, sql_service=None):
        """Initialize the CustomerChurnService with SQL service dependency."""
        self.sql_service = sql_service or get_sql_service()
    
    def get_churn_by_country(self) -> pd.DataFrame:
        """
        Get customer distribution by country - binary: At Risk vs Not at Risk.
        
        Returns:
            pd.DataFrame: Customer counts by country and risk status (At Risk / Not at Risk)
        """
        query = f"""
            SELECT 
                country,
                CASE 
                    WHEN churn = 1 THEN 'At Risk'
                    WHEN churn = 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as risk_category,
                COUNT(*) as customer_count
            FROM {get_table_name('churn_features')}
            WHERE country IS NOT NULL
            GROUP BY country, risk_category
            ORDER BY country, 
                CASE risk_category
                    WHEN 'Not at Risk' THEN 1
                    WHEN 'At Risk' THEN 2
                    ELSE 3
                END
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_platform(self) -> pd.DataFrame:
        """
        Get churn statistics by platform.
        
        Returns:
            pd.DataFrame: Churn metrics grouped by platform
        """
        query = f"""
            SELECT 
                platform,
                COUNT(*) as total_customers,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned_customers,
                SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) as active_customers,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate_pct,
                ROUND(AVG(session_count), 2) as avg_sessions,
                ROUND(AVG(event_count), 2) as avg_events
            FROM {get_table_name('churn_features')}
            WHERE platform IS NOT NULL
            GROUP BY platform
            ORDER BY churn_rate_pct DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_age_group(self) -> pd.DataFrame:
        """
        Get customer distribution by age group - binary: At Risk vs Not at Risk.
        
        Returns:
            pd.DataFrame: Customer counts by age group and risk status (At Risk / Not at Risk)
        """
        query = f"""
            SELECT 
                age_group,
                CASE 
                    WHEN churn = 1 THEN 'At Risk'
                    WHEN churn = 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as risk_category,
                COUNT(*) as customer_count
            FROM {get_table_name('churn_features')}
            WHERE age_group IS NOT NULL
            GROUP BY age_group, risk_category
            ORDER BY age_group, 
                CASE risk_category
                    WHEN 'Not at Risk' THEN 1
                    WHEN 'At Risk' THEN 2
                    ELSE 3
                END
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_gender(self) -> pd.DataFrame:
        """
        Get churn statistics by gender.
        
        Returns:
            pd.DataFrame: Churn metrics grouped by gender
        """
        query = f"""
            SELECT 
                gender,
                COUNT(*) as total_customers,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned_customers,
                SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) as active_customers,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate_pct
            FROM {get_table_name('churn_features')}
            WHERE gender IS NOT NULL
            GROUP BY gender
            ORDER BY churn_rate_pct DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_canal(self) -> pd.DataFrame:
        """
        Get churn statistics by acquisition channel (canal).
        
        Returns:
            pd.DataFrame: Churn metrics grouped by canal
        """
        query = f"""
            SELECT 
                canal,
                COUNT(*) as total_customers,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned_customers,
                SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) as active_customers,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate_pct
            FROM {get_table_name('churn_features')}
            WHERE canal IS NOT NULL
            GROUP BY canal
            ORDER BY churn_rate_pct DESC
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_top_customers_likely_to_churn(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top N customers most likely to churn based on activity patterns.
        Currently not churned but showing high risk indicators.
        
        Args:
            limit: Number of customers to return
            
        Returns:
            pd.DataFrame: Top at-risk customers
        """
        query = f"""
            SELECT 
                user_id,
                email,
                firstname,
                lastname,
                country,
                platform,
                days_since_last_activity,
                order_count,
                total_amount,
                session_count,
                event_count,
                last_activity_date,
                CASE 
                    WHEN days_since_last_activity > 60 THEN 'Critical'
                    WHEN days_since_last_activity > 30 THEN 'High'
                    ELSE 'Medium'
                END as risk_level
            FROM {get_table_name('churn_features')}
            WHERE churn = 0
                AND days_since_last_activity IS NOT NULL
            ORDER BY days_since_last_activity DESC, order_count ASC
            LIMIT {limit}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_total_customers_at_risk(self) -> pd.DataFrame:
        """
        Get count of customers by risk status (churn=1 vs churn=0).
        
        Returns:
            pd.DataFrame: Customer counts and revenue by risk status
        """
        query = f"""
            SELECT 
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as total_at_risk,
                SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) as total_not_at_risk,
                SUM(CASE WHEN churn = 1 THEN total_amount ELSE 0 END) as total_revenue_at_risk,
                SUM(CASE WHEN churn = 0 THEN total_amount ELSE 0 END) as total_revenue_not_at_risk
            FROM {get_table_name('churn_features')}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_risk_distribution(self) -> pd.DataFrame:
        """
        Get binary distribution: At Risk vs Not at Risk.
        
        Note: churn = 1 means at risk of churning, churn = 0 means not at risk.
        
        Returns:
            pd.DataFrame: Binary risk distribution (At Risk / Not at Risk)
        """
        query = f"""
            SELECT 
                CASE 
                    WHEN churn = 1 THEN 'At Risk'
                    WHEN churn = 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as risk_category,
                COUNT(*) as customer_count,
                ROUND(AVG(total_amount), 2) as avg_revenue,
                ROUND(SUM(total_amount), 2) as total_revenue
            FROM {get_table_name('churn_features')}
            GROUP BY risk_category
            ORDER BY 
                CASE risk_category
                    WHEN 'Not at Risk' THEN 1
                    WHEN 'At Risk' THEN 2
                    ELSE 3
                END
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_timeline_analysis(self) -> pd.DataFrame:
        """
        Analyze churn patterns over customer lifecycle using standard deviation-based buckets.
        
        Returns:
            pd.DataFrame: Churn by customer tenure (bucketed by standard deviation)
        """
        query = f"""
            WITH stats AS (
                SELECT 
                    AVG(days_since_creation) as mean_tenure,
                    STDDEV(days_since_creation) as stddev_tenure
                FROM {get_table_name('churn_features')}
                WHERE days_since_creation IS NOT NULL
            )
            SELECT 
                CASE 
                    WHEN days_since_creation < (mean_tenure - stddev_tenure) THEN 'Very New (< -1 SD)'
                    WHEN days_since_creation BETWEEN (mean_tenure - stddev_tenure) AND mean_tenure THEN 'New (-1 SD to Mean)'
                    WHEN days_since_creation BETWEEN mean_tenure AND (mean_tenure + stddev_tenure) THEN 'Established (Mean to +1 SD)'
                    WHEN days_since_creation BETWEEN (mean_tenure + stddev_tenure) AND (mean_tenure + 2 * stddev_tenure) THEN 'Mature (+1 to +2 SD)'
                    ELSE 'Veteran (> +2 SD)'
                END as customer_tenure,
                COUNT(*) as total_customers,
                SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) as churned_customers,
                ROUND(AVG(CASE WHEN churn = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as churn_rate_pct,
                ROUND(MIN(days_since_creation), 0) as min_days,
                ROUND(MAX(days_since_creation), 0) as max_days,
                ROUND(AVG(days_since_creation), 0) as avg_days
            FROM {get_table_name('churn_features')}
            CROSS JOIN stats
            WHERE days_since_creation IS NOT NULL
            GROUP BY customer_tenure
            ORDER BY 
                CASE customer_tenure
                    WHEN 'Very New (< -1 SD)' THEN 1
                    WHEN 'New (-1 SD to Mean)' THEN 2
                    WHEN 'Established (Mean to +1 SD)' THEN 3
                    WHEN 'Mature (+1 to +2 SD)' THEN 4
                    WHEN 'Veteran (> +2 SD)' THEN 5
                    ELSE 6
                END
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_churn_by_engagement_level(self) -> pd.DataFrame:
        """
        Get customer distribution by engagement level - binary: At Risk vs Not at Risk.
        
        Returns:
            pd.DataFrame: Customer counts by engagement quartile and risk status (At Risk / Not at Risk)
        """
        query = f"""
            WITH engagement_quartiles AS (
                SELECT 
                    user_id,
                    churn,
                    order_count,
                    NTILE(4) OVER (ORDER BY order_count) as order_quartile
                FROM {get_table_name('churn_features')}
                WHERE order_count IS NOT NULL
            )
            SELECT 
                CONCAT('Q', order_quartile) as engagement_level,
                CASE 
                    WHEN churn = 1 THEN 'At Risk'
                    WHEN churn = 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as risk_category,
                COUNT(*) as customer_count
            FROM engagement_quartiles
            GROUP BY order_quartile, risk_category
            ORDER BY order_quartile, 
                CASE risk_category
                    WHEN 'Not at Risk' THEN 1
                    WHEN 'At Risk' THEN 2
                    ELSE 3
                END
        """
        return self.sql_service.execute_query_as_dataframe(query)


# Default singleton instance
_churn_service = None


def get_churn_service() -> CustomerChurnService:
    """Get or create the default CustomerChurnService instance."""
    global _churn_service
    if _churn_service is None:
        _churn_service = CustomerChurnService()
    return _churn_service

