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
        Fetch enriched customer churn features data with ML attributes.
        Joins churn_features with customer_ml_attributes for complete customer view.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            pd.DataFrame: Enriched churn features data with CLV, VIP, and segment info
        """
        query = f"""
            SELECT 
                cf.user_id,
                cf.email,
                cf.firstname,
                cf.lastname,
                cf.country,
                cf.gender,
                CASE cf.age_group
                    WHEN 1 THEN '15-19'
                    WHEN 2 THEN '20-24'
                    WHEN 3 THEN '25-29'
                    WHEN 4 THEN '30-34'
                    WHEN 5 THEN '35-39'
                    WHEN 6 THEN '40-49'
                    WHEN 7 THEN '50-59'
                    WHEN 8 THEN '60-69'
                    WHEN 9 THEN '70-79'
                    WHEN 10 THEN '80-100'
                    ELSE 'Unknown'
                END as age_group,
                cf.platform,
                CASE cf.churn
                    WHEN 1 THEN 'At Risk'
                    WHEN 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as churn_status,
                cf.order_count,
                cf.total_amount,
                cf.session_count,
                cf.event_count,
                cf.days_since_creation,
                cf.days_since_last_activity,
                cf.last_activity_date,
                ml.lat,
                ml.lon,
                ml.customer_lifetime_value,
                ml.vip_customer_probability,
                ml.market_segment
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features cf
            INNER JOIN mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes ml
                ON cf.user_id = ml.user_id
            ORDER BY ml.customer_lifetime_value DESC
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
    
    def get_filtered_customer_data(
        self, 
        clv_min: Optional[float] = None,
        clv_max: Optional[float] = None,
        vip_min: Optional[float] = None,
        market_segment: Optional[str] = None,
        churn_risk: Optional[int] = None,
        country: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get enriched customer data with filters for CLV, VIP, segment, churn risk, and country.
        
        Args:
            clv_min: Minimum CLV value
            clv_max: Maximum CLV value
            vip_min: Minimum VIP probability
            market_segment: Market segment filter
            churn_risk: Churn status (0 or 1)
            country: Country filter
            limit: Maximum records to return
            
        Returns:
            pd.DataFrame: Filtered customer data with CLV, VIP, and segment info
        """
        # Build WHERE clause conditions
        conditions = []
        
        if clv_min is not None:
            conditions.append(f"ml.customer_lifetime_value >= {clv_min}")
        if clv_max is not None:
            conditions.append(f"ml.customer_lifetime_value <= {clv_max}")
        if vip_min is not None:
            conditions.append(f"ml.vip_customer_probability >= {vip_min}")
        if market_segment:
            conditions.append(f"ml.market_segment = '{market_segment}'")
        if churn_risk is not None:
            conditions.append(f"cf.churn = {churn_risk}")
        if country:
            conditions.append(f"cf.country = '{country}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT 
                cf.user_id,
                cf.firstname,
                cf.lastname,
                cf.email,
                    CASE cf.churn
                    WHEN 1 THEN 'At Risk'
                    WHEN 0 THEN 'Not at Risk'
                    ELSE 'Unknown'
                END as churn_status,
                ml.customer_lifetime_value,
                ml.vip_customer_probability,
                ml.market_segment,
                cf.country,
                cf.gender,
                CASE cf.age_group
                    WHEN 1 THEN '15-19'
                    WHEN 2 THEN '20-24'
                    WHEN 3 THEN '25-29'
                    WHEN 4 THEN '30-34'
                    WHEN 5 THEN '35-39'
                    WHEN 6 THEN '40-49'
                    WHEN 7 THEN '50-59'
                    WHEN 8 THEN '60-69'
                    WHEN 9 THEN '70-79'
                    WHEN 10 THEN '80-100'
                    ELSE 'Unknown'
                END as age_group,
                cf.platform,
                cf.order_count,
                cf.total_amount,
                cf.session_count,
                cf.event_count,
                cf.days_since_creation,
                cf.days_since_last_activity,
                ml.lat,
                ml.lon
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features cf
            INNER JOIN mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes ml
                ON cf.user_id = ml.user_id
            WHERE {where_clause}
            ORDER BY ml.customer_lifetime_value DESC
            LIMIT {limit}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_filter_options(self) -> dict:
        """
        Get available filter options for dropdowns.
        
        Returns:
            dict: Dictionary containing available countries and segments
        """
        countries_query = """
            SELECT DISTINCT country 
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features
            WHERE country IS NOT NULL
            ORDER BY country
        """
        
        segments_query = """
            SELECT DISTINCT market_segment 
            FROM mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes
            WHERE market_segment IS NOT NULL
            ORDER BY market_segment
        """
        
        countries_df = self.sql_service.execute_query_as_dataframe(countries_query)
        segments_df = self.sql_service.execute_query_as_dataframe(segments_query)
        
        return {
            'countries': countries_df['country'].tolist() if not countries_df.empty else [],
            'segments': segments_df['market_segment'].tolist() if not segments_df.empty else []
        }
    
    def get_customer_statistics(
        self,
        clv_min: Optional[float] = None,
        clv_max: Optional[float] = None,
        vip_min: Optional[float] = None,
        market_segment: Optional[str] = None,
        churn_risk: Optional[int] = None,
        country: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get aggregated customer statistics based on filters.
        Returns avg age, avg CLV, total profiles.
        
        Args:
            clv_min: Minimum CLV value
            clv_max: Maximum CLV value
            vip_min: Minimum VIP probability
            market_segment: Market segment filter
            churn_risk: Churn status (0 or 1)
            country: Country filter
            
        Returns:
            pd.DataFrame: Statistics (avg_age, avg_clv, total_profiles)
        """
        # Build WHERE clause conditions
        conditions = []
        
        if clv_min is not None:
            conditions.append(f"ml.customer_lifetime_value >= {clv_min}")
        if clv_max is not None:
            conditions.append(f"ml.customer_lifetime_value <= {clv_max}")
        if vip_min is not None:
            conditions.append(f"ml.vip_customer_probability >= {vip_min}")
        if market_segment:
            conditions.append(f"ml.market_segment = '{market_segment}'")
        if churn_risk is not None:
            conditions.append(f"cf.churn = {churn_risk}")
        if country:
            conditions.append(f"cf.country = '{country}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT 
                COUNT(*) as total_profiles,
                ROUND(AVG(ml.customer_lifetime_value), 2) as avg_clv,
                ROUND(AVG(
                    CASE cf.age_group
                        WHEN 1 THEN 17
                        WHEN 2 THEN 22
                        WHEN 3 THEN 27
                        WHEN 4 THEN 32
                        WHEN 5 THEN 37
                        WHEN 6 THEN 44.5
                        WHEN 7 THEN 54.5
                        WHEN 8 THEN 64.5
                        WHEN 9 THEN 74.5
                        WHEN 10 THEN 90
                        ELSE NULL
                    END
                ), 1) as avg_age
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features cf
            INNER JOIN mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes ml
                ON cf.user_id = ml.user_id
            WHERE {where_clause}
        """
        return self.sql_service.execute_query_as_dataframe(query)
    
    def get_age_gender_distribution(
        self,
        clv_min: Optional[float] = None,
        clv_max: Optional[float] = None,
        vip_min: Optional[float] = None,
        market_segment: Optional[str] = None,
        churn_risk: Optional[int] = None,
        country: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get age and gender distribution for histogram based on filters.
        
        Args:
            clv_min: Minimum CLV value
            clv_max: Maximum CLV value
            vip_min: Minimum VIP probability
            market_segment: Market segment filter
            churn_risk: Churn status (0 or 1)
            country: Country filter
            
        Returns:
            pd.DataFrame: Age midpoints and gender for each customer
        """
        # Build WHERE clause conditions
        conditions = []
        
        if clv_min is not None:
            conditions.append(f"ml.customer_lifetime_value >= {clv_min}")
        if clv_max is not None:
            conditions.append(f"ml.customer_lifetime_value <= {clv_max}")
        if vip_min is not None:
            conditions.append(f"ml.vip_customer_probability >= {vip_min}")
        if market_segment:
            conditions.append(f"ml.market_segment = '{market_segment}'")
        if churn_risk is not None:
            conditions.append(f"cf.churn = {churn_risk}")
        if country:
            conditions.append(f"cf.country = '{country}'")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT 
                CASE cf.age_group
                    WHEN 1 THEN 17
                    WHEN 2 THEN 22
                    WHEN 3 THEN 27
                    WHEN 4 THEN 32
                    WHEN 5 THEN 37
                    WHEN 6 THEN 44.5
                    WHEN 7 THEN 54.5
                    WHEN 8 THEN 64.5
                    WHEN 9 THEN 74.5
                    WHEN 10 THEN 90
                    ELSE NULL
                END as age,
                cf.gender
            FROM mmolony_catalog.dbdemo_customer_churn.churn_features cf
            INNER JOIN mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes ml
                ON cf.user_id = ml.user_id
            WHERE {where_clause}
                AND cf.age_group IS NOT NULL
                AND cf.gender IS NOT NULL
            LIMIT 10000
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

