"""
Data loading and service initialization.
"""

import pandas as pd
import numpy as np
from services.C360Service import get_c360_service
from services.CustomerChurnService import get_churn_service


class DataLoader:
    """Centralized data loading for the application."""
    
    def __init__(self):
        # Initialize services
        self.c360_service = get_c360_service()
        self.churn_service = get_churn_service()
        
        # Initialize data containers
        self.churn_features_df = pd.DataFrame()
        self.churn_summary_df = pd.DataFrame()
        self.churn_by_country_df = pd.DataFrame()
        self.churn_by_platform_df = pd.DataFrame()
        self.churn_by_age_df = pd.DataFrame()
        self.churn_by_gender_df = pd.DataFrame()
        self.top_at_risk_df = pd.DataFrame()
        self.customers_at_risk_df = pd.DataFrame()
        self.risk_distribution_df = pd.DataFrame()
        self.churn_timeline_df = pd.DataFrame()
        self.churn_engagement_df = pd.DataFrame()
        
        # Sample data
        self.customer_demo = None
        self.clv_data = None
        self.vip_data = None
        self.segment_data = None
        self.churn_data = None
    
    def load_all_data(self):
        """Load all data from services and generate sample data."""
        self._load_churn_features()
        self._load_churn_analysis()
        self._generate_sample_data()
    
    def _load_churn_features(self):
        """Load churn features data from C360 service."""
        try:
            self.churn_features_df = self.c360_service.get_all_churn_features()
            self.churn_summary_df = self.c360_service.get_churn_summary()
        except Exception as e:
            print(f"Error loading churn features: {e}")
    
    def _load_churn_analysis(self):
        """Load churn analysis data from CustomerChurn service."""
        try:
            self.churn_by_country_df = self.churn_service.get_churn_by_country()
            self.churn_by_platform_df = self.churn_service.get_churn_by_platform()
            self.churn_by_age_df = self.churn_service.get_churn_by_age_group()
            self.churn_by_gender_df = self.churn_service.get_churn_by_gender()
            self.top_at_risk_df = self.churn_service.get_top_customers_likely_to_churn(limit=10)
            self.customers_at_risk_df = self.churn_service.get_total_customers_at_risk()
            self.risk_distribution_df = self.churn_service.get_churn_risk_distribution()
            self.churn_timeline_df = self.churn_service.get_churn_timeline_analysis()
            self.churn_engagement_df = self.churn_service.get_churn_by_engagement_level()
        except Exception as e:
            print(f"Error loading churn analysis data: {e}")
    
    def _generate_sample_data(self):
        """Generate sample data for demo purposes."""
        np.random.seed(42)
        
        # Customer demographic data for Customer 360 tab
        self.customer_demo = pd.DataFrame({
            'customer_id': range(1, 101),
            'age': np.random.randint(18, 75, 100),
            'gender': np.random.choice(['Male', 'Female', 'Other'], 100),
            'income': np.random.randint(30000, 150000, 100),
            'state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL'], 100)
        })
        
        # CLV data
        self.clv_data = pd.DataFrame({
            'customer_id': range(1, 51),
            'customer_name': [f'Customer {i}' for i in range(1, 51)],
            'lifetime_value': np.random.randint(500, 10000, 50)
        })
        
        # VIP customers data
        self.vip_data = pd.DataFrame({
            'customer_name': [f'VIP {i}' for i in range(1, 21)],
            'total_spent': np.random.randint(15000, 50000, 20),
            'membership_years': np.random.randint(3, 15, 20)
        })
        
        # Customer segmentation data
        self.segment_data = pd.DataFrame({
            'segment': ['High Value', 'Medium Value', 'Low Value', 'At Risk', 'New'],
            'count': [250, 450, 300, 150, 200]
        })
        
        # Churn data
        self.churn_data = pd.DataFrame({
            'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
            'churn_rate': [5.2, 4.8, 6.1, 5.5, 4.9, 5.8]
        })


# Singleton instance
_data_loader = None


def get_data_loader() -> DataLoader:
    """Get or create the singleton DataLoader instance."""
    global _data_loader
    if _data_loader is None:
        _data_loader = DataLoader()
        _data_loader.load_all_data()
    return _data_loader

