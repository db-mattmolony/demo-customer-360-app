"""
Application configuration and constants.
"""

# Database configuration
DB_CATALOG = 'mmolony_catalog'
DB_SCHEMA = 'dbdemo_customer_churn'

# Helper function to build fully qualified table names
def get_table_name(table: str) -> str:
    """
    Build a fully qualified table name from catalog, schema, and table.
    
    Args:
        table: The table name
        
    Returns:
        Fully qualified table name: catalog.schema.table
    """
    return f"{DB_CATALOG}.{DB_SCHEMA}.{table}"

# Color scheme matching Betashares branding (Orange theme)
COLORS = {
    'background': '#f5f5f5',
    'card_background': '#ffffff',
    'text': '#1a1a1a',
    'text_secondary': '#666666',
    'accent': '#ff5722',
    'accent_light': '#ff7043',
    'border': '#e0e0e0'
}

# Chart layout defaults
CHART_LAYOUT_DEFAULTS = {
    'template': 'plotly_white',
    'paper_bgcolor': COLORS['card_background'],
    'plot_bgcolor': COLORS['card_background'],
    'font': {'color': COLORS['text']},
    'height': 400,
    'margin': dict(t=40, b=60, l=60, r=20)
}

# App metadata
APP_TITLE = 'Customer 360 Dashboard'
APP_DESCRIPTION = 'Comprehensive customer analytics and insights'

# Age group level to age range mapping
AGE_RANGE_MAP = {
    1: '15-19',
    2: '20-24',
    3: '25-29',
    4: '30-34',
    5: '35-39',
    6: '40-49',
    7: '50-59',
    8: '60-69',
    9: '70-79',
    10: '80-100'
}

# Risk category color scheme (Binary: At Risk vs Not at Risk)
# Green for healthy, Orange for at-risk
RISK_COLORS = {
    'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
    'At Risk': '#ff5722',               # Orange (accent) - at risk (churn=1)
    'Unknown': '#9e9e9e'                # Gray
}



