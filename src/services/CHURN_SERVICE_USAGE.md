# CustomerChurnService Usage Guide

## Overview
The `CustomerChurnService` provides specialized methods for analyzing customer churn patterns across various demographic and behavioral attributes.

## Quick Start

```python
from services.CustomerChurnService import get_churn_service

# Get the service instance
churn_service = get_churn_service()

# Get churn analysis by country
country_churn = churn_service.get_churn_by_country()
```

## Available Methods

### 1. Demographic Analysis

#### `get_churn_by_country()` → DataFrame
Analyze churn rates by country with revenue metrics.

**Returns:** country, total_customers, churned_customers, active_customers, churn_rate_pct, avg_revenue, avg_orders

**Use for:** Geographic churn visualization, country-level strategies

---

#### `get_churn_by_platform()` → DataFrame
Analyze churn patterns across different platforms.

**Returns:** platform, total_customers, churned_customers, active_customers, churn_rate_pct, avg_sessions, avg_events

**Use for:** Platform comparison charts, platform-specific interventions

---

#### `get_churn_by_age_group()` → DataFrame
Churn analysis by age demographics.

**Returns:** age_group, total_customers, churned_customers, active_customers, churn_rate_pct

**Use for:** Age-based segmentation visualizations

---

#### `get_churn_by_gender()` → DataFrame
Churn breakdown by gender.

**Returns:** gender, total_customers, churned_customers, active_customers, churn_rate_pct

**Use for:** Gender-based churn comparison

---

#### `get_churn_by_canal()` → DataFrame
Analyze churn by acquisition channel.

**Returns:** canal, total_customers, churned_customers, active_customers, churn_rate_pct

**Use for:** Channel effectiveness analysis

---

### 2. Risk Assessment

#### `get_top_customers_likely_to_churn(limit=10)` → DataFrame
Identify customers at highest risk of churning.

**Args:** 
- limit: Number of top at-risk customers (default: 10)

**Returns:** user_id, email, firstname, lastname, country, platform, days_since_last_activity, order_count, total_amount, session_count, event_count, last_activity_date, risk_level

**Use for:** Customer retention dashboards, intervention targeting

---

#### `get_total_customers_at_risk(days_threshold=30)` → DataFrame
Get summary counts of at-risk customers.

**Args:**
- days_threshold: Days of inactivity threshold (default: 30)

**Returns:** total_at_risk, critical_risk, high_risk, medium_risk, avg_revenue_at_risk, total_revenue_at_risk

**Use for:** Executive dashboards, KPI cards, alert systems

---

#### `get_churn_risk_distribution()` → DataFrame
Overall risk distribution across customer base.

**Returns:** risk_category, customer_count, avg_revenue, total_revenue

**Risk Categories:** Churned, Critical Risk, High Risk, Medium Risk, Low Risk

**Use for:** Pie charts, risk distribution visualizations

---

### 3. Behavioral Analysis

#### `get_churn_timeline_analysis()` → DataFrame
Churn patterns by customer tenure.

**Returns:** customer_tenure, total_customers, churned_customers, churn_rate_pct

**Tenure Buckets:** 0-30 days, 30-90 days, 91-180 days, 181-365 days, 365+ days

**Use for:** Lifecycle churn analysis, onboarding effectiveness

---

#### `get_churn_by_engagement_level()` → DataFrame
Churn by customer engagement quartiles.

**Returns:** engagement_quartile, total_customers, churned_customers, churn_rate_pct, avg_sessions, avg_events, avg_orders

**Quartiles:** Q1 (lowest engagement) to Q4 (highest engagement)

**Use for:** Engagement-churn correlation charts

---

## Example Usage in Dash App

```python
from services.CustomerChurnService import get_churn_service
import plotly.express as px

churn_service = get_churn_service()

# Country churn visualization
country_data = churn_service.get_churn_by_country()
fig = px.bar(country_data, 
             x='country', 
             y='churn_rate_pct',
             color='total_customers',
             title='Churn Rate by Country')

# Top at-risk customers table
at_risk = churn_service.get_top_customers_likely_to_churn(limit=20)
# Display in dash_table.DataTable

# Risk summary metrics
risk_summary = churn_service.get_total_customers_at_risk()
critical_count = risk_summary['critical_risk'].iloc[0]
# Display in metric cards
```

## Visualization Examples

### 1. Customer Risk Distribution by Country (Stacked Bar Chart)
```python
country_risk = churn_service.get_churn_by_country()
# Returns customer counts broken down by country and risk_category
# Columns: country, risk_category, customer_count
# Risk categories: Not at Risk, Low Risk, Medium Risk, High Risk, Critical Risk

fig = px.bar(country_risk, 
             x='country', 
             y='customer_count',
             color='risk_category',
             title='Customer Risk Distribution by Country',
             barmode='stack')
```

### 2. Platform Churn Comparison
```python
platform_churn = churn_service.get_churn_by_platform()
fig = px.bar(platform_churn,
             x='platform',
             y='churn_rate_pct',
             title='Churn Rate by Platform (%)',
             color='churn_rate_pct',
             color_continuous_scale='Reds')
```

### 3. Risk Distribution Pie Chart (All Customer Segments)
```python
risk_dist = churn_service.get_churn_risk_distribution()
# Returns all customer segments:
# NOTE: churn=1 means "at risk", churn=0 means "not at risk"
# - Not at Risk (churn=0, healthy customers)
# - Low Risk (churn=1, < 14 days inactive)
# - Medium Risk (churn=1, 14-29 days inactive)
# - High Risk (churn=1, 30-60 days inactive)
# - Critical Risk (churn=1, > 60 days inactive)

fig = px.pie(risk_dist,
             values='customer_count',
             names='risk_category',
             title='Customer Risk Distribution')
```

### 4. Timeline Analysis Line Chart (Standard Deviation-Based)
```python
timeline = churn_service.get_churn_timeline_analysis()
# Returns data with columns: customer_tenure, total_customers, churned_customers, 
# churn_rate_pct, min_days, max_days, avg_days
# Customer tenure buckets are based on standard deviation from mean:
# - Very New (< -1 SD)
# - New (-1 SD to Mean)
# - Established (Mean to +1 SD)
# - Mature (+1 to +2 SD)
# - Veteran (> +2 SD)

fig = px.line(timeline,
              x='customer_tenure',
              y='churn_rate_pct',
              title='Churn Rate by Customer Tenure (SD-Based)',
              markers=True)
```

## Integration with Existing Services

The CustomerChurnService uses SQLService for data access:

```python
# Direct SQL query if needed
custom_query = """
    SELECT country, COUNT(*) 
    FROM mmolony_catalog.dbdemo_customer_churn.churn_features
    GROUP BY country
"""
custom_data = churn_service.sql_service.execute_query_as_dataframe(custom_query)
```

## Best Practices

1. **Cache Results**: Store service results in app state to avoid repeated queries
2. **Use Appropriate Methods**: Choose the right granularity for your visualization
3. **Handle Empty DataFrames**: Always check `if not df.empty:` before visualizing
4. **Combine Methods**: Create comprehensive dashboards by combining multiple service calls
5. **Monitor Performance**: Large datasets may benefit from pagination or filtering

