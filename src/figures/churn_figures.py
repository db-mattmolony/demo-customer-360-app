"""
Chart creation functions for the Customer Churn tab.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from config import COLORS, CHART_LAYOUT_DEFAULTS, AGE_RANGE_MAP


def create_country_churn_fig(churn_by_country_df):
    """Create stacked bar chart showing At Risk vs Not at Risk by country."""
    if churn_by_country_df.empty:
        return go.Figure()
    
    # Simple binary color mapping: Green for healthy, Orange for at-risk
    risk_colors = {
        'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
        'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
        'Unknown': '#9e9e9e'                # Gray
    }
    
    fig = px.bar(
        churn_by_country_df,
        x='country',
        y='customer_count',
        color='risk_category',
        title='Customer Status by Country',
        labels={'customer_count': 'Number of Customers', 'country': 'Country', 'risk_category': 'Status'},
        color_discrete_map=risk_colors,
        category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        barmode='stack',
        legend=dict(
            title='Status',
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        )
    )
    return fig


def create_platform_churn_fig(churn_by_platform_df):
    """Create stacked bar chart for platform churn analysis."""
    fig = go.Figure()
    
    if not churn_by_platform_df.empty:
        fig.add_trace(go.Bar(
            x=churn_by_platform_df['platform'],
            y=churn_by_platform_df['churned_customers'],
            name='At Risk',
            marker_color=COLORS['accent']  # Orange for at-risk
        ))
        fig.add_trace(go.Bar(
            x=churn_by_platform_df['platform'],
            y=churn_by_platform_df['active_customers'],
            name='Not at Risk',
            marker_color='#7FB800'  # Green for healthy
        ))
    
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        title='Customer Status by Platform',
        xaxis_title='Platform',
        yaxis_title='Number of Customers',
        barmode='stack',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    return fig


def create_risk_distribution_fig(risk_distribution_df):
    """Create pie chart for binary customer risk distribution (At Risk vs Not at Risk)."""
    if risk_distribution_df.empty:
        return go.Figure()
    
    # Simple binary color mapping
    color_map = {
        'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
        'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
        'Unknown': '#9e9e9e'                # Gray
    }
    
    fig = px.pie(
        risk_distribution_df,
        values='customer_count',
        names='risk_category',
        title='Customer Status Distribution',
        color='risk_category',
        color_discrete_map=color_map,
        hole=0.4
    )
    fig.update_layout(
        template='plotly_white',
        paper_bgcolor=COLORS['card_background'],
        plot_bgcolor=COLORS['card_background'],
        font={'color': COLORS['text']},
        height=400,
        margin=dict(t=40, b=20, l=20, r=20)
    )
    return fig


def create_timeline_fig(churn_timeline_df):
    """Create line chart for churn timeline analysis (standard deviation-based buckets)."""
    if churn_timeline_df.empty:
        return go.Figure()
    
    # Create custom hover text with additional statistics
    hover_template = (
        '<b>%{x}</b><br>'
        'Churn Rate: %{y:.2f}%<br>'
        'Customers: %{customdata[0]:,}<br>'
        'Avg Days: %{customdata[1]:.0f}<br>'
        'Range: %{customdata[2]:.0f} - %{customdata[3]:.0f} days<br>'
        '<extra></extra>'
    )
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=churn_timeline_df['customer_tenure'],
        y=churn_timeline_df['churn_rate_pct'],
        mode='lines+markers',
        line=dict(color=COLORS['accent'], width=3),
        marker=dict(size=10, color=COLORS['accent']),
        customdata=churn_timeline_df[['total_customers', 'avg_days', 'min_days', 'max_days']].values,
        hovertemplate=hover_template,
        name='Churn Rate'
    ))
    
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        title='Churn Rate by Customer Tenure (SD-Based)',
        xaxis_title='Customer Tenure',
        yaxis_title='Churn Rate (%)',
        xaxis={'tickangle': -30}
    )
    return fig


def create_age_churn_fig(churn_by_age_df):
    """Create stacked bar chart showing At Risk vs Not at Risk by age group."""
    if churn_by_age_df.empty:
        return go.Figure()
    
    # Create a copy and map age groups to ranges using centralized mapping
    df = churn_by_age_df.copy()
    df['age_range'] = df['age_group'].map(AGE_RANGE_MAP).fillna(df['age_group'].astype(str))
    
    # Simple binary color mapping: Green for healthy, Orange for at-risk
    risk_colors = {
        'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
        'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
        'Unknown': '#9e9e9e'                # Gray
    }
    
    fig = px.bar(
        df,
        x='age_range',
        y='customer_count',
        color='risk_category',
        title='Customer Status by Age Group',
        labels={'customer_count': 'Number of Customers', 'age_range': 'Age Range', 'risk_category': 'Status'},
        color_discrete_map=risk_colors,
        category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        barmode='stack',
        legend=dict(
            title='Status',
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        )
    )
    return fig


def create_engagement_fig(churn_engagement_df):
    """Create stacked bar chart showing At Risk vs Not at Risk by engagement level."""
    if churn_engagement_df.empty:
        return go.Figure()
    
    # Simple binary color mapping: Green for healthy, Orange for at-risk
    risk_colors = {
        'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
        'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
        'Unknown': '#9e9e9e'                # Gray
    }
    
    fig = px.bar(
        churn_engagement_df,
        x='engagement_level',
        y='customer_count',
        color='risk_category',
        title='Customer Status by Engagement Level',
        labels={'customer_count': 'Number of Customers', 'engagement_level': 'Engagement Quartile', 'risk_category': 'Status'},
        color_discrete_map=risk_colors,
        category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        barmode='stack',
        legend=dict(
            title='Status',
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        )
    )
    return fig

