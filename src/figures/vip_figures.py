"""
VIP Customer Figure Generation

This module contains functions for creating Plotly figures
specific to VIP customer analysis.
"""

import plotly.express as px
import plotly.graph_objects as go
from config import COLORS


# Common layout defaults for all VIP charts
CHART_LAYOUT_DEFAULTS = {
    'template': 'plotly_white',
    'paper_bgcolor': COLORS['card_background'],
    'plot_bgcolor': COLORS['card_background'],
    'font': {'color': COLORS['text']},
    'height': 400,
    'margin': dict(t=40, b=60, l=60, r=20)
}


def create_vip_tier_distribution_fig(vip_tiers_df):
    """Create pie chart for VIP tier distribution."""
    if vip_tiers_df.empty:
        return go.Figure()
    
    # Define custom color palette for VIP tiers
    vip_colors = {
        'Platinum VIP (0.9+)': '#0D2C54',           # Dark Blue/Navy
        'Gold VIP (0.7-0.9)': '#7FB800',            # Green
        'Silver VIP (0.5-0.7)': '#00A6ED',          # Blue
        'Bronze VIP (0.3-0.5)': '#FFB400',          # Yellow/Gold
        'Standard (<0.3)': '#F35B27'               # Orange
    }
    
    fig = px.pie(
        vip_tiers_df,
        values='customer_count',
        names='vip_tier',
        title='Customer Distribution by VIP Tier',
        color='vip_tier',
        color_discrete_map=vip_colors,
        hole=0.4
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_vip_clv_by_tier_fig(vip_tiers_df):
    """Create bar chart for average CLV by VIP tier."""
    if vip_tiers_df.empty:
        return go.Figure()
    
    fig = px.bar(
        vip_tiers_df,
        x='vip_tier',
        y='avg_clv',
        title='Average CLV by VIP Tier',
        labels={'avg_clv': 'Average CLV ($)', 'vip_tier': 'VIP Tier'},
        text='avg_clv'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='$%{text:,.0f}',
        textposition='outside'
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        xaxis={'tickangle': -30}
    )
    return fig


def create_vip_by_segment_fig(vip_segment_df):
    """Create bar chart for VIP rate by market segment."""
    if vip_segment_df.empty:
        return go.Figure()
    
    fig = px.bar(
        vip_segment_df,
        x='market_segment',
        y='vip_rate_pct',
        title='VIP Rate by Market Segment',
        labels={'vip_rate_pct': 'VIP Rate (%)', 'market_segment': 'Market Segment'},
        text='vip_rate_pct'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='%{text:.1f}%',
        textposition='outside'
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_vip_by_country_fig(vip_country_df):
    """Create bar chart for VIP customers by country."""
    if vip_country_df.empty:
        return go.Figure()
    
    fig = px.bar(
        vip_country_df,
        x='country',
        y='vip_customers',
        title='VIP Customers by Country',
        labels={'vip_customers': 'Number of VIP Customers', 'country': 'Country'},
        text='vip_customers'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='%{text:,}',
        textposition='outside'
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_vip_clv_correlation_fig(correlation_df):
    """Create scatter plot for VIP probability vs CLV correlation."""
    if correlation_df.empty:
        return go.Figure()
    
    # Calculate correlation
    corr = correlation_df['vip_customer_probability'].corr(correlation_df['customer_lifetime_value'])
    
    # Custom color map for market segments
    segment_colors = {
        'Blue Chip': '#00A6ED',           # Blue
        'Crypto': '#FFB400',              # Gold/Yellow
        'Social Impact': '#7FB800',       # Green
        'Sustainability Focused': COLORS['accent']  # Orange
    }
    
    fig = px.scatter(
        correlation_df,
        x='vip_customer_probability',
        y='customer_lifetime_value',
        color='market_segment',
        title=f'VIP Probability vs CLV (Correlation: {corr:.3f})',
        labels={'vip_customer_probability': 'VIP Probability', 'customer_lifetime_value': 'Customer Lifetime Value ($)'},
        opacity=0.6,
        color_discrete_map=segment_colors
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        )
    )
    return fig


def create_vip_behavioral_comparison_fig(behavioral_df):
    """Create grouped bar chart comparing VIP vs non-VIP behavior."""
    if behavioral_df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Add bars for each metric
    metrics = [
        ('avg_spending', 'Avg Spending ($)', '#ff7043'),
        ('avg_orders', 'Avg Orders', '#ffab91'),
        ('avg_sessions', 'Avg Sessions', '#ffccbc')
    ]
    
    for metric, name, color in metrics:
        fig.add_trace(go.Bar(
            name=name,
            x=behavioral_df['vip_status'],
            y=behavioral_df[metric],
            marker_color=color,
            text=behavioral_df[metric].round(1),
            textposition='outside',
            texttemplate='%{text}'
        ))
    
    fig.update_layout(
        title='VIP vs Non-VIP Customer Behavior',
        xaxis_title='Customer Type',
        yaxis_title='Value',
        barmode='group',
        **CHART_LAYOUT_DEFAULTS,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        )
    )
    return fig


def create_vip_value_concentration_fig(vip_tiers_df):
    """Create stacked bar showing total CLV value concentration by tier."""
    if vip_tiers_df.empty:
        return go.Figure()
    
    fig = px.bar(
        vip_tiers_df,
        x='vip_tier',
        y='total_clv',
        title='Total CLV Value by VIP Tier',
        labels={'total_clv': 'Total CLV Value ($)', 'vip_tier': 'VIP Tier'},
        text='total_clv'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='$%{text:,.0f}',
        textposition='outside'
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        xaxis={'tickangle': -30}
    )
    return fig

