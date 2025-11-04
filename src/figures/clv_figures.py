"""
CLV (Customer Lifetime Value) Figure Generation

This module contains functions for creating Plotly figures
specific to CLV analysis.
"""

import plotly.express as px
import plotly.graph_objects as go
from config import COLORS


# Common layout defaults for all CLV charts
CHART_LAYOUT_DEFAULTS = {
    'template': 'plotly_white',
    'paper_bgcolor': COLORS['card_background'],
    'plot_bgcolor': COLORS['card_background'],
    'font': {'color': COLORS['text']},
    'height': 400,
    'margin': dict(t=40, b=60, l=60, r=20)
}


def create_clv_distribution_fig(clv_segments_df):
    """Create pie chart for CLV segment distribution."""
    if clv_segments_df.empty:
        return go.Figure()
    
    # Define color gradient for CLV segments (light to dark orange)
    clv_colors = {
        'High Value (≥$2000)': '#d84315',          # Darkest orange
        'Medium-High ($1000-$2000)': '#ff5722',    # Dark orange
        'Medium ($500-$1000)': '#ff7043',          # Medium orange
        'Low-Medium ($200-$500)': '#ff8a65',       # Light orange
        'Low (<$200)': '#ffab91'                   # Lightest orange
    }
    
    fig = px.pie(
        clv_segments_df,
        values='customer_count',
        names='clv_segment',
        title='Customer Distribution by CLV Segment',
        color='clv_segment',
        color_discrete_map=clv_colors,
        hole=0.4
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_clv_by_market_segment_fig(clv_market_df):
    """Create bar chart for average CLV by market segment."""
    if clv_market_df.empty:
        return go.Figure()
    
    fig = px.bar(
        clv_market_df,
        x='market_segment',
        y='avg_clv',
        title='Average CLV by Market Segment',
        labels={'avg_clv': 'Average CLV ($)', 'market_segment': 'Market Segment'},
        text='avg_clv'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='$%{text:.2f}',
        textposition='outside'
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_clv_by_country_fig(clv_country_df):
    """Create bar chart for total CLV by country."""
    if clv_country_df.empty:
        return go.Figure()
    
    fig = px.bar(
        clv_country_df,
        x='country',
        y='total_clv',
        title='Total CLV by Country',
        labels={'total_clv': 'Total CLV ($)', 'country': 'Country'},
        text='total_clv'
    )
    fig.update_traces(
        marker_color=COLORS['accent'],
        texttemplate='$%{text:,.0f}',
        textposition='outside'
    )
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_vip_distribution_fig(vip_segments_df):
    """Create bar chart for VIP probability segments."""
    if vip_segments_df.empty:
        return go.Figure()
    
    # Define color gradient for VIP segments
    vip_colors = ['#d84315', '#ff5722', '#ff7043', '#ffab91']
    
    fig = px.bar(
        vip_segments_df,
        x='vip_segment',
        y='customer_count',
        title='Customer Distribution by VIP Probability',
        labels={'customer_count': 'Number of Customers', 'vip_segment': 'VIP Segment'},
        color='avg_clv',
        color_continuous_scale='Oranges'
    )
    fig.update_layout(
        **CHART_LAYOUT_DEFAULTS,
        showlegend=False
    )
    return fig


def create_clv_vs_spending_scatter(behavioral_df):
    """Create scatter plot for CLV vs historical spending correlation."""
    if behavioral_df.empty:
        return go.Figure()
    
    # Calculate correlation
    corr = behavioral_df['customer_lifetime_value'].corr(behavioral_df['total_amount'])
    
    fig = px.scatter(
        behavioral_df,
        x='total_amount',
        y='customer_lifetime_value',
        title=f'CLV vs Historical Spending (Correlation: {corr:.3f})',
        labels={'total_amount': 'Historical Spending ($)', 'customer_lifetime_value': 'Predicted CLV ($)'},
        opacity=0.6
    )
    fig.update_traces(marker=dict(color=COLORS['accent'], size=5))
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_clv_vs_orders_scatter(behavioral_df):
    """Create box plot for CLV distribution by order count."""
    if behavioral_df.empty:
        return go.Figure()
    
    # Calculate correlation
    corr = behavioral_df['customer_lifetime_value'].corr(behavioral_df['order_count'])
    
    fig = px.box(
        behavioral_df,
        x='order_count',
        y='customer_lifetime_value',
        title=f'CLV Distribution by Order Count (Correlation: {corr:.3f})',
        labels={'order_count': 'Number of Orders', 'customer_lifetime_value': 'Predicted CLV ($)'}
    )
    fig.update_traces(marker_color=COLORS['accent'], line_color=COLORS['accent'])
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig


def create_segment_behavior_fig(segment_behavior_df):
    """Create grouped bar chart showing behavioral metrics by CLV segment."""
    if segment_behavior_df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Add bars for each metric
    fig.add_trace(go.Bar(
        name='Avg Orders',
        x=segment_behavior_df['clv_segment'],
        y=segment_behavior_df['avg_orders'],
        marker_color='#ff7043'
    ))
    
    fig.add_trace(go.Bar(
        name='Avg Sessions',
        x=segment_behavior_df['clv_segment'],
        y=segment_behavior_df['avg_sessions'],
        marker_color='#ffab91'
    ))
    
    fig.update_layout(
        title='Customer Behavior by CLV Segment',
        xaxis_title='CLV Segment',
        yaxis_title='Average Count',
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


def create_clv_segment_value_fig(clv_segments_df):
    """Create bar chart showing total CLV value by segment."""
    if clv_segments_df.empty:
        return go.Figure()
    
    fig = px.bar(
        clv_segments_df,
        x='clv_segment',
        y='total_clv_value',
        title='Total CLV Value by Customer Segment',
        labels={'total_clv_value': 'Total CLV Value ($)', 'clv_segment': 'CLV Segment'},
        text='total_clv_value'
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
