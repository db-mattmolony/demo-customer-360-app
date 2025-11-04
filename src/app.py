"""
Customer 360 Dash Application
A multi-tab dashboard for comprehensive customer analytics.
"""

from dash import Dash, html, dcc, callback, Output, Input, State, dash_table
import dash
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowResponse
import os
from pathlib import Path

# Load environment variables if .env.local exists
env_local_path = Path(__file__).parent.parent / '.env.local'
if env_local_path.exists():
    with open(env_local_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value

from services.C360Service import get_c360_service
from services.CustomerChurnService import get_churn_service
from services.CLVService import get_clv_service
from services.SegmentationService import get_segmentation_service
from services.VIPService import get_vip_service
from segment_profiles import SEGMENT_PROFILES

# Initialize the Dash app
app = Dash(__name__, assets_folder='assets')

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

# Helper function to create customer map using Plotly
def create_customer_map(df):
    """
    Create a Plotly Scattergeo map with customer locations.
    
    Args:
        df: DataFrame with lat, lon, and customer info columns
        
    Returns:
        plotly.graph_objects.Figure: Interactive map figure
    """
    if df.empty or 'lat' not in df.columns or 'lon' not in df.columns:
        # Return empty map
        fig = go.Figure()
        fig.add_annotation(
            text="No customer data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color=COLORS['text_secondary'])
        )
        fig.update_layout(
            template='plotly_white',
            paper_bgcolor=COLORS['card_background'],
            height=500,
            margin=dict(t=0, b=0, l=0, r=0)
        )
        return fig
    
    # Filter out rows with null lat/lon
    map_df = df.dropna(subset=['lat', 'lon']).copy()
    
    if map_df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No location data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color=COLORS['text_secondary'])
        )
        fig.update_layout(
            template='plotly_white',
            paper_bgcolor=COLORS['card_background'],
            height=500,
            margin=dict(t=0, b=0, l=0, r=0)
        )
        return fig
    
    # Add risk status and color (using churn_status from SQL)
    map_df['risk_status'] = map_df['churn_status'].apply(lambda x: f"🔴 {x}" if x == 'At Risk' else f"🟢 {x}")
    map_df['marker_color'] = map_df['churn_status'].apply(lambda x: COLORS['accent'] if x == 'At Risk' else '#7FB800')
    
    # Create hover text
    map_df['hover_text'] = map_df.apply(lambda row: 
        f"<b>{row.get('firstname', 'N/A')} {row.get('lastname', 'N/A')}</b><br>" +
        f"Email: {row.get('email', 'N/A')}<br>" +
        f"Country: {row.get('country', 'N/A')}<br>" +
        f"Status: {row['risk_status']}<br>" +
        f"<br>" +
        f"CLV: ${row.get('customer_lifetime_value', 0):,.2f}<br>" +
        f"VIP Probability: {row.get('vip_customer_probability', 0):.1%}<br>" +
        f"Segment: {row.get('market_segment', 'N/A')}<br>" +
        f"Orders: {row.get('order_count', 0)}<br>" +
        f"Total Spent: ${row.get('total_amount', 0):,.2f}",
        axis=1
    )
    
    # Create the map
    fig = go.Figure()
    
    # Add traces for each risk category
    for risk_status in map_df['risk_status'].unique():
        mask = map_df['risk_status'] == risk_status
        filtered = map_df[mask]
        
        fig.add_trace(go.Scattergeo(
            lon=filtered['lon'],
            lat=filtered['lat'],
            text=filtered['hover_text'],
            mode='markers',
            marker=dict(
                size=10,
                color=filtered['marker_color'].iloc[0],
                line=dict(width=1, color='white'),
                opacity=0.8
            ),
            name=risk_status,
            hovertemplate='%{text}<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        geo=dict(
            scope='world',
            showland=True,
            landcolor='rgb(243, 243, 243)',
            coastlinecolor='rgb(204, 204, 204)',
            projection_type='natural earth',
            showlakes=True,
            lakecolor='rgb(255, 255, 255)',
            showcountries=True,
            countrycolor='rgb(204, 204, 204)'
        ),
        template='plotly_white',
        paper_bgcolor=COLORS['card_background'],
        height=500,
        margin=dict(t=0, b=0, l=0, r=0),
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=0.02,
            xanchor='right',
            x=0.98,
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor=COLORS['border'],
            borderwidth=1
        )
    )
    
    return fig

# Initialize services
c360_service = get_c360_service()
churn_service = get_churn_service()
clv_service = get_clv_service()
segmentation_service = get_segmentation_service()
vip_service = get_vip_service()

# Initialize empty dataframe for C360 tab (data loads on first filter application)
try:
    # Don't load table data initially - only load filter options
    churn_features_df = pd.DataFrame()  # Empty - will load on Apply Filters click
    churn_summary_df = c360_service.get_churn_summary()
    filter_options = c360_service.get_filter_options()
    # Create empty initial map
    initial_map_fig = create_customer_map(pd.DataFrame())
except Exception as e:
    print(f"Error loading filter options: {e}")
    churn_features_df = pd.DataFrame()
    churn_summary_df = pd.DataFrame()
    filter_options = {'countries': [], 'segments': []}
    initial_map_fig = create_customer_map(pd.DataFrame())

# Fetch churn analysis data
try:
    churn_by_country_df = churn_service.get_churn_by_country()
    churn_by_platform_df = churn_service.get_churn_by_platform()
    churn_by_age_df = churn_service.get_churn_by_age_group()
    churn_by_gender_df = churn_service.get_churn_by_gender()
    top_at_risk_df = churn_service.get_top_customers_likely_to_churn(limit=10)
    customers_at_risk_df = churn_service.get_total_customers_at_risk()
    risk_distribution_df = churn_service.get_churn_risk_distribution()
    churn_timeline_df = churn_service.get_churn_timeline_analysis()
    churn_engagement_df = churn_service.get_churn_by_engagement_level()
except Exception as e:
    print(f"Error loading churn analysis data: {e}")
    churn_by_country_df = pd.DataFrame()
    churn_by_platform_df = pd.DataFrame()
    churn_by_age_df = pd.DataFrame()
    churn_by_gender_df = pd.DataFrame()
    top_at_risk_df = pd.DataFrame()
    customers_at_risk_df = pd.DataFrame()
    risk_distribution_df = pd.DataFrame()
    churn_timeline_df = pd.DataFrame()
    churn_engagement_df = pd.DataFrame()

# Fetch CLV analysis data
try:
    clv_statistics_df = clv_service.get_clv_statistics()
    clv_segments_df = clv_service.get_clv_segments()
    clv_by_market_df = clv_service.get_clv_by_market_segment()
    clv_by_country_df = clv_service.get_clv_by_country()
    clv_behavioral_df = clv_service.get_clv_with_behavioral_metrics()
    vip_segments_df = clv_service.get_vip_segments()
    top_clv_customers_df = clv_service.get_top_clv_customers(limit=10)
    clv_segment_behavior_df = clv_service.get_clv_segments_with_behavior()
except Exception as e:
    print(f"Error loading CLV data: {e}")
    clv_statistics_df = pd.DataFrame()
    clv_segments_df = pd.DataFrame()
    clv_by_market_df = pd.DataFrame()
    clv_by_country_df = pd.DataFrame()
    clv_behavioral_df = pd.DataFrame()
    vip_segments_df = pd.DataFrame()
    top_clv_customers_df = pd.DataFrame()
    clv_segment_behavior_df = pd.DataFrame()

# Fetch Segmentation analysis data (lightweight overview only on initial load)
try:
    segment_overview_df = segmentation_service.get_segment_overview()
except Exception as e:
    print(f"Error loading segmentation overview: {e}")
    segment_overview_df = pd.DataFrame()

# Note: Heavy queries (behavioral analysis) are loaded lazily when segmentation tab is viewed

# Fetch VIP customer analysis data
try:
    vip_overview_df = vip_service.get_vip_overview()
    vip_tiers_df = vip_service.get_vip_tiers()
    vip_by_segment_df = vip_service.get_vip_by_market_segment()
    vip_by_country_df = vip_service.get_vip_by_country()
    top_vip_customers_df = vip_service.get_top_vip_customers(limit=20)
    vip_clv_correlation_df = vip_service.get_vip_clv_correlation()
    vip_behavioral_df = vip_service.get_vip_behavioral_metrics()
except Exception as e:
    print(f"Error loading VIP data: {e}")
    vip_overview_df = pd.DataFrame()
    vip_tiers_df = pd.DataFrame()
    vip_by_segment_df = pd.DataFrame()
    vip_by_country_df = pd.DataFrame()
    top_vip_customers_df = pd.DataFrame()
    vip_clv_correlation_df = pd.DataFrame()
    vip_behavioral_df = pd.DataFrame()

# Sample data generation
np.random.seed(42)

# Customer demographic data for Customer 360 tab
customer_demo = pd.DataFrame({
    'customer_id': range(1, 101),
    'age': np.random.randint(18, 75, 100),
    'gender': np.random.choice(['Male', 'Female', 'Other'], 100),
    'income': np.random.randint(30000, 150000, 100),
    'state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'IL'], 100)
})

# CLV figures (imported from figures module for consistency)
from figures.clv_figures import (
    create_clv_distribution_fig,
    create_clv_by_market_segment_fig,
    create_clv_by_country_fig,
    create_vip_distribution_fig,
    create_clv_vs_spending_scatter,
    create_clv_vs_orders_scatter,
    create_segment_behavior_fig,
    create_clv_segment_value_fig
)

# VIP figures
from figures.vip_figures import (
    create_vip_tier_distribution_fig,
    create_vip_clv_by_tier_fig,
    create_vip_by_segment_fig,
    create_vip_by_country_fig,
    create_vip_clv_correlation_fig,
    create_vip_behavioral_comparison_fig,
    create_vip_value_concentration_fig
)

# Create CLV figures
clv_distribution_fig = create_clv_distribution_fig(clv_segments_df)
clv_market_fig = create_clv_by_market_segment_fig(clv_by_market_df)
clv_country_fig = create_clv_by_country_fig(clv_by_country_df)
vip_dist_fig = create_vip_distribution_fig(vip_segments_df)
clv_spending_scatter = create_clv_vs_spending_scatter(clv_behavioral_df)
clv_orders_scatter = create_clv_vs_orders_scatter(clv_behavioral_df)
segment_behavior_fig = create_segment_behavior_fig(clv_segment_behavior_df)
clv_value_fig = create_clv_segment_value_fig(clv_segments_df)

# Create VIP figures
vip_tier_dist_fig = create_vip_tier_distribution_fig(vip_tiers_df)
vip_clv_tier_fig = create_vip_clv_by_tier_fig(vip_tiers_df)
vip_segment_fig = create_vip_by_segment_fig(vip_by_segment_df)
vip_country_fig = create_vip_by_country_fig(vip_by_country_df)
vip_correlation_fig = create_vip_clv_correlation_fig(vip_clv_correlation_df)
vip_behavioral_fig = create_vip_behavioral_comparison_fig(vip_behavioral_df)
vip_value_fig = create_vip_value_concentration_fig(vip_tiers_df)

# VIP customers data
vip_data = pd.DataFrame({
    'customer_name': [f'VIP {i}' for i in range(1, 21)],
    'total_spent': np.random.randint(15000, 50000, 20),
    'membership_years': np.random.randint(3, 15, 20)
})

# Customer segmentation data
segment_data = pd.DataFrame({
    'segment': ['High Value', 'Medium Value', 'Low Value', 'At Risk', 'New'],
    'count': [250, 450, 300, 150, 200]
})

# Churn data
churn_data = pd.DataFrame({
    'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
    'churn_rate': [5.2, 4.8, 6.1, 5.5, 4.9, 5.8]
})

# Template for dark theme charts
chart_template = {
    'layout': {
        'paper_bgcolor': COLORS['card_background'],
        'plot_bgcolor': COLORS['card_background'],
        'font': {'color': COLORS['text']},
        'xaxis': {
            'gridcolor': COLORS['border'],
            'linecolor': COLORS['border']
        },
        'yaxis': {
            'gridcolor': COLORS['border'],
            'linecolor': COLORS['border']
        }
    }
}

# Create visualizations with dark theme
# 2. VIP Customers Chart
vip_fig = px.scatter(
    vip_data,
    x='membership_years',
    y='total_spent',
    size='total_spent',
    hover_data=['customer_name'],
    title='VIP Customers: Spend vs Membership Duration',
    labels={'total_spent': 'Total Spent ($)', 'membership_years': 'Years as Member'}
)
vip_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20)
)
vip_fig.update_traces(marker=dict(color=COLORS['accent'], line=dict(width=1, color=COLORS['accent_light'])))

# 3. Customer Segmentation Chart
segment_fig = px.pie(
    segment_data,
    values='count',
    names='segment',
    title='Customer Distribution by Segment',
    hole=0.4,
    color_discrete_sequence=[COLORS['accent'], COLORS['accent_light'], '#ff8a65', '#ff9800', '#ffb74d']
)
segment_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=20, l=20, r=20)
)

# 4. Customer 360 Demographics Chart
demo_fig = px.histogram(
    customer_demo,
    x='age',
    color='gender',
    title='Customer Age Distribution by Gender',
    labels={'age': 'Age', 'count': 'Number of Customers'},
    barmode='overlay',
    opacity=0.8,
    color_discrete_sequence=[COLORS['accent'], COLORS['accent_light'], '#ff8a65']
)
demo_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20)
)

# 5. Churn Rate Chart
churn_fig = go.Figure()
churn_fig.add_trace(go.Scatter(
    x=churn_data['month'],
    y=churn_data['churn_rate'],
    mode='lines+markers',
    name='Churn Rate',
    line=dict(color=COLORS['accent'], width=3),
    marker=dict(size=10, color=COLORS['accent'], line=dict(width=2, color=COLORS['accent_light']))
))
churn_fig.update_layout(
    template='plotly_white',
    title='Monthly Customer Churn Rate',
    xaxis_title='Month',
    yaxis_title='Churn Rate (%)',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20),
    hovermode='x unified'
)

# 6. Customer Status by Country (Binary: At Risk vs Not at Risk)
# Simple binary color mapping
country_risk_colors = {
    'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
    'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
    'Unknown': '#9e9e9e'                # Gray
}

country_churn_fig = px.bar(
    churn_by_country_df if not churn_by_country_df.empty else pd.DataFrame(),
    x='country',
    y='customer_count',
    color='risk_category',
    title='Customer Status by Country',
    labels={'customer_count': 'Number of Customers', 'country': 'Country', 'risk_category': 'Status'},
    color_discrete_map=country_risk_colors,
    category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
)
country_churn_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20),
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

# 7. Churn by Platform Chart
platform_churn_fig = go.Figure()
if not churn_by_platform_df.empty:
    platform_churn_fig.add_trace(go.Bar(
        x=churn_by_platform_df['platform'],
        y=churn_by_platform_df['churned_customers'],
        name='At Risk',
        marker_color=COLORS['accent']  # Orange for at-risk
    ))
    platform_churn_fig.add_trace(go.Bar(
        x=churn_by_platform_df['platform'],
        y=churn_by_platform_df['active_customers'],
        name='Not at Risk',
        marker_color='#7FB800'  # Green for healthy
    ))
platform_churn_fig.update_layout(
    template='plotly_white',
    title='Customer Status by Platform',
    xaxis_title='Platform',
    yaxis_title='Number of Customers',
    barmode='stack',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20),
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
)

# 8. Risk Distribution Pie Chart (Binary: At Risk vs Not at Risk)
# Simple binary color mapping
risk_color_map = {
    'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
    'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
    'Unknown': '#9e9e9e'                # Gray
}

risk_dist_fig = px.pie(
    risk_distribution_df if not risk_distribution_df.empty else pd.DataFrame(),
    values='customer_count',
    names='risk_category',
    title='Customer Status Distribution',
    color='risk_category',
    color_discrete_map=risk_color_map,
    hole=0.4
)
risk_dist_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=20, l=20, r=20)
)

# 9. Churn Timeline Analysis (Standard Deviation-Based Buckets)
if not churn_timeline_df.empty:
    # Create custom hover text with additional statistics
    hover_template = (
        '<b>%{x}</b><br>'
        'Churn Rate: %{y:.2f}%<br>'
        'Customers: %{customdata[0]:,}<br>'
        'Avg Days: %{customdata[1]:.0f}<br>'
        'Range: %{customdata[2]:.0f} - %{customdata[3]:.0f} days<br>'
        '<extra></extra>'
    )
    
    timeline_fig = go.Figure()
    timeline_fig.add_trace(go.Scatter(
        x=churn_timeline_df['customer_tenure'],
        y=churn_timeline_df['churn_rate_pct'],
        mode='lines+markers',
        line=dict(color=COLORS['accent'], width=3),
        marker=dict(size=10, color=COLORS['accent']),
        customdata=churn_timeline_df[['total_customers', 'avg_days', 'min_days', 'max_days']].values,
        hovertemplate=hover_template,
        name='Churn Rate'
    ))
    
    timeline_fig.update_layout(
        template='plotly_white',
        paper_bgcolor=COLORS['card_background'],
        plot_bgcolor=COLORS['card_background'],
        font={'color': COLORS['text']},
        title='Churn Rate by Customer Tenure (SD-Based)',
        xaxis_title='Customer Tenure',
        yaxis_title='Churn Rate (%)',
        height=400,
        margin=dict(t=40, b=60, l=60, r=20),
        xaxis={'tickangle': -30}
    )
else:
    timeline_fig = go.Figure()
    timeline_fig.update_layout(
        template='plotly_white',
        paper_bgcolor=COLORS['card_background'],
        title='Churn Rate by Customer Tenure (No Data)'
    )

# 10. Churn by Age Group
# Map age group levels to descriptive ranges
# (After refactoring, this will use AGE_RANGE_MAP from config.py)
age_range_map = {
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

if not churn_by_age_df.empty:
    churn_by_age_df_mapped = churn_by_age_df.copy()
    churn_by_age_df_mapped['age_range'] = churn_by_age_df_mapped['age_group'].map(age_range_map).fillna(churn_by_age_df_mapped['age_group'].astype(str))
else:
    churn_by_age_df_mapped = pd.DataFrame()

# Simple binary color mapping: Green for healthy, Orange for at-risk
risk_colors = {
    'Not at Risk': '#7FB800',           # Green - healthy (churn=0)
    'At Risk': COLORS['accent'],        # Orange - at risk (churn=1)
    'Unknown': '#9e9e9e'                # Gray
}

age_churn_fig = px.bar(
    churn_by_age_df_mapped,
    x='age_range',
    y='customer_count',
    color='risk_category',
    title='Customer Status by Age Group',
    labels={'customer_count': 'Number of Customers', 'age_range': 'Age Range', 'risk_category': 'Status'},
    color_discrete_map=risk_colors,
    category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
)
age_churn_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20),
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

# 11. Engagement vs Churn
engagement_fig = px.bar(
    churn_engagement_df if not churn_engagement_df.empty else pd.DataFrame(),
    x='engagement_level',
    y='customer_count',
    color='risk_category',
    title='Customer Status by Engagement Level',
    labels={'customer_count': 'Number of Customers', 'engagement_level': 'Engagement Quartile', 'risk_category': 'Status'},
    color_discrete_map=risk_colors,
    category_orders={'risk_category': ['Not at Risk', 'At Risk', 'Unknown']}
)
engagement_fig.update_layout(
    template='plotly_white',
    paper_bgcolor=COLORS['card_background'],
    plot_bgcolor=COLORS['card_background'],
    font={'color': COLORS['text']},
    height=400,
    margin=dict(t=40, b=60, l=60, r=20),
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

# Helper function to create metric cards
def create_metric_card(value, label, color=COLORS['accent']):
    return html.Div([
        html.Div([
            html.H3(value, style={
                'color': COLORS['text'],
                'marginBottom': '5px',
                'fontSize': '32px',
                'fontWeight': '600'
            }),
            html.P(label, style={
                'color': COLORS['text_secondary'],
                'fontSize': '14px',
                'margin': '0'
            })
        ], style={'flex': '1'})
    ], style={
        'flex': '1',
        'padding': '24px',
        'backgroundColor': COLORS['card_background'],
        'borderRadius': '8px',
        'margin': '10px',
        'borderLeft': f'4px solid {color}',
        'boxShadow': '0 2px 8px rgba(0,0,0,0.1)'
    })

# Define the app layout with tabs
app.layout = html.Div([
    # Fixed logo at the top
    html.Div([
        html.Img(src='/assets/Betashares_Logo.svg.png', 
                style={
                    'height': '50px'
                }),
    ], style={
        'position': 'fixed',
        'top': '0',
        'left': '0',
        'right': '0',
        'backgroundColor': 'white',
        'zIndex': '1000',
        'padding': '15px',
        'textAlign': 'center',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
        'borderBottom': f'2px solid {COLORS["accent"]}'
    }),
    
    # Main content with top padding to account for fixed header
    html.Div([
        
        html.H1('Customer 360 Dashboard', 
                style={
                    'textAlign': 'center', 
                    'color': COLORS['accent'], 
                    'marginBottom': '10px',
                    'fontSize': '42px',
                    'fontWeight': '700',
                    'marginTop': '20px'
                }),
        
        html.P('Comprehensive customer analytics and insights',
               style={
                   'textAlign': 'center',
                   'fontSize': '18px',
                   'color': COLORS['text'],
                   'marginBottom': '30px'
               }),
        
        # Tabs component
        dcc.Tabs(id='tabs', value='customer360', children=[
            dcc.Tab(label='Customer 360', value='customer360',
                   style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['text_secondary'],
                       'border': 'none',
                       'borderBottom': f'2px solid {COLORS["border"]}'
                   },
                   selected_style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['accent'],
                       'border': 'none',
                       'borderBottom': f'4px solid {COLORS["accent"]}',
                       'fontWeight': '600'
                   }),
            
            dcc.Tab(label='Customer Lifetime Value (CLV)', value='clv', 
                   style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['text_secondary'],
                       'border': 'none',
                       'borderBottom': f'2px solid {COLORS["border"]}'
                   },
                   selected_style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['accent'],
                       'border': 'none',
                       'borderBottom': f'4px solid {COLORS["accent"]}',
                       'fontWeight': '600'
                   }),
            
            dcc.Tab(label='VIP Customers', value='vip',
                   style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['text_secondary'],
                       'border': 'none',
                       'borderBottom': f'2px solid {COLORS["border"]}'
                   },
                   selected_style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['accent'],
                       'border': 'none',
                       'borderBottom': f'4px solid {COLORS["accent"]}',
                       'fontWeight': '600'
                   }),
            
            dcc.Tab(label='Customer Segmentation', value='segmentation',
                   style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['text_secondary'],
                       'border': 'none',
                       'borderBottom': f'2px solid {COLORS["border"]}'
                   },
                   selected_style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['accent'],
                       'border': 'none',
                       'borderBottom': f'4px solid {COLORS["accent"]}',
                       'fontWeight': '600'
                   }),
            
            dcc.Tab(label='Customer Churn', value='churn',
                   style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['text_secondary'],
                       'border': 'none',
                       'borderBottom': f'2px solid {COLORS["border"]}'
                   },
                   selected_style={
                       'padding': '12px 24px',
                       'backgroundColor': COLORS['card_background'],
                       'color': COLORS['accent'],
                       'border': 'none',
                       'borderBottom': f'4px solid {COLORS["accent"]}',
                       'fontWeight': '600'
                   }),
        ], style={
            'backgroundColor': COLORS['card_background'],
            'borderRadius': '8px 8px 0 0',
            'boxShadow': '0 1px 3px rgba(0,0,0,0.1)'
        }),
        
        # Tab content container
        html.Div(id='tab-content', style={
            'padding': '40px',
            'backgroundColor': COLORS['card_background'],
            'borderRadius': '0 0 8px 8px',
            'boxShadow': '0 2px 8px rgba(0,0,0,0.1)'
        })
        
    ], style={
        'margin': '0',
        'padding': '20px',
        'paddingTop': '100px',  # Extra padding for fixed logo header
        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
        'backgroundColor': COLORS['background'],
        'minHeight': '100vh'
    }),
    
    # Author credit in bottom left corner
    html.Div([
        html.P('Authored by Matthew Molony', style={
            'margin': '0',
            'fontSize': '16px',
            'color': COLORS['text_secondary'],
            'fontStyle': 'italic'
        })
    ], style={
        'position': 'fixed',
        'bottom': '20px',
        'left': '20px',
        'padding': '12px 18px',
        'backgroundColor': COLORS['card_background'],
        'borderRadius': '6px',
        'boxShadow': '0 2px 8px rgba(0,0,0,0.15)',
        'borderLeft': f'4px solid {COLORS["accent"]}',
        'zIndex': '1000'
    })
])

# Callback to render tab content
@callback(
    Output('tab-content', 'children'),
    Input('tabs', 'value')
)
def render_tab_content(selected_tab):
    if selected_tab == 'clv':
        # Calculate metrics from CLV statistics
        total_customers = clv_statistics_df['total_customers'].iloc[0] if not clv_statistics_df.empty else 0
        avg_clv = clv_statistics_df['avg_clv'].iloc[0] if not clv_statistics_df.empty else 0
        max_clv = clv_statistics_df['max_clv'].iloc[0] if not clv_statistics_df.empty else 0
        total_clv_value = clv_statistics_df['total_clv'].iloc[0] if not clv_statistics_df.empty else 0
        
        return html.Div([
            # Header
            html.H2('Customer Lifetime Value Analysis', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Identify and track the total value each customer brings to your business over their entire relationship.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Key Metrics Row
            html.Div([
                create_metric_card(f'${avg_clv:,.2f}' if avg_clv else '$0', 'Average CLV', COLORS['accent']),
                create_metric_card(f'${max_clv:,.2f}' if max_clv else '$0', 'Highest CLV', COLORS['accent_light']),
                create_metric_card(f'{total_customers:,}' if total_customers else '0', 'Total Customers', '#ff8a65'),
                create_metric_card(f'${total_clv_value:,.0f}' if total_clv_value else '$0', 'Total CLV Value', '#ffab91'),
            ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
            
            # CLV Distribution and Market Segment (Row 1)
            html.Div([
                html.Div([
                    dcc.Graph(figure=clv_distribution_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=clv_market_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # CLV by Country and VIP Distribution (Row 2)
            html.Div([
                html.Div([
                    dcc.Graph(figure=clv_country_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=vip_dist_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Correlation Analysis Section
            html.H3('📈 CLV Correlation Analysis', 
                   style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '20px', 'fontWeight': '600', 'marginTop': '20px'}),
            html.P('Understand how CLV relates to customer behavior metrics',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
            
            # Correlation Scatter Plots (Row 3)
            html.Div([
                html.Div([
                    dcc.Graph(figure=clv_spending_scatter)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=clv_orders_scatter)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Segment Behavior and Value (Row 4)
            html.Div([
                html.Div([
                    dcc.Graph(figure=segment_behavior_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=clv_value_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Top 10 CLV Customers Table
            html.Div([
                html.H3('Top 10 Customers by Lifetime Value', 
                       style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P('Highest predicted lifetime value customers',
                       style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
                
                dash_table.DataTable(
                    id='top-clv-table',
                    columns=[{"name": i, "id": i} for i in top_clv_customers_df.columns] if not top_clv_customers_df.empty else [],
                    data=top_clv_customers_df.to_dict('records') if not top_clv_customers_df.empty else [],
                    page_size=10,
                    style_table={
                        'overflowX': 'auto'
                    },
                    style_header={
                        'backgroundColor': COLORS['accent'],
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'left',
                        'padding': '12px'
                    },
                    style_cell={
                        'textAlign': 'left',
                        'padding': '12px',
                        'fontSize': '13px',
                        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
                    },
                    style_data={
                        'backgroundColor': COLORS['card_background'],
                        'color': COLORS['text'],
                        'border': f'1px solid {COLORS["border"]}'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        },
                        {
                            'if': {'column_id': 'clv', 'filter_query': '{clv} >= 500'},
                            'backgroundColor': '#fff3e6',
                            'color': COLORS['accent'],
                            'fontWeight': '600'
                        }
                    ]
                ) if not top_clv_customers_df.empty else html.P(
                    'No CLV data available.',
                    style={'color': COLORS['text_secondary'], 'fontStyle': 'italic'}
                )
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])
    
    elif selected_tab == 'vip':
        # Calculate metrics from VIP overview
        total_vip = vip_overview_df['medium_high_vip_count'].iloc[0] if not vip_overview_df.empty else 0
        high_vip = vip_overview_df['high_vip_count'].iloc[0] if not vip_overview_df.empty else 0
        avg_vip_clv = vip_overview_df['avg_clv_high_vip'].iloc[0] if not vip_overview_df.empty else 0
        total_vip_value = vip_overview_df['total_vip_value'].iloc[0] if not vip_overview_df.empty else 0
        
        return html.Div([
            # Header
            html.H2('VIP Customer Analysis', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Track your most valuable customers and their contribution to business growth.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Key Metrics Row
            html.Div([
                create_metric_card(f'{total_vip:,}' if total_vip else '0', 'VIP Customers (≥0.5)', COLORS['accent']),
                create_metric_card(f'{high_vip:,}' if high_vip else '0', 'High VIP (≥0.7)', '#FFD700'),
                create_metric_card(f'${avg_vip_clv:,.0f}' if avg_vip_clv else '$0', 'Avg VIP CLV', COLORS['accent_light']),
                create_metric_card(f'${total_vip_value:,.0f}' if total_vip_value else '$0', 'Total VIP Value', '#ff8a65'),
            ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
            
            # VIP Tier Distribution and CLV by Tier (Row 1)
            html.Div([
                html.Div([
                    dcc.Graph(figure=vip_tier_dist_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=vip_clv_tier_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # VIP by Segment and Country (Row 2)
            html.Div([
                html.Div([
                    dcc.Graph(figure=vip_segment_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=vip_country_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # VIP Correlation and Behavioral Comparison (Row 3)
            html.Div([
                html.Div([
                    dcc.Graph(figure=vip_correlation_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(figure=vip_behavioral_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # VIP Value Concentration (Full Width)
            html.Div([
                dcc.Graph(figure=vip_value_fig)
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'marginBottom': '30px'
            }),
            
            # Top 20 VIP Customers Table
            html.Div([
                html.H3('Top 20 VIP Customers', 
                       style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P('Highest VIP probability customers sorted by value',
                       style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
                
                dash_table.DataTable(
                    id='top-vip-table',
                    columns=[{"name": i, "id": i} for i in top_vip_customers_df.columns] if not top_vip_customers_df.empty else [],
                    data=top_vip_customers_df.to_dict('records') if not top_vip_customers_df.empty else [],
                    page_size=20,
                    style_table={
                        'overflowX': 'auto'
                    },
                    style_header={
                        'backgroundColor': COLORS['accent'],  # Orange for consistency
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'left',
                        'padding': '12px'
                    },
                    style_cell={
                        'textAlign': 'left',
                        'padding': '12px',
                        'fontSize': '13px',
                        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
                    },
                    style_data={
                        'backgroundColor': COLORS['card_background'],
                        'color': COLORS['text'],
                        'border': f'1px solid {COLORS["border"]}'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        },
                        {
                            'if': {'column_id': 'vip_probability', 'filter_query': '{vip_probability} >= 0.7'},
                            'backgroundColor': '#fff9e6',
                            'color': '#ff8c00',
                            'fontWeight': '600'
                        }
                    ]
                ) if not top_vip_customers_df.empty else html.P(
                    'No VIP customer data available.',
                    style={'color': COLORS['text_secondary'], 'fontStyle': 'italic'}
                )
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])
    
    elif selected_tab == 'segmentation':
        # Lazy load behavioral analysis (only when this tab is viewed)
        # Using ULTRA-FAST version with sampling for instant results
        try:
            # Option 1: ULTRA-FAST with fixed limit (recommended for large datasets)
            segment_behavioral_df = segmentation_service.get_segment_behavioral_analysis_fast()
            
            # Option 2: Fast with statistical sampling (uncomment to use)
            # segment_behavioral_df = segmentation_service.get_segment_behavioral_analysis(use_sampling=True, sample_fraction=0.1)
            
            # Option 3: Full accuracy, slower (uncomment to use)
            # segment_behavioral_df = segmentation_service.get_segment_behavioral_analysis(use_sampling=False)
        except Exception as e:
            print(f"Error loading segmentation behavioral data: {e}")
            segment_behavioral_df = pd.DataFrame()
        
        # Helper function to create segment profile card
        def create_segment_card(segment_name, segment_data):
            profile = SEGMENT_PROFILES.get(segment_name, {})
            
            # Get metrics from segment_overview_df
            segment_metrics = segment_overview_df[segment_overview_df['market_segment'] == segment_name]
            customer_count = segment_metrics['customer_count'].iloc[0] if not segment_metrics.empty else 0
            avg_clv = segment_metrics['avg_clv'].iloc[0] if not segment_metrics.empty else 0
            total_clv = segment_metrics['total_clv'].iloc[0] if not segment_metrics.empty else 0
            avg_vip_prob = segment_metrics['avg_vip_probability'].iloc[0] if not segment_metrics.empty else 0
            
            # Get behavioral metrics from segment_behavioral_df
            behavioral_metrics = segment_behavioral_df[segment_behavioral_df['market_segment'] == segment_name]
            avg_spending = behavioral_metrics['avg_spending'].iloc[0] if not behavioral_metrics.empty else 0
            avg_orders = behavioral_metrics['avg_orders'].iloc[0] if not behavioral_metrics.empty else 0
            avg_order_value = behavioral_metrics['avg_order_value'].iloc[0] if not behavioral_metrics.empty else 0
            avg_sessions = behavioral_metrics['avg_sessions'].iloc[0] if not behavioral_metrics.empty else 0
            avg_engagement = behavioral_metrics['avg_engagement_ratio'].iloc[0] if not behavioral_metrics.empty else 0
            retention_rate = behavioral_metrics['retention_rate_pct'].iloc[0] if not behavioral_metrics.empty else 0
            avg_tenure = behavioral_metrics['avg_tenure_days'].iloc[0] if not behavioral_metrics.empty else 0
            avg_inactive = behavioral_metrics['avg_days_inactive'].iloc[0] if not behavioral_metrics.empty else 0
            
            # Calculate segment percentage
            total_customers = segment_overview_df['customer_count'].sum() if not segment_overview_df.empty else 1
            segment_pct = (customer_count / total_customers * 100) if total_customers > 0 else 0
            
            return html.Div([
                # Header with icon and tagline
                html.Div([
                    html.Span(profile.get('icon', ''), style={'fontSize': '48px', 'marginRight': '15px'}),
                    html.Div([
                        html.H3(segment_name, style={'color': COLORS['text'], 'marginBottom': '5px', 'fontSize': '24px', 'fontWeight': '700'}),
                        html.P(profile.get('tagline', ''), style={'color': COLORS['accent'], 'marginBottom': '0px', 'fontSize': '14px', 'fontWeight': '600', 'textTransform': 'uppercase', 'letterSpacing': '0.5px'}),
                    ])
                ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '20px'}),
                
                # Description
                html.P(profile.get('description', ''), style={
                    'color': COLORS['text_secondary'], 
                    'marginBottom': '20px', 
                    'fontSize': '14px', 
                    'lineHeight': '1.8'
                }),
                
                # Key Metrics Row (4 metrics)
                html.Div([
                    html.Div([
                        html.H4(f'{customer_count:,}', style={'color': COLORS['accent'], 'fontSize': '20px', 'fontWeight': '700', 'marginBottom': '5px'}),
                        html.P(f'Customers ({segment_pct:.1f}%)', style={'color': COLORS['text_secondary'], 'fontSize': '11px', 'marginBottom': '0px'}),
                    ], style={'flex': '1', 'textAlign': 'center', 'padding': '12px', 'backgroundColor': '#fafafa', 'borderRadius': '8px', 'marginRight': '8px'}),
                    html.Div([
                        html.H4(f'${avg_clv:,.0f}', style={'color': COLORS['accent'], 'fontSize': '20px', 'fontWeight': '700', 'marginBottom': '5px'}),
                        html.P('Avg CLV', style={'color': COLORS['text_secondary'], 'fontSize': '11px', 'marginBottom': '0px'}),
                    ], style={'flex': '1', 'textAlign': 'center', 'padding': '12px', 'backgroundColor': '#fafafa', 'borderRadius': '8px', 'marginRight': '8px'}),
                    html.Div([
                        html.H4(f'{retention_rate:.1f}%', style={'color': COLORS['accent'], 'fontSize': '20px', 'fontWeight': '700', 'marginBottom': '5px'}),
                        html.P('Retention', style={'color': COLORS['text_secondary'], 'fontSize': '11px', 'marginBottom': '0px'}),
                    ], style={'flex': '1', 'textAlign': 'center', 'padding': '12px', 'backgroundColor': '#fafafa', 'borderRadius': '8px', 'marginRight': '8px'}),
                    html.Div([
                        html.H4(f'{avg_vip_prob:.1%}', style={'color': COLORS['accent'], 'fontSize': '20px', 'fontWeight': '700', 'marginBottom': '5px'}),
                        html.P('VIP Rate', style={'color': COLORS['text_secondary'], 'fontSize': '11px', 'marginBottom': '0px'}),
                    ], style={'flex': '1', 'textAlign': 'center', 'padding': '12px', 'backgroundColor': '#fafafa', 'borderRadius': '8px'}),
                ], style={'display': 'flex', 'marginBottom': '25px'}),
                
                # Behavioral Insights Section
                html.Div([
                    html.H4('📊 Behavioral Analysis', style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '16px', 'fontWeight': '600'}),
                    html.Div([
                        # Column 1: Spending Behavior
                        html.Div([
                            html.P('💰 Spending Behavior', style={'fontWeight': '600', 'color': COLORS['accent'], 'marginBottom': '10px', 'fontSize': '13px'}),
                            html.P(f'Avg Total Spending: ${avg_spending:,.2f}', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Avg Orders: {avg_orders:.1f}', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Avg Order Value: ${avg_order_value:,.2f}', style={'marginBottom': '0px', 'color': COLORS['text'], 'fontSize': '12px'}),
                        ], style={'flex': '1', 'marginRight': '20px'}),
                        
                        # Column 2: Engagement
                        html.Div([
                            html.P('📱 Engagement', style={'fontWeight': '600', 'color': COLORS['accent'], 'marginBottom': '10px', 'fontSize': '13px'}),
                            html.P(f'Avg Sessions: {avg_sessions:.1f}', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Engagement Ratio: {avg_engagement:.2f}', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Retention Rate: {retention_rate:.1f}%', style={'marginBottom': '0px', 'color': COLORS['text'], 'fontSize': '12px'}),
                        ], style={'flex': '1', 'marginRight': '20px'}),
                        
                        # Column 3: Activity
                        html.Div([
                            html.P('⏱️ Activity Patterns', style={'fontWeight': '600', 'color': COLORS['accent'], 'marginBottom': '10px', 'fontSize': '13px'}),
                            html.P(f'Avg Tenure: {avg_tenure:.0f} days', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Avg Days Inactive: {avg_inactive:.1f}', style={'marginBottom': '6px', 'color': COLORS['text'], 'fontSize': '12px'}),
                            html.P(f'Total Value: ${total_clv:,.0f}', style={'marginBottom': '0px', 'color': COLORS['text'], 'fontSize': '12px'}),
                        ], style={'flex': '1'}),
                    ], style={'display': 'flex', 'backgroundColor': '#f9f9f9', 'padding': '15px', 'borderRadius': '8px', 'marginBottom': '20px'})
                ]),
                
                # Characteristics
                html.H4('🎯 Segment Characteristics', style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '16px', 'fontWeight': '600'}),
                html.Div([
                    html.P(char, style={'marginBottom': '12px', 'color': COLORS['text'], 'fontSize': '13px', 'lineHeight': '1.6'})
                    for char in profile.get('characteristics', [])
                ], style={'marginBottom': '20px'}),
                
                # Typical Products
                html.Div([
                    html.H4('📈 Typical Products', style={'color': COLORS['text'], 'marginBottom': '8px', 'fontSize': '14px', 'fontWeight': '600'}),
                    html.P(profile.get('typical_products', ''), style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '15px', 'fontStyle': 'italic'}),
                ]),
                
                # Engagement Strategy
                html.Div([
                    html.H4('🚀 Engagement Strategy', style={'color': COLORS['text'], 'marginBottom': '8px', 'fontSize': '14px', 'fontWeight': '600'}),
                    html.P(profile.get('engagement_strategy', ''), style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '0px'}),
                ])
                
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '12px',
                'padding': '30px',
                'boxShadow': '0 4px 8px rgba(0,0,0,0.1)',
                'borderTop': f'5px solid {COLORS["accent"]}',
                'marginBottom': '30px'
            })
        
        # Create segment overview visualization
        segment_overview_fig = px.bar(
            segment_overview_df if not segment_overview_df.empty else pd.DataFrame(),
            x='market_segment',
            y='customer_count',
            title='Customer Distribution by Market Segment',
            labels={'customer_count': 'Number of Customers', 'market_segment': 'Market Segment'},
            text='customer_count',
            color='avg_clv',
            color_continuous_scale='Oranges'
        )
        segment_overview_fig.update_traces(
            texttemplate='%{text:,}',
            textposition='outside'
        )
        segment_overview_fig.update_layout(
            template='plotly_white',
            paper_bgcolor=COLORS['card_background'],
            plot_bgcolor=COLORS['card_background'],
            font={'color': COLORS['text']},
            height=400,
            margin=dict(t=40, b=60, l=60, r=20),
            showlegend=False
        )
        
        return html.Div([
            # Header
            html.H2('Customer Market Segmentation', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Understand the unique investment preferences and behaviors of your customer segments.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Overview Chart
            html.Div([
                dcc.Graph(figure=segment_overview_fig)
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'marginBottom': '40px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            }),
            
            # Segment Profile Cards (2x2 grid)
            html.Div([
                html.Div([
                    create_segment_card('Blue Chip', segment_overview_df),
                    create_segment_card('Crypto', segment_overview_df),
                ], style={'flex': '1', 'marginRight': '20px'}),
                html.Div([
                    create_segment_card('Social Impact', segment_overview_df),
                    create_segment_card('Sustainability Focused', segment_overview_df),
                ], style={'flex': '1'})
            ], style={'display': 'flex'})
        ])
    
    elif selected_tab == 'customer360':
        # Create customer location distribution chart
        if not churn_features_df.empty and 'country' in churn_features_df.columns:
            country_distribution = churn_features_df['country'].value_counts().reset_index()
            country_distribution.columns = ['country', 'customer_count']
            
            location_fig = px.bar(
                country_distribution,
                x='country',
                y='customer_count',
                title='Customer Distribution by Location',
                labels={'customer_count': 'Number of Customers', 'country': 'Country'},
                text='customer_count'
            )
            location_fig.update_traces(
                marker_color=COLORS['accent'],
                textposition='outside'
            )
            location_fig.update_layout(
                template='plotly_white',
                paper_bgcolor=COLORS['card_background'],
                plot_bgcolor=COLORS['card_background'],
                font={'color': COLORS['text']},
                height=400,
                margin=dict(t=40, b=60, l=60, r=20),
                xaxis={'tickangle': -30}
            )
        else:
            location_fig = go.Figure()
        
        return html.Div([
            html.H2('Customer 360 Profile', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Complete demographic overview of your customer base.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Target Customer Filters (MOVED TO TOP)
            html.Div([
                html.H3('🎯 Target Customer Selection for Braze Marketing', 
                       style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P([
                    html.Span('Identify and segment customers for targeted marketing campaigns. ', style={'color': COLORS['text_secondary']}),
                    html.Span('Apply filters to isolate your target audience for export to Braze.', 
                             style={'color': COLORS['accent'], 'fontWeight': '600'})
                ], style={'marginBottom': '20px', 'fontSize': '14px'}),
                
                # Filter Section
                html.Div([
                    html.H4('🔍 Filter Customers', 
                           style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '16px', 'fontWeight': '600'}),
                    
                    # Filter Row 1: CLV Range Slider and VIP
                    html.Div([
                        html.Div([
                            html.Label('Customer Lifetime Value (CLV) Range', style={'fontWeight': '600', 'marginBottom': '5px', 'display': 'block', 'fontSize': '13px'}),
                            html.Div([
                                dcc.RangeSlider(
                                    id='filter-clv-range',
                                    min=0,
                                    max=2500,
                                    step=50,
                                    value=[0, 2500],
                                    marks={
                                        0: '$0',
                                        500: '$500',
                                        1000: '$1K',
                                        1500: '$1.5K',
                                        2000: '$2K',
                                        2500: '$2.5K'
                                    },
                                    tooltip={"placement": "bottom", "always_visible": False},
                                    className='orange-slider'
                                )
                            ], style={'paddingTop': '10px'})
                        ], style={'flex': '1', 'marginRight': '20px'}),
                        html.Div([
                            html.Label('VIP Probability (Min)', style={'fontWeight': '600', 'marginBottom': '5px', 'display': 'block', 'fontSize': '13px'}),
                            dcc.Dropdown(
                                id='filter-vip-min',
                                options=[
                                    {'label': 'Any', 'value': ''},
                                    {'label': '≥ 0.3 (Bronze)', 'value': 0.3},
                                    {'label': '≥ 0.5 (Silver)', 'value': 0.5},
                                    {'label': '≥ 0.7 (Gold)', 'value': 0.7},
                                    {'label': '≥ 0.9 (Platinum)', 'value': 0.9}
                                ],
                                value='',
                                clearable=True,
                                style={'width': '100%'}
                            )
                        ], style={'flex': '1'})
                    ], style={'display': 'flex', 'marginBottom': '25px', 'alignItems': 'flex-start'}),
                    
                    # Filter Row 2: Segment, Churn Risk, Country
                    html.Div([
                        html.Div([
                            html.Label('Market Segment', style={'fontWeight': '600', 'marginBottom': '5px', 'display': 'block', 'fontSize': '13px'}),
                            dcc.Dropdown(
                                id='filter-segment',
                                options=[
                                    {
                                        'label': f"{'🏛️' if s == 'Blue Chip' else '₿' if s == 'Crypto' else '🤝' if s == 'Social Impact' else '🌱'} {s}",
                                        'value': s
                                    } for s in filter_options['segments']
                                ],
                                placeholder='All Segments',
                                clearable=True,
                                style={'width': '100%'}
                            )
                        ], style={'flex': '1', 'marginRight': '10px'}),
                        html.Div([
                            html.Label('Churn Risk', style={'fontWeight': '600', 'marginBottom': '5px', 'display': 'block', 'fontSize': '13px'}),
                            dcc.Dropdown(
                                id='filter-churn-risk',
                                options=[
                                    {'label': 'All Customers', 'value': ''},
                                    {'label': '🔴 At Risk', 'value': 1},
                                    {'label': '🟢 Not at Risk', 'value': 0}
                                ],
                                value='',
                                clearable=True,
                                style={'width': '100%'}
                            )
                        ], style={'flex': '1', 'marginRight': '10px'}),
                        html.Div([
                            html.Label('Country', style={'fontWeight': '600', 'marginBottom': '5px', 'display': 'block', 'fontSize': '13px'}),
                            dcc.Dropdown(
                                id='filter-country',
                                options=[{'label': c, 'value': c} for c in filter_options['countries']],
                                placeholder='All Countries',
                                clearable=True,
                                style={'width': '100%'}
                            )
                        ], style={'flex': '1'})
                    ], style={'display': 'flex', 'marginBottom': '15px'}),
                    
                    # Apply Filter Button
                    html.Div([
                        html.Button(
                            '🔍 Apply Filters',
                            id='apply-filters-btn',
                            n_clicks=0,
                            style={
                                'backgroundColor': COLORS['accent'],
                                'color': 'white',
                                'border': 'none',
                                'padding': '12px 30px',
                                'borderRadius': '6px',
                                'fontSize': '14px',
                                'fontWeight': '600',
                                'cursor': 'pointer',
                                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
                            }
                        ),
                        html.Button(
                            '🔄 Reset',
                            id='reset-filters-btn',
                            n_clicks=0,
                            style={
                                'backgroundColor': '#9e9e9e',
                                'color': 'white',
                                'border': 'none',
                                'padding': '12px 30px',
                                'borderRadius': '6px',
                                'fontSize': '14px',
                                'fontWeight': '600',
                                'cursor': 'pointer',
                                'marginLeft': '10px',
                                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
                            }
                        )
                    ], style={'textAlign': 'center'})
                ], style={
                    'backgroundColor': '#fff8f0',
                    'borderRadius': '6px',
                    'padding': '20px',
                    'marginBottom': '20px',
                    'border': f'2px solid {COLORS["accent"]}'
                }),
                
                # Results Count
                html.Div(
                    '👆 Click "Apply Filters" above to load customer data',
                    id='filter-results-count',
                    style={'marginBottom': '30px', 'fontSize': '14px', 'color': COLORS['text_secondary'], 'fontWeight': '600', 'textAlign': 'center'}
                ),
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'marginBottom': '30px'
            }),
            
            # Metric cards (Dynamic with Loading)
            dcc.Loading(
                id="loading-metrics",
                type="circle",
                color=COLORS['accent'],
                children=html.Div(
                    id='c360-metric-cards',
                    children=[
                        create_metric_card('--', 'Average Age', COLORS['accent']),
                        create_metric_card('--', 'Avg CLV', COLORS['accent_light']),
                        create_metric_card('--', 'Total Profiles', '#ff8a65'),
                    ],
                    style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}
                )
            ),
            
            # Demographics and Location Charts (Row 1)
            html.Div([
                html.Div([
                    dcc.Loading(
                        id="loading-age-dist",
                        type="circle",
                        color=COLORS['accent'],
                        children=dcc.Graph(id='c360-age-distribution', figure=demo_fig)
                    )
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Loading(
                        id="loading-location-dist",
                        type="circle",
                        color=COLORS['accent'],
                        children=dcc.Graph(id='c360-location-distribution', figure=location_fig)
                    )
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Customer Location Map
            html.Div([
                html.H3('🗺️ Customer Locations Map', 
                       style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P('Interactive map showing customer locations. Hover over markers for details. Map updates with filters below.',
                       style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
                
                dcc.Loading(
                    id="loading-map",
                    type="circle",
                    color=COLORS['accent'],
                    children=dcc.Graph(
                        id='customer-map',
                        figure=initial_map_fig,
                        config={'displayModeBar': True, 'displaylogo': False}
                    )
                )
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'marginBottom': '30px'
            }),
            
            # Customer Data Table
            html.Div([
                html.H3('📊 Filtered Customer Data', 
                       style={'color': COLORS['text'], 'marginBottom': '20px', 'fontSize': '20px', 'fontWeight': '600'}),
                
                dcc.Loading(
                    id="loading-table",
                    type="circle",
                    color=COLORS['accent'],
                    children=dash_table.DataTable(
                    id='churn-features-table',
                    columns=[
                        {"name": "User ID", "id": "user_id"},
                        {"name": "First Name", "id": "firstname"},
                        {"name": "Last Name", "id": "lastname"},
                        {"name": "Email", "id": "email"},
                        {"name": "Churn Status", "id": "churn_status"},
                        {"name": "CLV", "id": "customer_lifetime_value"},
                        {"name": "VIP Probability", "id": "vip_customer_probability"},
                        {"name": "Market Segment", "id": "market_segment"},
                        {"name": "Country", "id": "country"},
                        {"name": "Gender", "id": "gender"},
                        {"name": "Age Group", "id": "age_group"},
                        {"name": "Platform", "id": "platform"},
                        {"name": "Order Count", "id": "order_count"},
                        {"name": "Total Amount", "id": "total_amount"},
                        {"name": "Session Count", "id": "session_count"},
                        {"name": "Event Count", "id": "event_count"},
                        {"name": "Days Since Creation", "id": "days_since_creation"},
                        {"name": "Days Since Last Activity", "id": "days_since_last_activity"},
                        {"name": "Latitude", "id": "lat"},
                        {"name": "Longitude", "id": "lon"}
                    ],
                    data=[],
                    page_size=25,
                    page_action='native',
                    sort_action='native',
                    sort_mode='multi',
                    filter_action='native',
                    style_table={
                        'overflowX': 'auto',
                        'minWidth': '100%'
                    },
                    style_header={
                        'backgroundColor': COLORS['accent'],
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'left',
                        'padding': '12px',
                        'border': f'1px solid {COLORS["accent_light"]}'
                    },
                    style_header_conditional=[
                        {
                            'if': {'column_id': 'vip_customer_probability'},
                            'backgroundColor': '#FFB400',
                            'color': 'white'
                        },
                        {
                            'if': {'column_id': 'market_segment'},
                            'backgroundColor': '#00A6ED',
                            'color': 'white'
                        },
                        {
                            'if': {'column_id': 'customer_lifetime_value'},
                            'backgroundColor': '#7FB800',
                            'color': 'white'
                        },
                        {
                            'if': {'column_id': 'churn_status'},
                            'backgroundColor': '#0D2C54',
                            'color': 'white'
                        }
                    ],
                    style_cell={
                        'textAlign': 'left',
                        'padding': '12px',
                        'fontSize': '13px',
                        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
                        'minWidth': '100px',
                        'maxWidth': '300px',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis'
                    },
                    style_data={
                        'backgroundColor': COLORS['card_background'],
                        'color': COLORS['text'],
                        'border': f'1px solid {COLORS["border"]}'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        },
                        # Churn Status Column - Red/Green based on risk
                        {
                            'if': {'column_id': 'churn_status', 'filter_query': '{churn_status} = "At Risk"'},
                            'backgroundColor': '#ffe6e6',
                            'color': '#cc0000',
                            'fontWeight': '600'
                        },
                        {
                            'if': {'column_id': 'churn_status', 'filter_query': '{churn_status} = "Not at Risk"'},
                            'backgroundColor': '#e8f5e9',
                            'color': '#2e7d32',
                            'fontWeight': '600'
                        },
                        # VIP Probability Column - Gold/Yellow
                        {
                            'if': {'column_id': 'vip_customer_probability'},
                            'backgroundColor': '#fff8e1',
                            'color': '#f57c00',
                            'fontWeight': '600',
                            'border': '1px solid #FFB400'
                        },
                        # Market Segment Column - Blue
                        {
                            'if': {'column_id': 'market_segment'},
                            'backgroundColor': '#e3f2fd',
                            'color': '#0D2C54',
                            'fontWeight': '600',
                            'border': '1px solid #00A6ED'
                        },
                        # Customer Lifetime Value Column - Green
                        {
                            'if': {'column_id': 'customer_lifetime_value'},
                            'backgroundColor': '#f1f8e9',
                            'color': '#558b2f',
                            'fontWeight': '600',
                            'border': '1px solid #7FB800'
                        }
                    ],
                    style_filter={
                        'backgroundColor': '#fff8f0',
                        'border': f'1px solid {COLORS["accent"]}'
                    },
                    tooltip_data=[],
                    tooltip_duration=None
                    )
                ),
                
                # Push to Braze Section
                html.Div([
                    # Description
                    html.Div([
                        html.H4('📤 Export Customer Cohort to Braze', 
                               style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '18px', 'fontWeight': '600'}),
                        html.P([
                            html.Span('Use the filters above to identify your target customer cohort, then push this segment to ', 
                                     style={'color': COLORS['text_secondary']}),
                            html.Span('Braze', style={'color': COLORS['accent'], 'fontWeight': '600'}),
                            html.Span(' for targeted marketing campaigns. ', style={'color': COLORS['text_secondary']}),
                            html.Span('This workflow enables you to:', style={'color': COLORS['text_secondary']})
                        ], style={'marginBottom': '10px', 'fontSize': '14px', 'lineHeight': '1.6'}),
                        html.Ul([
                            html.Li('🎯 Target high-value customers (CLV, VIP segments)', 
                                   style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '5px'}),
                            html.Li('🔴 Engage at-risk customers with retention campaigns', 
                                   style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '5px'}),
                            html.Li('📊 Create segment-specific messaging (Blue Chip, Crypto, etc.)', 
                                   style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '5px'}),
                            html.Li('🌍 Run geo-targeted campaigns by country', 
                                   style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginBottom': '5px'}),
                        ], style={'marginLeft': '20px', 'marginBottom': '15px'}),
                        html.P([
                            html.Span('💡 ', style={'fontSize': '16px'}),
                            html.Span('Tip: ', style={'fontWeight': '600', 'color': COLORS['text']}),
                            html.Span('Apply filters, review the filtered customer list above, then click the button below to export this cohort for your marketing campaign.',
                                     style={'color': COLORS['text_secondary'], 'fontStyle': 'italic'})
                        ], style={'fontSize': '13px', 'backgroundColor': '#fff8f0', 'padding': '12px', 'borderRadius': '6px', 'border': f'1px solid {COLORS["accent"]}', 'marginBottom': '20px'})
                    ], style={'marginTop': '30px', 'marginBottom': '20px'}),
                    
                    # Push to Braze Button and Info Section
                    html.Div([
                        # Left side: Button
                        html.Div([
                            html.Button([
                                html.Img(
                                    src='/assets/braze-logo.webp',
                                    style={
                                        'height': '24px',
                                        'marginRight': '12px',
                                        'verticalAlign': 'middle'
                                    }
                                ),
                                html.Span('Push to Braze', style={'verticalAlign': 'middle', 'fontSize': '15px', 'fontWeight': '600'})
                            ],
                            id='push-to-braze-btn',
                            n_clicks=0,
                            style={
                                'backgroundColor': COLORS['accent'],
                                'color': 'white',
                                'border': 'none',
                                'padding': '14px 32px',
                                'borderRadius': '8px',
                                'fontSize': '15px',
                                'fontWeight': '600',
                                'cursor': 'pointer',
                                'boxShadow': '0 4px 6px rgba(0,0,0,0.1)',
                                'display': 'inline-flex',
                                'alignItems': 'center',
                                'transition': 'all 0.3s ease',
                                'marginBottom': '15px'
                            },
                            className='braze-button'
                            ),
                            # Notification message
                            html.Div(id='braze-notification', style={'marginTop': '15px'})
                        ], style={'flex': '0 0 auto', 'display': 'flex', 'flexDirection': 'column', 'alignItems': 'center', 'paddingRight': '30px', 'borderRight': f'2px solid {COLORS["border"]}'}),
                        
                        # Right side: Braze Information
                        html.Div([
                            html.H5('About Braze', style={'color': COLORS['text'], 'marginBottom': '12px', 'fontSize': '16px', 'fontWeight': '600'}),
                            html.P([
                                'Braze provides a software platform for ',
                                html.Span('engaging customers across multiple touchpoints', style={'fontWeight': '600', 'color': COLORS['accent']}),
                                ', leveraging data, real-time triggers, journey orchestration, and personalisation.'
                            ], style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'lineHeight': '1.6', 'marginBottom': '15px'}),
                            
                            html.Div([
                                html.P('Core Capabilities:', style={'fontWeight': '600', 'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '14px'}),
                                
                                html.Div([
                                    html.Span('📊 ', style={'fontSize': '16px', 'marginRight': '8px'}),
                                    html.Span('Data Activation & Unification: ', style={'fontWeight': '600', 'color': COLORS['text'], 'fontSize': '13px'}),
                                    html.Span('Collect and unify data from multiple sources to build rich customer profiles for targeting.',
                                             style={'color': COLORS['text_secondary'], 'fontSize': '13px'})
                                ], style={'marginBottom': '10px'}),
                                
                                html.Div([
                                    html.Span('🎯 ', style={'fontSize': '16px', 'marginRight': '8px'}),
                                    html.Span('Segmentation & Orchestration: ', style={'fontWeight': '600', 'color': COLORS['text'], 'fontSize': '13px'}),
                                    html.Span('Segment users based on behaviour and attributes, then design automated journeys that respond in real time.',
                                             style={'color': COLORS['text_secondary'], 'fontSize': '13px'})
                                ], style={'marginBottom': '10px'}),
                                
                                html.Div([
                                    html.Span('📱 ', style={'fontSize': '16px', 'marginRight': '8px'}),
                                    html.Span('Cross-Channel Messaging: ', style={'fontWeight': '600', 'color': COLORS['text'], 'fontSize': '13px'}),
                                    html.Span('Support for email, push notifications, in-app messages, SMS, web push, and other channels.',
                                             style={'color': COLORS['text_secondary'], 'fontSize': '13px'})
                                ], style={'marginBottom': '10px'}),
                                
                                html.Div([
                                    html.Span('🤖 ', style={'fontSize': '16px', 'marginRight': '8px'}),
                                    html.Span('Personalisation & Optimisation: ', style={'fontWeight': '600', 'color': COLORS['text'], 'fontSize': '13px'}),
                                    html.Span('Intelligent timing, channel preferences, ML-driven predictive events, and A/B/n testing.',
                                             style={'color': COLORS['text_secondary'], 'fontSize': '13px'})
                                ], style={'marginBottom': '10px'}),
                                
                                html.Div([
                                    html.Span('⚡ ', style={'fontSize': '16px', 'marginRight': '8px'}),
                                    html.Span('Real-Time Engagement: ', style={'fontWeight': '600', 'color': COLORS['text'], 'fontSize': '13px'}),
                                    html.Span('Real-time API/SDK integration designed to act quickly on user behaviour.',
                                             style={'color': COLORS['text_secondary'], 'fontSize': '13px'})
                                ], style={'marginBottom': '10px'}),
                            ])
                        ], style={'flex': '1', 'paddingLeft': '30px', 'textAlign': 'left'})
                    ], style={'display': 'flex', 'alignItems': 'flex-start', 'marginTop': '20px', 'padding': '20px', 'backgroundColor': '#f9f9f9', 'borderRadius': '8px', 'border': f'1px solid {COLORS["border"]}'})
                ], style={'marginTop': '20px'})
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])
    
    elif selected_tab == 'churn':
        # Calculate metrics from data
        total_at_risk = customers_at_risk_df['total_at_risk'].iloc[0] if not customers_at_risk_df.empty else 0
        total_not_at_risk = customers_at_risk_df['total_not_at_risk'].iloc[0] if not customers_at_risk_df.empty else 0
        revenue_at_risk = customers_at_risk_df['total_revenue_at_risk'].iloc[0] if not customers_at_risk_df.empty else 0
        
        return html.Div([
            # Header with Filter
            html.Div([
                html.Div([
                    html.H2('Customer Churn Analysis', 
                           style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
                    html.P('Monitor customer retention, identify at-risk customers, and analyze churn patterns across demographics.',
                           style={'color': COLORS['text_secondary'], 'marginBottom': '0px', 'fontSize': '14px'}),
                ], style={'flex': '1'}),
                
                # Filter Dropdown - Prominent styling
                html.Div([
                    html.Label('🔍 FILTER BY STATUS', style={
                        'color': COLORS['accent'], 
                        'fontWeight': '700', 
                        'marginBottom': '8px', 
                        'display': 'block', 
                        'fontSize': '16px',
                        'letterSpacing': '0.5px'
                    }),
                    dcc.Dropdown(
                        id='churn-filter',
                        options=[
                            {'label': '📊 All Customers', 'value': 'all'},
                            {'label': '⚠️ At Risk Only', 'value': 'at_risk'},
                            {'label': '✅ Not at Risk Only', 'value': 'not_at_risk'}
                        ],
                        value='all',
                        clearable=False,
                        style={
                            'width': '280px', 
                            'fontSize': '16px',
                            'fontWeight': '600'
                        }
                    )
                ], style={
                    'textAlign': 'right',
                    'padding': '15px 20px',
                    'backgroundColor': 'white',
                    'borderRadius': '8px',
                    'border': f'3px solid {COLORS["accent"]}',
                    'boxShadow': f'0 4px 6px rgba(255, 87, 34, 0.2)'
                })
            ], style={'display': 'flex', 'alignItems': 'flex-start', 'marginBottom': '30px'}),
            
            # Key Metrics Row
            html.Div([
                create_metric_card(f'{total_at_risk:,}', 'Customers at Risk', COLORS['accent']),
                create_metric_card(f'{total_not_at_risk:,}', 'Customers Not at Risk', '#7FB800'),
                create_metric_card(f'${revenue_at_risk:,.0f}' if revenue_at_risk else '$0', 'Revenue at Risk', '#ff9800'),
            ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
            
            # Risk Distribution and Timeline (Row 1)
            html.Div([
                html.Div([
                    dcc.Graph(id='risk-dist-graph', figure=risk_dist_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(id='timeline-graph', figure=timeline_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Country and Platform Analysis (Row 2)
            html.Div([
                html.Div([
                    dcc.Graph(id='country-graph', figure=country_churn_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(id='platform-graph', figure=platform_churn_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Age and Engagement Analysis (Row 3)
            html.Div([
                html.Div([
                    dcc.Graph(id='age-graph', figure=age_churn_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginRight': '15px'
                }),
                html.Div([
                    dcc.Graph(id='engagement-graph', figure=engagement_fig)
                ], style={
                    'backgroundColor': COLORS['card_background'],
                    'borderRadius': '8px',
                    'padding': '20px',
                    'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                    'flex': '1',
                    'marginLeft': '15px'
                })
            ], style={'display': 'flex', 'marginBottom': '30px'}),
            
            # Top 10 At-Risk Customers Table
            html.Div([
                html.H3('Top 10 Customers Most Likely to Churn', 
                       style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P('Active customers with highest inactivity and risk indicators',
                       style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
                
                dash_table.DataTable(
                    id='top-at-risk-table',
                    columns=[{"name": i, "id": i} for i in top_at_risk_df.columns] if not top_at_risk_df.empty else [],
                    data=top_at_risk_df.to_dict('records') if not top_at_risk_df.empty else [],
                    page_size=10,
                    style_table={
                        'overflowX': 'auto'
                    },
                    style_header={
                        'backgroundColor': COLORS['accent'],
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'left',
                        'padding': '12px'
                    },
                    style_cell={
                        'textAlign': 'left',
                        'padding': '12px',
                        'fontSize': '13px',
                        'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
                    },
                    style_data={
                        'backgroundColor': COLORS['card_background'],
                        'color': COLORS['text'],
                        'border': f'1px solid {COLORS["border"]}'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        },
                        {
                            'if': {'column_id': 'risk_level', 'filter_query': '{risk_level} = "Critical"'},
                            'backgroundColor': '#ffe6e6',
                            'color': '#cc0000',
                            'fontWeight': '600'
                        },
                        {
                            'if': {'column_id': 'risk_level', 'filter_query': '{risk_level} = "High"'},
                            'backgroundColor': '#fff3e6',
                            'color': '#ff6600',
                            'fontWeight': '600'
                        }
                    ]
                ) if not top_at_risk_df.empty else html.P(
                    'No at-risk customer data available.',
                    style={'color': COLORS['text_secondary'], 'fontStyle': 'italic'}
                )
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])


# Callback to filter churn visualizations
@callback(
    [
        Output('risk-dist-graph', 'figure'),
        Output('timeline-graph', 'figure'),
        Output('country-graph', 'figure'),
        Output('platform-graph', 'figure'),
        Output('age-graph', 'figure'),
        Output('engagement-graph', 'figure')
    ],
    Input('churn-filter', 'value')
)
def update_churn_visualizations(filter_value):
    """Filter all churn visualizations based on selected risk status."""
    
    # Filter the base datasets
    if filter_value == 'at_risk':
        # Filter for churn = 1 (at risk customers)
        filtered_risk_dist = risk_distribution_df[risk_distribution_df['risk_category'] == 'At Risk'].copy()
        filtered_country = churn_by_country_df[churn_by_country_df['risk_category'] == 'At Risk'].copy()
        filtered_platform = churn_by_platform_df.copy()
        # For platform, we'll just show the churned_customers
        filtered_platform['active_customers'] = 0
        filtered_timeline = churn_timeline_df.copy()
        filtered_age = churn_by_age_df.copy()
        filtered_engagement = churn_engagement_df.copy()
        
    elif filter_value == 'not_at_risk':
        # Filter for churn = 0 (not at risk customers)
        filtered_risk_dist = risk_distribution_df[risk_distribution_df['risk_category'] == 'Not at Risk'].copy()
        filtered_country = churn_by_country_df[churn_by_country_df['risk_category'] == 'Not at Risk'].copy()
        filtered_platform = churn_by_platform_df.copy()
        # For platform, we'll just show the active_customers
        filtered_platform['churned_customers'] = 0
        filtered_timeline = pd.DataFrame()  # No timeline for not at risk
        filtered_age = pd.DataFrame()  # No age breakdown for not at risk
        filtered_engagement = pd.DataFrame()  # No engagement for not at risk
    else:
        # Show all customers
        filtered_risk_dist = risk_distribution_df.copy()
        filtered_country = churn_by_country_df.copy()
        filtered_platform = churn_by_platform_df.copy()
        filtered_timeline = churn_timeline_df.copy()
        filtered_age = churn_by_age_df.copy()
        filtered_engagement = churn_engagement_df.copy()
    
    # Recreate figures with filtered data
    from figures.churn_figures import (
        create_risk_distribution_fig,
        create_timeline_fig,
        create_country_churn_fig,
        create_platform_churn_fig,
        create_age_churn_fig,
        create_engagement_fig
    )
    
    new_risk_fig = create_risk_distribution_fig(filtered_risk_dist)
    new_timeline_fig = create_timeline_fig(filtered_timeline)
    new_country_fig = create_country_churn_fig(filtered_country)
    new_platform_fig = create_platform_churn_fig(filtered_platform)
    new_age_fig = create_age_churn_fig(filtered_age)
    new_engagement_fig = create_engagement_fig(filtered_engagement)
    
    return new_risk_fig, new_timeline_fig, new_country_fig, new_platform_fig, new_age_fig, new_engagement_fig


# Callback to filter Customer 360 table, map, metrics, and demographics
@callback(
    [
        Output('customer-map', 'figure'),
        Output('churn-features-table', 'data'),
        Output('churn-features-table', 'columns'),
        Output('filter-results-count', 'children'),
        Output('filter-clv-range', 'value'),
        Output('filter-vip-min', 'value'),
        Output('filter-segment', 'value'),
        Output('filter-churn-risk', 'value'),
        Output('filter-country', 'value'),
        Output('c360-metric-cards', 'children'),
        Output('c360-age-distribution', 'figure'),
        Output('c360-location-distribution', 'figure')
    ],
    [
        Input('apply-filters-btn', 'n_clicks'),
        Input('reset-filters-btn', 'n_clicks')
    ],
    [
        State('filter-clv-range', 'value'),
        State('filter-vip-min', 'value'),
        State('filter-segment', 'value'),
        State('filter-churn-risk', 'value'),
        State('filter-country', 'value')
    ],
    prevent_initial_call=True
)
def update_customer_table(apply_clicks, reset_clicks, clv_range, vip_min, segment, churn_risk, country):
    """Update the customer table, map, metrics, and demographics based on filter selections."""
    
    # Determine which button was clicked
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Define standard column structure (matching SQL SELECT order and layout)
    standard_columns = [
        {"name": "User ID", "id": "user_id"},
        {"name": "First Name", "id": "firstname"},
        {"name": "Last Name", "id": "lastname"},
        {"name": "Email", "id": "email"},
        {"name": "Churn Status", "id": "churn_status"},
        {"name": "CLV", "id": "customer_lifetime_value"},
        {"name": "VIP Probability", "id": "vip_customer_probability"},
        {"name": "Market Segment", "id": "market_segment"},
        {"name": "Country", "id": "country"},
        {"name": "Gender", "id": "gender"},
        {"name": "Age Group", "id": "age_group"},
        {"name": "Platform", "id": "platform"},
        {"name": "Order Count", "id": "order_count"},
        {"name": "Total Amount", "id": "total_amount"},
        {"name": "Session Count", "id": "session_count"},
        {"name": "Event Count", "id": "event_count"},
        {"name": "Days Since Creation", "id": "days_since_creation"},
        {"name": "Days Since Last Activity", "id": "days_since_last_activity"},
        {"name": "Latitude", "id": "lat"},
        {"name": "Longitude", "id": "lon"}
    ]
    
    # Helper function to create age distribution figure
    def create_age_dist_figure(age_gender_df):
        """Create age distribution histogram by gender."""
        if age_gender_df.empty:
            return go.Figure()
        
        fig = px.histogram(
            age_gender_df,
            x='age',
            color='gender',
            title='Customer Age Distribution by Gender',
            labels={'age': 'Age', 'count': 'Number of Customers'},
            color_discrete_sequence=[COLORS['accent'], COLORS['accent_light'], '#ff8a65']
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
    
    # Helper function to create location distribution figure
    def create_location_dist_figure(customer_df):
        """Create bar chart for customer distribution by country."""
        if customer_df.empty or 'country' not in customer_df.columns:
            return go.Figure()
        
        country_distribution = customer_df['country'].value_counts().reset_index()
        country_distribution.columns = ['country', 'customer_count']
        
        fig = px.bar(
            country_distribution,
            x='country',
            y='customer_count',
            title='Customer Distribution by Location',
            labels={'customer_count': 'Number of Customers', 'country': 'Country'},
            text='customer_count'
        )
        fig.update_traces(
            marker_color=COLORS['accent'],
            textposition='outside'
        )
        fig.update_layout(
            template='plotly_white',
            paper_bgcolor=COLORS['card_background'],
            plot_bgcolor=COLORS['card_background'],
            font={'color': COLORS['text']},
            height=400,
            margin=dict(t=40, b=60, l=60, r=20),
            xaxis={'tickangle': -30}
        )
        return fig
    
    # If reset button clicked, clear all filters and show default data
    if button_id == 'reset-filters-btn':
        # Get all data
        default_df = c360_service.get_churn_features(limit=10000)
        stats_df = c360_service.get_customer_statistics()
        age_gender_df = c360_service.get_age_gender_distribution()
        
        # Create map and table data
        map_fig = create_customer_map(default_df)
        data = default_df.to_dict('records') if not default_df.empty else []
        count_msg = f"Showing all {len(default_df)} records (filters reset)" if not default_df.empty else "No data available"
        
        # Create metric cards
        if not stats_df.empty:
            stats = stats_df.iloc[0]
            metric_cards = [
                create_metric_card(f"{stats['avg_age']:.1f}", 'Average Age', COLORS['accent']),
                create_metric_card(f"${stats['avg_clv']:,.0f}", 'Avg CLV', COLORS['accent_light']),
                create_metric_card(f"{stats['total_profiles']:,}", 'Total Profiles', '#ff8a65'),
            ]
        else:
            metric_cards = [
                create_metric_card('--', 'Average Age', COLORS['accent']),
                create_metric_card('--', 'Avg CLV', COLORS['accent_light']),
                create_metric_card('--', 'Total Profiles', '#ff8a65'),
            ]
        
        # Create age distribution figure
        age_dist_fig = create_age_dist_figure(age_gender_df)
        
        # Create location distribution figure
        location_dist_fig = create_location_dist_figure(default_df)
        
        return map_fig, data, standard_columns, count_msg, [0, 2500], '', None, '', None, metric_cards, age_dist_fig, location_dist_fig
    
    # Apply filters
    try:
        # Extract CLV min and max from range slider
        clv_min_val = clv_range[0] if clv_range and len(clv_range) == 2 else None
        clv_max_val = clv_range[1] if clv_range and len(clv_range) == 2 else None
        
        # Convert empty string or None to actual None for optional filters
        vip_min_val = vip_min if vip_min and vip_min != '' else None
        segment_val = segment if segment else None
        churn_risk_val = churn_risk if churn_risk != '' else None
        country_val = country if country else None
        
        # Fetch filtered data
        filtered_df = c360_service.get_filtered_customer_data(
            clv_min=clv_min_val,
            clv_max=clv_max_val,
            vip_min=vip_min_val,
            market_segment=segment_val,
            churn_risk=churn_risk_val,
            country=country_val,
            limit=10000
        )
        
        # Fetch statistics with same filters
        stats_df = c360_service.get_customer_statistics(
            clv_min=clv_min_val,
            clv_max=clv_max_val,
            vip_min=vip_min_val,
            market_segment=segment_val,
            churn_risk=churn_risk_val,
            country=country_val
        )
        
        # Fetch age/gender distribution with same filters
        age_gender_df = c360_service.get_age_gender_distribution(
            clv_min=clv_min_val,
            clv_max=clv_max_val,
            vip_min=vip_min_val,
            market_segment=segment_val,
            churn_risk=churn_risk_val,
            country=country_val
        )
        
        if filtered_df.empty:
            empty_map = create_customer_map(pd.DataFrame())
            empty_metric_cards = [
                create_metric_card('--', 'Average Age', COLORS['accent']),
                create_metric_card('--', 'Avg CLV', COLORS['accent_light']),
                create_metric_card('0', 'Total Profiles', '#ff8a65'),
            ]
            empty_age_fig = create_age_dist_figure(pd.DataFrame())
            empty_location_fig = create_location_dist_figure(pd.DataFrame())
            return empty_map, [], standard_columns, "⚠️ No customers match your filter criteria. Try adjusting the filters.", clv_range, vip_min, segment, churn_risk, country, empty_metric_cards, empty_age_fig, empty_location_fig
        
        # Create map with filtered data
        map_fig = create_customer_map(filtered_df)
        data = filtered_df.to_dict('records')
        count_msg = f"✅ Found {len(filtered_df)} customers matching your criteria"
        
        # Create metric cards from statistics
        if not stats_df.empty:
            stats = stats_df.iloc[0]
            metric_cards = [
                create_metric_card(f"{stats['avg_age']:.1f}", 'Average Age', COLORS['accent']),
                create_metric_card(f"${stats['avg_clv']:,.0f}", 'Avg CLV', COLORS['accent_light']),
                create_metric_card(f"{stats['total_profiles']:,}", 'Total Profiles', '#ff8a65'),
            ]
        else:
            metric_cards = [
                create_metric_card('--', 'Average Age', COLORS['accent']),
                create_metric_card('--', 'Avg CLV', COLORS['accent_light']),
                create_metric_card('--', 'Total Profiles', '#ff8a65'),
            ]
        
        # Create age distribution figure
        age_dist_fig = create_age_dist_figure(age_gender_df)
        
        # Create location distribution figure
        location_dist_fig = create_location_dist_figure(filtered_df)
        
        return map_fig, data, standard_columns, count_msg, clv_range, vip_min, segment, churn_risk, country, metric_cards, age_dist_fig, location_dist_fig
        
    except Exception as e:
        print(f"Error filtering customer data: {e}")
        error_msg = f"❌ Error applying filters: {str(e)}"
        error_map = create_customer_map(pd.DataFrame())
        error_metric_cards = [
            create_metric_card('--', 'Average Age', COLORS['accent']),
            create_metric_card('--', 'Avg CLV', COLORS['accent_light']),
            create_metric_card('--', 'Total Profiles', '#ff8a65'),
        ]
        error_age_fig = create_age_dist_figure(pd.DataFrame())
        error_location_fig = create_location_dist_figure(pd.DataFrame())
        return error_map, [], standard_columns, error_msg, clv_range, vip_min, segment, churn_risk, country, error_metric_cards, error_age_fig, error_location_fig


# Callback to trigger Databricks job when Push to Braze button is clicked
@callback(
    Output('braze-notification', 'children'),
    Input('push-to-braze-btn', 'n_clicks'),
    prevent_initial_call=True
)
def trigger_braze_job(n_clicks):
    """Trigger the Databricks job to push customer data to Braze."""
    if n_clicks is None or n_clicks == 0:
        raise dash.exceptions.PreventUpdate
    
    try:
        print(f"[DEBUG] Button clicked! n_clicks={n_clicks}")
        
        # Initialize Databricks workspace client
        # The SDK will automatically use the environment variables or default auth
        w = WorkspaceClient()
        print("[DEBUG] WorkspaceClient initialized successfully")
        
        # Get the job by name
        job_name = "push-to-braze-job"
        print(f"[DEBUG] Looking for job: {job_name}")
        
        # List all jobs and find the one matching our job name
        try:
            jobs = list(w.jobs.list())
            print(f"[DEBUG] Found {len(jobs)} total jobs in workspace")
            
            # Debug: print all job names
            for job in jobs:
                if job.settings:
                    print(f"[DEBUG] Job found: '{job.settings.name}' (ID: {job.job_id})")
        except Exception as list_error:
            print(f"[DEBUG] Error listing jobs: {list_error}")
            raise
        
        target_job = None
        
        for job in jobs:
            if job.settings and job.settings.name == job_name:
                target_job = job
                print(f"[DEBUG] ✅ Matched target job! ID: {job.job_id}")
                break
        
        if not target_job:
            print(f"[DEBUG] ❌ Job '{job_name}' not found in workspace")
            # List available jobs for debugging
            available_jobs = [job.settings.name for job in jobs if job.settings]
            print(f"[DEBUG] Available jobs: {available_jobs}")
            
            return html.Div([
                html.Span("❌ ", style={'fontSize': '18px'}),
                html.Span(f"Job '{job_name}' not found. Please ensure the Databricks Asset Bundle is deployed.",
                         style={'color': '#d32f2f', 'fontWeight': '600'}),
                html.Br(),
                html.Span(f"Available jobs: {', '.join(available_jobs[:5])}",
                         style={'color': COLORS['text_secondary'], 'fontSize': '12px', 'marginTop': '5px', 'display': 'block'})
            ], style={'padding': '12px', 'backgroundColor': '#ffebee', 'borderRadius': '6px', 'border': '1px solid #ef5350'})
        
        # Trigger the job
        print(f"[DEBUG] Triggering job with ID: {target_job.job_id}")
        run = w.jobs.run_now(job_id=target_job.job_id)
        print(f"[DEBUG] ✅ Job triggered successfully! Run ID: {run.run_id}")
        
        return html.Div([
            html.Span("✅ ", style={'fontSize': '18px'}),
            html.Span(f"Successfully triggered job! Run ID: {run.run_id}",
                     style={'color': '#2e7d32', 'fontWeight': '600'}),
            html.Br(),
            html.Span("The customer cohort is being pushed to Braze. Check the Databricks Jobs UI for progress.",
                     style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginTop': '5px', 'display': 'block'})
        ], style={'padding': '12px', 'backgroundColor': '#e8f5e9', 'borderRadius': '6px', 'border': '1px solid #66bb6a'})
        
    except Exception as e:
        print(f"[DEBUG] ❌ Exception occurred: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return html.Div([
            html.Span("❌ ", style={'fontSize': '18px'}),
            html.Span(f"Error triggering job: {type(e).__name__}",
                     style={'color': '#d32f2f', 'fontWeight': '600'}),
            html.Br(),
            html.Span(f"Details: {str(e)}",
                     style={'color': COLORS['text_secondary'], 'fontSize': '12px', 'marginTop': '5px', 'display': 'block'}),
            html.Br(),
            html.Span("Please check your Databricks connection and ensure the job is deployed with 'databricks bundle deploy'.",
                     style={'color': COLORS['text_secondary'], 'fontSize': '13px', 'marginTop': '5px', 'display': 'block'})
        ], style={'padding': '12px', 'backgroundColor': '#ffebee', 'borderRadius': '6px', 'border': '1px solid #ef5350'})


# Run the app
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8050)
