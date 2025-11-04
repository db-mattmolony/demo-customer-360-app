"""
Customer 360 Dash Application
A multi-tab dashboard for comprehensive customer analytics.
"""

from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from services.C360Service import get_c360_service
from services.CustomerChurnService import get_churn_service
from services.CLVService import get_clv_service

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

# Initialize services
c360_service = get_c360_service()
churn_service = get_churn_service()
clv_service = get_clv_service()

# Fetch churn features data from Databricks (limited to 100 rows for C360 tab)
try:
    churn_features_df = c360_service.get_churn_features(limit=100)
    churn_summary_df = c360_service.get_churn_summary()
except Exception as e:
    print(f"Error loading churn features: {e}")
    churn_features_df = pd.DataFrame()
    churn_summary_df = pd.DataFrame()

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

# Create CLV figures
clv_distribution_fig = create_clv_distribution_fig(clv_segments_df)
clv_market_fig = create_clv_by_market_segment_fig(clv_by_market_df)
clv_country_fig = create_clv_by_country_fig(clv_by_country_df)
vip_dist_fig = create_vip_distribution_fig(vip_segments_df)
clv_spending_scatter = create_clv_vs_spending_scatter(clv_behavioral_df)
clv_orders_scatter = create_clv_vs_orders_scatter(clv_behavioral_df)
segment_behavior_fig = create_segment_behavior_fig(clv_segment_behavior_df)
clv_value_fig = create_clv_segment_value_fig(clv_segments_df)

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
        return html.Div([
            html.H2('VIP Customers', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Track your most valuable customers and their engagement patterns.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Metric cards
            html.Div([
                create_metric_card('20', 'VIP Customers', COLORS['accent']),
                create_metric_card('$28,500', 'Avg VIP Spend', COLORS['accent_light']),
                create_metric_card('8.5 years', 'Avg Membership', '#ff8a65'),
            ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
            
            # Chart
            html.Div([
                dcc.Graph(figure=vip_fig)
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])
    
    elif selected_tab == 'segmentation':
        return html.Div([
            html.H2('Customer Segmentation', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Understand how your customer base is distributed across different value segments.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Chart
            html.Div([
                dcc.Graph(figure=segment_fig)
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'marginBottom': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            }),
            
            # Segment definitions
            html.Div([
                html.H4('Segment Definitions:', style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '18px'}),
                html.Ul([
                    html.Li('High Value: Customers with CLV > $7,500', style={'marginBottom': '10px', 'color': COLORS['text_secondary']}),
                    html.Li('Medium Value: Customers with CLV $3,000 - $7,500', style={'marginBottom': '10px', 'color': COLORS['text_secondary']}),
                    html.Li('Low Value: Customers with CLV < $3,000', style={'marginBottom': '10px', 'color': COLORS['text_secondary']}),
                    html.Li('At Risk: Previously high-value customers with declining engagement', style={'marginBottom': '10px', 'color': COLORS['text_secondary']}),
                    html.Li('New: Customers joined within the last 6 months', style={'marginBottom': '10px', 'color': COLORS['text_secondary']}),
                ], style={'fontSize': '14px', 'lineHeight': '1.6'})
            ], style={
                'backgroundColor': COLORS['card_background'],
                'padding': '25px',
                'borderRadius': '8px',
                'borderLeft': f'4px solid {COLORS["accent"]}',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
            })
        ])
    
    elif selected_tab == 'customer360':
        return html.Div([
            html.H2('Customer 360 Profile', 
                   style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
            html.P('Complete demographic overview of your customer base.',
                   style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
            
            # Metric cards
            html.Div([
                create_metric_card('42.5', 'Average Age', COLORS['accent']),
                create_metric_card('$78,400', 'Avg Income', COLORS['accent_light']),
                create_metric_card('100', 'Total Profiles', '#ff8a65'),
            ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
            
            # Chart
            html.Div([
                dcc.Graph(figure=demo_fig)
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'marginBottom': '30px'
            }),
            
            # Churn Features Table from Databricks (First 100 Rows)
            html.Div([
                html.H3('Customer Churn Features Dataset', 
                       style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '20px', 'fontWeight': '600'}),
                html.P([
                    html.Span('Live data from Databricks SQL Warehouse - ', style={'color': COLORS['text_secondary']}),
                    html.Span(f'Showing first 100 records' if not churn_features_df.empty else 'No data', 
                             style={'color': COLORS['accent'], 'fontWeight': '600'})
                ], style={'marginBottom': '20px', 'fontSize': '14px'}),
                
                dash_table.DataTable(
                    id='churn-features-table',
                    columns=[{"name": i, "id": i} for i in churn_features_df.columns] if not churn_features_df.empty else [],
                    data=churn_features_df.to_dict('records') if not churn_features_df.empty else [],
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
                        {
                            'if': {'column_id': 'churn', 'filter_query': '{churn} = 1'},
                            'backgroundColor': '#ffe6e6',
                            'color': '#cc0000',
                            'fontWeight': '600'
                        }
                    ],
                    style_filter={
                        'backgroundColor': '#fff8f0',
                        'border': f'1px solid {COLORS["accent"]}'
                    },
                    tooltip_data=[
                        {
                            column: {'value': str(value), 'type': 'markdown'}
                            for column, value in row.items()
                        } for row in churn_features_df.to_dict('records')
                    ] if not churn_features_df.empty else [],
                    tooltip_duration=None
                ) if not churn_features_df.empty else html.P(
                    'No data available. Check Databricks connection.',
                    style={'color': COLORS['text_secondary'], 'fontStyle': 'italic'}
                )
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


# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
