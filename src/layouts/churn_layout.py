"""
Customer Churn tab layout.
"""

from dash import html, dcc, dash_table
from config import COLORS
from components import create_metric_card


def create_churn_layout(data_loader, figures):
    """
    Create the Customer Churn tab layout.
    
    Args:
        data_loader: DataLoader instance with loaded data
        figures: Dict of figure objects
        
    Returns:
        html.Div: The churn tab layout
    """
    # Calculate metrics from data
    customers_at_risk_df = data_loader.customers_at_risk_df
    top_at_risk_df = data_loader.top_at_risk_df
    
    total_at_risk = customers_at_risk_df['total_at_risk'].iloc[0] if not customers_at_risk_df.empty else 0
    critical_risk = customers_at_risk_df['critical_risk'].iloc[0] if not customers_at_risk_df.empty else 0
    revenue_at_risk = customers_at_risk_df['total_revenue_at_risk'].iloc[0] if not customers_at_risk_df.empty else 0
    
    return html.Div([
        html.H2('Customer Churn Analysis', 
               style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
        html.P('Monitor customer retention, identify at-risk customers, and analyze churn patterns across demographics.',
               style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
        
        # Key Metrics Row
        html.Div([
            create_metric_card(f'{total_at_risk:,}', 'Customers at Risk', COLORS['accent']),
            create_metric_card(f'{critical_risk:,}', 'Critical Risk', '#f44336'),
            create_metric_card(f'${revenue_at_risk:,.0f}' if revenue_at_risk else '$0', 'Revenue at Risk', '#ff9800'),
        ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
        
        # Risk Distribution and Timeline (Row 1)
        html.Div([
            _create_chart_card(figures['risk_distribution']),
            _create_chart_card(figures['timeline'])
        ], style={'display': 'flex', 'marginBottom': '30px'}),
        
        # Country and Platform Analysis (Row 2)
        html.Div([
            _create_chart_card(figures['country_churn']),
            _create_chart_card(figures['platform_churn'])
        ], style={'display': 'flex', 'marginBottom': '30px'}),
        
        # Age and Engagement Analysis (Row 3)
        html.Div([
            _create_chart_card(figures['age_churn']),
            _create_chart_card(figures['engagement'])
        ], style={'display': 'flex', 'marginBottom': '30px'}),
        
        # Top 10 At-Risk Customers Table
        _create_at_risk_table(top_at_risk_df)
    ])


def _create_chart_card(figure):
    """Helper to create a chart card with consistent styling."""
    return html.Div([
        dcc.Graph(figure=figure)
    ], style={
        'backgroundColor': COLORS['card_background'],
        'borderRadius': '8px',
        'padding': '20px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
        'flex': '1',
        'margin': '0 15px'
    })


def _create_at_risk_table(top_at_risk_df):
    """Helper to create the at-risk customers table."""
    return html.Div([
        html.H3('Top 10 Customers Most Likely to Churn', 
               style={'color': COLORS['text'], 'marginBottom': '15px', 'fontSize': '20px', 'fontWeight': '600'}),
        html.P('Active customers with highest inactivity and risk indicators',
               style={'color': COLORS['text_secondary'], 'marginBottom': '20px', 'fontSize': '14px'}),
        
        dash_table.DataTable(
            id='top-at-risk-table',
            columns=[{"name": i, "id": i} for i in top_at_risk_df.columns] if not top_at_risk_df.empty else [],
            data=top_at_risk_df.to_dict('records') if not top_at_risk_df.empty else [],
            page_size=10,
            style_table={'overflowX': 'auto'},
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
                {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'},
                {'if': {'column_id': 'risk_level', 'filter_query': '{risk_level} = "Critical"'}, 
                 'backgroundColor': '#ffe6e6', 'color': '#cc0000', 'fontWeight': '600'},
                {'if': {'column_id': 'risk_level', 'filter_query': '{risk_level} = "High"'}, 
                 'backgroundColor': '#fff3e6', 'color': '#ff6600', 'fontWeight': '600'}
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

