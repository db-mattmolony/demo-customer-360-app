"""
Customer Lifetime Value tab layout.
"""

from dash import html, dcc
import numpy as np
from config import COLORS
from components import create_metric_card


def create_clv_layout(data_loader, figures):
    """
    Create the Customer Lifetime Value tab layout.
    
    Args:
        data_loader: DataLoader instance with loaded data
        figures: Dict of figure objects
        
    Returns:
        html.Div: The CLV tab layout
    """
    clv_data = data_loader.clv_data
    
    # Calculate metrics
    total_clv = clv_data['lifetime_value'].sum() if not clv_data.empty else 0
    avg_clv = clv_data['lifetime_value'].mean() if not clv_data.empty else 0
    top_customer_clv = clv_data['lifetime_value'].max() if not clv_data.empty else 0
    
    return html.Div([
        html.H2('Customer Lifetime Value Analysis', 
               style={'color': COLORS['text'], 'marginBottom': '10px', 'fontSize': '24px', 'fontWeight': '600'}),
        html.P('Understand the long-term value of your customer base and identify high-value segments.',
               style={'color': COLORS['text_secondary'], 'marginBottom': '30px', 'fontSize': '14px'}),
        
        # Key Metrics Row
        html.Div([
            create_metric_card(f'${total_clv:,.0f}', 'Total CLV', COLORS['accent']),
            create_metric_card(f'${avg_clv:,.0f}', 'Average CLV', COLORS['accent_light']),
            create_metric_card(f'${top_customer_clv:,.0f}', 'Top Customer CLV', '#4caf50'),
        ], style={'display': 'flex', 'marginBottom': '30px', 'marginLeft': '-10px', 'marginRight': '-10px'}),
        
        # Top Customers Chart
        html.Div([
            dcc.Graph(figure=figures['clv_bar'])
        ], style={
            'backgroundColor': COLORS['card_background'],
            'borderRadius': '8px',
            'padding': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
            'marginBottom': '30px'
        }),
        
        # Distribution and Scatter (Row 2)
        html.Div([
            html.Div([
                dcc.Graph(figure=figures['clv_histogram'])
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'flex': '1',
                'marginRight': '15px'
            }),
            html.Div([
                dcc.Graph(figure=figures['clv_scatter'])
            ], style={
                'backgroundColor': COLORS['card_background'],
                'borderRadius': '8px',
                'padding': '20px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.08)',
                'flex': '1',
                'marginLeft': '15px'
            })
        ], style={'display': 'flex', 'marginBottom': '30px'}),
    ])

