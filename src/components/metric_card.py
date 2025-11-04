"""
Metric card component for displaying KPIs.
"""

from dash import html
from config import COLORS


def create_metric_card(value, label, color=COLORS['accent']):
    """
    Create a metric card component.
    
    Args:
        value: The metric value to display
        label: The metric label
        color: The accent color for the left border
        
    Returns:
        html.Div: The metric card component
    """
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

