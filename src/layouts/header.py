"""
Application header layout.
"""

from dash import html
from config import COLORS, APP_TITLE, APP_DESCRIPTION


def create_header():
    """
    Create the application header with fixed logo and title.
    
    Returns:
        tuple: (fixed_logo_div, content_header_div)
    """
    # Fixed logo at top
    fixed_logo = html.Div([
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
    })
    
    # Content header with title and description
    content_header = html.Div([
        html.H1(APP_TITLE, 
                style={
                    'textAlign': 'center', 
                    'color': COLORS['accent'], 
                    'marginBottom': '10px',
                    'fontSize': '42px',
                    'fontWeight': '700',
                    'marginTop': '20px'
                }),
        
        html.P(APP_DESCRIPTION,
               style={
                   'textAlign': 'center',
                   'fontSize': '18px',
                   'color': COLORS['text'],
                   'marginBottom': '30px'
               }),
    ])
    
    return fixed_logo, content_header

