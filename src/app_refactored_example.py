"""
EXAMPLE: Refactored app.py structure

This is a template showing what app.py would look like after full refactoring.
NOT meant to replace the current app.py yet - use as a reference.
"""

from dash import Dash, html, dcc, callback, Output, Input
from config import COLORS
from data_loader import get_data_loader
from layouts import (
    create_header,
    create_clv_layout,
    create_churn_layout
    # Add other layouts as they're created:
    # create_vip_layout,
    # create_segmentation_layout,
    # create_customer360_layout,
)
from figures.churn_figures import (
    create_country_churn_fig,
    create_platform_churn_fig,
    create_risk_distribution_fig,
    create_timeline_fig,
    create_age_churn_fig,
    create_engagement_fig
)
from figures.clv_figures import (
    create_clv_bar_chart,
    create_clv_distribution_histogram,
    create_clv_scatter
)

# Initialize app
app = Dash(__name__, assets_folder='assets')
app.title = 'Customer 360 Dashboard'

# Load all data once at startup
data_loader = get_data_loader()

# Create all figures once at startup
churn_figures = {
    'country_churn': create_country_churn_fig(data_loader.churn_by_country_df),
    'platform_churn': create_platform_churn_fig(data_loader.churn_by_platform_df),
    'risk_distribution': create_risk_distribution_fig(data_loader.risk_distribution_df),
    'timeline': create_timeline_fig(data_loader.churn_timeline_df),
    'age_churn': create_age_churn_fig(data_loader.churn_by_age_df),
    'engagement': create_engagement_fig(data_loader.churn_engagement_df)
}

clv_figures = {
    'clv_bar': create_clv_bar_chart(data_loader.clv_data),
    'clv_histogram': create_clv_distribution_histogram(data_loader.clv_data),
    'clv_scatter': create_clv_scatter(data_loader.clv_data, data_loader.customer_demo)
}

# Main layout
app.layout = html.Div([
    # Fixed logo at top (from updated create_header)
    # Note: create_header now returns (fixed_logo, content_header)
    # For now, keeping old style. Update when refactoring is complete.
    
    # Full-width background
    html.Div([
        # Header placeholder (logo is now fixed at top)
        # create_header(),  # TODO: Update to use new tuple return
        
        # Tabs
        dcc.Tabs(
            id='tabs',
            value='customer360',
            children=[
                dcc.Tab(label='Customer 360', value='customer360', style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['text_secondary'],
                    'padding': '12px 24px',
                    'borderBottom': f'2px solid {COLORS["border"]}'
                }, selected_style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['accent'],
                    'padding': '12px 24px',
                    'borderBottom': f'3px solid {COLORS["accent"]}',
                    'fontWeight': '600'
                }),
                dcc.Tab(label='Customer Churn', value='churn', style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['text_secondary'],
                    'padding': '12px 24px',
                    'borderBottom': f'2px solid {COLORS["border"]}'
                }, selected_style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['accent'],
                    'padding': '12px 24px',
                    'borderBottom': f'3px solid {COLORS["accent"]}',
                    'fontWeight': '600'
                }),
                dcc.Tab(label='Customer Lifetime Value', value='clv', style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['text_secondary'],
                    'padding': '12px 24px',
                    'borderBottom': f'2px solid {COLORS["border"]}'
                }, selected_style={
                    'backgroundColor': COLORS['card_background'],
                    'color': COLORS['accent'],
                    'padding': '12px 24px',
                    'borderBottom': f'3px solid {COLORS["accent"]}',
                    'fontWeight': '600'
                }),
                # Add other tabs here
            ]
        ),
        
        # Tab content
        html.Div(id='tab-content', style={'padding': '30px'})
        
    ], style={
        'maxWidth': '1400px',
        'margin': '0 auto',
        'padding': '20px'
    }),
    
    # Author credit
    html.Div([
        html.P('Authored by Matthew Molony', style={
            'margin': '0',
            'color': COLORS['text_secondary'],
            'fontSize': '16px'
        })
    ], style={
        'position': 'fixed',
        'bottom': '0',
        'left': '0',
        'padding': '20px',
        'backgroundColor': 'rgba(255, 255, 255, 0.95)',
        'borderTop': f'1px solid {COLORS["border"]}',
        'zIndex': '1000'
    })
], style={
    'backgroundColor': COLORS['background'],
    'minHeight': '100vh',
    'margin': '0',
    'fontFamily': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'
})


@callback(
    Output('tab-content', 'children'),
    Input('tabs', 'value')
)
def render_tab_content(selected_tab):
    """
    Render the selected tab content.
    
    This callback is now extremely simple - just route to the appropriate layout function.
    All the complexity is hidden in the layout modules.
    """
    if selected_tab == 'customer360':
        # TODO: Implement create_customer360_layout
        return html.Div([html.H2('Customer 360 - Coming soon')])
    
    elif selected_tab == 'churn':
        return create_churn_layout(data_loader, churn_figures)
    
    elif selected_tab == 'clv':
        return create_clv_layout(data_loader, clv_figures)
    
    elif selected_tab == 'vip':
        # TODO: Implement create_vip_layout
        return html.Div([html.H2('VIP Customers - Coming soon')])
    
    elif selected_tab == 'segmentation':
        # TODO: Implement create_segmentation_layout
        return html.Div([html.H2('Customer Segmentation - Coming soon')])
    
    # Default fallback
    return html.Div([
        html.H2('Welcome to Customer 360 Dashboard'),
        html.P('Select a tab to view analytics.')
    ])


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)

