

# App Refactoring Guide

## Problem
The `app.py` file has grown to 889 lines, making it difficult to maintain, test, and collaborate on.

## Solution
Modularize the application into logical, reusable components following separation of concerns principles.

## New Architecture

```
src/
├── app.py                      # Main entry point (~50-100 lines)
├── config.py                   # ✅ Colors, constants
├── data_loader.py              # ✅ Service initialization & data loading
├── components/
│   ├── __init__.py             # ✅
│   └── metric_card.py          # ✅ Reusable UI components
├── figures/
│   ├── __init__.py             # ✅
│   ├── churn_figures.py        # ✅ Chart creation functions
│   ├── clv_figures.py          # TODO
│   ├── vip_figures.py          # TODO
│   ├── segmentation_figures.py # TODO
│   └── customer360_figures.py  # TODO
├── layouts/
│   ├── __init__.py             # ✅
│   ├── header.py               # ✅
│   ├── churn_layout.py         # ✅ Tab content layouts
│   ├── clv_layout.py           # TODO
│   ├── vip_layout.py           # TODO
│   ├── segmentation_layout.py  # TODO
│   └── customer360_layout.py   # TODO
└── services/                   # ✅ (Already exists)
    ├── SQLService.py
    ├── C360Service.py
    └── CustomerChurnService.py
```

## Files Created

### ✅ `config.py`
- `COLORS` dict
- `CHART_LAYOUT_DEFAULTS` dict
- `APP_TITLE`, `APP_DESCRIPTION` constants

### ✅ `data_loader.py`
- `DataLoader` class - Centralizes all data loading
- `get_data_loader()` singleton
- Methods: `load_all_data()`, `_load_churn_features()`, `_load_churn_analysis()`, `_generate_sample_data()`

### ✅ `components/metric_card.py`
- `create_metric_card(value, label, color)` function
- Reusable across all tabs

### ✅ `layouts/header.py`
- `create_header()` function
- Logo, title, and description

### ✅ `figures/churn_figures.py` (Example)
- `create_country_churn_fig()`
- `create_platform_churn_fig()`
- `create_risk_distribution_fig()`
- `create_timeline_fig()`
- `create_age_churn_fig()`
- `create_engagement_fig()`

### ✅ `layouts/churn_layout.py` (Example)
- `create_churn_layout(data_loader, figures)` function
- Returns complete Churn tab HTML structure

## Benefits

### 1. **Maintainability**
- Each file has a single responsibility
- Changes to charts don't affect layouts
- Easier to find and fix bugs

### 2. **Reusability**
- Components like `metric_card` used across tabs
- Chart functions can be used in multiple contexts
- Layout patterns can be templated

### 3. **Testability**
- Test chart creation independently
- Test layouts with mock data
- Test components in isolation

### 4. **Collaboration**
- Multiple developers can work on different tabs
- Clearer git diffs
- Easier code reviews

### 5. **Scalability**
- Easy to add new tabs
- Simple to add new visualizations
- Clear patterns to follow

## Migration Steps

### Step 1: Extract Figures (Complete for Churn tab)

Create `figures/<tab>_figures.py` for each tab:

```python
# figures/clv_figures.py
import plotly.express as px
from config import COLORS, CHART_LAYOUT_DEFAULTS

def create_clv_bar_chart(clv_data):
    fig = px.bar(clv_data, x='customer_name', y='lifetime_value')
    fig.update_layout(**CHART_LAYOUT_DEFAULTS)
    return fig
```

### Step 2: Extract Layouts (Complete for Churn tab)

Create `layouts/<tab>_layout.py` for each tab:

```python
# layouts/clv_layout.py
from dash import html, dcc
from components import create_metric_card

def create_clv_layout(data_loader, figures):
    return html.Div([
        html.H2('Customer Lifetime Value'),
        # ... rest of layout
    ])
```

### Step 3: Simplify app.py

The main `app.py` becomes much simpler:

```python
from dash import Dash, html, dcc, callback, Output, Input
from config import COLORS
from data_loader import get_data_loader
from layouts import (
    create_header,
    create_clv_layout,
    create_vip_layout,
    create_segmentation_layout,
    create_customer360_layout,
    create_churn_layout
)
from figures import (
    create_country_churn_fig,
    create_platform_churn_fig,
    # ... other figures
)

# Initialize
app = Dash(__name__, assets_folder='assets')
data_loader = get_data_loader()

# Create all figures once
churn_figures = {
    'country_churn': create_country_churn_fig(data_loader.churn_by_country_df),
    'platform_churn': create_platform_churn_fig(data_loader.churn_by_platform_df),
    # ... other figures
}

# Main layout
app.layout = html.Div([
    html.Div([
        create_header(),
        dcc.Tabs(id='tabs', value='customer360', children=[...]),
        html.Div(id='tab-content')
    ])
])

# Callback
@callback(Output('tab-content', 'children'), Input('tabs', 'value'))
def render_tab_content(selected_tab):
    if selected_tab == 'churn':
        return create_churn_layout(data_loader, churn_figures)
    # ... other tabs
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
```

## Pattern to Follow

For each new tab:

1. **Create figures file:**
   ```python
   # figures/<tab>_figures.py
   def create_<chart_name>_fig(data):
       # Create and return figure
   ```

2. **Create layout file:**
   ```python
   # layouts/<tab>_layout.py
   def create_<tab>_layout(data_loader, figures):
       # Create and return layout
   ```

3. **Update app.py:**
   - Import the functions
   - Create figures in initialization
   - Call layout function in callback

## Next Steps

1. **Complete remaining tabs:**
   - Extract CLV figures and layout
   - Extract VIP figures and layout
   - Extract Segmentation figures and layout
   - Extract Customer360 figures and layout

2. **Add unit tests:**
   ```python
   # tests/test_figures.py
   def test_create_country_churn_fig():
       df = pd.DataFrame(...)
       fig = create_country_churn_fig(df)
       assert fig is not None
   ```

3. **Add documentation:**
   - Docstrings for all functions
   - Type hints for parameters
   - Examples in README

## Example: Complete Churn Tab Refactored

Already implemented:
- ✅ `figures/churn_figures.py` - All 6 chart functions
- ✅ `layouts/churn_layout.py` - Complete layout with table
- ✅ Clean, testable, maintainable code

Follow this pattern for remaining tabs!

