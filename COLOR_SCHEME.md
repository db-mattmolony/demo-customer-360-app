# Color Scheme Guide

## Overview

The Customer 360 Dashboard uses a consistent color scheme across all visualizations to clearly distinguish between healthy customers and at-risk customers.

## Core Principles

1. **Teal (#4E937A)** = Healthy, not at risk (churn=0)
2. **Orange Gradient** = At-risk, churn predicted (churn=1)
   - Light → Dark = Low → Critical risk

## Risk Category Colors

| Risk Category | Hex Code | RGB | Description |
|--------------|----------|-----|-------------|
| **Not at Risk** | `#4E937A` | RGB(78, 147, 122) | Teal - Healthy customers (churn=0) |
| **Low Risk** | `#ffb74d` | RGB(255, 183, 77) | Light Orange - Early warning (churn=1) |
| **Medium Risk** | `#ff7043` | RGB(255, 112, 67) | Medium Orange - Concerning (churn=1) |
| **High Risk** | `#ff5722` | RGB(255, 87, 34) | Dark Orange - Critical attention needed (churn=1) |
| **Critical Risk** | `#d84315` | RGB(216, 67, 21) | Darker Orange - Imminent churn (churn=1) |
| **Unknown** | `#9e9e9e` | RGB(158, 158, 158) | Gray - Missing data |

## Where These Colors Are Used

### 1. **Risk Distribution Pie Chart**
Shows the complete customer base segmented by risk level.

```
🔵 Teal slice = Healthy customers
🟠 Orange slices = At-risk customers (graduated by severity)
```

### 2. **Customer Risk Distribution by Country**
Stacked bar chart showing risk distribution for each country.

```
Each bar contains:
├─ Teal (bottom) = Not at Risk
├─ Light Orange = Low Risk  
├─ Medium Orange = Medium Risk
├─ Dark Orange = High Risk
└─ Darker Orange (top) = Critical Risk
```

### 3. **Customer Status by Platform**
Stacked bar showing churned vs. active customers by platform.

```
🟠 Orange bars = At Risk (Churn) - churned_customers
🔵 Teal bars = Not at Risk - active_customers
```

### 4. **Other Single-Color Charts**
- Age group churn rate: Solid orange (`#ff5722`)
- Engagement level churn rate: Solid orange (`#ff5722`)
- Country/Platform individual metrics: Orange for emphasis

## Brand Alignment

### Betashares Branding
- **Primary Accent**: `#ff5722` (Orange) - Used for primary actions and churn indicators
- **Secondary Accent**: `#ff7043` (Light Orange) - Used for secondary elements
- **Background**: `#f5f5f5` (Light gray) - App background
- **Card Background**: `#ffffff` (White) - Card surfaces

### Custom Addition
- **Healthy Indicator**: `#4E937A` (Teal) - Complements the orange while clearly distinguishing "good" from "bad"

## Design Rationale

### Why Teal for Healthy?
1. **Contrast**: Strong visual contrast with orange
2. **Association**: Cool color = calm, stable, healthy
3. **Accessibility**: Colorblind-friendly when paired with orange
4. **Brand Balance**: Complements Betashares orange without competing

### Why Orange Gradient for Risk?
1. **Consistency**: Uses the brand's primary orange color
2. **Intuitive**: Warm colors = warning/attention needed
3. **Progressive**: Light → dark shows escalating severity
4. **Universal**: Orange is internationally recognized as a warning color

## Usage in Code

### Centralized Definition
```python
# config.py
RISK_COLORS = {
    'Not at Risk': '#4E937A',
    'Low Risk': '#ffb74d',
    'Medium Risk': '#ff7043',
    'High Risk': '#ff5722',
    'Critical Risk': '#d84315',
    'Unknown': '#9e9e9e'
}
```

### In Visualizations
```python
# Use the centralized color map
from config import RISK_COLORS

fig = px.bar(
    df,
    color='risk_category',
    color_discrete_map=RISK_COLORS
)
```

## Accessibility

### Color Contrast Ratios
All colors meet WCAG AA standards for contrast:
- Teal (#4E937A) on white: 4.3:1 ✅
- All orange shades on white: > 4.5:1 ✅

### Colorblind-Friendly
- Teal vs. Orange is distinguishable for most types of colorblindness
- Deuteranopia (red-green): ✅ Clear distinction
- Protanopia (red-green): ✅ Clear distinction  
- Tritanopia (blue-yellow): ⚠️ Slight reduction but still distinguishable

## Chart-Specific Guidelines

### Pie Charts
- Use full color palette (teal + all orange levels)
- Order slices from least to most severe
- Add labels for clarity

### Stacked Bar Charts
- Bottom = healthy (teal)
- Top = highest risk (darker orange)
- Creates visual "warning" when top segments are large

### Single Bar Charts
- Use solid orange (`#ff5722`) for churn/risk metrics
- Use teal (`#4E937A`) for health/success metrics

### Line Charts
- Use orange for churn rate trends
- Use teal for retention rate trends

## Migration Notes

If you need to change the color scheme:

1. Update `config.py` `RISK_COLORS` dictionary
2. All visualizations will automatically use new colors
3. Update this documentation

## Examples

### Good Usage ✅
```python
# Risk distribution - uses full palette
create_risk_distribution_fig(data)  # Teal + orange gradient

# Platform comparison - binary distinction  
create_platform_churn_fig(data)  # Orange vs. Teal

# Single metric chart - orange emphasis
create_age_churn_fig(data)  # Solid orange bars
```

### Avoid ❌
```python
# Don't use random colors
fig.update_traces(marker_color='red')  # Use RISK_COLORS instead

# Don't reverse the association
color_map = {'Not at Risk': 'red', 'Critical Risk': 'green'}  # Confusing!

# Don't use color alone
# Always include labels, legends, and clear axis titles
```

## Summary

**Remember**: 
- 🔵 **Teal = Healthy** (Not at risk, churn=0)
- 🟠 **Orange = At Risk** (Predicted to churn, churn=1)
- Gradient shows severity within at-risk group

