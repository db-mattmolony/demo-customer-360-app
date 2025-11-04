# Customer Risk Distribution Guide

## Overview

The Customer Risk Distribution chart shows **ALL customer segments**, providing a complete view of your customer base health.

**Important**: 
- `churn = 1` means the customer IS predicted to churn (at risk)
- `churn = 0` means the customer is NOT at risk (healthy)

## Risk Categories

### Healthy Customers (churn = 0)

#### 1. **Not at Risk** 🟢
- **Definition**: Customers predicted NOT to churn (`churn = 0`)
- **Color**: Green (#4caf50)
- **Status**: Healthy, stable customers
- **Action**: Maintain engagement, upsell opportunities, loyalty programs

### At-Risk Customers (churn = 1)

Customers where `churn = 1` are predicted to churn. They're further segmented by inactivity:

#### 2. **Low Risk** 🟡
- **Definition**: Predicted to churn with < 14 days since last activity
- **Color**: Yellow (#ffb74d)
- **Status**: Early warning signs despite recent activity
- **Action**: Proactive engagement, understand friction points

#### 3. **Medium Risk** 🟠
- **Definition**: Predicted to churn with 14-29 days since last activity
- **Color**: Light Orange (#ff7043)
- **Status**: Concerning inactivity pattern
- **Action**: Re-engagement campaigns, special offers

#### 4. **High Risk** 🔴
- **Definition**: Predicted to churn with 30-60 days since last activity
- **Color**: Dark Orange (#ff5722)
- **Status**: Significant inactivity, likely to churn
- **Action**: Urgent outreach, win-back campaigns

#### 5. **Critical Risk** ⚫
- **Definition**: Predicted to churn with > 60 days since last activity
- **Color**: Dark Red (#d32f2f)
- **Status**: Severe inactivity, imminent churn
- **Action**: Last-chance retention efforts, feedback surveys

## SQL Logic

```sql
CASE 
    -- churn = 1 means predicted to churn, segment by inactivity
    WHEN churn = 1 AND days_since_last_activity > 60 THEN 'Critical Risk'
    WHEN churn = 1 AND days_since_last_activity BETWEEN 30 AND 60 THEN 'High Risk'
    WHEN churn = 1 AND days_since_last_activity BETWEEN 14 AND 29 THEN 'Medium Risk'
    WHEN churn = 1 AND days_since_last_activity < 14 THEN 'Low Risk'
    -- churn = 0 means not predicted to churn (healthy)
    WHEN churn = 0 THEN 'Not at Risk'
    ELSE 'Unknown'
END as risk_category
```

## Understanding the Data

### Churn Field Meaning
- **`churn = 0`**: Customer is predicted NOT to churn (healthy) → "Not at Risk"
- **`churn = 1`**: Customer IS predicted to churn → "Low/Medium/High/Critical Risk" (based on inactivity)

### Complete Picture
The chart shows the complete distribution:
- **Not at Risk** (churn=0): Healthy customers
- **Low Risk** (churn=1, recent activity): Predicted to churn despite activity
- **Medium Risk** (churn=1, some inactivity): Predicted to churn with concerning patterns
- **High Risk** (churn=1, significant inactivity): Likely to churn soon
- **Critical Risk** (churn=1, severe inactivity): Imminent churn

**Benefits**:
- ✅ See what % of customers are healthy vs. at-risk
- ✅ Understand severity of at-risk customers (inactivity level)
- ✅ Track if retention efforts move customers from churn=1 to churn=0
- ✅ Monitor overall customer health trends

## Interpreting the Chart

### Healthy Customer Base
```
Not at Risk: 60%
Low Risk: 20%
Medium Risk: 10%
High Risk: 5%
Critical Risk: 3%
Churned: 2%
```
✅ **Interpretation**: Strong, healthy customer base with most customers actively engaged.

### Warning Signs
```
Not at Risk: 30%
Low Risk: 15%
Medium Risk: 20%
High Risk: 20%
Critical Risk: 10%
Churned: 5%
```
⚠️ **Interpretation**: Concerning distribution with too many customers at risk. Need intervention.

### Critical Situation
```
Not at Risk: 15%
Low Risk: 10%
Medium Risk: 15%
High Risk: 25%
Critical Risk: 20%
Churned: 15%
```
🚨 **Interpretation**: Severe retention issues. Immediate action required across multiple fronts.

## Color Scheme

The colors follow a **consistent brand palette**:
- 🔵 **Teal (#4E937A)**: Not at Risk (churn=0, healthy)
- 🟡 **Light Orange (#ffb74d)**: Low Risk (churn=1, but active recently)
- 🟠 **Medium Orange (#ff7043)**: Medium Risk (churn=1, moderate inactivity)
- 🔴 **Dark Orange (#ff5722)**: High Risk (churn=1, high inactivity)
- ⚫ **Darker Orange (#d84315)**: Critical Risk (churn=1, severe inactivity)

**Design Logic**: 
- Teal = healthy, stable customers
- Orange gradient = at-risk customers (light → dark = low → critical risk)

This makes it immediately clear which segments are healthy vs. at what level of risk.

## Business Actions by Segment

### Not at Risk (< 7 days)
- **Strategy**: Retention & Growth
- **Actions**: 
  - Upsell premium features
  - Request referrals
  - Gather product feedback
  - Encourage social sharing

### Low Risk (7-13 days)
- **Strategy**: Maintain Engagement
- **Actions**:
  - Send newsletter/updates
  - Feature announcements
  - Community engagement
  - Usage tips & tricks

### Medium Risk (14-29 days)
- **Strategy**: Re-engagement
- **Actions**:
  - "We miss you" emails
  - Limited-time offers
  - Product updates
  - Personalized recommendations

### High Risk (30-60 days)
- **Strategy**: Win-back
- **Actions**:
  - Discount offers
  - Feature highlights they haven't tried
  - Direct outreach from account manager
  - Survey: "What can we improve?"

### Critical Risk (> 60 days)
- **Strategy**: Last Chance
- **Actions**:
  - Deep discount or free trial extension
  - Executive outreach
  - Personalized recovery plan
  - Exit interview to learn why

### Churned
- **Strategy**: Learn & Potentially Recover
- **Actions**:
  - Exit survey analysis
  - Win-back campaign (long-term)
  - Identify common churn patterns
  - Improve product to prevent future churn

## Data Columns Returned

The `get_churn_risk_distribution()` method returns:

| Column | Description |
|--------|-------------|
| `risk_category` | The risk segment name |
| `customer_count` | Number of customers in segment |
| `avg_revenue` | Average revenue per customer in segment |
| `total_revenue` | Total revenue from segment |

## Monitoring Over Time

Track how the distribution changes:

**Month 1**:
- Not at Risk: 45%
- At Risk (all levels): 50%
- Churned: 5%

**Month 2** (after interventions):
- Not at Risk: 55% ⬆️ +10%
- At Risk (all levels): 40% ⬇️ -10%
- Churned: 5%

This shows your retention efforts are working!

## Implementation

### In CustomerChurnService
```python
churn_service = get_churn_service()
risk_dist_df = churn_service.get_churn_risk_distribution()
```

### In Figures
```python
from figures.churn_figures import create_risk_distribution_fig
risk_fig = create_risk_distribution_fig(risk_dist_df)
```

### Color Mapping
The colors are automatically applied based on the risk category name using a color map defined in `create_risk_distribution_fig()`.

## Best Practices

1. **Review Weekly**: Monitor the distribution weekly to catch trends early
2. **Set Thresholds**: Define acceptable % for each segment
3. **Automate Alerts**: Alert when "Critical Risk" exceeds threshold
4. **Segment Actions**: Different campaigns for each risk level
5. **Track Impact**: Measure if interventions move customers to lower risk categories

## Related Metrics

This chart pairs well with:
- **Customers at Risk** (total count metric)
- **Revenue at Risk** (dollar value at stake)
- **Churn Rate by Tenure** (when do customers churn?)
- **Top At-Risk Customers** (who specifically needs attention?)

## Summary

By including "Not at Risk" customers, the Risk Distribution chart now provides:
- ✅ Complete customer base visibility
- ✅ Clear health indicators with traffic light colors
- ✅ Actionable segmentation for targeted campaigns
- ✅ Ability to track overall customer health trends
- ✅ Better context for retention metrics

