# Age Group Mapping Documentation

## Overview

Age groups in the churn analysis are stored as numerical levels (1-10) in the database. For better readability in visualizations, these are mapped to descriptive age ranges.

## Age Group Levels

| Level | Age Range |
|-------|-----------|
| 1     | 15 – 19   |
| 2     | 20 – 24   |
| 3     | 25 – 29   |
| 4     | 30 – 34   |
| 5     | 35 – 39   |
| 6     | 40 – 49   |
| 7     | 50 – 59   |
| 8     | 60 – 69   |
| 9     | 70 – 79   |
| 10    | 80 – 100  |

## Implementation

### In Refactored Code

The age range mapping is centralized in `config.py`:

```python
# config.py
AGE_RANGE_MAP = {
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
```

And used in chart creation:

```python
# figures/churn_figures.py
from config import AGE_RANGE_MAP

def create_age_churn_fig(churn_by_age_df):
    df = churn_by_age_df.copy()
    df['age_range'] = df['age_group'].map(AGE_RANGE_MAP).fillna(df['age_group'].astype(str))
    
    fig = px.bar(df, x='age_range', y='churn_rate_pct', ...)
```

### In Current app.py

The mapping is defined inline (will be migrated to use `AGE_RANGE_MAP` from `config.py` during full refactoring):

```python
# app.py
age_range_map = {
    1: '15-19',
    2: '20-24',
    # ... etc
}

if not churn_by_age_df.empty:
    churn_by_age_df_mapped = churn_by_age_df.copy()
    churn_by_age_df_mapped['age_range'] = churn_by_age_df_mapped['age_group'].map(age_range_map)
```

## Chart Behavior

### Before
- X-axis shows: `1, 2, 3, 4, 5, 6, 7, 8, 9, 10`
- Labels: Not immediately clear what ages these represent

### After
- X-axis shows: `15-19, 20-24, 25-29, 30-34, 35-39, 40-49, 50-59, 60-69, 70-79, 80-100`
- Labels: Clear age ranges for better understanding

## Usage in Other Components

If you need to use age group mapping in other parts of the application:

```python
from config import AGE_RANGE_MAP

# Convert a single age group
age_level = 5
age_range = AGE_RANGE_MAP.get(age_level, 'Unknown')  # Returns '51-59'

# Map a DataFrame column
df['age_range'] = df['age_group'].map(AGE_RANGE_MAP)
```

## Data Source

The age group levels come from the `mmolony_catalog.dbdemo_customer_churn.churn_features` table, specifically the `age_group` column returned by:

```sql
SELECT age_group, ...
FROM mmolony_catalog.dbdemo_customer_churn.churn_features
GROUP BY age_group
```

This is fetched via the `CustomerChurnService.get_churn_by_age_group()` method.

## Notes

- The mapping handles missing or unexpected values gracefully by converting them to strings as fallback
- The age ranges are non-overlapping and cover the full customer age spectrum
- This mapping is consistent across all churn analysis visualizations

