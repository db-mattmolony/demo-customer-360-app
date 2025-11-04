# Customer Tenure Bucketing Strategy

## Overview

Customer tenure analysis now uses **standard deviation-based bucketing** instead of fixed time periods. This provides more meaningful insights based on your actual data distribution.

## Problem with Fixed Buckets

**Previous Approach:**
```
0-30 days
30-90 days
91-180 days
181-365 days
365+ days  ← Only 1 customer in this bucket!
```

**Issues:**
- Uneven distribution of customers across buckets
- The "365+" bucket is too broad and contains very few customers
- Doesn't adapt to your actual customer lifecycle patterns
- Arbitrary cutoff points don't reflect business reality

## Solution: Standard Deviation-Based Buckets

**New Approach:**
```
Very New (< -1 SD)        - Customers below mean - 1 standard deviation
New (-1 SD to Mean)       - Customers between mean - 1 SD and mean
Established (Mean to +1 SD) - Customers between mean and mean + 1 SD
Mature (+1 to +2 SD)      - Customers between mean + 1 SD and mean + 2 SD
Veteran (> +2 SD)         - Customers above mean + 2 standard deviations
```

**Benefits:**
- ✅ Automatically adapts to your data distribution
- ✅ More balanced bucket sizes
- ✅ Statistically meaningful groupings
- ✅ Better insights into lifecycle patterns
- ✅ Provides context with min/max/avg days in each bucket

## How It Works

### Step 1: Calculate Statistics
```sql
WITH stats AS (
    SELECT 
        AVG(days_since_creation) as mean_tenure,
        STDDEV(days_since_creation) as stddev_tenure
    FROM mmolony_catalog.dbdemo_customer_churn.churn_features
)
```

### Step 2: Create Buckets
Each customer is assigned to a bucket based on how many standard deviations they are from the mean:

```sql
CASE 
    WHEN days_since_creation < (mean - stddev) THEN 'Very New (< -1 SD)'
    WHEN days_since_creation < mean THEN 'New (-1 SD to Mean)'
    WHEN days_since_creation < (mean + stddev) THEN 'Established (Mean to +1 SD)'
    WHEN days_since_creation < (mean + 2*stddev) THEN 'Mature (+1 to +2 SD)'
    ELSE 'Veteran (> +2 SD)'
END
```

### Step 3: Include Statistics
For each bucket, we calculate:
- **Total customers** in the bucket
- **Churn rate** for the bucket
- **Min/Max/Avg days** in the bucket (for context)

## Example Output

| customer_tenure | total_customers | churn_rate_pct | min_days | max_days | avg_days |
|----------------|----------------|----------------|----------|----------|----------|
| Very New (< -1 SD) | 5,234 | 12.5% | 0 | 45 | 28 |
| New (-1 SD to Mean) | 18,456 | 8.2% | 46 | 120 | 85 |
| Established (Mean to +1 SD) | 32,890 | 5.1% | 121 | 220 | 165 |
| Mature (+1 to +2 SD) | 10,234 | 3.8% | 221 | 350 | 280 |
| Veteran (> +2 SD) | 1,676 | 2.1% | 351 | 800 | 425 |

## Interpretation

### Normal Distribution
If your data follows a normal distribution:
- **~68%** of customers fall within 1 SD of mean (New + Established)
- **~95%** fall within 2 SD (Very New through Mature)
- **~5%** are outliers (Veteran)

### Business Insights

**Very New (< -1 SD):**
- Newest customers, still evaluating your service
- Higher churn expected (adjustment period)
- Focus on onboarding and early engagement

**New (-1 SD to Mean):**
- Getting established but still relatively new
- Key period for building habits and loyalty
- Monitor engagement closely

**Established (Mean to +1 SD):**
- Your core, stable customer base
- Should have lower churn rates
- Good candidates for upselling/cross-selling

**Mature (+1 to +2 SD):**
- Long-term customers
- Very low churn expected
- Protect from competitive threats

**Veteran (> +2 SD):**
- Your most loyal, longest-tenure customers
- Extremely low churn
- Brand advocates and referral sources

## Visualization Enhancement

The chart now includes enhanced tooltips showing:
```
Very New (< -1 SD)
Churn Rate: 12.50%
Customers: 5,234
Avg Days: 28
Range: 0 - 45 days
```

This provides context about what "Very New" actually means in terms of days.

## Implementation Location

- **SQL Query**: `CustomerChurnService.get_churn_timeline_analysis()` in `services/CustomerChurnService.py`
- **Chart Creation**: `create_timeline_fig()` in `figures/churn_figures.py`
- **Current App**: Lines 286-328 in `app.py`

## Migration Notes

When fully migrating to the refactored architecture, the `app.py` inline implementation will be replaced with a call to `create_timeline_fig()` from the `figures` module.

## Further Customization

### Adjust Number of Buckets
If you want more granular analysis, you could add buckets:
```sql
WHEN days_since_creation < (mean - 2*stddev) THEN 'Very Early (< -2 SD)'
WHEN days_since_creation < (mean - stddev) THEN 'Early (-2 to -1 SD)'
-- ... etc
```

### Use Different Metrics
Instead of `days_since_creation`, you could bucket by:
- `total_amount` (revenue-based cohorts)
- `order_count` (purchase frequency cohorts)
- `session_count` (engagement cohorts)

### Combine with Other Dimensions
Cross-reference tenure buckets with:
- Platform
- Country
- Age group
- Product category

## SQL Performance

The query uses a CTE (Common Table Expression) with `CROSS JOIN` to calculate statistics once and apply to all rows. This is efficient for datasets up to millions of rows.

For extremely large datasets (>10M rows), consider:
1. Materializing the statistics as a table
2. Pre-computing buckets during ETL
3. Using approximate percentile functions

## References

- [Standard Deviation in Statistics](https://en.wikipedia.org/wiki/Standard_deviation)
- [Normal Distribution & 68-95-99.7 Rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
- Databricks SQL: `STDDEV()` function

