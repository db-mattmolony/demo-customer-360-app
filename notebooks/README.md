# Pipeline Notebooks

This folder contains Databricks notebooks used in the Customer 360 Delta Live Tables (DLT) pipeline.

## 📓 Notebooks

### `push_to_braze.py`
Pipeline notebook that prints the current datetime when executed.

This notebook serves as a simple example for pipeline execution and can be extended to push customer data to Braze.

## 🔧 Development

### Running Locally

1. Open the notebook in Databricks workspace
2. Attach to a cluster
3. Run cells interactively for development

### Testing Changes

```python
# Test a specific table function
# Note: Catalog and schema names are configured in src/config.py
# Default: mmolony_catalog.dbdemo_customer_churn
df = spark.table("mmolony_catalog.dbdemo_customer_churn.churn_features")
# ... test transformations ...
display(df)
```

### Deploying via DAB

```bash
# From project root
databricks bundle deploy -t dev
databricks bundle run customer_360_dlt_pipeline -t dev
```

## 📋 Notebook Guidelines

When adding new notebooks to this folder:

1. **Use DLT decorators** (`@dlt.table`, `@dlt.view`)
2. **Include data quality checks** (`@dlt.expect_or_drop`)
3. **Add documentation** in markdown cells
4. **Follow naming conventions**:
   - Bronze: `bronze_<table_name>`
   - Silver: `silver_<table_name>`
   - Gold: `gold_<table_name>`
5. **Add to pipeline config** in `databricks.yaml`

## 🎯 Best Practices

- **Idempotent**: Notebooks should be rerunnable without side effects
- **Parameterized**: Use widgets or configuration for flexibility
- **Documented**: Include markdown cells explaining logic
- **Tested**: Test transformations before adding to pipeline
- **Quality checks**: Add expectations for critical data fields

## 📊 Data Quality

All notebooks should include:
- Input validation (null checks, data types)
- Business logic validation (ranges, formats)
- Output metrics for monitoring

Example:
```python
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect("reasonable_clv", "customer_lifetime_value BETWEEN 0 AND 100000")
```

## 🔍 Debugging

View pipeline logs in Databricks UI:
1. Navigate to **Workflows → Delta Live Tables**
2. Select **customer-360-dlt-pipeline**
3. Click **Updates** tab to see execution history
4. Review **Data Quality** tab for expectation results

## 📚 Resources

- [DLT Python API](https://docs.databricks.com/delta-live-tables/python-ref.html)
- [Data Quality Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Notebook Best Practices](https://docs.databricks.com/notebooks/best-practices.html)

