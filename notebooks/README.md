# Pipeline Notebooks

This folder contains Databricks notebooks used in the Customer 360 synthetic data generation pipeline.

## 📓 Notebooks

### Synthetic Data Creation (Run in Parallel)

- **`_create_synthetic_clv.py`** - Generates synthetic Customer Lifetime Value (CLV) data
- **`_create_synthetic_location.py`** - Creates synthetic geographic coordinates by city
- **`_create_synthetic_segments.py`** - Generates market segments (Blue Chip, Crypto, Sustainability, Social Impact)
- **`_create_synthetic_vip_probability.py`** - Calculates VIP customer probability scores

### Sequential Tasks (Run After Parallel Tasks Complete)

- **`create_synthetic_attributes.py`** - Joins all synthetic tables into final `customer_ml_attributes` table
- **`_generate_synthetic_emails.py`** - Uses AI to generate realistic email addresses, updates `churn_features` table, and cleans up intermediate tables

### Legacy

- **`push_to_braze.py`** - Legacy notebook for uploading CSV to Unity Catalog (not used in pipeline)

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
databricks bundle run create_synthetic_ml_attributes_pipeline -t dev
```

The job will:
1. Run all synthetic data creation scripts in parallel (`_create_*.py`)
2. Wait for all parallel tasks to complete
3. Run `create_synthetic_attributes.py` to join all tables into final output
4. Run `_generate_synthetic_emails.py` to generate AI-powered email addresses
5. Clean up all intermediate tables (only keeping the final `customer_ml_attributes` table)

## 📋 Notebook Guidelines

When adding new notebooks to this folder:

1. **Keep it simple** - Focus on table creation, avoid exploratory code
2. **No displays** - Remove prints and displays for production
3. **Add documentation** in markdown cells
4. **Prefix with underscore** if the notebook should run in parallel (e.g., `_create_synthetic_*.py`)
5. **Add to pipeline** in `databricks.yaml` as a task

## 🎯 Best Practices

- **Idempotent**: Notebooks should be rerunnable without side effects (use `.write.mode("overwrite").option("overwriteSchema", "true")`)
- **Focused**: Keep notebooks focused on creating one table
- **Clean**: Remove exploratory code before committing
- **Documented**: Include markdown cells explaining the logic
- **Tested**: Test transformations interactively before adding to pipeline

## 📊 Output Tables

The pipeline creates the following tables in Unity Catalog:

| Table | Description | Created By | Status |
|-------|-------------|------------|--------|
| `mmolony_catalog.default.customer_360_clv` | Customer Lifetime Value | `_create_synthetic_clv.py` | ⚠️ Dropped at end |
| `mmolony_catalog.default.customer_360_locations` | Geographic coordinates | `_create_synthetic_location.py` | ⚠️ Dropped at end |
| `mmolony_catalog.default.customer_360_customer_segments` | Market segments | `_create_synthetic_segments.py` | ⚠️ Dropped at end |
| `mmolony_catalog.default.customer_360_vip` | VIP probabilities | `_create_synthetic_vip_probability.py` | ⚠️ Dropped at end |
| `mmolony_catalog.dbdemo_customer_churn.customer_ml_attributes` | Final joined table | `create_synthetic_attributes.py` | ✅ Kept |
| `mmolony_catalog.dbdemo_customer_churn.churn_features` | Updated with AI-generated emails | `_generate_synthetic_emails.py` | ✅ Kept |

## 🔍 Debugging

View job logs in Databricks UI:
1. Navigate to **Workflows → Jobs**

2. Select **create-synthetic-ml-attributes-pipeline**
3. Click on a run to see task execution details
4. Review individual task logs for errors

## 📚 Resources

- [Databricks Jobs](https://docs.databricks.com/workflows/jobs/jobs.html)
- [Task Dependencies](https://docs.databricks.com/workflows/jobs/jobs-user-guide.html#task-dependencies)
- [Notebook Best Practices](https://docs.databricks.com/notebooks/best-practices.html)

