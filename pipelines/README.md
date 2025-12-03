# Pipeline Configurations

⚠️ **Note**: This folder contains legacy DLT pipeline configurations. The project now uses **Databricks Jobs** for orchestration (configured in `databricks.yaml`).

## 📄 Configuration Files

### `customer_360_pipeline_config.json` (Legacy)
Original JSON configuration for a DLT pipeline approach. This is kept for reference but is no longer used.

**Current Approach:**
The synthetic data generation pipeline is now defined as a Databricks Job in `databricks.yaml` with:
- 4 parallel tasks creating individual synthetic data tables
- 1 final task that joins all tables after parallel tasks complete

## 🚀 Current Pipeline (Databricks Jobs)

The current pipeline is configured in `databricks.yaml` as a job:

```bash
# Deploy and run the pipeline
databricks bundle deploy -t dev
databricks bundle run create_synthetic_ml_attributes_pipeline -t dev
```

The job executes:
1. **Parallel Tasks** (run simultaneously):
   - `create_clv` → Creates CLV table
   - `create_location` → Creates location table
   - `create_segments` → Creates segments table
   - `create_vip` → Creates VIP probability table

2. **Sequential Tasks** (run after all parallel tasks complete):
   - `create_final_table` → Joins all tables into `customer_ml_attributes`
   - `generate_synthetic_emails` → Uses AI to generate and update email addresses

## 📋 Legacy DLT Approach

### Option 1: Deploy via DAB (Legacy)

The old DLT approach used `databricks.yaml` with pipeline resources:

```bash
databricks bundle deploy -t dev
```

### Option 2: Manual Import in Databricks UI (Legacy)

1. Navigate to **Workflows → Delta Live Tables**
2. Click **Create Pipeline**
3. Select **Import Pipeline** (or use JSON settings)
4. Paste contents of `customer_360_pipeline_config.json`
5. Adjust paths and settings as needed
6. Click **Create**

## 🔄 Why Jobs Instead of DLT?

**Advantages of Jobs approach:**
- ✅ More flexible task orchestration (parallel + sequential)
- ✅ Better suited for one-off data generation tasks
- ✅ Simpler configuration and debugging
- ✅ Lower overhead for non-streaming workloads

**When to use DLT:**
- Continuous streaming pipelines
- Complex data quality requirements
- Automatic schema evolution needs
- Incremental processing with change data capture

## ⚙️ Configuration Structure

```json
{
  "name": "customer-360-dlt-pipeline",
  "storage": "/pipelines/customer_360",
  "configuration": {
    // Pipeline-level parameters accessible via spark.conf
    // Note: Catalog and schema names are also configured in src/config.py
    "source_catalog": "mmolony_catalog",
    "target_catalog": "mmolony_catalog",
    "target_schema": "dbdemo_customer_churn"
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 5
      }
    }
  ],
  "libraries": [
    {
      "notebook": {
        "path": "../notebooks/example_pipeline_notebook.py"
      }
    }
  ],
  "target": "customer_360_pipeline",  // Target schema
  "photon": true,                     // Enable Photon
  "continuous": false,                // Triggered mode
  "development": true                 // Dev mode
}
```

## 📝 Adding New Configurations

When creating a new pipeline configuration:

1. **Copy template**:
   ```bash
   cp customer_360_pipeline_config.json new_pipeline_config.json
   ```

2. **Update settings**:
   - Change `name` to unique identifier
   - Update `storage` path
   - Modify `target` schema
   - Add/remove notebook references
   - Adjust cluster sizing

3. **Update databricks.yaml**:
   ```yaml
   pipelines:
     new_pipeline:
       name: new-pipeline-name
       libraries:
         - notebook:
             path: ./notebooks/new_notebook.py
   ```

## 🚀 Configuration Best Practices

### Development vs Production

**Development:**
```json
{
  "development": true,
  "continuous": false,
  "clusters": [{
    "autoscale": {
      "min_workers": 1,
      "max_workers": 3
    }
  }]
}
```

**Production:**
```json
{
  "development": false,
  "continuous": true,
  "clusters": [{
    "autoscale": {
      "min_workers": 2,
      "max_workers": 10
    }
  }]
}
```

### Cost Optimization

- Use **Photon**: 2-3x faster, lower cost
- Enable **autoscaling**: Pay only for what you use
- Set **min_workers** appropriately: Avoid over-provisioning
- Use **development mode** for testing: Faster, less validation

### Performance Tuning

```json
{
  "photon": true,                    // Enable Photon engine
  "clusters": [{
    "autoscale": {
      "min_workers": 2,
      "max_workers": 10,
      "mode": "ENHANCED"             // Better autoscaling
    },
    "spark_conf": {
      "spark.databricks.delta.optimizeWrite.enabled": "true",
      "spark.databricks.delta.autoCompact.enabled": "true"
    }
  }]
}
```

## 🔄 Environment-Specific Configs

Create separate configs for each environment:

```
pipelines/
├── customer_360_pipeline_config.json       # Base template
├── customer_360_pipeline_dev_config.json   # Dev settings
└── customer_360_pipeline_prod_config.json  # Prod settings
```

## 📊 Monitoring

Monitor pipeline performance:

```bash
# Get pipeline status
databricks pipelines get --pipeline-id <PIPELINE_ID>

# List recent updates
databricks pipelines list-updates --pipeline-id <PIPELINE_ID>

# Get update details
databricks pipelines get-update --pipeline-id <PIPELINE_ID> --update-id <UPDATE_ID>
```

## 🔐 Security Considerations

- **Credentials**: Never hardcode credentials in config files
- **Use secrets**: Reference Databricks secrets for sensitive data
- **Access control**: Set appropriate permissions on pipeline objects
- **Audit logging**: Enable audit logs for compliance

Example with secrets:
```json
{
  "configuration": {
    "api_key": "{{secrets/my_scope/api_key}}"
  }
}
```

## 📚 Resources

- [DLT Pipeline Settings](https://docs.databricks.com/delta-live-tables/settings.html)
- [Pipeline Configuration](https://docs.databricks.com/delta-live-tables/configure-pipeline.html)
- [Cluster Configuration](https://docs.databricks.com/clusters/configure.html)
- [Photon Documentation](https://docs.databricks.com/runtime/photon.html)

