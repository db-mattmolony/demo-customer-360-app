# Pipeline Configurations

This folder contains configuration files for Databricks Delta Live Tables (DLT) pipelines.

## 📄 Configuration Files

### `customer_360_pipeline_config.json`
JSON configuration for the Customer 360 DLT pipeline. This file can be imported directly into Databricks UI or used as a reference.

**Key Settings:**
- Pipeline name and storage location
- Cluster configuration (autoscaling, Photon)
- Notebook references
- Source/target catalog configuration

## 🎯 Purpose

While the main pipeline configuration lives in `databricks.yaml` (for DAB deployment), this folder provides:

1. **Standalone configs**: Can be imported directly in Databricks UI
2. **Reference templates**: Easy to copy for new pipelines
3. **Version control**: Track pipeline settings changes over time
4. **Documentation**: Explicit configuration examples

## 🔧 Using Configuration Files

### Option 1: Deploy via DAB (Recommended)

The `databricks.yaml` in the project root automatically references notebooks. Deploy with:

```bash
databricks bundle deploy -t dev
```

### Option 2: Manual Import in Databricks UI

1. Navigate to **Workflows → Delta Live Tables**
2. Click **Create Pipeline**
3. Select **Import Pipeline** (or use JSON settings)
4. Paste contents of `customer_360_pipeline_config.json`
5. Adjust paths and settings as needed
6. Click **Create**

### Option 3: Via Databricks CLI

```bash
databricks pipelines create \
  --config @pipelines/customer_360_pipeline_config.json
```

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

