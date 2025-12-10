# Customer 360 Dash Application

A comprehensive Customer 360 analytics dashboard built on Databricks, featuring churn prediction, customer lifetime value analysis, and Braze integration for customer engagement.

## About This Demo

This application is built on top of the [Databricks Customer 360 Platform demo](https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/c360-platform-reduce-churn), which provides a complete Customer 360 solution for reducing customer churn using the Databricks Lakehouse Platform.

## Project Structure

```
demo-customer-360-app/
├── databricks.yaml          # DAB configuration file
├── requirements.txt         # Python dependencies
├── src/
│   └── app.py              # Main Dash application
└── README.md               # This file
```

## Prerequisites

- Databricks CLI installed (`pip install databricks-cli`)
- Databricks workspace access with the base C360 demo data
- Python 3.8 or higher
- Braze account (request access through Opal)

## Setup Instructions

### Step 1: Deploy Base Databricks C360 Demo (Optional)

If you haven't already, deploy the base Databricks C360 demo to your workspace:

1. Follow the instructions at: https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/c360-platform-reduce-churn
2. This will create the required `churn_features` and `churn_users` tables

### Step 2: Run the Synthetic Data Pipeline

Before deploying the app, you must run the synthetic data generation pipeline to create the ML attributes:

```bash
# Deploy the bundle (includes the pipeline)
databricks bundle deploy -t dev

# Run the synthetic attributes pipeline
databricks bundle run create_synthetic_ml_attributes_pipeline -t dev
```

This pipeline will:
- Generate synthetic Customer Lifetime Value (CLV) data
- Create geographic location data
- Generate market segments (Blue Chip, Crypto, Sustainability, Social Impact)
- Calculate VIP customer probability scores
- Create AI-generated email addresses
- Join all data into the final `customer_ml_attributes` table

See [`notebooks/README.md`](notebooks/README.md) for more details on the pipeline notebooks.

### Step 3: Configure Braze Integration (Optional)

To enable the "Push to Braze" functionality:

1. **Request Braze Access**: Request access to Braze through Opal
2. **Set up Cloud Data Ingestion**: Follow the [Braze Cloud Data Ingestion guide](https://www.braze.com/docs/user_guide/data/unification/cloud_ingestion/integrations) to configure Databricks as a data source
3. **Configure Sync**: Set up a sync between your `braze_target_segment_sync` table and Braze
4. **Run the Braze Sync Notebook**: Execute `notebooks/create_braze_sync.py` to create the sync tables

The app includes a "Push to Braze" button in the Customer 360 tab that allows you to export filtered customer segments directly to Braze for targeted campaigns.

## Local Development

### 1. Set up virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the application locally

```bash
cd src
python app.py
```

The application will be available at `http://localhost:8050`

## Databricks Asset Bundle Deployment

### 1. Configure Databricks CLI

First, authenticate with your Databricks workspace:

```bash
databricks configure --token
```

You'll need:
- Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- Personal access token (generate from User Settings > Access Tokens)

### 2. Update databricks.yaml

Edit the `databricks.yaml` file and update the workspace URLs for your environment:

```yaml
targets:
  dev:
    workspace:
      host: https://<your-workspace-url>.cloud.databricks.com
  prod:
    workspace:
      host: https://<your-workspace-url>.cloud.databricks.com
```

### 3. Validate the bundle

```bash
databricks bundle validate -t dev
```

### 4. Deploy to Databricks

Deploy to development environment:

```bash
databricks bundle deploy -t dev
```

Deploy to production environment:

```bash
databricks bundle deploy -t prod
```

### 5. Run the deployed app

```bash
databricks bundle run -t dev customer_360_app
```

## Features

This comprehensive Customer 360 application includes:

### Analytics & Insights
- ✅ **Customer 360 Dashboard**: Interactive map and filterable customer profiles
- ✅ **Churn Analysis**: Predict and analyze customer churn risk
- ✅ **CLV Analysis**: Customer Lifetime Value segmentation and trends
- ✅ **VIP Customer Insights**: Identify and analyze high-value customers
- ✅ **Market Segmentation**: Blue Chip, Crypto, Sustainability, and Social Impact segments

### Visualizations
- ✅ Interactive Plotly charts and geographic maps
- ✅ Real-time filtering and drill-down capabilities
- ✅ Multi-dimensional customer views
- ✅ Behavioral and engagement metrics

### Integration
- ✅ **Braze Integration**: Push filtered customer segments to Braze for targeted campaigns
- ✅ Unity Catalog data governance
- ✅ Databricks SQL Warehouse connectivity
- ✅ DAB configuration for multi-environment deployment

## Configuration

### Database Configuration

The application uses centralized database configuration in `src/config.py`:

```python
# Database configuration
DB_CATALOG = 'mmolony_catalog'
DB_SCHEMA = 'dbdemo_customer_churn'
```

To use your own catalog and schema:
1. Edit `src/config.py`
2. Update `DB_CATALOG` and `DB_SCHEMA` values
3. All queries will automatically use the new configuration

This eliminates the need to update hardcoded table names throughout the codebase.

## Customization

To customize this template for your needs:

1. **Update app.py**: Modify the Dash application logic and UI
2. **Add data sources**: Connect to Databricks tables, Unity Catalog, or external APIs
3. **Configure database**: Update `DB_CATALOG` and `DB_SCHEMA` in `src/config.py`
4. **Extend requirements.txt**: Add additional Python packages as needed
5. **Configure databricks.yaml**: Adjust compute resources, permissions, and deployment settings

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:
- Verify your Databricks token is valid
- Check that your workspace URL is correct
- Ensure you have proper permissions in the workspace

### Deployment Issues

If bundle deployment fails:
- Run `databricks bundle validate` to check for configuration errors
- Verify all file paths in databricks.yaml are correct
- Check that the workspace has Apps API enabled

## Next Steps

After successful deployment:

1. Monitor your app in the Databricks workspace
2. Configure compute resources for production workloads
3. Set up CI/CD pipelines for automated deployment
4. Add authentication and security controls
5. Integrate with Unity Catalog for data access

## Architecture

This application leverages:
- **Databricks Lakehouse**: Unified data platform for all customer data
- **Unity Catalog**: Data governance and security
- **Databricks SQL Warehouse**: Fast analytical queries
- **Dash/Plotly**: Interactive visualizations
- **Braze**: Customer engagement platform integration

## Data Pipeline

The data pipeline consists of:
1. **Base C360 Demo Data**: `churn_features` and `churn_users` tables from the Databricks C360 demo
2. **Synthetic ML Attributes**: Generated by the pipeline in this repo
3. **Braze Sync Tables**: Created by `create_braze_sync.py` for Braze integration

## Support

For issues or questions:
- **Databricks C360 Demo**: https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/c360-platform-reduce-churn
- **Braze Integration Docs**: https://www.braze.com/docs/user_guide/data/unification/cloud_ingestion/integrations
- **Databricks Documentation**: https://docs.databricks.com
- **Dash Documentation**: https://dash.plotly.com
- **Databricks Asset Bundles**: https://docs.databricks.com/dev-tools/bundles/

