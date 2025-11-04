# Customer 360 Dash Application

A baseline Databricks Dash application template for testing Databricks Asset Bundle (DAB) deployment.

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
- Databricks workspace access
- Python 3.8 or higher

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

This baseline template includes:

- ✅ Basic Dash application with sample data
- ✅ Interactive visualizations using Plotly
- ✅ Dropdown filtering capability
- ✅ Clean, modern UI design
- ✅ DAB configuration for multi-environment deployment
- ✅ Development and Production targets

## Customization

To customize this template for your needs:

1. **Update app.py**: Modify the Dash application logic and UI
2. **Add data sources**: Connect to Databricks tables, Unity Catalog, or external APIs
3. **Extend requirements.txt**: Add additional Python packages as needed
4. **Configure databricks.yaml**: Adjust compute resources, permissions, and deployment settings

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

## Support

For issues or questions:
- Databricks Documentation: https://docs.databricks.com
- Dash Documentation: https://dash.plotly.com
- Databricks Asset Bundles: https://docs.databricks.com/dev-tools/bundles/

