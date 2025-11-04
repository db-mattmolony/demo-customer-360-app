"""
SQL Service for Databricks operations.

This service handles:
- Database connection management
- Query execution
- Error handling
"""

import os
from typing import List, Dict, Any, Optional
from databricks.sdk.core import Config  
from databricks import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SQLService:
    """Service for executing SQL queries and formatting results for language models."""
    
    def __init__(self, config: Optional[Config] = None):
        """Initialize the SQL service with Databricks configuration."""
        self.config = config or Config()
        self.warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
        
        if not self.warehouse_id:
            raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable is required")
    
    def execute_query(self, query: str) -> tuple[List[tuple], List[str]]:
        """
        Execute a SQL query and return raw results with column names.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            Tuple of (results, column_names) where:
            - results: List of tuples containing query results
            - column_names: List of column names
            
        Raises:
            Exception: If query execution fails
        """
        try:
            with sql.connect(
                server_hostname=self.config.host,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                credentials_provider=lambda: self.config.authenticate,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    # Get column names from cursor description
                    column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                    return results, column_names
        except Exception as e:
            raise Exception(f"Error querying Databricks Warehouse: {e}")
    
    def execute_query_as_dataframe(self, query: str):
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query string to execute
            
        Returns:
            pd.DataFrame: Results as a pandas DataFrame
        """
        import pandas as pd
        try:
            results, column_names = self.execute_query(query)
            df = pd.DataFrame(results, columns=column_names)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Query: {query}")
            return pd.DataFrame()


# Default singleton instance
_sql_service = None


def get_sql_service() -> SQLService:
    """Get or create the default SQL service instance."""
    global _sql_service
    if _sql_service is None:
        _sql_service = SQLService()
    return _sql_service