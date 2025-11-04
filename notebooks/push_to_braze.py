# Databricks notebook source
"""
Push to Braze Pipeline

This notebook prints the current datetime when executed.

Author: Matthew Molony
"""

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# Print current datetime
current_time = datetime.now()
print(f"Pipeline executed at: {current_time}")
print(f"Formatted: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
