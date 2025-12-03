# Databricks notebook source
# DBTITLE 1,Load Data and Create Synthetic Lat/Lon Coordinates
from pyspark.sql.functions import col, when, rand, lit, round as spark_round
from pyspark.sql.types import DoubleType
import sys

# Add src directory to path to import config
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-2])
sys.path.append(f"/Workspace{repo_root}/src")

# Import configuration
from config import DB_CATALOG, DB_SCHEMA

# Load the customer features data
df = spark.sql(f"SELECT * FROM {DB_CATALOG}.{DB_SCHEMA}.churn_user_features")

# COMMAND ----------

# DBTITLE 1,Generate Country-Specific Coordinates with Realistic Variations
# Create synthetic lat/lon coordinates based on specific cities and customer characteristics
# Using realistic city boundaries to avoid ocean/overlap issues

# Define realistic coordinate ranges for major cities
# These represent approximate bounding boxes for metropolitan areas
city_coords = {
    # USA Cities
    "New York": {"lat_min": 40.4774, "lat_max": 40.9176, "lon_min": -74.2591, "lon_max": -73.7004},
    "Los Angeles": {"lat_min": 33.7037, "lat_max": 34.3373, "lon_min": -118.6682, "lon_max": -118.1553},
    "Chicago": {"lat_min": 41.6444, "lat_max": 42.0230, "lon_min": -87.9401, "lon_max": -87.5240},
    "Miami": {"lat_min": 25.7617, "lat_max": 25.8557, "lon_min": -80.3183, "lon_max": -80.1918},
    
    # France Cities
    "Paris": {"lat_min": 48.8155, "lat_max": 48.9022, "lon_min": 2.2241, "lon_max": 2.4699},
    "Lyon": {"lat_min": 45.7267, "lat_max": 45.7845, "lon_min": 4.7849, "lon_max": 4.8951},
    "Marseille": {"lat_min": 43.2695, "lat_max": 43.3178, "lon_min": 5.3581, "lon_max": 5.4474},
    
    # Spain Cities
    "Madrid": {"lat_min": 40.3119, "lat_max": 40.5640, "lon_min": -3.8317, "lon_max": -3.5156},
    "Barcelona": {"lat_min": 41.3200, "lat_max": 41.4695, "lon_min": 2.0695, "lon_max": 2.2280},
    "Seville": {"lat_min": 37.3344, "lat_max": 37.4288, "lon_min": -6.0679, "lon_max": -5.9289}
}

# Map countries to their cities for distribution
country_to_cities = {
    "USA": ["New York", "Los Angeles", "Chicago", "Miami"],
    "FR": ["Paris", "Lyon", "Marseille"],
    "SPAIN": ["Madrid", "Barcelona", "Seville"]
}

# Create a city assignment based on country and customer characteristics
# First, assign cities to customers based on their country and characteristics
coords_df = df.withColumn(
    "assigned_city",
    when(col("country") == "USA",
         # Distribute USA customers across 4 cities based on characteristics
         when((col("platform") == "ios") & (col("total_amount") >= 150), lit("New York"))  # High-value iOS users to NYC
         .when(col("age_group") <= 3, lit("Los Angeles"))  # Young users to LA
         .when(col("canal") == "PHONE", lit("Chicago"))  # Phone users to Chicago
         .otherwise(lit("Miami"))  # Others to Miami
    ).when(col("country") == "FR",
           # Distribute France customers across 3 cities
           when(col("total_amount") >= 150, lit("Paris"))  # High spenders to Paris
           .when(col("platform") == "android", lit("Lyon"))  # Android users to Lyon
           .otherwise(lit("Marseille"))  # Others to Marseille
    ).when(col("country") == "SPAIN",
           # Distribute Spain customers across 3 cities
           when(col("age_group") <= 4, lit("Barcelona"))  # Younger users to Barcelona
           .when(col("canal") == "WEBAPP", lit("Madrid"))  # Web users to Madrid
           .otherwise(lit("Seville"))  # Others to Seville
    ).otherwise(lit("New York"))  # Default fallback
)

# COMMAND ----------

# DBTITLE 1,Apply Location Clustering and Final Coordinate Generation
# Generate precise coordinates within city boundaries
# This ensures all coordinates fall within realistic metropolitan areas

# Create base coordinates using city-specific bounding boxes
coords_df = coords_df.withColumn(
    # Generate latitude within city bounds
    "base_latitude",
    # USA Cities
    when(col("assigned_city") == "New York", 
         lit(40.4774) + (rand() * lit(0.4402)))  # NYC bounds
    .when(col("assigned_city") == "Los Angeles", 
          lit(33.7037) + (rand() * lit(0.6336)))  # LA bounds
    .when(col("assigned_city") == "Chicago", 
          lit(41.6444) + (rand() * lit(0.3786)))  # Chicago bounds
    .when(col("assigned_city") == "Miami", 
          lit(25.7617) + (rand() * lit(0.0940)))  # Miami bounds
    # France Cities
    .when(col("assigned_city") == "Paris", 
          lit(48.8155) + (rand() * lit(0.0867)))  # Paris bounds
    .when(col("assigned_city") == "Lyon", 
          lit(45.7267) + (rand() * lit(0.0578)))  # Lyon bounds
    .when(col("assigned_city") == "Marseille", 
          lit(43.2695) + (rand() * lit(0.0483)))  # Marseille bounds
    # Spain Cities
    .when(col("assigned_city") == "Madrid", 
          lit(40.3119) + (rand() * lit(0.2521)))  # Madrid bounds
    .when(col("assigned_city") == "Barcelona", 
          lit(41.3200) + (rand() * lit(0.1495)))  # Barcelona bounds
    .when(col("assigned_city") == "Seville", 
          lit(37.3344) + (rand() * lit(0.0944)))  # Seville bounds
    .otherwise(lit(40.7589))  # Default to NYC center
).withColumn(
    # Generate longitude within city bounds
    "base_longitude",
    # USA Cities
    when(col("assigned_city") == "New York", 
         lit(-74.2591) + (rand() * lit(0.5587)))  # NYC bounds
    .when(col("assigned_city") == "Los Angeles", 
          lit(-118.6682) + (rand() * lit(0.5129)))  # LA bounds
    .when(col("assigned_city") == "Chicago", 
          lit(-87.9401) + (rand() * lit(0.4161)))  # Chicago bounds
    .when(col("assigned_city") == "Miami", 
          lit(-80.3183) + (rand() * lit(0.1265)))  # Miami bounds
    # France Cities
    .when(col("assigned_city") == "Paris", 
          lit(2.2241) + (rand() * lit(0.2458)))  # Paris bounds
    .when(col("assigned_city") == "Lyon", 
          lit(4.7849) + (rand() * lit(0.1102)))  # Lyon bounds
    .when(col("assigned_city") == "Marseille", 
          lit(5.3581) + (rand() * lit(0.0893)))  # Marseille bounds
    # Spain Cities
    .when(col("assigned_city") == "Madrid", 
          lit(-3.8317) + (rand() * lit(0.3161)))  # Madrid bounds
    .when(col("assigned_city") == "Barcelona", 
          lit(2.0695) + (rand() * lit(0.1585)))  # Barcelona bounds
    .when(col("assigned_city") == "Seville", 
          lit(-6.0679) + (rand() * lit(0.1390)))  # Seville bounds
    .otherwise(lit(-74.0060))  # Default to NYC center
)

# Add variations based on customer characteristics (but keep within city bounds)
coords_df = coords_df.withColumn(
    # Urban vs Suburban bias within the city
    "urban_bias",
    when((col("platform") == "ios") | (col("canal") == "WEBAPP"), 0.8)  # More city center
    .when(col("platform") == "android", 0.6)  # Mixed areas
    .otherwise(0.4)  # More suburban areas of the city
).withColumn(
    # Age group influence on location within city (younger = more downtown)
    "age_location_factor",
    when(col("age_group") <= 3, 0.9)  # Younger, more downtown
    .when(col("age_group") <= 6, 0.6)  # Middle age, mixed
    .otherwise(0.3)  # Older, more suburban areas
).withColumn(
    # Spending influence (higher spenders in premium areas)
    "spending_factor",
    when(col("total_amount") >= 200, 0.9)  # High spenders, premium areas
    .when(col("total_amount") >= 100, 0.7)  # Medium spenders, good areas
    .otherwise(0.5)  # Lower spenders, more distributed
)

# COMMAND ----------

# DBTITLE 1,Apply Micro-Clustering and Boundary Checks
# Apply micro-clustering within cities and generate final realistic coordinates
# This creates neighborhood-level clustering within each metropolitan area

coords_df = coords_df.withColumn(
    # Combine all factors to determine urban clustering within the city
    "urban_probability",
    (col("urban_bias") * 0.4 + col("age_location_factor") * 0.3 + col("spending_factor") * 0.3)
).withColumn(
    # Create micro-clustering effect within city boundaries
    "cluster_adjustment_lat",
    when(rand() < col("urban_probability"), 
         (rand() - 0.5) * 0.01)  # Very tight clustering for city centers (0.01 degrees ~ 1km)
    .otherwise((rand() - 0.5) * 0.03)  # Slightly wider spread for suburban areas (0.03 degrees ~ 3km)
).withColumn(
    "cluster_adjustment_lon",
    when(rand() < col("urban_probability"), 
         (rand() - 0.5) * 0.01)  # Very tight clustering for city centers
    .otherwise((rand() - 0.5) * 0.03)  # Slightly wider spread for suburban areas
).withColumn(
    # Generate final latitude with micro-clustering
    "latitude",
    spark_round(col("base_latitude") + col("cluster_adjustment_lat"), 6)
).withColumn(
    # Generate final longitude with micro-clustering
    "longitude", 
    spark_round(col("base_longitude") + col("cluster_adjustment_lon"), 6)
)

# Ensure coordinates stay strictly within city boundaries (safety check)
coords_df = coords_df.withColumn(
    "latitude",
    # USA Cities bounds checking
    when(col("assigned_city") == "New York",
         when(col("latitude") < 40.4774, lit(40.4774))
         .when(col("latitude") > 40.9176, lit(40.9176))
         .otherwise(col("latitude")))
    .when(col("assigned_city") == "Los Angeles",
          when(col("latitude") < 33.7037, lit(33.7037))
          .when(col("latitude") > 34.3373, lit(34.3373))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Chicago",
          when(col("latitude") < 41.6444, lit(41.6444))
          .when(col("latitude") > 42.0230, lit(42.0230))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Miami",
          when(col("latitude") < 25.7617, lit(25.7617))
          .when(col("latitude") > 25.8557, lit(25.8557))
          .otherwise(col("latitude")))
    # France Cities bounds checking
    .when(col("assigned_city") == "Paris",
          when(col("latitude") < 48.8155, lit(48.8155))
          .when(col("latitude") > 48.9022, lit(48.9022))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Lyon",
          when(col("latitude") < 45.7267, lit(45.7267))
          .when(col("latitude") > 45.7845, lit(45.7845))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Marseille",
          when(col("latitude") < 43.2695, lit(43.2695))
          .when(col("latitude") > 43.3178, lit(43.3178))
          .otherwise(col("latitude")))
    # Spain Cities bounds checking
    .when(col("assigned_city") == "Madrid",
          when(col("latitude") < 40.3119, lit(40.3119))
          .when(col("latitude") > 40.5640, lit(40.5640))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Barcelona",
          when(col("latitude") < 41.3200, lit(41.3200))
          .when(col("latitude") > 41.4695, lit(41.4695))
          .otherwise(col("latitude")))
    .when(col("assigned_city") == "Seville",
          when(col("latitude") < 37.3344, lit(37.3344))
          .when(col("latitude") > 37.4288, lit(37.4288))
          .otherwise(col("latitude")))
    .otherwise(col("latitude"))
).withColumn(
    "longitude",
    # USA Cities longitude bounds checking
    when(col("assigned_city") == "New York",
         when(col("longitude") < -74.2591, lit(-74.2591))
         .when(col("longitude") > -73.7004, lit(-73.7004))
         .otherwise(col("longitude")))
    .when(col("assigned_city") == "Los Angeles",
          when(col("longitude") < -118.6682, lit(-118.6682))
          .when(col("longitude") > -118.1553, lit(-118.1553))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Chicago",
          when(col("longitude") < -87.9401, lit(-87.9401))
          .when(col("longitude") > -87.5240, lit(-87.5240))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Miami",
          when(col("longitude") < -80.3183, lit(-80.3183))
          .when(col("longitude") > -80.1918, lit(-80.1918))
          .otherwise(col("longitude")))
    # France Cities longitude bounds checking
    .when(col("assigned_city") == "Paris",
          when(col("longitude") < 2.2241, lit(2.2241))
          .when(col("longitude") > 2.4699, lit(2.4699))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Lyon",
          when(col("longitude") < 4.7849, lit(4.7849))
          .when(col("longitude") > 4.8951, lit(4.8951))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Marseille",
          when(col("longitude") < 5.3581, lit(5.3581))
          .when(col("longitude") > 5.4474, lit(5.4474))
          .otherwise(col("longitude")))
    # Spain Cities longitude bounds checking
    .when(col("assigned_city") == "Madrid",
          when(col("longitude") < -3.8317, lit(-3.8317))
          .when(col("longitude") > -3.5156, lit(-3.5156))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Barcelona",
          when(col("longitude") < 2.0695, lit(2.0695))
          .when(col("longitude") > 2.2280, lit(2.2280))
          .otherwise(col("longitude")))
    .when(col("assigned_city") == "Seville",
          when(col("longitude") < -6.0679, lit(-6.0679))
          .when(col("longitude") > -5.9289, lit(-5.9289))
          .otherwise(col("longitude")))
    .otherwise(col("longitude"))
)

# COMMAND ----------

# DBTITLE 1,Save Location Table
coords_df.select("user_id", "assigned_city", "latitude", "longitude").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB_CATALOG}.default.customer_360_locations")