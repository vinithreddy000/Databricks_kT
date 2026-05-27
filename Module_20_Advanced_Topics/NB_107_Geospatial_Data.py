# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 107: Geospatial Data Processing
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **Geospatial processing** in Spark handles location-based data: GPS coordinates, polygons, distances between points, and spatial joins ("which customers are within 5km of a store?"). Databricks supports geospatial via H3 (Uber's hexagonal grid), Mosaic library, and built-in Spark functions.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC Imagine overlaying a **hexagonal grid** on a city map. Each hexagon has a unique ID. Now you can ask: "How many deliveries were in hex #ABC?" or "Which hexagons overlap with our warehouse zones?" This is exactly what H3 does.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Geospatial Tools in Databricks:
# MAGIC
# MAGIC   1. H3 Functions (built-in):
# MAGIC      h3_pointash3(lat, lng, resolution) → H3 index.
# MAGIC      h3_h3tostring(h3_index) → human-readable hex ID.
# MAGIC      h3_kring(h3_index, k) → neighboring hexagons.
# MAGIC
# MAGIC   2. Mosaic Library (databricks-mosaic):
# MAGIC      Point-in-polygon, spatial joins, geometry operations.
# MAGIC      grid_pointascellid(), grid_polyfill(), st_distance().
# MAGIC
# MAGIC   3. Native Spark (basic):
# MAGIC      Haversine formula for distance between coordinates.
# MAGIC      Bounding box filters for coarse spatial filtering.
# MAGIC
# MAGIC H3 Resolutions:
# MAGIC   Resolution 0:  ~4,357 km² per hex (continent-level).
# MAGIC   Resolution 5:  ~253 km² per hex (city-level).
# MAGIC   Resolution 9:  ~0.1 km² per hex (neighborhood).
# MAGIC   Resolution 12: ~0.003 km² per hex (building-level).
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: Geospatial Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — GEOSPATIAL DATA
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, lit, expr, radians, sin, cos, asin, sqrt  # Imports.
import math  # For constants.

print("="*70)
print("SECTIONS 3-7: Geospatial Data Processing")
print("="*70)

# ─── EXAMPLE 1: H3 indexing ───
print("\n" + "-"*60)
print("EXAMPLE 1: H3 hexagonal indexing (built-in functions)")
print("-"*60)

# Sample locations (lat, lng).
locations = spark.createDataFrame([
    ("Berlin", 52.5200, 13.4050),
    ("Munich", 48.1351, 11.5820),
    ("Hamburg", 53.5511, 9.9937),
    ("Stuttgart", 48.7758, 9.1829),
    ("Cologne", 50.9375, 6.9603)
], ["city", "latitude", "longitude"])

# Convert to H3 index (resolution 7 = ~5km hexagons).
try:
    h3_df = locations.withColumn(
        "h3_index", expr("h3_pointash3(latitude, longitude, 7)")  # Built-in H3 function.
    )
    print("\nLocations with H3 index:")
    display(h3_df)  # display() for output.
    print("\n✓ H3 index = unique hexagon ID. Same hex = nearby locations.")
except Exception as e:
    print(f"  H3 functions may require DBR 11.3+: {e}")

# ─── EXAMPLE 2: Haversine distance calculation ───
print("\n" + "-"*60)
print("EXAMPLE 2: Calculate distance between points (Haversine)")
print("-"*60)

# Haversine formula in PySpark.
def haversine_expr(lat1, lon1, lat2, lon2):
    """Return Spark expression for haversine distance in km."""
    R = 6371.0  # Earth radius in km.
    dlat = radians(col(lat2)) - radians(col(lat1))  # Delta latitude.
    dlon = radians(col(lon2)) - radians(col(lon1))  # Delta longitude.
    a = sin(dlat / 2) ** 2 + cos(radians(col(lat1))) * cos(radians(col(lat2))) * sin(dlon / 2) ** 2
    return lit(2 * R) * asin(sqrt(a))  # Distance formula.

# Calculate distance from Berlin to all cities.
berlin_lat, berlin_lon = 52.5200, 13.4050  # Reference point.
dist_df = locations.withColumn(
    "distance_from_berlin_km",
    haversine_expr("latitude", "longitude", lit(berlin_lat), lit(berlin_lon))
)
print("\nDistance from Berlin:")
display(dist_df.select("city", "distance_from_berlin_km").orderBy("distance_from_berlin_km"))

print("\n✓ Haversine: great-circle distance between two lat/lng points.")
print("  Use for: nearest neighbor, radius queries, delivery routing.")

# ─── EXAMPLE 3: Spatial filtering (bounding box) ───
print("\n" + "-"*60)
print("EXAMPLE 3: Bounding box filter (fast spatial pre-filter)")
print("-"*60)

# Find cities within a bounding box (Southern Germany).
bbox_min_lat, bbox_max_lat = 47.5, 49.5  # South Germany.
bbox_min_lon, bbox_max_lon = 8.0, 13.0   # West to East.

south_germany = locations.filter(
    (col("latitude").between(bbox_min_lat, bbox_max_lat)) &  # Latitude range.
    (col("longitude").between(bbox_min_lon, bbox_max_lon))   # Longitude range.
)
print(f"\nCities in Southern Germany bounding box:")
display(south_germany)  # display() for output.

print("\n✓ Bounding box: fast, coarse filter. Follow with precise distance check.")
print("  Pattern: bbox filter (fast) → Haversine distance (precise) → final result.")

# ─── HOMEWORK ───
print("\n" + "="*70)
print("HOMEWORK — Geospatial")
print("="*70)
print("  Level 1-3: H3 indexing, Haversine distance, bounding box filters.")
print("  Level 4-6: Spatial joins (points in polygons), Mosaic library.")
print("  Level 7-10: H3 aggregation (hexbin analysis), KNN spatial search.")
print("  Use cases: Delivery routing, store location, geofencing, IoT tracking.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 107")
print("="*70)