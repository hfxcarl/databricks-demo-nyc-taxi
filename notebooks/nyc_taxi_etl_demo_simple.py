 # Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Data ETL Pipeline Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Ingestion & Initial Exploration

# COMMAND ----------
dp = "demo_nyc_taxi.default"

data_store = "/Volumes/demo_nyc_taxi/default/demo_taxi_volume/nyc_taxi_sample.csv"

# Load the raw CSV data
df_raw = spark.read.option("header", True).option("inferSchema", True).csv(data_store)

# Display basic information about the dataset
print(f"Dataset shape: {df_raw.count()} rows, {len(df_raw.columns)} columns")
print(f"Schema:")
df_raw.printSchema()

# Show sample data
print("\nSample data:")
df_raw.show(5, truncate=False)

# COMMAND ----------

# 2. Clean and Transform

from pyspark.sql.functions import col, to_timestamp

df_clean = df_raw.select(
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    to_timestamp(col("tpep_pickup_datetime")).alias("pickup_time"),
    to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_time"),
    col("PULocationID").cast("int"),
    col("DOLocationID").cast("int")
).dropna()

df_clean.show(5)

# COMMAND ----------

# 3. Write to Delta Lake

df_clean.write.format("delta").mode("overwrite").save("/tmp/nyc_taxi_delta")

# COMMAND ----------

# 4. Read from Delta Lake

df_delta = spark.read.format("delta").load("/tmp/nyc_taxi_delta")
df_delta.createOrReplaceTempView("trips")

# COMMAND ----------

# 5. Query: Average Trip Distance

spark.sql("""
    SELECT
        date(pickup_time) AS trip_date,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM trips
    GROUP BY trip_date
    ORDER BY trip_date
""").show()

# COMMAND ----------

# 6. Optional: Visualize

display(
    spark.sql("""
        SELECT
            date(pickup_time) AS trip_date,
            AVG(trip_distance) AS avg_distance
        FROM trips
        GROUP BY trip_date
        ORDER BY trip_date
    """)
)
