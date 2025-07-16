# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Data ETL Pipeline Demo
# MAGIC 
# MAGIC This notebook demonstrates a complete ETL pipeline using PySpark and Delta Lake:
# MAGIC - **Extract**: Load CSV data from DBFS
# MAGIC - **Transform**: Clean, validate, and enrich data
# MAGIC - **Load**: Write to Delta Lake with schema evolution
# MAGIC - **Analytics**: Generate business insights with SQL
# MAGIC - **Visualization**: Create interactive charts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Ingestion & Initial Exploration

# COMMAND ----------

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

# Data quality assessment
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum

# Check for missing values
print("Missing values per column:")
df_raw.select([
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) 
    for c in df_raw.columns
]).show()

# Basic statistics
print("\nDataset statistics:")
df_raw.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Cleaning & Transformation

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, unix_timestamp, when, avg, stddev, abs as spark_abs
from pyspark.sql.types import DoubleType, IntegerType

# Define cleaning transformations
df_clean = df_raw.select(
    col("passenger_count").cast(IntegerType()).alias("passenger_count"),
    col("trip_distance").cast(DoubleType()).alias("trip_distance"),
    to_timestamp(col("tpep_pickup_datetime")).alias("pickup_time"),
    to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_time"),
    col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
    col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
    col("fare_amount").cast(DoubleType()).alias("fare_amount"),
    col("total_amount").cast(DoubleType()).alias("total_amount")
).filter(
    # Remove invalid records
    (col("passenger_count") > 0) & 
    (col("passenger_count") <= 8) &
    (col("trip_distance") > 0) &
    (col("trip_distance") < 100) &  # Remove outliers
    (col("fare_amount") > 0) &
    (col("pickup_time").isNotNull()) &
    (col("dropoff_time").isNotNull())
)

# Add calculated fields
df_transformed = df_clean.withColumn(
    "trip_duration_minutes", 
    (unix_timestamp(col("dropoff_time")) - unix_timestamp(col("pickup_time"))) / 60
).withColumn(
    "speed_mph",
    when(col("trip_duration_minutes") > 0, 
         col("trip_distance") / (col("trip_duration_minutes") / 60))
    .otherwise(0)
).withColumn(
    "fare_per_mile",
    when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
    .otherwise(0)
).filter(
    # Remove unrealistic trips
    (col("trip_duration_minutes") > 1) &
    (col("trip_duration_minutes") < 180) &  # Less than 3 hours
    (col("speed_mph") < 80)  # Reasonable speed limit
)

print(f"Records after cleaning: {df_transformed.count()}")
df_transformed.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Validation

# COMMAND ----------

# Calculate data quality metrics
total_raw = df_raw.count()
total_clean = df_transformed.count()
rejection_rate = (total_raw - total_clean) / total_raw * 100

print(f"Data Quality Summary:")
print(f"- Raw records: {total_raw:,}")
print(f"- Clean records: {total_clean:,}")
print(f"- Rejection rate: {rejection_rate:.2f}%")

# Statistical validation
print(f"\nData Statistics:")
df_transformed.select("trip_distance", "trip_duration_minutes", "fare_amount").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Lake

# COMMAND ----------

# Write to Delta Lake with partitioning for better performance
df_transformed.withColumn("pickup_date", col("pickup_time").cast("date")) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .option("overwriteSchema", "true") \
    .save("/tmp/nyc_taxi_delta")

print("Data successfully written to Delta Lake")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read from Delta Lake & Create Views

# COMMAND ----------

# Read from Delta Lake
df_delta = spark.read.format("delta").load("/tmp/nyc_taxi_delta")

# Create temporary view for SQL queries
df_delta.createOrReplaceTempView("taxi_trips")

# Verify the data
print(f"Delta Lake table contains {df_delta.count()} records")
df_delta.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Business Analytics & Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily trip patterns
# MAGIC SELECT
# MAGIC     pickup_date,
# MAGIC     COUNT(*) as total_trips,
# MAGIC     ROUND(AVG(trip_distance), 2) as avg_distance,
# MAGIC     ROUND(AVG(trip_duration_minutes), 2) as avg_duration_min,
# MAGIC     ROUND(AVG(fare_amount), 2) as avg_fare,
# MAGIC     ROUND(AVG(passenger_count), 2) as avg_passengers
# MAGIC FROM taxi_trips
# MAGIC GROUP BY pickup_date
# MAGIC ORDER BY pickup_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Hourly demand patterns
# MAGIC SELECT
# MAGIC     HOUR(pickup_time) as hour_of_day,
# MAGIC     COUNT(*) as trip_count,
# MAGIC     ROUND(AVG(trip_distance), 2) as avg_distance,
# MAGIC     ROUND(AVG(fare_amount), 2) as avg_fare
# MAGIC FROM taxi_trips
# MAGIC GROUP BY HOUR(pickup_time)
# MAGIC ORDER BY hour_of_day

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Popular routes analysis
# MAGIC SELECT
# MAGIC     pickup_location_id,
# MAGIC     dropoff_location_id,
# MAGIC     COUNT(*) as trip_count,
# MAGIC     ROUND(AVG(trip_distance), 2) as avg_distance,
# MAGIC     ROUND(AVG(fare_amount), 2) as avg_fare
# MAGIC FROM taxi_trips
# MAGIC GROUP BY pickup_location_id, dropoff_location_id
# MAGIC HAVING COUNT(*) > 5
# MAGIC ORDER BY trip_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Advanced Analytics with PySpark

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Calculate percentiles and outliers
percentiles = df_delta.select("trip_distance", "fare_amount").approxQuantile(
    ["trip_distance", "fare_amount"], 
    [0.25, 0.5, 0.75, 0.95], 
    0.05
)

print("Percentiles (25th, 50th, 75th, 95th):")
print(f"Trip Distance: {percentiles[0]}")
print(f"Fare Amount: {percentiles[1]}")

# Window functions for ranking
window_spec = Window.partitionBy("pickup_date").orderBy(col("fare_amount").desc())

df_ranked = df_delta.withColumn(
    "fare_rank", 
    rank().over(window_spec)
).filter(col("fare_rank") <= 3)

print("\nTop 3 highest fares per day:")
df_ranked.select("pickup_date", "fare_amount", "trip_distance", "fare_rank").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Interactive Visualizations

# COMMAND ----------

# Trip distance distribution
display(
    spark.sql("""
        SELECT
            CASE
                WHEN trip_distance < 1 THEN '0-1 miles'
                WHEN trip_distance < 3 THEN '1-3 miles'
                WHEN trip_distance < 5 THEN '3-5 miles'
                WHEN trip_distance < 10 THEN '5-10 miles'
                ELSE '10+ miles'
            END as distance_range,
            COUNT(*) as trip_count
        FROM taxi_trips
        GROUP BY distance_range
        ORDER BY trip_count DESC
    """)
)

# COMMAND ----------

# Hourly demand visualization
display(
    spark.sql("""
        SELECT
            HOUR(pickup_time) as hour_of_day,
            COUNT(*) as trip_count,
            AVG(trip_distance) as avg_distance
        FROM taxi_trips
        GROUP BY HOUR(pickup_time)
        ORDER BY hour_of_day
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Performance Optimization Demo

# COMMAND ----------

# Cache frequently accessed data
df_delta.cache()

# Show execution plan
spark.sql("EXPLAIN SELECT * FROM taxi_trips WHERE trip_distance > 5").show(truncate=False)

# Create optimized aggregation
optimized_summary = df_delta.groupBy("pickup_date") \
    .agg(
        count("*").alias("total_trips"),
        avg("trip_distance").alias("avg_distance"),
        avg("fare_amount").alias("avg_fare")
    ) \
    .orderBy("pickup_date")

print("Optimized daily summary:")
optimized_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary & Next Steps
# MAGIC 
# MAGIC This demo showcased:
# MAGIC - ✅ **Data Ingestion**: Loading CSV data with schema inference
# MAGIC - ✅ **Data Quality**: Validation, cleaning, and outlier removal
# MAGIC - ✅ **ETL Pipeline**: Complete Extract-Transform-Load process
# MAGIC - ✅ **Delta Lake**: Modern data lake storage with ACID transactions
# MAGIC - ✅ **SQL Analytics**: Complex aggregations and business insights
# MAGIC - ✅ **PySpark**: Advanced transformations and window functions
# MAGIC - ✅ **Visualizations**: Interactive charts and dashboards
# MAGIC - ✅ **Performance**: Caching and query optimization
# MAGIC 
# MAGIC **Production Considerations:**
# MAGIC - Implement proper error handling and logging
# MAGIC - Add data lineage tracking
# MAGIC - Set up automated data quality monitoring
# MAGIC - Configure incremental processing for large datasets
# MAGIC - Implement proper security and access controls
