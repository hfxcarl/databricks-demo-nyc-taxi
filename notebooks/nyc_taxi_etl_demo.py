# Databricks notebook source
# NYC Taxi ETL Demo

# COMMAND ----------

# 1. Load CSV Data

df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/data/nyc_taxi_sample.csv")
df.show(5)

# COMMAND ----------

# 2. Clean and Transform

from pyspark.sql.functions import col, to_timestamp

df_clean = df.select(
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
