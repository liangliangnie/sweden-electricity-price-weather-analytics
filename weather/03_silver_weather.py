# Databricks notebook source
spark.sql("USE CATALOG sweden_el_price")

database = "weather_bronze"

spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Load bronze data to df silver

# COMMAND ----------

spark.conf.set(
    "spark.sql.session.timeZone",
    "Europe/Stockholm"
)

spark.conf.get(
    "spark.sql.session.timeZone"
)

# COMMAND ----------

from pyspark.sql import functions as F


def transform_weather_to_silver(bronze_df):

    silver = (
        bronze_df

        # Parse the UTC string without applying the session timezone
        .withColumn(
            "weather_time_utc",
            F.to_timestamp_ntz(
                F.col("weather_time_utc")
            )
        )

        # Convert UTC → Europe/Stockholm
        .withColumn(
            "weather_time_local",
            F.convert_timezone(
                F.lit("UTC"),
                F.lit("Europe/Stockholm"),
                F.col("weather_time_utc")
            )
        )

        .withColumn(
            "weather_date",
            F.to_date("weather_time_local")
        )

        .withColumn(
            "weather_hour",
            F.hour("weather_time_local")
        )

        .select(
            "weather_date",
            "weather_time_utc",
            "weather_time_local",
            "weather_hour",
            "region",
            "location",
            "latitude",
            "longitude",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
            "ingestion_time",
            "source"
        )
    )

    return silver

# COMMAND ----------


bronze = spark.table("sweden_el_price.weather_bronze.raw_weather")

silver_test = transform_weather_to_silver(
    bronze
)

# COMMAND ----------

silver_test.printSchema()

# COMMAND ----------

silver_test.filter(
    (F.col("region") == "SE1") &
    (F.col("weather_date") == "2026-03-29")
).select(
    "weather_time_utc",
    "weather_time_local",
    "weather_hour"
).orderBy(
    "weather_time_utc"
).show(30, truncate=False)

# COMMAND ----------

#QC 1 Spring DST
spring_qc = (
    silver_test
    .filter(
        F.col("weather_date") == "2026-03-29"
    )
    .groupBy("region")
    .count()
    .orderBy("region")
)

spring_qc.show()

# COMMAND ----------

#QC 2 — Autumn DST
autumn_qc = (
    silver_test
    .filter(
        F.col("weather_date") == "2025-10-26"
    )
    .groupBy("region")
    .count()
    .orderBy("region")
)

autumn_qc.show()

# COMMAND ----------

#QC 3 — Duplicate
duplicates = (
    silver_test
    .groupBy(
        "region",
        "weather_time_utc"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicates.show()

# COMMAND ----------

#QC 4 — Bronze vs Silver
bronze_count = (
    spark.table(
        "weather_bronze.raw_weather"
    ).count()
)

silver_count = silver_test.count()

print("Bronze:", bronze_count)
print("Silver:", silver_count)

# COMMAND ----------

#Create weather silver table
spark.sql("USE CATALOG sweden_el_price")

spark.sql("""
CREATE DATABASE IF NOT EXISTS weather_silver
""")

spark.sql("""
USE weather_silver
""")

# COMMAND ----------

(
    silver_test.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "weather_silver.weather"
    )
)

# COMMAND ----------

weather_silver = spark.table(
    "weather_silver.weather"
)

weather_silver.printSchema()

# COMMAND ----------

print(
    "Weather Silver rows:",
    weather_silver.count()
)

# COMMAND ----------

weather_silver.select(
    F.min("weather_date").alias("min_date"),
    F.max("weather_date").alias("max_date")
).show()

# COMMAND ----------

weather_silver.groupBy(
    "region",
    "location"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

duplicates = (
    weather_silver
    .groupBy(
        "region",
        "weather_time_utc"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicates.show()