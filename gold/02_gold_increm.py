# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
spark.sql("USE CATALOG sweden_el_price")

GOLD_TABLE = "gold.electricity_weather"

gold_target = spark.table(
    GOLD_TABLE
)

electricity_silver = spark.table(
    "electricity_silver.silver_prices"
)

weather_silver = spark.table(
    "weather_silver.weather"
)

# COMMAND ----------

electricity_hourly_gold = (
    electricity_silver
    .withColumn(
        "electricity_hour_utc",
        F.date_trunc(
            "hour",
            F.col("time_start")
        )
    )
    .groupBy(
        "region",
        "electricity_hour_utc"
    )
    .agg(
        F.avg("SEK_per_kWh").alias("SEK_per_kWh"),
        F.avg("EUR_per_kWh").alias("EUR_per_kWh"),
        F.first("EXR", ignorenulls=True).alias("EXR")
    )
)

# COMMAND ----------

weather_hourly_gold = (
    weather_silver
    .withColumn(
        "hour_utc",
        F.date_trunc(
            "hour",
            F.col("weather_time_utc")
        )
    )
)

weather_hourly_gold.printSchema()

# COMMAND ----------

electricity_hourly_gold.select(
    F.min("electricity_hour_utc").alias("min_utc"),
    F.max("electricity_hour_utc").alias("max_utc")
).show()

weather_hourly_gold.select(
    F.min("hour_utc").alias("min_utc"),
    F.max("hour_utc").alias("max_utc")
).show()

# COMMAND ----------

print("Electricity hourly rows:", electricity_hourly_gold.count())

print(
    "Electricity hourly distinct keys:",
    electricity_hourly_gold
    .select("region", "electricity_hour_utc")
    .distinct()
    .count()
)

electricity_hourly_gold \
    .groupBy("region") \
    .count() \
    .orderBy("region") \
    .show()

# COMMAND ----------

electricity_hourly_gold \
    .groupBy(
        "region",
        "electricity_hour_utc"
    ) \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# COMMAND ----------

print("Weather hourly rows:", weather_hourly_gold.count())

print(
    "Weather hourly distinct keys:",
    weather_hourly_gold
    .select("region", "hour_utc")
    .distinct()
    .count()
)

weather_hourly_gold \
    .groupBy("region") \
    .count() \
    .orderBy("region") \
    .show()

# COMMAND ----------

gold_max_hour = (
    gold_target
    .select(
        F.max("hour_utc").alias("max_hour")
    )
    .collect()[0]["max_hour"]
)

print("Latest Gold hour UTC:", gold_max_hour)

# COMMAND ----------

electricity_new = electricity_hourly_gold.filter(
    F.col("electricity_hour_utc") > F.lit(gold_max_hour)
)

# COMMAND ----------

electricity_new = (
    electricity_hourly_gold
    .filter(F.col("electricity_hour_utc") > gold_max_hour)
    .withColumn(
        "join_hour_utc",
        F.col("electricity_hour_utc")
    )
)

weather_new = (
    weather_hourly_gold
    .filter(F.col("hour_utc") > gold_max_hour)
    .withColumn(
        "join_hour_utc",
        F.col("hour_utc")
    )
)

# COMMAND ----------

print("Weather new rows:", weather_new.count())

weather_new.groupBy(
    "region",
    "hour_utc"
).count().filter(
    F.col("count") > 1
).show(100, truncate=False)

# COMMAND ----------

e = electricity_new.select(
    F.col("region").alias("e_region"),
    F.col("electricity_hour_utc").alias("e_hour_utc"),
    "SEK_per_kWh",
    "EUR_per_kWh",
    "EXR"
)

w = weather_new.select(
    F.col("region").alias("w_region"),
    F.col("hour_utc").alias("w_hour_utc"),
    "location",
    "latitude",
    "longitude",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code"
)

# COMMAND ----------

gold_incremental = (
    e.join(
        w,
        (F.col("e_region") == F.col("w_region")) &
        (F.col("e_hour_utc") == F.col("w_hour_utc")),
        "inner"
    )
    .select(
        F.col("e_region").alias("region"),
        F.col("e_hour_utc").alias("hour_utc"),

        "location",
        "latitude",
        "longitude",

        "SEK_per_kWh",
        "EUR_per_kWh",
        "EXR",

        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code"
    )
)

# COMMAND ----------

electricity_silver.printSchema()
weather_silver.printSchema()

# COMMAND ----------

print("New Gold rows:", gold_incremental.count())

# COMMAND ----------

gold_incremental.groupBy("region").count().orderBy("region").show()

# COMMAND ----------

gold_incremental.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()

# COMMAND ----------

gold_incremental.groupBy(
    "region",
    "hour_utc"
).count().filter(
    F.col("count") > 1
).show()

# COMMAND ----------

new_gold_rows = gold_incremental.count()

print("New Gold rows:", new_gold_rows)

if new_gold_rows > 0:
    gold_incremental.createOrReplaceTempView("gold_incremental")

    spark.sql("""
        MERGE INTO gold.electricity_weather AS target
        USING gold_incremental AS source
        ON target.region = source.region
           AND target.hour_utc = source.hour_utc

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Merged {new_gold_rows} rows into Gold.")
else:
    print("No new complete Gold rows. Nothing to merge.")

# COMMAND ----------

gold_after = spark.table("gold.electricity_weather")

print("Gold rows:", gold_after.count())

print(
    "Gold distinct keys:",
    gold_after.select(
        "region",
        "hour_utc"
    ).distinct().count()
)

gold_after.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

gold_after.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()

# COMMAND ----------

gold = spark.table("gold.electricity_weather")

gold_duplicates = (
    gold
    .groupBy(
        "region",
        "hour_utc"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

print(
    "Duplicate keys:",
    gold_duplicates.count()
)

gold_duplicates.orderBy(
    "hour_utc",
    "region"
).show(100, truncate=False)

# COMMAND ----------

gold_duplicates.groupBy(
    "count"
).count().orderBy(
    "count"
).show()

# COMMAND ----------

from pyspark.sql import Window

w = Window.partitionBy("region").orderBy("hour_utc")

gold_gap_check = (
    gold
    .select(
        "region",
        "hour_utc",
        F.lag("hour_utc").over(w).alias("previous_hour")
    )
    .withColumn(
        "hour_diff",
        (
            F.col("hour_utc").cast("long")
            - F.col("previous_hour").cast("long")
        ) / 3600
    )
    .filter(
        F.col("previous_hour").isNotNull() &
        (F.col("hour_diff") != 1)
    )
)

gold_gap_check.show()

# COMMAND ----------

gold.filter(
    F.col("SEK_per_kWh").isNull() |
    F.col("temperature_2m").isNull()
).count()

# COMMAND ----------

gold.filter(
    F.to_date("hour_utc") == "2026-08-18"
).groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

gold.filter(
    F.to_date("hour_utc") == "2026-08-18"
).select(
    "region",
    "hour_utc"
).groupBy(
    "region",
    "hour_utc"
).count().filter(
    F.col("count") > 1
).show(100, truncate=False)