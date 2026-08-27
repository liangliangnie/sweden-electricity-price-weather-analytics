# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("USE CATALOG sweden_el_price")

electricity_silver = spark.table(
    "electricity_silver.silver_prices"
)

weather_silver = spark.table(
    "weather_silver.weather"
)


# COMMAND ----------

electricity_silver.printSchema()
weather_silver.printSchema()

# COMMAND ----------


electricity_hourly = (
    electricity_silver
    .withColumn(
        "electricity_hour_utc",
        F.date_trunc("hour", F.col("time_start"))
    )
    .groupBy(
        "region",
        "electricity_hour_utc"
    )
    .agg(
        F.avg("SEK_per_kWh").alias("SEK_per_kWh"),
        F.avg("EUR_per_kWh").alias("EUR_per_kWh"),
        F.first("EXR").alias("EXR")
    )
)

# COMMAND ----------

weather_hourly = (
    weather_silver
    .select(
        "region",
        "location",
        "latitude",
        "longitude",
        "weather_time_utc",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code"
    )
    .withColumnRenamed(
        "weather_time_utc",
        "hour_utc"
    )
)

# COMMAND ----------

electricity_min = (
    electricity_hourly
    .select(
        F.min("electricity_hour_utc").alias("min_hour")
    )
    .collect()[0]["min_hour"]
)

electricity_max = (
    electricity_hourly
    .select(
        F.max("electricity_hour_utc").alias("max_hour")
    )
    .collect()[0]["max_hour"]
)

weather_min = (
    weather_hourly
    .select(
        F.min("hour_utc").alias("min_hour")
    )
    .collect()[0]["min_hour"]
)

weather_max = (
    weather_hourly
    .select(
        F.max("hour_utc").alias("max_hour")
    )
    .collect()[0]["max_hour"]
)

gold_min = max(electricity_min, weather_min)
gold_max = min(electricity_max, weather_max)

print("Electricity range:", electricity_min, "→", electricity_max)
print("Weather range:", weather_min, "→", weather_max)
print("Gold range:", gold_min, "→", gold_max)

# COMMAND ----------

electricity_for_gold = (
    electricity_hourly
    .filter(
        (F.col("electricity_hour_utc") >= F.lit(gold_min)) &
        (F.col("electricity_hour_utc") <= F.lit(gold_max))
    )
)

weather_for_gold = (
    weather_hourly
    .filter(
        (F.col("hour_utc") >= F.lit(gold_min)) &
        (F.col("hour_utc") <= F.lit(gold_max))
    )
)

# COMMAND ----------

electricity_for_gold.select(
    F.min("electricity_hour_utc").alias("min"),
    F.max("electricity_hour_utc").alias("max")
).show()

weather_for_gold.select(
    F.min("hour_utc").alias("min"),
    F.max("hour_utc").alias("max")
).show()

# COMMAND ----------

e = electricity_for_gold.select(
    F.col("region").alias("e_region"),
    F.col("electricity_hour_utc").alias("e_hour_utc"),
    "SEK_per_kWh",
    "EUR_per_kWh",
    "EXR"
)

w = weather_for_gold.select(
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

gold_initial = (
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

print("Gold rows:", gold_initial.count())

print(
    "Gold distinct keys:",
    gold_initial.select(
        "region",
        "hour_utc"
    ).distinct().count()
)

# COMMAND ----------

gold_initial.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

print(
    "Gold distinct keys:",
    gold_initial.select(
        "region",
        "hour_utc"
    ).distinct().count()
)

# COMMAND ----------

gold_initial.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()

# COMMAND ----------

gold_initial.groupBy(
    "region",
    "hour_utc"
).count().filter(
    F.col("count") > 1
).show()

# COMMAND ----------


spark.sql("USE CATALOG sweden_el_price")

spark.sql("""
CREATE DATABASE IF NOT EXISTS gold
""")

spark.sql("""
USE gold
""")

# COMMAND ----------

GOLD_TABLE = "gold.electricity_weather"

(
    gold_initial.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print(f"Gold table created: {GOLD_TABLE}")

# COMMAND ----------

gold_table = spark.table(GOLD_TABLE)

print("Gold table rows:", gold_table.count())

# COMMAND ----------

print(
    "Gold distinct keys:",
    gold_table.select(
        "region",
        "hour_utc"
    ).distinct().count()
)

# COMMAND ----------

gold_table.groupBy(
    "region",
    "hour_utc"
).count().filter(
    F.col("count") > 1
).show()

# COMMAND ----------

gold_table.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

gold_table.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()

# COMMAND ----------

print(
    "Gold rows:",
    gold_table.count()
)

print(
    "Gold distinct keys:",
    gold_table.select(
        "region",
        "hour_utc"
    ).distinct().count()
)

# COMMAND ----------

gold_table.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

gold_keys = (
    gold_table
    .select(
        "region",
        "hour_utc"
    )
    .distinct()
)

weather_keys = (
    weather_silver
    .select(
        "region",
        F.col("weather_time_utc").alias("hour_utc")
    )
    .distinct()
)

gold_without_weather = (
    gold_keys
    .join(
        weather_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

weather_without_gold = (
    weather_keys
    .join(
        gold_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

print(
    "Gold keys without weather:",
    gold_without_weather.count()
)

print(
    "Weather keys without gold:",
    weather_without_gold.count()
)