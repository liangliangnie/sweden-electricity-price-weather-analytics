# Databricks notebook source
from datetime import date, timedelta
from pyspark.sql import functions as F

spark.sql("USE CATALOG sweden_el_price")

spark.sql("""
CREATE DATABASE IF NOT EXISTS weather_silver
""")

spark.sql("""
USE weather_silver
""")

# COMMAND ----------

bronze = spark.table(
    "weather_bronze.raw_weather"
)

silver = spark.table(
    "weather_silver.weather"
)

print("Bronze:", bronze.count())
print("Silver:", silver.count())

# COMMAND ----------

latest_silver_utc = (
    silver
    .select(
        F.max("weather_time_utc").alias(
            "latest_utc"
        )
    )
    .collect()[0]["latest_utc"]
)

print(
    "Latest Silver UTC:",
    latest_silver_utc
)

# COMMAND ----------

new_bronze = (
    bronze
    .filter(
        F.col("weather_time_utc") >
        F.lit(latest_silver_utc)
    )
)

print(
    "New Bronze rows:",
    new_bronze.count()
)

# COMMAND ----------

new_bronze.groupBy(
    F.to_date(
        "weather_time_utc"
    ).alias("weather_date_utc")
).count().orderBy(
    "weather_date_utc"
).show()

# COMMAND ----------

new_bronze.groupBy(
    F.to_date(
        "weather_time_utc"
    ).alias("weather_date_utc"),
    "region"
).count().orderBy(
    "weather_date_utc",
    "region"
).show()

# COMMAND ----------

def transform_weather_to_silver(bronze_df):

    silver = (
        bronze_df

        .withColumn(
            "weather_time_utc",
            F.to_timestamp_ntz(
                F.col("weather_time_utc")
            )
        )

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

new_silver = transform_weather_to_silver(
    new_bronze
)

# COMMAND ----------

new_silver.printSchema()

# COMMAND ----------

new_silver.groupBy(
    F.to_date("weather_time_utc").alias("utc_date")
).count().orderBy(
    "utc_date"
).show()

# COMMAND ----------

print(
    "New Silver rows:",
    new_silver.count()
)

# COMMAND ----------

new_silver.groupBy(
    F.to_date("weather_time_utc").alias("utc_date"),
    "region"
).count().orderBy(
    "utc_date",
    "region"
).show()

# COMMAND ----------

new_duplicates = (
    new_silver
    .groupBy(
        "region",
        "weather_time_utc"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

new_duplicates.show()

# COMMAND ----------

existing_keys = (
    silver
    .select(
        "region",
        "weather_time_utc"
    )
)

overlap = (
    new_silver
    .select(
        "region",
        "weather_time_utc"
    )
    .join(
        existing_keys,
        on=["region", "weather_time_utc"],
        how="inner"
    )
)

print(
    "Existing Silver overlap:",
    overlap.count()
)

# COMMAND ----------

from delta.tables import DeltaTable


def merge_weather_to_silver(source_df):

    target = DeltaTable.forName(
        spark,
        "weather_silver.weather"
    )

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            """
            target.region = source.region
            AND target.weather_time_utc =
                source.weather_time_utc
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

merge_weather_to_silver(
    new_silver
)

# COMMAND ----------

weather_silver = spark.table(
    "weather_silver.weather"
)

print(
    "Silver row count:",
    weather_silver.count()
)

# COMMAND ----------

weather_silver.select(
    F.max("weather_time_utc").alias(
        "latest_utc"
    )
).show()

# COMMAND ----------

weather_silver.select(
    F.max("weather_time_local").alias(
        "latest_local"
    )
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