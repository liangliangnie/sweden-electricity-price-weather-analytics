# Databricks notebook source
from datetime import date, timedelta
from pyspark.sql import functions as F

spark.sql("USE CATALOG sweden_el_price")

spark.sql("""
CREATE DATABASE IF NOT EXISTS weather_bronze
""")

spark.sql("""
USE weather_bronze
""")

REGIONS = ["SE1", "SE2", "SE3", "SE4"]

WEATHER_LOCATIONS = {
    "SE1": {
        "name": "Lulea",
        "latitude": 65.5848,
        "longitude": 22.1547
    },
    "SE2": {
        "name": "Umea",
        "latitude": 63.8258,
        "longitude": 20.2630
    },
    "SE3": {
        "name": "Stockholm",
        "latitude": 59.3293,
        "longitude": 18.0686
    },
    "SE4": {
        "name": "Malmo",
        "latitude": 55.6050,
        "longitude": 13.0038
    }
}

# COMMAND ----------

import requests


BASE_URL = "https://api.open-meteo.com/v1/forecast"


def get_weather_data(
    start_date,
    end_date,
    latitude,
    longitude
):

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code"
        ]),
        "timezone": "UTC"
    }

    try:

        response = requests.get(
            BASE_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        return response.json()

    except Exception as e:

        print(
            f"[WARNING] Failed to download weather: "
            f"{start_date} → {end_date}"
        )

        print(e)

        return None

# COMMAND ----------

def normalize_weather_data(data):

    if not data:
        return []

    hourly = data["hourly"]

    records = []

    for i in range(len(hourly["time"])):

        records.append({
            "weather_time_utc": hourly["time"][i],
            "temperature_2m": hourly["temperature_2m"][i],
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "precipitation": hourly["precipitation"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "weather_code": hourly["weather_code"][i]
        })

    return records

# COMMAND ----------

bronze = spark.table(
    "weather_bronze.raw_weather"
)

# COMMAND ----------

latest_utc_date = (
    bronze
    .select(
        F.to_date(
            "weather_time_utc"
        ).alias("utc_date")
    )
    .agg(
        F.max("utc_date").alias(
            "latest_date"
        )
    )
    .collect()[0]["latest_date"]
)

print(
    "Latest Bronze UTC date:",
    latest_utc_date
)

# COMMAND ----------

today = date.today()

target_end_date = today - timedelta(days=1)

print(
    "Target end date:",
    target_end_date
)

# COMMAND ----------

if latest_utc_date is None:

    print(
        "[WARNING] Bronze table is empty."
    )

else:

    start_date = (
        latest_utc_date + timedelta(days=1)
    )

    if start_date > target_end_date:

        missing_dates = []

    else:

        missing_dates = [
            start_date + timedelta(days=i)
            for i in range(
                (target_end_date - start_date).days + 1
            )
        ]

print(
    "Missing dates:",
    missing_dates
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Incremental Download + Load

# COMMAND ----------

all_weather_records = []

for target_date in missing_dates:

    print(
        f"[INFO] Downloading weather for {target_date}"
    )

    for region, config in WEATHER_LOCATIONS.items():

        print(
            f"[INFO] {region} - {config['name']}"
        )

        data = get_weather_data(
            str(target_date),
            str(target_date),
            config["latitude"],
            config["longitude"]
        )

        records = normalize_weather_data(data)

        if not records:
            print(
                f"[WARNING] No data for "
                f"{region} - {target_date}"
            )
            continue

        print(
            f"[INFO] {region}: "
            f"{len(records)} rows"
        )

        for record in records:

            record["region"] = region
            record["location"] = config["name"]
            record["latitude"] = config["latitude"]
            record["longitude"] = config["longitude"]

            all_weather_records.append(record)

# COMMAND ----------

print(
    "Total new records:",
    len(all_weather_records)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


weather_bronze_schema = StructType([
    StructField(
        "weather_time_utc",
        StringType(),
        True
    ),
    StructField(
        "temperature_2m",
        DoubleType(),
        True
    ),
    StructField(
        "relative_humidity_2m",
        DoubleType(),
        True
    ),
    StructField(
        "precipitation",
        DoubleType(),
        True
    ),
    StructField(
        "wind_speed_10m",
        DoubleType(),
        True
    ),
    StructField(
        "weather_code",
        IntegerType(),
        True
    ),
    StructField(
        "region",
        StringType(),
        True
    ),
    StructField(
        "location",
        StringType(),
        True
    ),
    StructField(
        "latitude",
        DoubleType(),
        True
    ),
    StructField(
        "longitude",
        DoubleType(),
        True
    )
])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Load function

# COMMAND ----------

def load_weather_to_bronze(records):

    if not records:
        return 0

    df = spark.createDataFrame(
        records,
        schema=weather_bronze_schema
    )

    df = (
        df
        .withColumn(
            "ingestion_time",
            F.current_timestamp()
        )
        .withColumn(
            "source",
            F.lit("open_meteo_api")
        )
    )

    rows = df.count()

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "weather_bronze.raw_weather"
        )
    )

    return rows

# COMMAND ----------

rows = load_weather_to_bronze(
    all_weather_records
)

print(f"Loaded {rows} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Verification

# COMMAND ----------

after = spark.table(
    "weather_bronze.raw_weather"
)

print(
    "Bronze row count:",
    after.count()
)

# COMMAND ----------

after.select(
    F.min(
        F.to_date("weather_time_utc")
    ).alias("min_date"),
    F.max(
        F.to_date("weather_time_utc")
    ).alias("max_date")
).show()

# COMMAND ----------

after.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

print("Latest Bronze UTC date:", latest_utc_date)
print("Target end date:", target_end_date)
print("Missing dates:", missing_dates)

# COMMAND ----------

duplicates = (
    after
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

after.select(
    F.min(
        F.to_date("weather_time_utc")
    ).alias("min_date"),
    F.max(
        F.to_date("weather_time_utc")
    ).alias("max_date")
).show()