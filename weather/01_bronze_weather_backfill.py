# Databricks notebook source
# MAGIC %md
# MAGIC ###Create Weather Bronze Notebook

# COMMAND ----------

spark.sql("USE CATALOG sweden_el_price")

database = "weather_bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

#Configuration
from datetime import date, timedelta

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
START_DATE = date(2023, 8, 22)
END_DATE = date(2026, 8, 18)

#BACKFILL_DAYS = 365
#START_DATE = date(2023, 8, 19)
#END_DATE = date.today()- timedelta(days=1)
#START_DATE = END_DATE - timedelta(days=BACKFILL_DAYS)

# COMMAND ----------

# Weather API Function
import requests

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


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
            f"[WARNING] Failed to download "
            f"weather data: {e}"
        )

        return None

# COMMAND ----------

weather_test = get_weather_data(
    str(date.today() - timedelta(days=2)),
    str(date.today()),
    WEATHER_LOCATIONS["SE3"]["latitude"],
    WEATHER_LOCATIONS["SE3"]["longitude"]
)

# COMMAND ----------

weather_test.keys()

# COMMAND ----------

weather_test["hourly"].keys()

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

all_weather_records = []

for region, config in WEATHER_LOCATIONS.items():

    print(
        f"[INFO] Downloading weather for "
        f"{region} - {config['name']}"
    )

    weather_data = get_weather_data(
        str(START_DATE),
        str(END_DATE),
        config["latitude"],
        config["longitude"]
    )

    records = normalize_weather_data(
        weather_data
    )

    for record in records:

        record["region"] = region
        record["location"] = config["name"]
        record["latitude"] = config["latitude"]
        record["longitude"] = config["longitude"]

        all_weather_records.append(record)

    print(
        f"[INFO] {region}: "
        f"{len(records)} rows"
    )

# COMMAND ----------

len(all_weather_records)

# COMMAND ----------

all_weather_records[0]

# COMMAND ----------

#Define bronze schema
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType
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

#Load weather bronze
from pyspark.sql import functions as F

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

weather_bronze = spark.table(
    "weather_bronze.raw_weather"
)

weather_bronze.printSchema()

# COMMAND ----------

weather_bronze.select(
    F.min("weather_time_utc").alias("min_time"),
    F.max("weather_time_utc").alias("max_time")
).show()

# COMMAND ----------

from collections import Counter

Counter(
    record["region"]
    for record in all_weather_records
)

# COMMAND ----------

weather_bronze.count()

# COMMAND ----------

weather_bronze.groupBy(
    "region",
    "location"
).count().orderBy(
    "region"
).show()