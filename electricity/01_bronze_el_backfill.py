# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Configurationn

# COMMAND ----------

spark.sql("USE CATALOG sweden_el_price")
database = "electricity_bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

BASE_URL = "https://www.elprisetjustnu.se/api/v1/prices"

def get_price_data(target_date, region):
    """
    Download electricity price data for one region and one date.

    Parameters
    ----------
    target_date : datetime.date
    region : str (SE1, SE2, SE3, SE4)

    Returns
    -------
    list
        Returns the JSON response as a list. If failed, returns empty list.
    """

    date_path = target_date.strftime("%Y/%m-%d")

    url = f"{BASE_URL}/{date_path}_{region}.json"

    try:

        response = requests.get(url)

        response.raise_for_status()

        return response.json()

    except Exception as e:

        print(f"[WARNING] Failed to download {target_date} - {region}")

        return []

# COMMAND ----------

from datetime import date, timedelta

# Configuration
REGIONS = ["SE1", "SE2", "SE3", "SE4"]
#BACKFILL_DAYS = 1095

#END_DATE = date.today() - timedelta(days=1)
START_DATE = date(2023, 8, 22)
END_DATE = date(2026, 8, 18)
#START_DATE = END_DATE - timedelta(days=BACKFILL_DAYS)

# COMMAND ----------

date_list = []

current_date = START_DATE

while current_date <= END_DATE:
    date_list.append(current_date)
    current_date += timedelta(days=1)

len(date_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType
)

bronze_schema = StructType([
    StructField("SEK_per_kWh", DoubleType(), True),
    StructField("EUR_per_kWh", DoubleType(), True),
    StructField("EXR", DoubleType(), True),
    StructField("time_start", StringType(), True),
    StructField("time_end", StringType(), True),
    StructField("region", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Helper Functions

# COMMAND ----------

def get_daily_prices(target_date):

    daily_prices = []

    for region in REGIONS:

        data = get_price_data(target_date, region)

        for record in data:

            record["region"] = region

            daily_prices.append(record)

    return daily_prices

# COMMAND ----------

from pyspark.sql import functions as F

def load_to_bronze(records):

    if not records:
        return 0
        
    df = spark.createDataFrame(records, schema=bronze_schema)

    df = (
        df
        .withColumn(
            "ingestion_time",
            F.current_timestamp()
        )
        .withColumn(
            "source",
            F.lit("elprisetjustnu_api")
        )
    )
    
    rows = df.count()
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "electricity_bronze.raw_prices"
        )
    )
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Historical Backfill

# COMMAND ----------

####below text runs only once for historical backfill!

for target_date in date_list:
       
    records = get_daily_prices(target_date)
    if not records:
        print(
            f"[WARNING] No data for {target_date}"
        )
        continue     
    rows = load_to_bronze(records)
    print(
        f"[INFO] {target_date}: {len(records)} rows"
        )
    print(f"Loaded {rows} rows")
   



# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Verification

# COMMAND ----------

electricity_bronze = spark.table("electricity_bronze.raw_prices")
print("Bronze rows:", electricity_bronze.count())

electricity_bronze.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

electricity_bronze.select(
    F.min("time_start").alias("min_time"),
    F.max("time_start").alias("max_time")
).show()

# COMMAND ----------

after_count = (
    spark.table(
        "electricity_bronze.raw_prices"
    ).count()
)
print(after_count)

# COMMAND ----------



electricity_bronze.select(
    F.min("time_start").alias("min_time"),
    F.max("time_start").alias("max_time")
).show()

electricity_bronze.groupBy(
    "region"
).count().orderBy(
    "region"
).show()