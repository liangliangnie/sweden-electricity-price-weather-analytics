# Databricks notebook source
# MAGIC %md
# MAGIC #### 1. Configurationn

# COMMAND ----------

spark.sql("USE CATALOG sweden_el_price")
database = "electricity_bronze"
spark.sql(f"USE {database}")
REGIONS = ["SE1", "SE2", "SE3", "SE4"]

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

# MAGIC %md
# MAGIC ####5.Incremental Load
# MAGIC ##### Run below code efter running historical backfill at least once!

# COMMAND ----------

from pyspark.sql import functions as F
bronze = spark.table(
    "electricity_bronze.raw_prices"
)

latest_date = (
    bronze
    .select(
        F.to_date("time_start").alias("price_date")
    )
    .agg(
        F.max("price_date").alias("latest_date")
    )
    .collect()[0]["latest_date"]
)

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

def get_missing_dates(latest_date, end_date):

    if latest_date is None:
        raise ValueError(
            "Bronze table is empty. "
            "Run historical backfill first."
        )

    start_date = latest_date + timedelta(days=1)

    if start_date > end_date:
        return []

    return [
        start_date + timedelta(days=i)
        for i in range(
            (end_date - start_date).days + 1
        )
    ]

# COMMAND ----------


records = get_daily_prices(datetime.today().date())

# COMMAND ----------

df = spark.createDataFrame(
    records,
    schema=bronze_schema
)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ####Merge

# COMMAND ----------

from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "sweden_el_price.electricity_bronze.raw_prices")

(
    target_table.alias("target")
    .merge(
        df.alias("source"),
        """
        target.region = source.region
        AND target.time_start = source.time_start
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("[INFO] Incremental data merged into bronze table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Verification

# COMMAND ----------

after_count = (
    spark.table(
        "electricity_bronze.raw_prices"
    ).count()
)
print(after_count)