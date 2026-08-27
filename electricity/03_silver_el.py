# Databricks notebook source
# MAGIC %md
# MAGIC ##1. Data Quality Check

# COMMAND ----------

bronze = spark.table(
    "sweden_el_price.electricity_bronze.raw_prices"
)
bronze.show(10, truncate=False)

# COMMAND ----------

bronze.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set(
    "spark.sql.session.timeZone",
    "Europe/Stockholm"
)

def transform_to_silver(bronze_df):

    silver = (
        bronze_df

        .withColumn(
            "time_start",
            F.to_timestamp(
                "time_start",
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        )

        .withColumn(
            "time_end",
            F.to_timestamp(
                "time_end",
                "yyyy-MM-dd'T'HH:mm:ssXXX"
            )
        )

        .withColumn(
            "price_date",
            F.to_date("time_start")
        )

        .withColumn(
            "price_hour",
            F.hour("time_start")
        )

        .select(
            "price_date",
            "time_start",
            "time_end",
            "price_hour",
            "region",
            "SEK_per_kWh",
            "EUR_per_kWh",
            "EXR",
            "ingestion_time",
            "source"
        )
    )

    return silver


# COMMAND ----------

silver = transform_to_silver(bronze)

# COMMAND ----------

silver.printSchema()

# COMMAND ----------

silver.count()

# COMMAND ----------

# QC 1. Null
from pyspark.sql import functions as F

null_check = silver.select(
    F.sum(F.col("price_date").isNull().cast("int")).alias("null_price_date"),
    F.sum(F.col("time_start").isNull().cast("int")).alias("null_time_start"),
    F.sum(F.col("time_end").isNull().cast("int")).alias("null_time_end"),
    F.sum(F.col("region").isNull().cast("int")).alias("null_region"),
    F.sum(F.col("SEK_per_kWh").isNull().cast("int")).alias("null_sek_price"),
    F.sum(F.col("EUR_per_kWh").isNull().cast("int")).alias("null_eur_price"),
    F.sum(F.col("EXR").isNull().cast("int")).alias("null_exr")
)

null_check.show()

# COMMAND ----------

#QC 2：Region
valid_regions = ["SE1", "SE2", "SE3", "SE4"]

invalid_regions = (
    silver
    .filter(
        ~F.col("region").isin(valid_regions)
    )
    .select("region")
    .distinct()
)

invalid_regions.show()

# COMMAND ----------

# QC 3. Time
invalid_time = (
    silver
    .filter(
        F.col("time_end") <= F.col("time_start")
    )
)

invalid_time.select(
    "price_date",
    "region",
    "time_start",
    "time_end"
).show(20, truncate=False)

# COMMAND ----------

#QC 4：Duplicate
from pyspark.sql import functions as F

def check_duplicates(df):
    """
    Detect duplicate records based on region + time_start.
    Returns a DataFrame containing only the duplicates.
    """
    duplicates = (
        df
        .groupBy("region", "time_start")
        .count()
        .filter(F.col("count") > 1)
    )
    
    return duplicates

duplicates = check_duplicates(silver)

duplicates.show(20, truncate=False)

# COMMAND ----------

# QC 5. Price profile
price_profile = silver.select(
    F.min("SEK_per_kWh").alias("min_sek"),
    F.max("SEK_per_kWh").alias("max_sek"),
    F.min("EUR_per_kWh").alias("min_eur"),
    F.max("EUR_per_kWh").alias("max_eur")
)

price_profile.show()

# COMMAND ----------

# QC 6：Coverage
coverage = (
    silver
    .groupBy(
        "price_date",
        "region"
    )
    .count()
    .orderBy(
        "price_date",
        "region"
    )
)

coverage.show(30)

# COMMAND ----------

hour_coverage = (
    silver
    .groupBy(
        "price_date",
        "region"
    )
    .agg(
        F.count("*").alias("interval_count"),
        F.min("price_hour").alias("min_hour"),
        F.max("price_hour").alias("max_hour"),
        F.countDistinct("price_hour").alias("distinct_hours")
    )
    .orderBy(
        "price_date",
        "region"
    )
)

hour_coverage.show(30)

# COMMAND ----------

# Date range
silver.select(
    F.min("price_date").alias("min_date"),
    F.max("price_date").alias("max_date")
).show()

# Region
silver.select(
    "region"
).distinct().show()

# COMMAND ----------

##2. Silver table

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS sweden_el_price.electricity_silver
""")
spark.sql("""
USE sweden_el_price.electricity_silver
""")


# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

### First time loading data to silver table
(
    silver.write
    .format("delta")
    .mode("overwrite") # The first time loading data, use "overwrite"
    .saveAsTable(
        "sweden_el_price.electricity_silver.silver_prices"
    )
)

# COMMAND ----------

silver_after = spark.table(
    "sweden_el_price.electricity_silver.silver_prices"
)

print(
    f"Silver row count: {silver_after.count()}"
)

# COMMAND ----------

silver_after.select(
    F.max("price_date").alias("latest_date")
).show()

# COMMAND ----------

duplicates_after_merge = check_duplicates(
    silver_after
)

print(
    f"Duplicate groups after MERGE: "
    f"{duplicates_after_merge.count()}"
)