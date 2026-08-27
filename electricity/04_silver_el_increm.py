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

coverage.show(300)

# COMMAND ----------

#QC Time range
silver.select(
    F.min("price_date").alias("min_date"),
    F.max("price_date").alias("max_date")
).show()

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

from pyspark.sql import functions as F

silver_table = spark.table(
    "sweden_el_price.electricity_silver.silver_prices"
)

latest_silver_date = (
    silver_table
    .select(
        F.max("price_date").alias("latest_date")
    )
    .collect()[0]["latest_date"]
)

print(f"Latest Silver date: {latest_silver_date}")

# COMMAND ----------

def get_new_bronze_data(latest_silver_date):

    bronze_table = spark.table(
        "sweden_el_price.electricity_bronze.raw_prices"
    )

    if latest_silver_date is None:
        return bronze_table

    return (
        bronze_table
        .filter(
            F.to_date("time_start") > latest_silver_date
        )
    )

# COMMAND ----------

new_bronze = get_new_bronze_data(
    latest_silver_date
)

print(f"New Bronze rows: {new_bronze.count()}")

# COMMAND ----------

new_silver = transform_to_silver(new_bronze)

print(f"New Silver rows: {new_silver.count()}")

duplicates = check_duplicates(new_silver)

print(
    f"Duplicate groups in new data: "
    f"{duplicates.count()}"
)

# COMMAND ----------

existing_matches = (
    new_silver.alias("source")
    .join(
        silver_table.alias("target"),
        (
            (F.col("source.region") == F.col("target.region")) &
            (F.col("source.time_start") == F.col("target.time_start"))
        ),
        "inner"
    )
)

print(
    f"Rows already existing in Silver: "
    f"{existing_matches.count()}"
)

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forName(
    spark,
    "sweden_el_price.electricity_silver.silver_prices"
)

# COMMAND ----------

# Regenerate new_silver from new_bronze since it was overwritten by check_duplicates


(
    silver_delta.alias("target")
    .merge(
        new_silver.alias("source"),
        """
        target.region = source.region
        AND target.time_start = source.time_start
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("[INFO] Silver MERGE completed.")

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