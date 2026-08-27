# Databricks notebook source
# ============================================================
# GOLD DATA QUALITY CHECK
# Production-style PASS / FAIL QC
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import Window

spark.sql("USE CATALOG sweden_el_price")

# COMMAND ----------

# ============================================================
# 0. Load tables
# ============================================================

gold = spark.table("gold.electricity_weather")

electricity_silver = spark.table(
    "electricity_silver.silver_prices"
)

weather_silver = spark.table(
    "weather_silver.weather"
)


# ============================================================
# Helper
# ============================================================

qc_results = []


def record_check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"

    qc_results.append({
        "check": name,
        "status": status,
        "detail": detail
    })


# COMMAND ----------

# ============================================
# QC 1 — Gold baseline
# ============================================

gold_rows = gold.count()

gold_distinct_keys = (
    gold
    .select("region", "hour_utc")
    .distinct()
    .count()
)

print("=" * 60)
print("GOLD BASELINE")
print("=" * 60)

print("Gold rows:", gold_rows)
print("Gold distinct keys:", gold_distinct_keys)

print("\nGold time range:")

gold.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()

print("Gold rows by region:")

gold.groupBy(
    "region"
).count().orderBy(
    "region"
).show()

# COMMAND ----------

# ============================================
# FINAL QC 2 — Duplicate keys
# ============================================
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

duplicate_count = gold_duplicates.count()

record_check(
    "Duplicate Gold keys",
    duplicate_count == 0,
    f"duplicate_keys={duplicate_count}"
)

print("\nDuplicate Gold keys:", duplicate_count)

if duplicate_count > 0:
    gold_duplicates.show(20, truncate=False)

# COMMAND ----------

# ============================================================
# QC 3 — Rows must equal distinct keys
# ============================================================

record_check(
    "Gold rows = distinct keys",
    gold_rows == gold_distinct_keys,
    f"rows={gold_rows}, distinct_keys={gold_distinct_keys}"
)

# COMMAND ----------

# ============================================================
# QC 4 — Required regions
# ============================================================

expected_regions = {"SE1", "SE2", "SE3", "SE4"}

actual_regions = {
    row["region"]
    for row in gold.select("region").distinct().collect()
}

missing_regions = expected_regions - actual_regions
unexpected_regions = actual_regions - expected_regions

regions_ok = (
    len(missing_regions) == 0
    and len(unexpected_regions) == 0
)

record_check(
    "Region completeness",
    regions_ok,
    f"actual={sorted(actual_regions)}, "
    f"missing={sorted(missing_regions)}, "
    f"unexpected={sorted(unexpected_regions)}"
)

# COMMAND ----------

# ============================================
# FINAL QC 5 — Hour continuity by region
# ============================================

w = Window.partitionBy(
    "region"
).orderBy(
    "hour_utc"
)

gold_gap_check = (
    gold
    .select(
        "region",
        "hour_utc"
    )
    .withColumn(
        "previous_hour",
        F.lag("hour_utc").over(w)
    )
    .withColumn(
        "hour_diff",
        (
            F.col("hour_utc").cast("long")
            - F.col("previous_hour").cast("long")
        ) / 3600
    )
)

gold_gaps = (
    gold_gap_check
    .filter(
        F.col("previous_hour").isNotNull()
        &
        (F.col("hour_diff") != 1)
    )
)

gap_count = gold_gaps.count()

record_check(
    "Hourly continuity",
    gap_count == 0,
    f"unexpected_gaps={gap_count}"
)

print("\nUnexpected hourly gaps:", gap_count)

if gap_count > 0:
    gold_gaps.show(20, truncate=False)

# COMMAND ----------

# ============================================
# FINAL QC 6 — Electricity / Weather keys mapping 
# ============================================

# ----------------------------
# Electricity hourly keys
# ----------------------------

electricity_hourly_gold = (
    electricity_silver
    .withColumn(
        "electricity_hour_utc",
        F.date_trunc(
            "hour",
            F.col("time_start")
        )
    )
    .select(
        "region",
        "electricity_hour_utc"
    )
    .dropDuplicates(
        ["region", "electricity_hour_utc"]
    )
)

electricity_keys = (
    electricity_hourly_gold
    .select(
        "region",
        F.col(
            "electricity_hour_utc"
        ).alias("hour_utc")
    )
    .distinct()
)


# ----------------------------
# Weather hourly keys
# ----------------------------

weather_hourly_gold = (
    weather_silver
    .select(
        "region",
        F.col(
            "weather_time_utc"
        ).alias("hour_utc")
    )
    .dropDuplicates(
        ["region", "hour_utc"]
    )
)

weather_keys = (
    weather_hourly_gold
    .select(
        "region",
        "hour_utc"
    )
    .distinct()
)


# ----------------------------
# Gold keys
# ----------------------------

gold_keys = (
    gold
    .select(
        "region",
        "hour_utc"
    )
    .distinct()
)


# ----------------------------
# Gold without Electricity
# ----------------------------

gold_without_electricity = (
    gold_keys
    .join(
        electricity_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

gold_without_electricity_count = (
    gold_without_electricity.count()
)

record_check(
    "Gold keys mapped to Electricity",
    gold_without_electricity_count == 0,
    f"missing_electricity_keys="
    f"{gold_without_electricity_count}"
)


# ----------------------------
# Gold without Weather
# ----------------------------

gold_without_weather = (
    gold_keys
    .join(
        weather_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

gold_without_weather_count = (
    gold_without_weather.count()
)

record_check(
    "Gold keys mapped to Weather",
    gold_without_weather_count == 0,
    f"missing_weather_keys="
    f"{gold_without_weather_count}"
)

# COMMAND ----------

# ============================================================
# QC 7 — Source coverage diagnostics
#
# These are NOT failures because electricity/weather
# can arrive at different times.
# ============================================================

electricity_only = (
    electricity_keys
    .join(
        gold_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

weather_only = (
    weather_keys
    .join(
        gold_keys,
        ["region", "hour_utc"],
        "left_anti"
    )
)

electricity_only_count = electricity_only.count()
weather_only_count = weather_only.count()

print("\n" + "=" * 60)
print("SOURCE COVERAGE DIAGNOSTICS")
print("=" * 60)

print(
    "Electricity-only keys:",
    electricity_only_count
)

print(
    "Weather-only keys:",
    weather_only_count
)

print(
    "INFO: Electricity-only keys are informational "
    "and do not fail the Job."
)

print(
    "INFO: Weather-only keys are informational "
    "and do not fail the Job."
)


# COMMAND ----------

# ============================================================
# QC 8 — Required Gold columns must not be NULL
# ============================================================

required_columns = [
    "region",
    "hour_utc",
    "SEK_per_kWh",
    "EUR_per_kWh",
    "EXR",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code"
]

null_condition = None

for column_name in required_columns:

    condition = F.col(column_name).isNull()

    if null_condition is None:
        null_condition = condition
    else:
        null_condition = (
            null_condition | condition
        )

null_rows = gold.filter(
    null_condition
).count()

record_check(
    "Required Gold fields not null",
    null_rows == 0,
    f"rows_with_required_nulls={null_rows}"
)


# COMMAND ----------

# ============================================
# FINAL QC 9 — DST validation
# ============================================

dst_dates = ["2025-10-26", "2026-03-29"]

dst_check = (
    gold
    .filter(
        F.to_date("hour_utc").isin(dst_dates)
    )
    .groupBy(
        "region",
        F.to_date("hour_utc").alias("date")
    )
    .count()
)

print("\n" + "=" * 60)
print("DST VALIDATION")
print("=" * 60)

dst_check.orderBy(
    "date",
    "region"
).show()

expected_dst_rows = 24

dst_wrong = (
    dst_check
    .filter(
        F.col("count") != expected_dst_rows
    )
)

dst_wrong_count = dst_wrong.count()

record_check(
    "DST UTC day completeness",
    dst_wrong_count == 0,
    f"unexpected_dst_counts={dst_wrong_count}"
)

if dst_wrong_count > 0:
    dst_wrong.show(20, truncate=False)

# COMMAND ----------

# ============================================================
# QC 10 — Final Gold time range
# ============================================================

print("\n" + "=" * 60)
print("FINAL GOLD RANGE")
print("=" * 60)

gold.select(
    F.min("hour_utc").alias("min_hour_utc"),
    F.max("hour_utc").alias("max_hour_utc")
).show()


# COMMAND ----------

# ============================================================
# FINAL QC REPORT
# ============================================================

print("\n")
print("=" * 60)
print("             GOLD DATA QUALITY REPORT")
print("=" * 60)

for result in qc_results:

    print(
        f"{result['status']:6} | "
        f"{result['check']} | "
        f"{result['detail']}"
    )


failed_checks = [
    result["check"]
    for result in qc_results
    if result["status"] == "FAIL"
]


print("=" * 60)

if len(failed_checks) == 0:

    print("              GOLD QC PASSED")
    print("=" * 60)

else:

    print("              GOLD QC FAILED")
    print("=" * 60)

    print("Failed checks:")

    for check in failed_checks:
        print(f" - {check}")

    raise RuntimeError(
        "Gold QC FAILED: "
        + ", ".join(failed_checks)
    )