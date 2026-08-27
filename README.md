# Swedish Electricity Price & Weather Analytics

An end-to-end data engineering and analytics project combining Swedish electricity prices with historical weather observations.

The project covers the complete data journey from API ingestion and transformation to data quality validation, automated incremental processing and Power BI analysis.

The analysis focuses on Sweden's four electricity bidding zones — **SE1, SE2, SE3 and SE4** — and explores regional electricity price differences and the relationship between electricity prices, temperature and wind speed.

---

## Project Overview

The project was built using **Databricks, PySpark, Delta tables and Power BI**.

Two independent data sources are integrated into a common analytical dataset:

- Swedish electricity prices
- Historical weather observations

The pipeline supports both an initial historical backfill and ongoing incremental updates.

The final Gold dataset is consumed by Power BI for interactive analysis.

---

## Architecture

The solution follows a **Bronze → Silver → Gold** data architecture.

```text
Electricity API          Weather API
       │                     │
       └──────────┬──────────┘
                  │
       Initial / Historical
             Backfill
                  │
       Daily Incremental
             Processing
                  │
        ┌─────────┴─────────┐
        │                   │
Electricity Bronze    Weather Bronze
        │                   │
Electricity Silver    Weather Silver
        │                   │
        └─────────┬─────────┘
                  │
            Gold Integration
                  │
               Gold QC
                  │
              Power BI
```
### Architecture Diagram

![Sweden Electricity Analytics Data Pipeline](architecture/architecture.png)

The two source datasets are integrated at hourly level using:

`region + hour_utc`

---

## Data Sources

### Electricity

Hourly electricity price data for Sweden's four bidding zones.

**Source:** Elprisetjustnu

The pipeline stores:

- SEK/kWh
- EUR/kWh
- Exchange rate
- Start and end timestamps
- Bidding zone

### Weather

Historical hourly weather observations from Open-Meteo.

**Source:** Open-Meteo

Variables used in the analysis include:

- Temperature
- Relative humidity
- Precipitation
- Wind speed
- Weather code

---

## Data Engineering Pipeline
### Bronze Layer

The Bronze layer stores ingested source data with minimal transformation.

Separate pipelines are used for electricity and weather data.
```
Electricity
├── Historical Backfill
└── Incremental Load

Weather
├── Historical Backfill
└── Incremental Load
```
### Silver Layer

The Silver layer cleans and standardizes the source data.

Key transformations include:

- Timestamp parsing
- Date and hour derivation
- Region validation
- Data quality checks
- UTC/local time handling
- Preparation of hourly keys

UTC is used as the common time reference when aligning the two data sources.

### Gold Layer

The Gold layer combines electricity and weather observations into a single analytical dataset.

The integration key is:

`region + hour_utc`

The resulting dataset contains electricity price information together with:

- Location
- Latitude / longitude
- Temperature
- Relative humidity
- Precipitation
- Wind speed
- Weather code

---

## Incremental Processing

After the initial historical load, new observations are processed incrementally.

The pipeline identifies new observations, transforms them through the Silver layer and updates the Gold dataset using Delta MERGE.

The incremental design allows new data to be added without rebuilding the complete historical dataset.

---

## Data Quality

Data quality validation is performed after the Gold layer has been created.

The Gold QC validates:

- Duplicate Gold keys
- Gold rows vs. distinct keys
- Region completeness
- Hourly continuity
- Electricity-to-Gold key mapping
- Weather-to-Gold key mapping
- Required field completeness
- DST day completeness

The QC produces explicit PASS / FAIL results.

### Final Gold QC

All implemented Gold quality checks passed successfully.

```
PASS | Duplicate Gold keys
PASS | Gold rows = distinct keys
PASS | Region completeness
PASS | Hourly continuity
PASS | Gold keys mapped to Electricity
PASS | Gold keys mapped to Weather
PASS | Required Gold fields not null
PASS | DST UTC day completeness

GOLD QC PASSED
```
--- 

## Power BI Analysis

The Gold dataset is connected to Power BI using **Import mode**.

The final analysis focuses on two perspectives.

### 1. Three-Year Regional Price Trend

A three-year monthly view comparing electricity prices across Sweden's four bidding zones.

The analysis focuses on:

- Long-term price development
- Regional differences
- Average, minimum and maximum price levels

### 2. Weather & Electricity Prices

Daily observations are used to explore relationships between electricity prices and:

- Temperature
- Wind speed

The analysis compares **SE1 and SE4** to explore differences between northern and southern bidding zones.

Comparative visuals use consistent units and axis scales to support direct interpretation.

--- 

## Key Analytical Observations
### Regional Differences

SE4 generally exhibits higher electricity prices than SE1 over the observed period.

### Temperature

The analysis shows a negative relationship between temperature and electricity price, with different correlation strengths across regions.

### Wind

Wind speed also shows a negative relationship with electricity price.

These correlations describe observed statistical relationships in the dataset and should not be interpreted as evidence of causality.

--- 

## Technical Challenges

Two challenges were particularly important during development.

### 1. Time Alignment and DST

Electricity and weather data come from independent systems and need to remain correctly aligned across daylight saving time transitions.

Using UTC as the common hourly reference provides a consistent basis for integration.

### 2. Integrating Independent Data Sources

The electricity and weather datasets have different structures and coverage.

The Gold layer integrates them using the shared:

`region + hour_utc`

key.

--- 

## Automation

The pipeline is connected to a scheduled Databricks Job for incremental processing.

The workflow runs:
```
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Gold QC
```
This allows the analytical dataset to be maintained as new observations become available.

--- 

## Technology Stack

### Data Engineering
- Databricks
- PySpark
- Delta Lake
- Python
- SQL
- REST APIs
  
### Data Architecture

- Bronze / Silver / Gold
- Historical backfill
- Incremental processing
- Delta MERGE
- Data quality validation
  
### Analytics & Visualization
- Power BI
- DAX
- Data modelling
- Statistical analysis
- Correlation analysis

---

## Explore the Project

- [Electricity pipeline notebooks](electricity/)
- [Weather pipeline notebooks](weather/)
- [Gold layer & QC notebooks](gold/)
- [Power BI screenshots](powerbi/screenshots/)
- [Data pipeline documentation](docs/pipeline.md)
- [Data quality documentation](docs/data_quality.md)
--- 

## Repository Structure

```
sweden-electricity-price-weather-analytics/
│
├── README.md
│
├── architecture/
│   └── architecture.png
│
├── electricity/
│   ├── 01_bronze_el_backfill.py
│   ├── 02_bronze_el_increm.py
│   ├── 03_silver_el.py
│   └── 04_silver_el_increm.py
│
├── weather/
│   ├── 01_bronze_weather_backfill.py
│   ├── 02_bronze_weather_increm.py
│   ├── 03_silver_weather.py
│   └── 04_silver_weather_increm.py
│
├── gold/
│   ├── 01_gold_initial.py
│   ├── 02_gold_increm.py
│   └── 99_QC.py
│
├── powerbi/
│   └── screenshots/
│
└── docs/
    ├── pipeline.md
    └── data_quality.md
```
--- 

## Future Extensions

Potential extensions include:

- Electricity price volatility analysis
- Seasonal price patterns
- Solar irradiation data
- Additional energy-market variables
- Further statistical modelling
- Further analysis of relationships between weather, renewable energy and electricity prices

--- 

## Project Status

### Completed — August 2026

The end-to-end data pipeline, incremental processing, Gold data quality validation, scheduled workflow and Power BI analysis have been implemented.

This repository documents the project as a portfolio example of data engineering and data analytics using Databricks and Power BI.
