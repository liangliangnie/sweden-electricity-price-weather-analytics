# Data Pipeline

## Overview

The project uses a Databricks-based data pipeline to ingest, transform, integrate and validate electricity and weather data.

The pipeline supports two main processing patterns:

- Historical backfill / initial load
- Scheduled incremental processing

---

## Data Flow

```text
Electricity API ──→ Electricity Bronze ──→ Electricity Silver ──┐
                                                                │
                                                                ├──→ Gold ──→ Gold QC
                                                                │
Weather API ──────→ Weather Bronze ──────→ Weather Silver ──────┘

```
The two datasets are integrated at hourly level using:

`region + hour_utc`

---

## Historical Backfill

The initial load is used to populate the historical dataset.

### Electricity

```
Electricity API
      ↓
Bronze Backfill
      ↓
Silver
      ↓
Gold Initial
```
### Weather
```
Open-Meteo API
      ↓
Weather Bronze Backfill
      ↓
Weather Silver
      ↓
Gold Initial
```

The Gold initial process uses the overlapping time range available in both source datasets before integrating the data.

---

## Incremental Processing

After the historical load, new observations are processed incrementally.

### Electricity

```
Electricity API
      ↓
Bronze Incremental
      ↓
Silver Incremental 
```

### Weather

```
Open-Meteo API
      ↓
Weather Bronze Incremental
      ↓
Weather Silver Incremental
```

The Gold incremental process then integrates the new electricity and weather observations and updates the Gold Delta table using `MERGE`.

---

## Databricks Job

The incremental pipeline is orchestrated through a scheduled Databricks Job.

```
The workflow follows:

Electricity Bronze Incremental
              ↓
Electricity Silver Incremental
              ↓
Weather Bronze Incremental
              ↓
Weather Silver Incremental
              ↓
        Gold Incremental
              ↓
           Gold QC
```

The scheduled workflow allows the analytical dataset to be maintained as new observations become available.

---

## Time Handling

UTC is used as the common reference for hourly data integration.

The pipeline also maintains local time information where required for analysis.

This approach is particularly important when handling daylight saving time transitions.

---

## Data Quality

Gold QC is executed after the Gold layer has been updated.

The validation checks include:

- Duplicate keys
- Distinct key consistency
- Region completeness
- Hourly continuity
- Electricity mapping
- Weather mapping
- Required field completeness
- DST completeness

The pipeline is considered successful only when the required Gold quality checks pass.
