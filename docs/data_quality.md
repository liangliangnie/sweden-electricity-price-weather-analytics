# Data Quality

Data quality validation is implemented as a dedicated Gold QC notebook.

The purpose of the QC process is to verify that the final analytical dataset is complete, consistent and correctly integrated before it is used by Power BI.

---

## Gold Key

The primary Gold key is:

`region + hour_utc`

The key is used to validate uniqueness and to verify the relationship between the Gold dataset and the underlying electricity and weather data.

---

## Quality Checks

The Gold QC performs the following checks:

### 1. Duplicate Gold Keys

Verifies that there are no duplicate:

`region + hour_utc`

keys in the Gold dataset.

**Expected result:** 0 duplicate keys.

### 2. Gold Rows vs. Distinct Keys

Verifies that the total number of Gold rows equals the number of distinct Gold keys.

**Expected result:** PASS.

### 3. Region Completeness

Verifies that all expected Swedish bidding zones are present:

- SE1
- SE2
- SE3
- SE4

**Expected result:** no missing or unexpected regions.

### 4. Hourly Continuity

Checks for unexpected gaps in the hourly Gold dataset.

The validation is performed separately for each bidding zone.

**Expected result:** no unexpected gaps.

### 5. Electricity Mapping

Verifies that every Gold key can be mapped back to an electricity observation.

**Expected result:** no missing electricity keys.

### 6. Weather Mapping

Verifies that every Gold key can be mapped back to a weather observation.

**Expected result:** no missing weather keys.

### 7. Required Fields

Checks that required analytical fields are not null.

The validated fields include:

- Region
- UTC timestamp
- Electricity price
- Temperature
- Relative humidity
- Precipitation
- Wind speed
- Weather code

**Expected result:** no rows with required null values.

### 8. Daylight Saving Time

Specific DST transition dates are validated to ensure that the expected number of UTC observations is present for every region.

This provides an additional check that hourly time alignment remains correct across daylight saving time transitions.

---

## Final QC Result

The final Gold data quality validation passed all implemented checks.

```text
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

## Why Data Quality Matters

The Gold layer is the analytical source consumed by Power BI.

The QC process provides a validation step between data processing and reporting, helping ensure that the dashboard is based on a structurally consistent and correctly integrated dataset.

The checks are designed to catch issues related to:

- Duplicate observations
- Missing hours
- Missing regions
- Source integration
- Null analytical fields
- Daylight saving time transitions
