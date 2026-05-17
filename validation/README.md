# Validation SQL

This folder contains SQL validation checks for the Bronze, Silver, and Gold
layers of the airline flight operations Lakehouse project.

## Files

- `bronze_validation.sql`: raw layer row-count and completeness checks
- `silver_validation.sql`: cleaned layer and quarantine checks
- `gold_validation.sql`: reporting-layer KPI and consistency checks

## Suggested Run Order

1. Run Bronze validation after Bronze ingestion completes
2. Run Silver validation after Silver transformations complete
3. Run Gold validation after Gold transformations complete

## Goal

These checks help confirm:

- expected row counts
- valid clean/quarantine split
- route and airport coverage
- cancellation consistency
- KPI sanity for reporting outputs
