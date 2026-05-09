# Synthetic Raw Data

This folder contains synthetic airline operations raw data aligned to the
Databricks Bronze DDL in `sql/ddl/010_bronze_tables.sql`.

## Datasets

- `raw/flight_operations/`: monthly CSV flight operations files
- `raw/airports/`: airport reference CSV file
- `raw/aircraft_reference/`: aircraft lookup CSV file
- `raw/weather_metar/`: weather observations CSV file
- `raw_json/flight_operations/`: monthly JSON Lines flight operations files
- `raw_json/airports/`: airport reference JSON Lines file
- `raw_json/aircraft_reference/`: aircraft lookup JSON Lines file
- `raw_json/weather_metar/`: weather observations JSON Lines file

## Regenerate

Run:

```bash
python3 scripts/generate_airline_raw_data.py
```

The generator creates enough variation in delays, cancellations, routes,
airlines, airports, and weather conditions to support Bronze, Silver, and Gold
reporting layers.
