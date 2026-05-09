from __future__ import annotations

import csv
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
FLIGHT_DIR = RAW_DIR / "flight_operations"
AIRPORT_DIR = RAW_DIR / "airports"
AIRCRAFT_DIR = RAW_DIR / "aircraft_reference"
WEATHER_DIR = RAW_DIR / "weather_metar"
RAW_JSON_DIR = ROOT_DIR / "data" / "raw_json"
FLIGHT_JSON_DIR = RAW_JSON_DIR / "flight_operations"
AIRPORT_JSON_DIR = RAW_JSON_DIR / "airports"
AIRCRAFT_JSON_DIR = RAW_JSON_DIR / "aircraft_reference"
WEATHER_JSON_DIR = RAW_JSON_DIR / "weather_metar"

BATCH_ID = "batch_20260509_001"
INGESTION_TS = "2026-05-09T09:00:00"
SOURCE_FLIGHTS = "synthetic_airline_ops"
SOURCE_AIRPORTS = "synthetic_airports"
SOURCE_AIRCRAFT = "synthetic_aircraft"
SOURCE_WEATHER = "synthetic_metar"


@dataclass(frozen=True)
class Airport:
    iata: str
    icao: str
    city: str
    state: str
    country: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Aircraft:
    code: str
    name: str
    iata_code: str
    icao_code: str


AIRPORTS = [
    Airport("JFK", "KJFK", "New York", "NY", "US", 40.6413, -73.7781),
    Airport("LAX", "KLAX", "Los Angeles", "CA", "US", 33.9416, -118.4085),
    Airport("ATL", "KATL", "Atlanta", "GA", "US", 33.6407, -84.4277),
    Airport("ORD", "KORD", "Chicago", "IL", "US", 41.9742, -87.9073),
    Airport("MIA", "KMIA", "Miami", "FL", "US", 25.7959, -80.2870),
    Airport("SEA", "KSEA", "Seattle", "WA", "US", 47.4502, -122.3088),
    Airport("BOS", "KBOS", "Boston", "MA", "US", 42.3656, -71.0096),
    Airport("SFO", "KSFO", "San Francisco", "CA", "US", 37.6213, -122.3790),
]

AIRCRAFT = [
    Aircraft("A35K", "Airbus A350-1000", "351", "A35K"),
    Aircraft("B789", "Boeing 787-9", "789", "B789"),
    Aircraft("A339", "Airbus A330-900neo", "339", "A339"),
    Aircraft("B77W", "Boeing 777-300ER", "77W", "B77W"),
]

AIRLINES = [
    ("VS", "Virgin Atlantic"),
    ("DL", "Delta Air Lines"),
    ("AA", "American Airlines"),
    ("UA", "United Airlines"),
]

ROUTES = [
    ("JFK", "LAX", 2475),
    ("LAX", "JFK", 2475),
    ("ATL", "MIA", 595),
    ("MIA", "ATL", 595),
    ("ORD", "SEA", 1721),
    ("SEA", "ORD", 1721),
    ("BOS", "SFO", 2704),
    ("SFO", "BOS", 2704),
    ("JFK", "ATL", 760),
    ("ATL", "JFK", 760),
    ("LAX", "SEA", 954),
    ("SEA", "LAX", 954),
]

CANCELLATION_REASONS = {
    "A": "Carrier",
    "B": "Weather",
    "C": "National Air System",
    "D": "Security",
}


def ensure_dirs() -> None:
    for path in (
        FLIGHT_DIR,
        AIRPORT_DIR,
        AIRCRAFT_DIR,
        WEATHER_DIR,
        FLIGHT_JSON_DIR,
        AIRPORT_JSON_DIR,
        AIRCRAFT_JSON_DIR,
        WEATHER_JSON_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def record_hash(record: dict[str, object]) -> str:
    payload = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hhmm(value: time) -> str:
    return value.strftime("%H%M")


def combine_dt(day: date, hhmm_str: str) -> datetime:
    return datetime.combine(day, time(int(hhmm_str[:2]), int(hhmm_str[2:])))


def generate_airports() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, airport in enumerate(AIRPORTS, start=1):
        base = {
            "airport_id": str(idx),
            "ident": airport.icao,
            "type": "large_airport",
            "name": f"{airport.city} International Airport",
            "latitude_deg": airport.lat,
            "longitude_deg": airport.lon,
            "elevation_ft": random.choice([13, 21, 125, 430, 1026]),
            "continent": "NA",
            "iso_country": airport.country,
            "iso_region": f"US-{airport.state}",
            "municipality": airport.city,
            "scheduled_service": "yes",
            "gps_code": airport.icao,
            "icao_code": airport.icao,
            "iata_code": airport.iata,
            "local_code": airport.iata,
            "home_link": "",
            "wikipedia_link": "",
            "keywords": airport.city.lower(),
            "source_file_name": "synthetic_airports.csv",
            "source_system": SOURCE_AIRPORTS,
            "ingestion_timestamp": INGESTION_TS,
            "batch_id": BATCH_ID,
        }
        base["raw_record"] = json.dumps(base, sort_keys=True)
        base["record_hash"] = record_hash(base)
        rows.append(base)
    return rows


def generate_aircraft() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for aircraft in AIRCRAFT:
        base = {
            "aircraft_code": aircraft.code,
            "aircraft_name": aircraft.name,
            "iata_code": aircraft.iata_code,
            "icao_code": aircraft.icao_code,
            "source_file_name": "synthetic_aircraft_reference.csv",
            "source_system": SOURCE_AIRCRAFT,
            "ingestion_timestamp": INGESTION_TS,
            "batch_id": BATCH_ID,
        }
        base["raw_record"] = json.dumps(base, sort_keys=True)
        base["record_hash"] = record_hash(base)
        rows.append(base)
    return rows


def build_weather_category(delay_driver: str | None) -> tuple[str, int, float]:
    if delay_driver == "weather":
        return random.choice([("IFR", 18, 1.4), ("MVFR", 22, 2.7), ("LIFR", 30, 4.0)])
    return random.choice([("VFR", 5, 10.0), ("MVFR", 12, 6.0)])


def generate_flights(start_day: date, days: int = 60, flights_per_day: int = 12) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    flight_sequence = 100
    airport_map = {airport.iata: airport for airport in AIRPORTS}

    for day_offset in range(days):
        flight_day = start_day + timedelta(days=day_offset)
        for route_idx in range(flights_per_day):
            origin, dest, distance = ROUTES[(day_offset * flights_per_day + route_idx) % len(ROUTES)]
            airline_code, airline_name = AIRLINES[(day_offset + route_idx) % len(AIRLINES)]
            aircraft = AIRCRAFT[(day_offset + route_idx) % len(AIRCRAFT)]
            scheduled_hour = 6 + ((route_idx * 2) % 14)
            scheduled_minute = random.choice([0, 10, 15, 20, 30, 40, 45, 50])
            dep_scheduled = time(scheduled_hour, scheduled_minute)
            block_minutes = int(distance / 8.2) + random.randint(35, 75)
            arr_scheduled_dt = datetime.combine(flight_day, dep_scheduled) + timedelta(minutes=block_minutes)

            cancelled = random.random() < 0.04
            diverted = 0 if cancelled else int(random.random() < 0.015)

            delay_driver: str | None = None
            if cancelled:
                delay_driver = random.choice(["carrier", "weather", "nas"])
            elif random.random() < 0.38:
                delay_driver = random.choices(
                    ["carrier", "weather", "nas", "late_aircraft", "security"],
                    weights=[35, 20, 25, 18, 2],
                    k=1,
                )[0]

            dep_delay = 0
            arr_delay = 0
            carrier_delay = 0
            weather_delay = 0
            nas_delay = 0
            security_delay = 0
            late_aircraft_delay = 0
            taxi_out = random.randint(11, 28)
            taxi_in = random.randint(5, 16)
            cancellation_code = ""

            if cancelled:
                cancellation_code = {"carrier": "A", "weather": "B", "nas": "C"}[delay_driver]
                dep_time = ""
                arr_time = ""
                wheels_off = ""
                wheels_on = ""
                dep_delay = None
                arr_delay = None
                taxi_out = None
                taxi_in = None
            else:
                if delay_driver == "carrier":
                    dep_delay = random.randint(18, 95)
                    carrier_delay = dep_delay
                elif delay_driver == "weather":
                    dep_delay = random.randint(25, 140)
                    weather_delay = dep_delay
                elif delay_driver == "nas":
                    dep_delay = random.randint(20, 80)
                    nas_delay = dep_delay
                elif delay_driver == "late_aircraft":
                    dep_delay = random.randint(15, 90)
                    late_aircraft_delay = dep_delay
                elif delay_driver == "security":
                    dep_delay = random.randint(20, 45)
                    security_delay = dep_delay
                else:
                    dep_delay = random.randint(-5, 12)

                arr_delay = dep_delay + random.randint(-12, 20)
                actual_dep_dt = datetime.combine(flight_day, dep_scheduled) + timedelta(minutes=dep_delay)
                actual_arr_dt = arr_scheduled_dt + timedelta(minutes=arr_delay)
                wheels_off_dt = actual_dep_dt + timedelta(minutes=taxi_out)
                wheels_on_dt = actual_arr_dt - timedelta(minutes=taxi_in)
                dep_time = hhmm(actual_dep_dt.time())
                arr_time = hhmm(actual_arr_dt.time())
                wheels_off = hhmm(wheels_off_dt.time())
                wheels_on = hhmm(wheels_on_dt.time())

            origin_meta = airport_map[origin]
            dest_meta = airport_map[dest]
            flight_number = str(flight_sequence)
            tail_number = f"N{random.randint(100, 999)}{aircraft.code[:2]}"

            base = {
                "flight_date": flight_day.isoformat(),
                "reporting_airline": airline_code,
                "reporting_airline_name": airline_name,
                "tail_number": tail_number,
                "flight_number": flight_number,
                "origin_airport": origin,
                "origin_airport_seq_id": f"{10000 + route_idx}",
                "origin_city_name": f"{origin_meta.city}, {origin_meta.state}",
                "origin_state_abbr": origin_meta.state,
                "dest_airport": dest,
                "dest_airport_seq_id": f"{20000 + route_idx}",
                "dest_city_name": f"{dest_meta.city}, {dest_meta.state}",
                "dest_state_abbr": dest_meta.state,
                "crs_dep_time": hhmm(dep_scheduled),
                "dep_time": dep_time,
                "dep_delay_minutes": dep_delay,
                "taxi_out": taxi_out,
                "wheels_off": wheels_off,
                "wheels_on": wheels_on,
                "taxi_in": taxi_in,
                "crs_arr_time": hhmm(arr_scheduled_dt.time()),
                "arr_time": arr_time,
                "arr_delay_minutes": arr_delay,
                "cancelled": 1 if cancelled else 0,
                "cancellation_code": cancellation_code,
                "diverted": diverted,
                "distance_miles": distance,
                "carrier_delay": carrier_delay,
                "weather_delay": weather_delay,
                "nas_delay": nas_delay,
                "security_delay": security_delay,
                "late_aircraft_delay": late_aircraft_delay,
                "source_file_name": f"synthetic_flight_operations_{flight_day.strftime('%Y_%m')}.csv",
                "source_system": SOURCE_FLIGHTS,
                "ingestion_timestamp": INGESTION_TS,
                "batch_id": BATCH_ID,
                "year_month": flight_day.strftime("%Y-%m"),
            }
            base["raw_record"] = json.dumps(base, sort_keys=True, default=str)
            base["record_hash"] = record_hash(base)
            rows.append(base)
            flight_sequence += 1
    return rows


def generate_weather(flights: list[dict[str, object]]) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    for flight in flights:
        flight_date = datetime.fromisoformat(str(flight["flight_date"])).date()
        route_driver = None
        if (flight.get("weather_delay") or 0) > 0 or flight.get("cancellation_code") == "B":
            route_driver = "weather"

        for airport_code, stamp_field in (
            (flight["origin_airport"], "crs_dep_time"),
            (flight["dest_airport"], "crs_arr_time"),
        ):
            hhmm_value = str(flight[stamp_field])
            obs_dt = combine_dt(flight_date, hhmm_value) - timedelta(minutes=35)
            key = (str(airport_code), obs_dt.isoformat())
            if key in seen:
                continue
            seen.add(key)

            category, wind_speed, visibility = build_weather_category(route_driver)
            airport = next(item for item in AIRPORTS if item.iata == airport_code)
            base = {
                "station_id": airport.icao,
                "observation_time": obs_dt.isoformat(sep=" "),
                "raw_text": f"{airport.icao} AUTO {obs_dt.strftime('%d%H%M')}Z {wind_speed:03d}08KT 10SM {category}",
                "temp_c": round(random.uniform(3.0, 30.0), 1),
                "dewpoint_c": round(random.uniform(-2.0, 22.0), 1),
                "wind_dir_degrees": random.choice([40, 90, 120, 180, 220, 270, 310]),
                "wind_speed_kt": wind_speed,
                "wind_gust_kt": wind_speed + random.choice([0, 4, 7]),
                "visibility_statute_mi": visibility,
                "altim_in_hg": round(random.uniform(29.20, 30.25), 2),
                "flight_category": category,
                "precip_in": round(random.choice([0.0, 0.0, 0.02, 0.08, 0.15]), 2),
                "sky_cover": random.choice(["CLR", "FEW", "SCT", "BKN", "OVC"]),
                "source_file_name": f"synthetic_weather_metar_{flight_date.strftime('%Y_%m')}.csv",
                "source_system": SOURCE_WEATHER,
                "ingestion_timestamp": INGESTION_TS,
                "batch_id": BATCH_ID,
                "observation_date": flight_date.isoformat(),
            }
            base["raw_record"] = json.dumps(base, sort_keys=True, default=str)
            base["record_hash"] = record_hash(base)
            observations.append(base)

    return observations


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str))
            handle.write("\n")


def split_and_write_flights(rows: list[dict[str, object]]) -> None:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row["year_month"]), []).append(row)

    flight_fields = [
        "flight_date",
        "reporting_airline",
        "reporting_airline_name",
        "tail_number",
        "flight_number",
        "origin_airport",
        "origin_airport_seq_id",
        "origin_city_name",
        "origin_state_abbr",
        "dest_airport",
        "dest_airport_seq_id",
        "dest_city_name",
        "dest_state_abbr",
        "crs_dep_time",
        "dep_time",
        "dep_delay_minutes",
        "taxi_out",
        "wheels_off",
        "wheels_on",
        "taxi_in",
        "crs_arr_time",
        "arr_time",
        "arr_delay_minutes",
        "cancelled",
        "cancellation_code",
        "diverted",
        "distance_miles",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
        "raw_record",
        "source_file_name",
        "source_system",
        "ingestion_timestamp",
        "batch_id",
        "record_hash",
        "year_month",
    ]
    for year_month, month_rows in grouped.items():
        write_csv(FLIGHT_DIR / f"synthetic_flight_operations_{year_month}.csv", month_rows, flight_fields)
        write_jsonl(
            FLIGHT_JSON_DIR / f"synthetic_flight_operations_{year_month}.jsonl",
            month_rows,
        )


def main() -> None:
    random.seed(42)
    ensure_dirs()

    airports = generate_airports()
    aircraft = generate_aircraft()
    flights = generate_flights(start_day=date(2025, 1, 1))
    weather = generate_weather(flights)

    write_csv(
        AIRPORT_DIR / "synthetic_airports.csv",
        airports,
        [
            "airport_id",
            "ident",
            "type",
            "name",
            "latitude_deg",
            "longitude_deg",
            "elevation_ft",
            "continent",
            "iso_country",
            "iso_region",
            "municipality",
            "scheduled_service",
            "gps_code",
            "icao_code",
            "iata_code",
            "local_code",
            "home_link",
            "wikipedia_link",
            "keywords",
            "raw_record",
            "source_file_name",
            "source_system",
            "ingestion_timestamp",
            "batch_id",
            "record_hash",
        ],
    )
    write_jsonl(AIRPORT_JSON_DIR / "synthetic_airports.jsonl", airports)
    write_csv(
        AIRCRAFT_DIR / "synthetic_aircraft_reference.csv",
        aircraft,
        [
            "aircraft_code",
            "aircraft_name",
            "iata_code",
            "icao_code",
            "raw_record",
            "source_file_name",
            "source_system",
            "ingestion_timestamp",
            "batch_id",
            "record_hash",
        ],
    )
    write_jsonl(AIRCRAFT_JSON_DIR / "synthetic_aircraft_reference.jsonl", aircraft)
    split_and_write_flights(flights)
    write_csv(
        WEATHER_DIR / "synthetic_weather_metar.csv",
        weather,
        [
            "station_id",
            "observation_time",
            "raw_text",
            "temp_c",
            "dewpoint_c",
            "wind_dir_degrees",
            "wind_speed_kt",
            "wind_gust_kt",
            "visibility_statute_mi",
            "altim_in_hg",
            "flight_category",
            "precip_in",
            "sky_cover",
            "raw_record",
            "source_file_name",
            "source_system",
            "ingestion_timestamp",
            "batch_id",
            "record_hash",
            "observation_date",
        ],
    )
    write_jsonl(WEATHER_JSON_DIR / "synthetic_weather_metar.jsonl", weather)

    print(
        "Generated synthetic raw data:",
        f"{len(flights)} flight rows,",
        f"{len(airports)} airport rows,",
        f"{len(aircraft)} aircraft rows,",
        f"{len(weather)} weather rows.",
    )


if __name__ == "__main__":
    main()
