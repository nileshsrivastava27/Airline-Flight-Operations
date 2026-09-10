"""
CDC (Change Data Capture) Event Generator.

Simulates Debezium-format CDC change events from operational databases for
airline reference tables: aircraft, airports, and flight schedules. Generates
INSERT, UPDATE, and DELETE operations with before/after snapshots.

Writes to Kafka topics or local JSONL files for consumption by the CDC
ingestion pipeline.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AIRCRAFT_FLEET = [
    {"aircraft_code": "A35K", "aircraft_name": "Airbus A350-1000", "iata_code": "351", "icao_code": "A35K", "manufacturer": "Airbus", "capacity": 350, "range_nm": 8700, "status": "ACTIVE"},
    {"aircraft_code": "B789", "aircraft_name": "Boeing 787-9", "iata_code": "789", "icao_code": "B789", "manufacturer": "Boeing", "capacity": 296, "range_nm": 7635, "status": "ACTIVE"},
    {"aircraft_code": "A339", "aircraft_name": "Airbus A330-900", "iata_code": "339", "icao_code": "A339", "manufacturer": "Airbus", "capacity": 287, "range_nm": 7200, "status": "ACTIVE"},
    {"aircraft_code": "B77W", "aircraft_name": "Boeing 777-300ER", "iata_code": "77W", "icao_code": "B77W", "manufacturer": "Boeing", "capacity": 396, "range_nm": 7370, "status": "ACTIVE"},
    {"aircraft_code": "A320", "aircraft_name": "Airbus A320neo", "iata_code": "32N", "icao_code": "A20N", "manufacturer": "Airbus", "capacity": 180, "range_nm": 3500, "status": "ACTIVE"},
    {"aircraft_code": "B738", "aircraft_name": "Boeing 737-800", "iata_code": "738", "icao_code": "B738", "manufacturer": "Boeing", "capacity": 189, "range_nm": 2935, "status": "ACTIVE"},
]

AIRPORTS = [
    {"airport_code": "JFK", "airport_name": "John F. Kennedy International", "city": "New York", "country": "US", "timezone": "America/New_York", "terminal_count": 6, "status": "OPERATIONAL"},
    {"airport_code": "LAX", "airport_name": "Los Angeles International", "city": "Los Angeles", "country": "US", "timezone": "America/Los_Angeles", "terminal_count": 9, "status": "OPERATIONAL"},
    {"airport_code": "ATL", "airport_name": "Hartsfield-Jackson Atlanta International", "city": "Atlanta", "country": "US", "timezone": "America/New_York", "terminal_count": 2, "status": "OPERATIONAL"},
    {"airport_code": "ORD", "airport_name": "O'Hare International", "city": "Chicago", "country": "US", "timezone": "America/Chicago", "terminal_count": 4, "status": "OPERATIONAL"},
    {"airport_code": "DFW", "airport_name": "Dallas/Fort Worth International", "city": "Dallas", "country": "US", "timezone": "America/Chicago", "terminal_count": 5, "status": "OPERATIONAL"},
    {"airport_code": "DEN", "airport_name": "Denver International", "city": "Denver", "country": "US", "timezone": "America/Denver", "terminal_count": 3, "status": "OPERATIONAL"},
    {"airport_code": "SFO", "airport_name": "San Francisco International", "city": "San Francisco", "country": "US", "timezone": "America/Los_Angeles", "terminal_count": 4, "status": "OPERATIONAL"},
    {"airport_code": "SEA", "airport_name": "Seattle-Tacoma International", "city": "Seattle", "country": "US", "timezone": "America/Los_Angeles", "terminal_count": 2, "status": "OPERATIONAL"},
]

CHANGE_SCENARIOS = {
    "aircraft": [
        ("UPDATE", "capacity_reconfig", lambda r: {**r, "capacity": r["capacity"] + random.choice([-6, -4, 4, 6, 8])}),
        ("UPDATE", "status_change", lambda r: {**r, "status": random.choice(["MAINTENANCE", "ACTIVE", "GROUNDED"])}),
        ("UPDATE", "range_update", lambda r: {**r, "range_nm": r["range_nm"] + random.randint(-100, 200)}),
        ("INSERT", "new_aircraft", lambda _: {
            "aircraft_code": f"N{random.randint(100,999)}",
            "aircraft_name": f"New Aircraft {random.randint(1,99)}",
            "iata_code": f"{random.randint(100,999)}",
            "icao_code": f"X{random.randint(100,999)}",
            "manufacturer": random.choice(["Airbus", "Boeing", "Embraer"]),
            "capacity": random.randint(100, 400),
            "range_nm": random.randint(2000, 9000),
            "status": "ACTIVE",
        }),
        ("DELETE", "aircraft_retired", lambda r: r),
    ],
    "airport": [
        ("UPDATE", "terminal_expansion", lambda r: {**r, "terminal_count": r["terminal_count"] + 1}),
        ("UPDATE", "name_change", lambda r: {**r, "airport_name": r["airport_name"] + " (Renamed)"}),
        ("UPDATE", "status_change", lambda r: {**r, "status": random.choice(["OPERATIONAL", "PARTIAL_CLOSURE", "RENOVATION"])}),
    ],
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class CDCGeneratorConfig:
    num_events: int = 30
    output_mode: str = "jsonl"
    kafka_bootstrap_servers: str = "localhost:9092"
    output_dir: str = "data/cdc"
    base_time: Optional[datetime] = None
    time_acceleration: float = 60.0
    change_interval_seconds: float = 5.0


# ---------------------------------------------------------------------------
# Debezium envelope builder
# ---------------------------------------------------------------------------

def build_debezium_envelope(
    *,
    op: str,
    table: str,
    before: dict | None,
    after: dict | None,
    ts_ms: int,
    source_db: str = "airline_ops_db",
    source_schema: str = "public",
    transaction_id: str | None = None,
) -> dict:
    return {
        "schema": None,
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "2.4.0",
                "connector": "postgresql",
                "name": f"{source_db}.{source_schema}",
                "ts_ms": ts_ms,
                "snapshot": "false",
                "db": source_db,
                "schema": source_schema,
                "table": table,
                "txId": transaction_id or str(random.randint(10000, 99999)),
                "lsn": random.randint(100000000, 999999999),
            },
            "op": op,
            "ts_ms": ts_ms,
            "transaction": None,
        },
    }


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class CDCEventGenerator:

    def __init__(self, config: CDCGeneratorConfig | None = None):
        self._config = config or CDCGeneratorConfig()
        self._base_time = self._config.base_time or datetime.utcnow()
        self._aircraft_state = [dict(r) for r in AIRCRAFT_FLEET]
        self._airport_state = [dict(r) for r in AIRPORTS]
        self._producer = None
        self._events_generated = 0

    def run(self) -> list[dict]:
        all_events: list[dict] = []

        if self._config.output_mode == "kafka":
            self._init_kafka()

        for i in range(self._config.num_events):
            event_time = self._base_time + timedelta(
                seconds=i * self._config.change_interval_seconds
            )
            ts_ms = int(event_time.timestamp() * 1000)

            table_type = random.choices(
                ["aircraft", "airport"],
                weights=[0.6, 0.4],
            )[0]

            event = self._generate_change(table_type, ts_ms)
            if event:
                self._emit(event, table_type)
                all_events.append(event)
                self._events_generated += 1

            time.sleep(0.05 / max(self._config.time_acceleration, 1.0))

        if self._producer:
            self._producer.flush()

        return all_events

    def _generate_change(self, table_type: str, ts_ms: int) -> dict | None:
        scenarios = CHANGE_SCENARIOS[table_type]
        op_type, scenario_name, transform_fn = random.choice(scenarios)

        if table_type == "aircraft":
            state = self._aircraft_state
            table_name = "aircraft_reference"
            key_field = "aircraft_code"
        else:
            state = self._airport_state
            table_name = "airports"
            key_field = "airport_code"

        if op_type == "INSERT":
            new_record = transform_fn(None)
            if any(r[key_field] == new_record[key_field] for r in state):
                new_record[key_field] = f"{new_record[key_field]}_{random.randint(1,99)}"
            state.append(new_record)
            return build_debezium_envelope(
                op="c",
                table=table_name,
                before=None,
                after=new_record,
                ts_ms=ts_ms,
            )

        if not state:
            return None

        record = random.choice(state)

        if op_type == "DELETE":
            state.remove(record)
            return build_debezium_envelope(
                op="d",
                table=table_name,
                before=record,
                after=None,
                ts_ms=ts_ms,
            )

        before = dict(record)
        after = transform_fn(record)
        idx = state.index(record)
        state[idx] = after

        return build_debezium_envelope(
            op="u",
            table=table_name,
            before=before,
            after=after,
            ts_ms=ts_ms,
        )

    # -- output backends ----------------------------------------------------

    def _emit(self, event: dict, table_type: str) -> None:
        if self._config.output_mode == "kafka":
            self._emit_kafka(event, table_type)
        else:
            self._emit_jsonl(event, table_type)

    def _emit_jsonl(self, event: dict, table_type: str) -> None:
        out_dir = Path(self._config.output_dir) / table_type
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcfromtimestamp(
            event["payload"]["ts_ms"] / 1000
        ).strftime("%Y-%m-%d")
        filename = out_dir / f"cdc_{table_type}_{ts}.jsonl"
        with open(filename, "a") as f:
            f.write(json.dumps(event) + "\n")

    def _init_kafka(self) -> None:
        try:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": self._config.kafka_bootstrap_servers,
            })
        except ImportError:
            print("WARNING: confluent_kafka not installed. Falling back to JSONL.")
            self._config.output_mode = "jsonl"

    def _emit_kafka(self, event: dict, table_type: str) -> None:
        if self._producer is None:
            self._emit_jsonl(event, table_type)
            return
        topic = f"airline.cdc.{table_type}"
        key = json.dumps({"table": event["payload"]["source"]["table"]})
        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )

    def summary(self) -> dict:
        return {
            "total_cdc_events": self._events_generated,
            "aircraft_fleet_size": len(self._aircraft_state),
            "airport_count": len(self._airport_state),
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Airline CDC Event Generator")
    parser.add_argument("--events", type=int, default=30, help="Number of CDC events")
    parser.add_argument("--output", choices=["jsonl", "kafka"], default="jsonl")
    parser.add_argument("--output-dir", default="data/cdc")
    parser.add_argument("--speed", type=float, default=60.0)
    args = parser.parse_args()

    config = CDCGeneratorConfig(
        num_events=args.events,
        output_mode=args.output,
        output_dir=args.output_dir,
        time_acceleration=args.speed,
    )

    gen = CDCEventGenerator(config)
    print(f"Generating {config.num_events} CDC events...")
    events = gen.run()

    summary = gen.summary()
    print(f"\nCDC simulation complete:")
    print(f"  Total events: {summary['total_cdc_events']}")
    print(f"  Aircraft fleet size: {summary['aircraft_fleet_size']}")
    print(f"  Airport count: {summary['airport_count']}")
    if config.output_mode == "jsonl":
        print(f"  Output dir: {config.output_dir}")


if __name__ == "__main__":
    main()
