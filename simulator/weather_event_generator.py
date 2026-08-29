"""
Airline Weather Event Simulator.

Generates METAR-style weather observations for each airport at configurable
intervals. Writes to Kafka or local JSONL files for consumption by the
streaming ingestion pipeline.
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

AIRPORTS = ["JFK", "LAX", "ATL", "ORD", "DFW", "DEN", "SFO", "SEA"]

SKY_COVER_OPTIONS = ["CLR", "FEW", "SCT", "BKN", "OVC"]

FLIGHT_CATEGORIES = ["VFR", "MVFR", "IFR", "LIFR"]
FLIGHT_CATEGORY_WEIGHTS = [0.55, 0.25, 0.15, 0.05]

WEATHER_PROFILES = {
    "CLEAR": {
        "temp_range": (15, 35),
        "dewpoint_offset": (-8, -3),
        "wind_speed_range": (2, 12),
        "gust_prob": 0.05,
        "gust_add": (5, 15),
        "visibility_range": (8.0, 10.0),
        "sky_covers": ["CLR", "FEW"],
        "precip_prob": 0.0,
        "flight_category": "VFR",
    },
    "MARGINAL": {
        "temp_range": (8, 25),
        "dewpoint_offset": (-5, -1),
        "wind_speed_range": (8, 22),
        "gust_prob": 0.30,
        "gust_add": (8, 25),
        "visibility_range": (3.0, 5.0),
        "sky_covers": ["SCT", "BKN"],
        "precip_prob": 0.40,
        "flight_category": "MVFR",
    },
    "INSTRUMENT": {
        "temp_range": (2, 18),
        "dewpoint_offset": (-3, 0),
        "wind_speed_range": (12, 30),
        "gust_prob": 0.55,
        "gust_add": (10, 30),
        "visibility_range": (1.0, 3.0),
        "sky_covers": ["BKN", "OVC"],
        "precip_prob": 0.70,
        "flight_category": "IFR",
    },
    "LOW_INSTRUMENT": {
        "temp_range": (-5, 10),
        "dewpoint_offset": (-2, 0),
        "wind_speed_range": (18, 45),
        "gust_prob": 0.80,
        "gust_add": (15, 40),
        "visibility_range": (0.25, 1.0),
        "sky_covers": ["OVC"],
        "precip_prob": 0.90,
        "flight_category": "LIFR",
    },
}


@dataclass
class WeatherSimulatorConfig:
    observation_interval_minutes: int = 30
    num_observations: int = 48
    output_mode: str = "jsonl"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "airline.weather_updates"
    output_dir: str = "data/streaming/weather_updates"
    base_time: Optional[datetime] = None
    time_acceleration: float = 60.0
    storm_probability: float = 0.10


class WeatherEventGenerator:

    def __init__(self, config: WeatherSimulatorConfig | None = None):
        self._config = config or WeatherSimulatorConfig()
        self._base_time = self._config.base_time or datetime.utcnow()
        self._airport_states: dict[str, str] = {}
        self._producer = None
        self._events_generated = 0

    def run(self) -> list[dict]:
        all_events: list[dict] = []

        for airport in AIRPORTS:
            self._airport_states[airport] = random.choices(
                list(WEATHER_PROFILES.keys()),
                weights=[0.55, 0.25, 0.15, 0.05],
            )[0]

        if self._config.output_mode == "kafka":
            self._init_kafka()

        for tick in range(self._config.num_observations):
            obs_time = self._base_time + timedelta(
                minutes=tick * self._config.observation_interval_minutes
            )

            for airport in AIRPORTS:
                self._maybe_transition_weather(airport)
                event = self._generate_observation(airport, obs_time)
                self._emit(event)
                all_events.append(event)
                self._events_generated += 1

            time.sleep(0.05 / max(self._config.time_acceleration, 1.0))

        if self._producer:
            self._producer.flush()

        return all_events

    def _maybe_transition_weather(self, airport: str) -> None:
        current = self._airport_states[airport]
        profiles = list(WEATHER_PROFILES.keys())
        idx = profiles.index(current)

        if random.random() < self._config.storm_probability:
            self._airport_states[airport] = random.choice(["INSTRUMENT", "LOW_INSTRUMENT"])
            return

        if random.random() < 0.20:
            new_idx = max(0, min(len(profiles) - 1, idx + random.choice([-1, 1])))
            self._airport_states[airport] = profiles[new_idx]

    def _generate_observation(self, airport: str, obs_time: datetime) -> dict:
        profile_name = self._airport_states[airport]
        p = WEATHER_PROFILES[profile_name]

        temp_c = round(random.uniform(*p["temp_range"]), 1)
        dewpoint_c = round(temp_c + random.uniform(*p["dewpoint_offset"]), 1)
        wind_speed_kt = random.randint(*p["wind_speed_range"])
        wind_dir = random.randint(0, 360)
        wind_gust_kt = (
            wind_speed_kt + random.randint(*p["gust_add"])
            if random.random() < p["gust_prob"]
            else None
        )
        visibility = round(random.uniform(*p["visibility_range"]), 2)
        altim_in_hg = round(random.uniform(29.70, 30.20), 2)
        sky_cover = random.choice(p["sky_covers"])
        precip_in = round(random.uniform(0.01, 0.50), 2) if random.random() < p["precip_prob"] else 0.0

        return {
            "observation_id": f"wx_{uuid.uuid4().hex[:12]}",
            "station_id": airport,
            "observation_time": obs_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "observation_date": obs_time.strftime("%Y-%m-%d"),
            "temp_c": temp_c,
            "dewpoint_c": dewpoint_c,
            "wind_dir_degrees": wind_dir,
            "wind_speed_kt": wind_speed_kt,
            "wind_gust_kt": wind_gust_kt,
            "visibility_statute_mi": visibility,
            "altim_in_hg": altim_in_hg,
            "flight_category": p["flight_category"],
            "sky_cover": sky_cover,
            "precip_in": precip_in,
            "weather_profile": profile_name,
        }

    def _emit(self, event: dict) -> None:
        if self._config.output_mode == "kafka":
            self._emit_kafka(event)
        else:
            self._emit_jsonl(event)

    def _emit_jsonl(self, event: dict) -> None:
        out_dir = Path(self._config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        date_str = event["observation_date"]
        filename = out_dir / f"weather_updates_{date_str}.jsonl"
        with open(filename, "a") as f:
            f.write(json.dumps(event) + "\n")

    def _init_kafka(self) -> None:
        try:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": self._config.kafka_bootstrap_servers,
            })
        except ImportError:
            print("WARNING: confluent_kafka not installed. Falling back to JSONL output.")
            self._config.output_mode = "jsonl"

    def _emit_kafka(self, event: dict) -> None:
        if self._producer is None:
            self._emit_jsonl(event)
            return
        self._producer.produce(
            self._config.kafka_topic,
            key=event["station_id"],
            value=json.dumps(event).encode("utf-8"),
        )

    def summary(self) -> dict:
        return {
            "airports": len(AIRPORTS),
            "observations_per_airport": self._config.num_observations,
            "total_events": self._events_generated,
            "current_conditions": dict(self._airport_states),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Airline Weather Event Simulator")
    parser.add_argument("--observations", type=int, default=48, help="Number of observation rounds")
    parser.add_argument("--interval", type=int, default=30, help="Minutes between observations")
    parser.add_argument("--storm-prob", type=float, default=0.10, help="Storm transition probability")
    parser.add_argument("--output", choices=["jsonl", "kafka"], default="jsonl", help="Output mode")
    parser.add_argument("--output-dir", default="data/streaming/weather_updates", help="JSONL output directory")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="airline.weather_updates", help="Kafka topic")
    parser.add_argument("--speed", type=float, default=60.0, help="Time acceleration factor")
    args = parser.parse_args()

    config = WeatherSimulatorConfig(
        num_observations=args.observations,
        observation_interval_minutes=args.interval,
        storm_probability=args.storm_prob,
        output_mode=args.output,
        output_dir=args.output_dir,
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        time_acceleration=args.speed,
    )

    gen = WeatherEventGenerator(config)
    print(f"Generating weather observations for {len(AIRPORTS)} airports...")
    print(f"  {config.num_observations} rounds x {config.observation_interval_minutes}min interval")

    events = gen.run()
    summary = gen.summary()
    print(f"\nSimulation complete:")
    print(f"  Total observations: {summary['total_events']}")
    print(f"  Current conditions:")
    for airport, condition in summary["current_conditions"].items():
        print(f"    {airport}: {condition}")

    if config.output_mode == "jsonl":
        print(f"  Output dir: {config.output_dir}")


if __name__ == "__main__":
    main()
