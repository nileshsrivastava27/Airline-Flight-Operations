"""
Airline Flight Event Simulator.

Generates realistic flight lifecycle events and writes them to Kafka or local
JSONL files. Each flight progresses through a state machine:

    FLIGHT_SCHEDULED -> BOARDING_STARTED -> BOARDING_COMPLETED -> DEPARTURE
    -> TAKEOFF -> LANDING -> ARRIVAL

With probabilistic branching into DELAY, CANCELLATION, or GATE_CHANGE events.
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AIRPORTS = ["JFK", "LAX", "ATL", "ORD", "DFW", "DEN", "SFO", "SEA"]
AIRLINES = ["VS", "DL", "AA", "UA", "WN", "B6", "AS", "NK"]
AIRCRAFT_TYPES = ["A35K", "B789", "A339", "B77W", "A320", "B738", "E190", "CRJ9"]
GATES = [f"{t}{n}" for t in ["A", "B", "C", "D"] for n in range(1, 16)]

NORMAL_LIFECYCLE = [
    "FLIGHT_SCHEDULED",
    "BOARDING_STARTED",
    "BOARDING_COMPLETED",
    "DEPARTURE",
    "TAKEOFF",
    "LANDING",
    "ARRIVAL",
]

DELAY_REASONS = [
    "WEATHER",
    "ATC_CONGESTION",
    "AIRCRAFT_MAINTENANCE",
    "CREW_AVAILABILITY",
    "LATE_ARRIVING_AIRCRAFT",
    "SECURITY",
    "AIRPORT_CONGESTION",
    "DE_ICING",
    "FUELING_DELAY",
    "PASSENGER_BOARDING",
]

CANCELLATION_REASONS = [
    "SEVERE_WEATHER",
    "MECHANICAL_ISSUE",
    "CREW_SHORTAGE",
    "ATC_RESTRICTION",
    "LOW_DEMAND",
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SimulatorConfig:
    num_flights: int = 20
    delay_probability: float = 0.30
    cancellation_probability: float = 0.05
    gate_change_probability: float = 0.10
    multiple_delay_probability: float = 0.15
    time_acceleration: float = 60.0
    output_mode: str = "jsonl"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "airline.flight_events"
    output_dir: str = "data/streaming/flight_events"
    base_time: Optional[datetime] = None
    run_duration_seconds: Optional[float] = None


@dataclass
class FlightState:
    flight_id: str
    flight_number: str
    origin: str
    destination: str
    airline: str
    aircraft_type: str
    tail_number: str
    gate: str
    scheduled_departure: datetime
    scheduled_arrival: datetime
    current_stage_index: int = 0
    delay_minutes: int = 0
    is_cancelled: bool = False
    is_completed: bool = False
    events_emitted: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Flight generator
# ---------------------------------------------------------------------------

class FlightEventGenerator:

    def __init__(self, config: SimulatorConfig | None = None):
        self._config = config or SimulatorConfig()
        self._flights: list[FlightState] = []
        self._base_time = self._config.base_time or datetime.utcnow()
        self._producer = None

    # -- public api ---------------------------------------------------------

    def generate_flights(self) -> list[FlightState]:
        self._flights = []
        for i in range(self._config.num_flights):
            origin, destination = random.sample(AIRPORTS, 2)
            airline = random.choice(AIRLINES)
            flight_num = f"{airline}{random.randint(100, 999)}"
            dep_offset = timedelta(minutes=random.randint(0, 720))
            scheduled_dep = self._base_time + dep_offset
            flight_hours = random.uniform(1.5, 12.0)
            scheduled_arr = scheduled_dep + timedelta(hours=flight_hours)

            self._flights.append(FlightState(
                flight_id=f"FLT-{uuid.uuid4().hex[:8].upper()}",
                flight_number=flight_num,
                origin=origin,
                destination=destination,
                airline=airline,
                aircraft_type=random.choice(AIRCRAFT_TYPES),
                tail_number=f"N{random.randint(100, 999)}{random.choice(AIRCRAFT_TYPES)[:2]}",
                gate=random.choice(GATES),
                scheduled_departure=scheduled_dep,
                scheduled_arrival=scheduled_arr,
            ))
        self._flights.sort(key=lambda f: f.scheduled_departure)
        return self._flights

    def run(self) -> list[dict]:
        if not self._flights:
            self.generate_flights()

        all_events: list[dict] = []
        pending = list(self._flights)
        sim_clock = self._base_time

        if self._config.output_mode == "kafka":
            self._init_kafka()

        start_wall = time.time()

        while pending:
            if (self._config.run_duration_seconds
                    and time.time() - start_wall > self._config.run_duration_seconds):
                break

            flight = pending[0]
            events = self._advance_flight(flight, sim_clock)
            for evt in events:
                self._emit(evt)
                all_events.append(evt)

            if flight.is_completed or flight.is_cancelled:
                pending.pop(0)
            else:
                pending.sort(key=lambda f: f.scheduled_departure
                             + timedelta(minutes=f.current_stage_index * 15))

            sim_clock += timedelta(seconds=60 / max(self._config.time_acceleration, 0.01))
            time.sleep(1.0 / max(self._config.time_acceleration, 1.0))

        if self._producer:
            self._producer.flush()

        return all_events

    # -- lifecycle engine ---------------------------------------------------

    def _advance_flight(self, flight: FlightState, sim_clock: datetime) -> list[dict]:
        events: list[dict] = []

        if flight.is_completed or flight.is_cancelled:
            return events

        stage = NORMAL_LIFECYCLE[flight.current_stage_index]

        if stage == "BOARDING_STARTED" and random.random() < self._config.gate_change_probability:
            old_gate = flight.gate
            flight.gate = random.choice([g for g in GATES if g != old_gate])
            events.append(self._build_event(
                flight, "GATE_CHANGE", sim_clock,
                delay_minutes=0,
                reason=None,
                metadata={"old_gate": old_gate, "new_gate": flight.gate},
            ))

        if stage in ("BOARDING_STARTED", "BOARDING_COMPLETED", "DEPARTURE"):
            if not flight.is_cancelled and random.random() < self._config.cancellation_probability:
                flight.is_cancelled = True
                events.append(self._build_event(
                    flight, "CANCELLATION", sim_clock,
                    delay_minutes=flight.delay_minutes,
                    reason=random.choice(CANCELLATION_REASONS),
                ))
                return events

            if random.random() < self._config.delay_probability:
                added_delay = random.choice([10, 15, 20, 30, 45, 60, 90, 120, 180])
                flight.delay_minutes += added_delay
                events.append(self._build_event(
                    flight, "DELAY", sim_clock,
                    delay_minutes=flight.delay_minutes,
                    reason=random.choice(DELAY_REASONS),
                ))
                if random.random() < self._config.multiple_delay_probability:
                    return events

        event_time = sim_clock + timedelta(minutes=flight.delay_minutes)
        events.append(self._build_event(
            flight, stage, event_time,
            delay_minutes=flight.delay_minutes,
        ))
        flight.current_stage_index += 1

        if flight.current_stage_index >= len(NORMAL_LIFECYCLE):
            flight.is_completed = True

        return events

    def _build_event(
        self,
        flight: FlightState,
        event_type: str,
        event_time: datetime,
        *,
        delay_minutes: int = 0,
        reason: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        evt = {
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "flight_id": flight.flight_id,
            "flight_number": flight.flight_number,
            "airline": flight.airline,
            "event_type": event_type,
            "airport_code": (
                flight.origin
                if flight.current_stage_index <= 3
                else flight.destination
            ),
            "gate": flight.gate,
            "origin": flight.origin,
            "destination": flight.destination,
            "aircraft_type": flight.aircraft_type,
            "tail_number": flight.tail_number,
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "scheduled_departure": flight.scheduled_departure.strftime("%Y-%m-%dT%H:%M:%S"),
            "scheduled_arrival": flight.scheduled_arrival.strftime("%Y-%m-%dT%H:%M:%S"),
            "status": event_type,
            "delay_minutes": delay_minutes,
            "reason": reason,
            "metadata": metadata or {},
        }
        flight.events_emitted.append(evt)
        return evt

    # -- output backends ----------------------------------------------------

    def _emit(self, event: dict) -> None:
        if self._config.output_mode == "kafka":
            self._emit_kafka(event)
        else:
            self._emit_jsonl(event)

    def _emit_jsonl(self, event: dict) -> None:
        out_dir = Path(self._config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        date_str = event["event_time"][:10]
        filename = out_dir / f"flight_events_{date_str}.jsonl"
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
            key=event["flight_id"],
            value=json.dumps(event).encode("utf-8"),
        )

    # -- summary ------------------------------------------------------------

    def summary(self) -> dict:
        total = len(self._flights)
        completed = sum(1 for f in self._flights if f.is_completed)
        cancelled = sum(1 for f in self._flights if f.is_cancelled)
        delayed = sum(1 for f in self._flights if f.delay_minutes > 0 and not f.is_cancelled)
        total_events = sum(len(f.events_emitted) for f in self._flights)
        return {
            "total_flights": total,
            "completed": completed,
            "cancelled": cancelled,
            "delayed": delayed,
            "on_time": total - cancelled - delayed,
            "total_events_generated": total_events,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Airline Flight Event Simulator")
    parser.add_argument("--flights", type=int, default=20, help="Number of flights to simulate")
    parser.add_argument("--delay-prob", type=float, default=0.30, help="Probability of delay per stage")
    parser.add_argument("--cancel-prob", type=float, default=0.05, help="Probability of cancellation")
    parser.add_argument("--output", choices=["jsonl", "kafka"], default="jsonl", help="Output mode")
    parser.add_argument("--output-dir", default="data/streaming/flight_events", help="JSONL output directory")
    parser.add_argument("--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="airline.flight_events", help="Kafka topic")
    parser.add_argument("--speed", type=float, default=60.0, help="Time acceleration factor")
    parser.add_argument("--duration", type=float, default=None, help="Max run duration in seconds")
    args = parser.parse_args()

    config = SimulatorConfig(
        num_flights=args.flights,
        delay_probability=args.delay_prob,
        cancellation_probability=args.cancel_prob,
        output_mode=args.output,
        output_dir=args.output_dir,
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        time_acceleration=args.speed,
        run_duration_seconds=args.duration,
    )

    gen = FlightEventGenerator(config)
    print(f"Generating {config.num_flights} flights...")
    gen.generate_flights()

    print("Running simulation...")
    events = gen.run()

    summary = gen.summary()
    print(f"\nSimulation complete:")
    print(f"  Flights: {summary['total_flights']}")
    print(f"  Completed: {summary['completed']}")
    print(f"  Cancelled: {summary['cancelled']}")
    print(f"  Delayed: {summary['delayed']}")
    print(f"  On-time: {summary['on_time']}")
    print(f"  Total events: {summary['total_events_generated']}")

    if config.output_mode == "jsonl":
        print(f"  Output dir: {config.output_dir}")


if __name__ == "__main__":
    main()
