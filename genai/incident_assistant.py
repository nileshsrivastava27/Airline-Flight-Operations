"""
Incident Root-Cause Assistant for airline operations.

Pulls recent pipeline audit logs, delay metrics, and anomaly data
from the Gold layer, then uses an LLM to produce a root-cause
analysis and recommended next steps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = Any

CATALOG = "airline_ops"
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"
AUDIT_TABLE = f"{CATALOG}.audit.pipeline_run_log"
DELAY_TABLE = f"{CATALOG}.gold.delay_summary"
WEATHER_TABLE = f"{CATALOG}.gold.weather_impact"


@dataclass
class IncidentContext:
    recent_failures: List[Dict[str, Any]] = field(default_factory=list)
    delay_stats: Dict[str, Any] = field(default_factory=dict)
    weather_alerts: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class IncidentReport:
    summary: str
    root_causes: List[str]
    recommendations: List[str]
    context: IncidentContext


def gather_context(
    spark: SparkSession,
    lookback_hours: int = 24,
) -> IncidentContext:
    """Collect recent failures, delay stats, and weather data."""
    cutoff = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()
    ctx = IncidentContext()

    try:
        failures = (
            spark.table(AUDIT_TABLE)
            .filter(F.col("status") == "FAILED")
            .filter(F.col("start_time") >= cutoff)
            .orderBy(F.col("start_time").desc())
            .limit(10)
            .collect()
        )
        ctx.recent_failures = [row.asDict() for row in failures]
    except Exception:
        pass

    try:
        delays = (
            spark.table(DELAY_TABLE)
            .filter(F.col("report_date") >= cutoff[:10])
            .agg(
                F.avg("avg_delay_minutes").alias("avg_delay"),
                F.sum("total_delayed_flights").alias("total_delayed"),
                F.max("max_delay_minutes").alias("max_delay"),
            )
            .collect()
        )
        if delays:
            ctx.delay_stats = delays[0].asDict()
    except Exception:
        pass

    try:
        weather = (
            spark.table(WEATHER_TABLE)
            .filter(F.col("event_date") >= cutoff[:10])
            .filter(F.col("severity").isin("SEVERE", "EXTREME"))
            .limit(10)
            .collect()
        )
        ctx.weather_alerts = [row.asDict() for row in weather]
    except Exception:
        pass

    return ctx


def analyze(
    spark: SparkSession,
    incident_description: str,
    lookback_hours: int = 24,
) -> IncidentReport:
    """Gather context and generate a root-cause analysis."""
    import mlflow.deployments

    ctx = gather_context(spark, lookback_hours)

    context_text = (
        f"Pipeline failures ({len(ctx.recent_failures)}):\n"
        + "\n".join(str(f) for f in ctx.recent_failures[:5])
        + f"\n\nDelay stats: {ctx.delay_stats}"
        + f"\n\nWeather alerts ({len(ctx.weather_alerts)}):\n"
        + "\n".join(str(w) for w in ctx.weather_alerts[:5])
    )

    prompt = (
        "You are an airline operations incident analyst. "
        "Given the incident description and supporting data, produce:\n"
        "1. A one-paragraph summary\n"
        "2. A numbered list of probable root causes\n"
        "3. A numbered list of recommended actions\n\n"
        f"Incident: {incident_description}\n\n"
        f"Supporting data:\n{context_text}"
    )

    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}]},
    )
    raw = response["choices"][0]["message"]["content"]

    lines = raw.strip().split("\n")
    summary_lines, causes, recs = [], [], []
    section = "summary"
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if "root cause" in lower:
            section = "causes"
            continue
        if "recommend" in lower or "action" in lower:
            section = "recs"
            continue
        if section == "summary":
            summary_lines.append(stripped)
        elif section == "causes":
            causes.append(stripped.lstrip("0123456789.-) "))
        elif section == "recs":
            recs.append(stripped.lstrip("0123456789.-) "))

    return IncidentReport(
        summary=" ".join(summary_lines) or raw[:500],
        root_causes=causes or ["See full response for details."],
        recommendations=recs or ["Review raw LLM output."],
        context=ctx,
    )
