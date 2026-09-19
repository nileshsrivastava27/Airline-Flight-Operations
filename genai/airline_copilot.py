"""
Airline Copilot — a unified assistant that routes user questions
to the right GenAI tool (RAG docs, Text-to-SQL, or direct LLM).

Databricks notebook usage:
    %run ./rag_pipeline
    %run ./text_to_sql
    %run ./airline_copilot

Then call:
    response = copilot_ask(spark, "Top 5 delayed routes last month?")
    print(response.answer)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = Any

LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


class Intent(Enum):
    DATA_QUERY = auto()
    DOCUMENT_SEARCH = auto()
    GENERAL = auto()


@dataclass
class CopilotResponse:
    intent: Intent
    answer: str
    sql: Optional[str] = None
    sources: Optional[list] = None


def classify_intent(question: str) -> Intent:
    """Simple keyword-based intent classifier."""
    q = question.lower()

    data_signals = [
        "how many", "count", "average", "total", "top", "bottom",
        "flights", "delays", "airports", "airlines", "routes",
        "revenue", "passengers", "cancellations", "on-time",
        "query", "table", "rows", "sum", "max", "min", "group by",
    ]
    doc_signals = [
        "policy", "procedure", "manual", "guideline", "sop",
        "regulation", "document", "faa", "safety", "protocol",
        "maintenance", "checklist", "handbook",
    ]

    data_score = sum(1 for s in data_signals if s in q)
    doc_score = sum(1 for s in doc_signals if s in q)

    if data_score > doc_score and data_score > 0:
        return Intent.DATA_QUERY
    if doc_score > 0:
        return Intent.DOCUMENT_SEARCH
    return Intent.GENERAL


def _general_answer(question: str) -> str:
    """Fallback: ask the LLM directly with an airline-ops system prompt."""
    import mlflow.deployments

    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an airline operations assistant. "
                        "Answer concisely using industry knowledge."
                    ),
                },
                {"role": "user", "content": question},
            ]
        },
    )
    return response["choices"][0]["message"]["content"]


def copilot_ask(spark: SparkSession, question: str) -> CopilotResponse:
    """Route the question to the best GenAI tool and return a response.

    Requires that rag_pipeline and text_to_sql modules are already loaded
    in the notebook scope (via %run).  The functions ``rag_ask`` and
    ``text_to_sql_ask`` must be available as globals.
    """
    intent = classify_intent(question)

    if intent == Intent.DATA_QUERY:
        # text_to_sql_ask is loaded via: %run ./text_to_sql
        result = text_to_sql_ask(spark, question)  # noqa: F821
        summary = f"**SQL:**\n```sql\n{result.sql}\n```\n\n"
        if result.rows:
            header = " | ".join(result.columns)
            body = "\n".join(" | ".join(str(v) for v in row) for row in result.rows[:20])
            summary += f"{header}\n{body}\n\n({result.row_count} rows)"
        else:
            summary += "No rows returned."
        return CopilotResponse(intent=intent, answer=summary, sql=result.sql)

    if intent == Intent.DOCUMENT_SEARCH:
        # rag_ask is loaded via: %run ./rag_pipeline
        rag = rag_ask(question)  # noqa: F821
        sources = [{"source": s.source, "score": s.score} for s in rag.sources]
        return CopilotResponse(intent=intent, answer=rag.answer, sources=sources)

    answer = _general_answer(question)
    return CopilotResponse(intent=intent, answer=answer)
