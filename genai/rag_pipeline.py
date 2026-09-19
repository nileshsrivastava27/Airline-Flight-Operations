"""
RAG pipeline over airline operations documents.

Uses Databricks Vector Search to retrieve relevant chunks and a
Foundation Model Serving endpoint to generate grounded answers.

Requires: Vector Search + Foundation Model Serving.
Notebook guard: check genai_enabled widget before running.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = Any

CATALOG = "airline_ops"
SCHEMA = "genai"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.airline_doc_chunks"
VS_INDEX = f"{CATALOG}.{SCHEMA}.doc_chunks_vs_index"
VS_ENDPOINT = "airline_ops_vs_endpoint"
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


@dataclass
class RetrievedChunk:
    text: str
    source: str
    score: float


@dataclass
class RAGResponse:
    answer: str
    sources: List[RetrievedChunk]


def create_vector_search_index(spark: SparkSession) -> None:
    """Create or sync the Vector Search index on the chunks table.

    Requires a running Vector Search endpoint named *VS_ENDPOINT*.
    """
    from databricks.vector_search.client import VectorSearchClient

    client = VectorSearchClient()

    try:
        client.get_index(VS_ENDPOINT, VS_INDEX)
        client.get_index(VS_ENDPOINT, VS_INDEX).sync()
        print(f"Synced existing index {VS_INDEX}")
    except Exception:
        client.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT,
            index_name=VS_INDEX,
            source_table_name=CHUNKS_TABLE,
            pipeline_type="TRIGGERED",
            primary_key="doc_id",
            embedding_source_column="text",
            embedding_model_endpoint_name="databricks-bge-large-en",
        )
        print(f"Created index {VS_INDEX}")


def retrieve(question: str, top_k: int = 5) -> List[RetrievedChunk]:
    """Return the top-k most relevant chunks for *question*."""
    from databricks.vector_search.client import VectorSearchClient

    client = VectorSearchClient()
    index = client.get_index(VS_ENDPOINT, VS_INDEX)

    results = index.similarity_search(
        columns=["text", "source"],
        query_text=question,
        num_results=top_k,
    )

    chunks: List[RetrievedChunk] = []
    for row in results.get("result", {}).get("data_array", []):
        chunks.append(RetrievedChunk(text=row[0], source=row[1], score=row[2]))
    return chunks


def generate(question: str, context_chunks: List[RetrievedChunk]) -> str:
    """Call the LLM endpoint with the retrieved context."""
    import mlflow.deployments

    client = mlflow.deployments.get_deploy_client("databricks")

    context = "\n\n---\n\n".join(c.text for c in context_chunks)
    prompt = (
        "You are an airline operations assistant. Use ONLY the context below "
        "to answer. If the context does not contain enough information, say so.\n\n"
        f"Context:\n{context}\n\nQuestion: {question}"
    )

    response = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}]},
    )
    return response["choices"][0]["message"]["content"]


def rag_ask(question: str, top_k: int = 5) -> RAGResponse:
    """End-to-end RAG: retrieve chunks then generate an answer."""
    chunks = retrieve(question, top_k=top_k)
    answer = generate(question, chunks)
    return RAGResponse(answer=answer, sources=chunks)
