"""
Airline document loader and chunker for the RAG pipeline.

Reads markdown, PDF, and plain-text airline operations documents,
splits them into overlapping chunks, and writes them to a Delta table
so the vector-search index can pick them up.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType
except ImportError:
    SparkSession = Any

CATALOG = "airline_ops"
SCHEMA = "genai"
DOCS_TABLE = f"{CATALOG}.{SCHEMA}.airline_documents"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.airline_doc_chunks"


@dataclass
class DocChunk:
    doc_id: str
    chunk_index: int
    text: str
    source: str
    metadata: dict = field(default_factory=dict)


def _split_text(text: str, chunk_size: int = 512, overlap: int = 64) -> List[str]:
    """Split text into overlapping windows of roughly *chunk_size* words."""
    words = text.split()
    chunks: List[str] = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunks.append(" ".join(words[start:end]))
        start = end - overlap
    return chunks


def load_and_chunk_docs(
    spark: SparkSession,
    docs_path: str,
    file_format: str = "text",
) -> None:
    """Read raw docs from *docs_path*, chunk them, and write to Delta.

    Parameters
    ----------
    spark : SparkSession
    docs_path : str
        Cloud path (dbfs/Volumes) containing raw documents.
    file_format : str
        ``"text"`` reads line-delimited text files; ``"binaryFile"``
        reads PDFs / binary blobs whose text is extracted with a simple
        UTF-8 decode (extend with a real PDF parser for production).
    """
    if file_format == "binaryFile":
        raw = (
            spark.read.format("binaryFile")
            .load(docs_path)
            .withColumn("text", F.col("content").cast("string"))
            .withColumn("source", F.col("path"))
        )
    else:
        raw = (
            spark.read.text(docs_path, wholetext=True)
            .withColumnRenamed("value", "text")
            .withColumn("source", F.input_file_name())
        )

    raw = raw.withColumn("doc_id", F.md5(F.col("source")))

    chunk_schema = StructType([
        StructField("doc_id", StringType()),
        StructField("chunk_index", StringType()),
        StructField("text", StringType()),
        StructField("source", StringType()),
    ])

    @F.udf(ArrayType(chunk_schema))
    def _chunk_udf(doc_id, text, source):
        parts = _split_text(text or "")
        return [
            {"doc_id": doc_id, "chunk_index": str(i), "text": c, "source": source}
            for i, c in enumerate(parts)
        ]

    chunks_df = (
        raw.withColumn("chunks", _chunk_udf("doc_id", "text", "source"))
        .select(F.explode("chunks").alias("chunk"))
        .select("chunk.*")
    )

    chunks_df.write.format("delta").mode("overwrite").saveAsTable(CHUNKS_TABLE)
    print(f"Wrote {chunks_df.count()} chunks to {CHUNKS_TABLE}")


def ensure_schema(spark: SparkSession) -> None:
    """Create the genai schema if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
