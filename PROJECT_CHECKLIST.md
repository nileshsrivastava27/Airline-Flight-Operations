# Project Checklist

## Completed

- [x] Finalize project idea and scope
- [x] Create high-level design
- [x] Create low-level design
- [x] Create Bronze, Silver, and Gold DDL
- [x] Create raw source datasets (CSV + JSONL)
- [x] Build reusable raw data generator
- [x] Push project foundation to private GitHub repo
- [x] Build Bronze ingestion helper
- [x] Add notebook-ready Bronze, Silver, and Gold examples
- [x] Create Silver and Gold transformation logic
- [x] Create sample validation and reconciliation queries
- [x] Add rollback / recovery mechanism for failed jobs
- [x] Add schema evolution handling
- [x] Add audit logging for each pipeline run
- [x] Add idempotent re-run behavior for Bronze ingestion
- [x] Add failure alerting / error reporting strategy
- [x] Build CDC ingestion module
- [x] Build SCD (Type 1 / Type 2) processor
- [x] Build data contract validator
- [x] Build Spark Structured Streaming (ingestion, Silver, Gold)
- [x] Build event simulators (flights, weather, CDC)
- [x] Build GenAI RAG pipeline over airline docs
- [x] Build Text-to-SQL with safety validation
- [x] Build Airline Copilot (unified assistant)
- [x] Build Incident Root-Cause Assistant
- [x] Reorganize repo into pipeline/, streaming/, genai/ structure
- [x] Rewrite README with full project scope

## Pending

- [ ] Run Bronze ingestion in Databricks and validate row counts
- [ ] Run and validate Silver and Gold layers in Databricks
- [ ] Build quarantine and data quality logic
- [ ] Add row-count and data-quality gate checks between layers
- [ ] Create Databricks workflow design
- [ ] Create Power BI dashboard
- [ ] Capture project screenshots and proof for resume/GitHub
- [ ] Deploy on Databricks
