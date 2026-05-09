# Project Checklist

## Completed

- [x] Finalize project idea and scope
- [x] Create high-level design
- [x] Create low-level design
- [x] Create Bronze, Silver, and Gold DDL
- [x] Create raw source datasets
- [x] Create both CSV and JSONL raw formats
- [x] Build reusable raw data generator
- [x] Push current project foundation to private GitHub repo
- [x] Build Bronze ingestion helper for CSV and JSONL
- [x] Add notebook-ready Bronze ingestion example

## Pending

- [ ] Run Bronze ingestion in Databricks and validate row counts
- [ ] Build Silver transformation layer
- [ ] Build quarantine and data quality logic
- [ ] Build Gold aggregation layer
- [ ] Create sample validation and reconciliation queries
- [ ] Create Databricks workflow design
- [ ] Create Power BI dashboard
- [ ] Capture project screenshots and proof for resume/GitHub
- [ ] Update GitHub repo documentation with execution steps and outputs

## Reliability and Change Handling

- [ ] Add rollback / recovery mechanism for failed jobs
- [ ] Add schema evolution handling for incoming column additions
- [ ] Add audit logging for each pipeline run
- [ ] Add idempotent re-run behavior for Bronze ingestion
- [ ] Add row-count and data-quality gate checks between layers
- [ ] Add failure alerting / error reporting strategy

## Optional

- [ ] Deploy on Databricks
- [ ] Publish or demo Power BI report
