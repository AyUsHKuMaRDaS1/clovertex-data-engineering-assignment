# Clovertex Data Engineering Assignment

## Overview

This repository contains a local data engineering pipeline for clinical and genomics data. It ingests raw files, cleans and unifies patient records, joins clinical events, filters genomics variants, organizes outputs into a data lake, performs analytics, and generates visualizations.

The pipeline runs inside Docker and is designed for CI/CD validation through Docker build and end-to-end execution checks.

---

## 1. Setup Instructions

### Run locally with Docker

1. Build and run the container:
   ```bash
   docker compose up --build
   ```

2. The pipeline will:
   - Run Tasks 1–4 end-to-end
   - Generate outputs in `datalake/`
   - Exit only if all steps complete successfully

3. Output persistence:
   - Host path: `./datalake`
   - Container path: `/app/datalake`

### Run without Docker

1. Create a Python environment with Python 3.11.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the pipeline:
   ```bash
   python pipeline/main.py
   ```

### Container details

- `Dockerfile` uses `python:3.11-slim`
- `docker-compose.yml` mounts `./datalake` into the container at `/app/datalake`
- Default command: `python pipeline/main.py`

---

## 2. Pipeline Architecture

Pipeline is orchestrated by `pipeline/main.py`.

### Flow

Raw Data → Ingestion → Cleaning → Transformation → Analytics → Visualization → Data Lake

### Design Philosophy

- Modular design: each stage is isolated for clarity and easier debugging
- Testable stages: independent modules support future unit and integration tests
- Scalable pattern: resembles production data pipelines with ingestion, cleaning, refinement, and consumption layers

### Stages

#### Ingestion

- `pipeline/ingestion/load_data.py`
- Reads files from `data/` with support for CSV, JSON, and Parquet
- Copies original raw inputs into `datalake/raw/`

#### Cleaning

- `pipeline/cleaning/clean_data.py`
- Standardizes columns, removes duplicates, handles nulls, and flattens nested JSON

#### Patient Unification

- `pipeline/cleaning/unify.py`
- Combines patient data across sites and standardizes site-specific schema differences
- Renames fields like `sex` → `gender`, `date_of_birth` / `birthdate` → `birth_date`, and nested name fields to `first_name` / `last_name`

#### Transformation / Join

- `pipeline/transformation/join.py`
- Left joins lab results, genomics variants, diagnoses, and medications onto the patient master table
- Preserves patient records even when event tables are missing related rows

#### Genomics Filtering

- `pipeline/transformation/genomics.py`
- Filters unreliable variants and retains clinically relevant genomics data

#### Saving

- `pipeline/utils/save.py`
- Writes Parquet outputs to `datalake/refined/`
- Partitions lab results by `test_name`

#### Analytics

- `pipeline/stats/analytics.py`
- Generates analytics outputs for Task 3 and anomaly detection

#### Visualization

- `plots/visualization.py`
- Generates PNG plots for Task 4

#### Manifest

- `pipeline/utils/manifest.py`
- Generates `manifest.json` files for `raw`, `refined`, and `consumption` zones

---

## 3. Data Cleaning Decisions

### Issues found

- Inconsistent column naming across files (e.g. `patient_ref`, `patientid`, `date_of_birth`)
- Nested JSON structures in some datasets
- Missing values in multiple fields
- Duplicate records from ingestion
- Mixed or invalid data types such as numeric fields stored as strings
- Inconsistent patient schemas across sites

### Solutions applied

- Standardized column names to lowercase with underscores
- Flattened nested JSON/dictionary columns using Pandas
- Filled missing values with `unknown` to retain records and preserve joins
- Removed duplicate rows to prevent inflated analytics
- Safely converted numeric columns when saving refined data
- Unified patient IDs and standard patient schema across site datasets

### Reasoning

- Preserving maximum data avoids losing records because of schema noise
- `unknown` helps maintain row shape while clearly marking missing values
- Schema standardization supports reliable downstream joins and analytics
- Deduplication improves data quality and trustworthiness
- Flattening nested objects makes the data compatible with Parquet and tabular processing

---

## 4. Data Lake Design

### Zone structure

- `datalake/raw/`
  - Stores exact copies of original source files from `data/`
  - Preserves raw inputs for reproducibility and auditability

- `datalake/refined/`
  - Stores cleaned and transformed Parquet outputs
  - Includes `patients.parquet`, `final_dataset.parquet`, `genomics_filtered.parquet`, and partitioned `lab_results/`

- `datalake/consumption/`
  - Stores analytics and operational outputs such as `anomaly_flags.parquet` and `high_risk_patients.parquet`

### Partitioning strategy

- Lab results are partitioned by `test_name`
- Rationale:
  - queries often filter by lab test type
  - reduces scan scope for targeted analysis
  - improves performance for per-test reporting

### Manifest structure

Each `manifest.json` entry includes:

- `file_name`
- `file_path`
- `row_count`
- `schema` (column → datatype)
- `processing_timestamp`
- `sha256_checksum`

This manifest structure supports simple data governance, file discovery, and checksum-based validation.

---

## 5. Anomaly Detection Logic

Anomalies are generated by `pipeline/stats/analytics.py` and written to `datalake/consumption/anomaly_flags.parquet`.

### Types of anomalies

- `invalid_age`
  - Age is missing, negative, or greater than 120
  - Not clinically plausible

- `critical_lab_value`
  - Lab value outside `critical_low` / `critical_high` from `data/reference/lab_test_ranges.json`
  - Indicates potential measurement error or urgent clinical condition

- `invalid_medication_dates`
  - `end_date` occurs before `start_date`
  - Logical inconsistency in medication timeline

- `genomics_inconsistency`
  - `allele_frequency` outside [0, 1] or `read_depth` ≤ 0
  - Violates basic genomics validity expectations

---

## 6. Genomics Filtering Criteria

Variants are retained only if:

- `read_depth > 10`
- `allele_frequency > 0.2`
- `clinical_significance` is `Pathogenic` or `Likely Pathogenic`

### Reasoning

- Higher read depth increases confidence in variant calls
- A minimum allele frequency removes low-confidence signal and sequencing noise
- Clinical significance focuses on variants that are most relevant for risk and clinical interpretation

---

## 7. Assumptions

- `patient_id` or `patient_ref` can be used as the unified patient identifier
- Missing categorical values may be represented as `unknown`
- Left joins preserve patient records even when event records are missing
- The pipeline uses 2026 as the reference year for age calculation
- Lab reference ranges in `data/reference/lab_test_ranges.json` are accurate and complete
- Data volumes are small enough for in-memory Pandas processing

---

## 8. Improvements (Production)

### With more time

- Add strict schema validation for each dataset
- Add richer logging and data quality reporting
- Add unit, integration, and regression tests for cleaning, joins, and analytics
- Enhance anomaly detection with statistical outlier and pattern detection
- Document data models and lineage explicitly

### For production deployment

- Use a workflow orchestrator such as Airflow, Prefect, or Dagster
- Use cloud storage for the data lake (S3, ADLS, or GCS)
- Add incremental ingestion and partition-aware processing
- Use a data validation framework like Great Expectations or Deequ
- Add monitoring, alerting, and automated failure recovery
- Implement access control and metadata cataloging

---

## CI/CD

- The pipeline is containerized for Docker-based CI validation
- CI should verify Docker build success and end-to-end pipeline execution
- Output persistence via `./datalake` makes results available after pipeline completion

---

## Project Files to Know

- `pipeline/main.py`
- `pipeline/ingestion/load_data.py`
- `pipeline/cleaning/clean_data.py`
- `pipeline/cleaning/unify.py`
- `pipeline/transformation/join.py`
- `pipeline/transformation/genomics.py`
- `pipeline/utils/save.py`
- `pipeline/utils/manifest.py`
- `pipeline/utils/datalake.py`
- `pipeline/stats/analytics.py`
- `plots/visualization.py`

---

## Author

Ayush Kumar Das

