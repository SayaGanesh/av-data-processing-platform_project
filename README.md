# Autonomous Vehicle Data Processing Platform

> **Azure-native cloud data platform** for ingesting, processing, and analyzing autonomous vehicle sensor and annotation data at scale — supporting AI model training, safety analytics, and operational monitoring.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
│  Sensor Logs │ Image Metadata │ Annotation JSONs │ Real-time Events     │
└──────┬───────────────┬───────────────┬──────────────────┬───────────────┘
       │               │               │                  │
       ▼               ▼               ▼                  ▼
┌─────────────────────────────┐   ┌──────────────────────────────────────┐
│    Azure Data Factory       │   │         Azure Event Hub              │
│  (Batch Ingestion Pipelines)│   │    (Real-time Annotation Stream)     │
└──────────────┬──────────────┘   └──────────────┬───────────────────────┘
               │                                  │
               ▼                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  BRONZE ZONE                           │
│             Raw sensor logs, metadata, annotation outputs              │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│               Azure Databricks  (PySpark Transformations)              │
│   Cleansing │ Normalization │ Feature Extraction │ Quality Validation  │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
               ┌───────────────────┴────────────────────┐
               ▼                                        ▼
┌──────────────────────────┐           ┌─────────────────────────────────┐
│  ADLS Gen2 - SILVER ZONE │           │   ADLS Gen2 - QUARANTINE ZONE   │
│   Cleaned, validated     │           │   Failed quality checks         │
│   annotation data        │           │   + error codes                 │
└──────────────┬───────────┘           └─────────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  —  GOLD ZONE                             │
│         Aggregated metrics, vehicle KPIs, annotation summaries         │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
        ┌───────────────────────┐     ┌──────────────────────┐
        │  Azure Synapse        │     │      Power BI        │
        │  Analytics Layer      │────▶│   Dashboards &       │
        │  (Reporting Tables)   │     │   KPI Reports        │
        └───────────────────────┘     └──────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   Azure Monitor +     │
        │   Log Analytics       │
        │   (Alerts & Logging)  │
        └───────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks (PySpark) |
| Storage | ADLS Gen2, Azure Blob Storage |
| Table Format | Delta Lake (Bronze / Silver / Gold) |
| Streaming | Azure Event Hub + Structured Streaming |
| Analytics | Azure Synapse Analytics |
| Monitoring | Azure Monitor, Log Analytics |
| Reporting | Power BI |
| Security | Azure Key Vault |

---

## Project Structure

```
av-data-processing-platform/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py          # Raw data landing from ADF
│   ├── 02_silver_transformation.py     # Cleansing, normalization, feature extraction
│   ├── 03_quality_validation.py        # Annotation quality checks
│   ├── 04_gold_aggregation.py          # KPI and metric aggregations
│   └── 05_streaming_eventhub.py        # Real-time Event Hub consumer
│
├── pipelines/
│   ├── adf_sensor_ingestion.json       # ADF pipeline: sensor log ingestion
│   ├── adf_annotation_ingestion.json   # ADF pipeline: annotation file ingestion
│   └── adf_master_pipeline.json        # Master orchestration pipeline
│
├── sql/
│   ├── synapse_ddl.sql                 # Synapse table definitions
│   ├── synapse_reporting_views.sql     # Reporting views for Power BI
│   └── quality_check_queries.sql       # Data quality validation queries
│
├── config/
│   ├── pipeline_config.json            # Pipeline source/sink configuration
│   └── schema_registry.json            # Annotation schema definitions per tool
│
├── docs/
│   └── architecture_diagram.md
│
└── README.md
```

---

## Key Features

### 1. Parameterized ADF Ingestion
- Single pipeline template handles multiple sensor types (LiDAR, camera, GPS, annotation outputs)
- Lookup + ForEach pattern reads source config table — no pipeline duplication
- Tumbling Window triggers for scheduled batch; Event-based triggers for file arrival
- Failure paths with Azure Monitor alerting

### 2. Bronze → Silver → Gold Delta Lake Architecture
- **Bronze**: Raw files landed as-is — append only, full history preserved
- **Silver**: PySpark cleansing — null handling, deduplication, schema normalization, type casting
- **Gold**: Aggregated fact tables for Synapse queries — vehicle KPIs, annotation accuracy trends

### 3. Real-time Streaming via Event Hub
- Event Hub captures annotation events (frame completions, QA results, model feedback)
- Databricks Structured Streaming consumes events with checkpointing (exactly-once)
- Writes to Bronze Delta in near real-time

### 4. Annotation Quality Validation
- Rule-based checks: field completeness, bounding box validity (x_max > x_min), label taxonomy
- Failed records routed to quarantine Delta table with error codes — nothing silently dropped
- Quality metrics surfaced in Power BI dashboards

### 5. Spark Optimization
- Broadcast joins for small lookup/dimension tables
- OPTIMIZE + ZORDER on Delta tables by commonly filtered columns
- Job clusters (not all-purpose) for all production scheduled runs
- Predicate pushdown — filters applied before file reads

---

## Business Impact

| Metric | Result |
|---|---|
| Data preparation time | Reduced by **50%** |
| Annotation quality visibility | Near real-time dashboards (was daily batch) |
| Spark processing cost | Reduced by **30%** via job optimization |
| Manual QA effort | Eliminated for standard validation checks |

---

## Setup & Configuration

### Prerequisites
- Azure subscription with Contributor access
- Databricks workspace (Premium tier for Unity Catalog)
- ADLS Gen2 storage account with hierarchical namespace enabled
- Azure Data Factory instance
- Azure Event Hub namespace
- Azure Key Vault for secrets

### Storage Layout
```
adls-account/
├── bronze/
│   ├── sensor_logs/
│   ├── image_metadata/
│   └── annotation_outputs/
├── silver/
│   └── annotations/
├── gold/
│   ├── vehicle_kpis/
│   └── annotation_summaries/
└── quarantine/
    └── failed_validations/
```

### Key Vault Secrets Required
```
ADLS-ACCOUNT-KEY
EVENT-HUB-CONNECTION-STRING
SYNAPSE-JDBC-URL
SQL-DB-PASSWORD
```

---

## Notebooks — Quick Reference

| Notebook | Purpose | Trigger |
|---|---|---|
| `01_bronze_ingestion.py` | Land raw files to Delta Bronze | ADF Notebook Activity |
| `02_silver_transformation.py` | Cleanse + normalize annotations | ADF after Bronze |
| `03_quality_validation.py` | Apply QA rules, quarantine failures | ADF after Silver |
| `04_gold_aggregation.py` | Build reporting aggregates | ADF after Silver |
| `05_streaming_eventhub.py` | Continuous Event Hub consumer | Always-on stream job |

---

## Data Quality Rules (Silver Layer)

| Check | Rule | Action on Fail |
|---|---|---|
| Completeness | `object_class IS NOT NULL` | Quarantine |
| Completeness | `bbox_x_min IS NOT NULL` | Quarantine |
| Geometry | `x_max > x_min AND y_max > y_min` | Quarantine |
| Taxonomy | `object_class IN (approved_labels)` | Quarantine |
| Geometry | Coordinates within image bounds | Quarantine |
| Duplicate | Unique on `annotation_id` | Drop duplicate |

---

## Monitoring & Alerting

- **Azure Monitor** alerts on: pipeline failure, processing lag > 30 min, quarantine record spike
- **Log Analytics** workspace captures all Databricks and ADF run logs
- **Power BI dashboards**: vehicle health metrics, error rates, annotation KPIs, daily processing volumes
