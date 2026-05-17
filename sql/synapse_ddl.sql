-- ============================================================
-- Project: Autonomous Vehicle Data Processing Platform
-- File: synapse_ddl.sql
-- Purpose: Create Synapse Dedicated SQL Pool tables for
--          analytics reporting layer (Gold zone mirror)
-- Distribution strategy:
--   Large fact tables  → HASH on most common join key
--   Dimension tables   → REPLICATE (small, frequently joined)
-- ============================================================

-- ── SCHEMA SETUP ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS audit;
GO

-- ── DIMENSION TABLES ─────────────────────────────────────────

CREATE TABLE dim.annotator (
    annotator_key       INT           NOT NULL,
    annotator_id        VARCHAR(100)  NOT NULL,
    annotator_name      VARCHAR(200),
    team_name           VARCHAR(100),
    region              VARCHAR(50),
    onboarded_date      DATE,
    is_active           BIT           DEFAULT 1,
    _created_at         DATETIME2     DEFAULT GETDATE(),
    _updated_at         DATETIME2     DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE dim.project (
    project_key         INT           NOT NULL,
    project_id          VARCHAR(100)  NOT NULL,
    project_name        VARCHAR(500),
    client_name         VARCHAR(200),
    start_date          DATE,
    end_date            DATE,
    annotation_type     VARCHAR(50),   -- bbox | polygon | keypoint
    object_classes      VARCHAR(MAX),
    is_active           BIT           DEFAULT 1
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE dim.object_class (
    class_key           INT           NOT NULL,
    object_class        VARCHAR(200)  NOT NULL,
    label_category      VARCHAR(100),
    label_id            INT,
    description         VARCHAR(500)
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE dim.date (
    date_key            INT           NOT NULL,
    full_date           DATE          NOT NULL,
    year                INT,
    quarter             INT,
    month               INT,
    month_name          VARCHAR(20),
    week                INT,
    day_of_week         INT,
    day_name            VARCHAR(20),
    is_weekday          BIT
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- ── FACT TABLES ──────────────────────────────────────────────

CREATE TABLE gold.fact_annotation_events (
    annotation_key      BIGINT        NOT NULL,
    annotation_id       VARCHAR(200)  NOT NULL,
    annotator_key       INT,
    project_key         INT,
    class_key           INT,
    date_key            INT,
    frame_id            VARCHAR(200),
    bbox_x_min          FLOAT,
    bbox_x_max          FLOAT,
    bbox_y_min          FLOAT,
    bbox_y_max          FLOAT,
    bbox_area           FLOAT,
    confidence_score    FLOAT,
    quality_score       FLOAT,
    quality_tier        VARCHAR(30),
    z_score             FLOAT,
    annotation_tool     VARCHAR(100),
    annotated_at        DATETIME2,
    _processed_at       DATETIME2,
    _source_file        VARCHAR(1000)
)
WITH (
    DISTRIBUTION = HASH(annotator_key),
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE gold.fact_daily_project_summary (
    summary_key             BIGINT       NOT NULL,
    project_key             INT,
    date_key                INT,
    annotation_date         DATE,
    total_annotations       BIGINT,
    active_annotators       INT,
    frames_annotated        INT,
    avg_quality_score       FLOAT,
    high_quality_count      BIGINT,
    rejected_count          BIGINT,
    high_quality_pct        FLOAT,
    _created_at             DATETIME2
)
WITH (
    DISTRIBUTION = HASH(project_key),
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE gold.fact_annotator_daily_performance (
    perf_key                BIGINT       NOT NULL,
    annotator_key           INT,
    project_key             INT,
    date_key                INT,
    total_annotations       INT,
    avg_quality_score       FLOAT,
    avg_confidence          FLOAT,
    high_quality_count      INT,
    rejected_count          INT,
    rejection_rate          FLOAT,
    _created_at             DATETIME2
)
WITH (
    DISTRIBUTION = HASH(annotator_key),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- ── AUDIT TABLE ───────────────────────────────────────────────

CREATE TABLE audit.pipeline_run_log (
    log_id              BIGINT        IDENTITY(1,1),
    run_time            DATETIME2     DEFAULT GETDATE(),
    pipeline_name       VARCHAR(200),
    source_type         VARCHAR(100),
    source_path         VARCHAR(1000),
    target_path         VARCHAR(1000),
    records_written     BIGINT,
    records_quarantined BIGINT        DEFAULT 0,
    status              VARCHAR(50),
    error_message       VARCHAR(MAX),
    run_duration_sec    INT
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- ── STATISTICS (improves query optimizer) ────────────────────

CREATE STATISTICS stat_fact_ann_annotator ON gold.fact_annotation_events (annotator_key);
CREATE STATISTICS stat_fact_ann_project   ON gold.fact_annotation_events (project_key);
CREATE STATISTICS stat_fact_ann_date      ON gold.fact_annotation_events (date_key);
CREATE STATISTICS stat_fact_ann_quality   ON gold.fact_annotation_events (quality_score);
GO
