-- ============================================================
-- File: synapse_reporting_views.sql
-- Purpose: Reporting views consumed by Power BI dashboards
--          Joins fact + dimension tables for clean report layer
-- ============================================================

-- ── VIEW 1: Daily Project Quality Dashboard ────────────────────
CREATE OR ALTER VIEW reporting.vw_daily_project_quality AS
SELECT
    p.project_name,
    p.client_name,
    d.full_date          AS annotation_date,
    d.month_name,
    d.year,
    f.total_annotations,
    f.active_annotators,
    f.frames_annotated,
    ROUND(f.avg_quality_score, 2) AS avg_quality_score,
    f.high_quality_count,
    f.rejected_count,
    ROUND(f.high_quality_pct, 2)  AS high_quality_pct,
    -- Week-over-week quality trend
    ROUND(
        f.avg_quality_score - LAG(f.avg_quality_score) OVER (
            PARTITION BY f.project_key ORDER BY d.full_date
        ), 2
    ) AS quality_score_wow_change
FROM
    gold.fact_daily_project_summary f
    JOIN dim.project p ON f.project_key = p.project_key
    JOIN dim.date    d ON f.date_key    = d.date_key;
GO

-- ── VIEW 2: Annotator Performance Leaderboard ─────────────────
CREATE OR ALTER VIEW reporting.vw_annotator_performance AS
SELECT
    a.annotator_name,
    a.team_name,
    a.region,
    p.project_name,
    d.full_date          AS perf_date,
    d.month_name,
    f.total_annotations,
    ROUND(f.avg_quality_score, 2)  AS avg_quality_score,
    ROUND(f.avg_confidence, 2)     AS avg_confidence,
    f.high_quality_count,
    f.rejected_count,
    ROUND(f.rejection_rate * 100, 2) AS rejection_rate_pct,
    -- Rank within project per date
    DENSE_RANK() OVER (
        PARTITION BY f.project_key, f.date_key
        ORDER BY f.avg_quality_score DESC
    ) AS quality_rank_in_project,
    -- 7-day moving average quality
    ROUND(
        AVG(f.avg_quality_score) OVER (
            PARTITION BY f.annotator_key, f.project_key
            ORDER BY d.full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS quality_score_7d_avg
FROM
    gold.fact_annotator_daily_performance f
    JOIN dim.annotator a ON f.annotator_key = a.annotator_key
    JOIN dim.project   p ON f.project_key   = p.project_key
    JOIN dim.date      d ON f.date_key       = d.date_key;
GO

-- ── VIEW 3: Object Class Coverage Report ──────────────────────
CREATE OR ALTER VIEW reporting.vw_object_class_distribution AS
SELECT
    p.project_name,
    oc.object_class,
    oc.label_category,
    d.full_date,
    d.month_name,
    COUNT(f.annotation_key)                AS annotation_count,
    ROUND(AVG(f.quality_score), 2)         AS avg_quality_score,
    ROUND(AVG(f.bbox_area), 2)             AS avg_bbox_area,
    -- % of total annotations for this project/date
    ROUND(
        COUNT(f.annotation_key) * 100.0 /
        SUM(COUNT(f.annotation_key)) OVER (
            PARTITION BY f.project_key, f.date_key
        ), 2
    ) AS pct_of_daily_total
FROM
    gold.fact_annotation_events f
    JOIN dim.project      p  ON f.project_key  = p.project_key
    JOIN dim.object_class oc ON f.class_key    = oc.class_key
    JOIN dim.date         d  ON f.date_key     = d.date_key
GROUP BY
    p.project_name, oc.object_class, oc.label_category,
    d.full_date, d.month_name, f.project_key, f.date_key;
GO

-- ── VIEW 4: Pipeline Health Monitor ───────────────────────────
CREATE OR ALTER VIEW reporting.vw_pipeline_health AS
SELECT
    pipeline_name,
    CAST(run_time AS DATE)       AS run_date,
    COUNT(*)                     AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed_runs,
    SUM(records_written)         AS total_records_written,
    SUM(records_quarantined)     AS total_quarantined,
    AVG(run_duration_sec)        AS avg_duration_sec,
    MAX(run_time)                AS last_run_time
FROM
    audit.pipeline_run_log
GROUP BY
    pipeline_name, CAST(run_time AS DATE);
GO
