-- Mart model: per-judge case load metrics
-- Helps court administrators monitor judge workloads

WITH cases AS (
    SELECT * FROM {{ ref('stg_cases') }}
),

judge_stats AS (
    SELECT
        judge_id,
        court_id,
        COUNT(*)                                    AS total_cases,
        COUNT(CASE WHEN status = 'open'
              THEN 1 END)                           AS active_cases,
        COUNT(CASE WHEN status = 'closed'
              THEN 1 END)                           AS closed_cases,
        COUNT(CASE WHEN case_type = 'criminal'
              THEN 1 END)                           AS criminal_cases,
        COUNT(CASE WHEN case_type = 'civil'
              THEN 1 END)                           AS civil_cases,
        ROUND(
            COUNT(CASE WHEN status = 'closed' THEN 1 END)::NUMERIC /
            NULLIF(COUNT(*), 0) * 100, 2
        )                                           AS closure_rate_pct,
        MIN(filed_date)                             AS first_case_date,
        MAX(filed_date)                             AS latest_case_date
    FROM cases
    WHERE judge_id IS NOT NULL
    GROUP BY judge_id, court_id
)

SELECT * FROM judge_stats
ORDER BY total_cases DESC
