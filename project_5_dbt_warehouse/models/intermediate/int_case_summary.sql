-- Intermediate model: aggregate case-level metrics
-- Used by mart models downstream

WITH cases AS (
    SELECT * FROM {{ ref('stg_cases') }}
),

summary AS (
    SELECT
        case_type,
        status,
        court_id,
        COUNT(*)                                    AS total_cases,
        COUNT(CASE WHEN status = 'open'
              THEN 1 END)                           AS open_cases,
        COUNT(CASE WHEN status = 'closed'
              THEN 1 END)                           AS closed_cases,
        COUNT(CASE WHEN status = 'dismissed'
              THEN 1 END)                           AS dismissed_cases,
        MIN(filed_date)                             AS earliest_filing,
        MAX(filed_date)                             AS latest_filing,
        COUNT(CASE WHEN filed_date IS NULL
              THEN 1 END)                           AS missing_date_count
    FROM cases
    GROUP BY case_type, status, court_id
)

SELECT * FROM summary
