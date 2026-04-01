-- Mart model: final analytics-ready case table
-- Used by dashboards and reporting teams

WITH cases AS (
    SELECT * FROM {{ ref('stg_cases') }}
),

final AS (
    SELECT
        case_number,
        case_type,
        status,
        filed_date,
        court_id,
        judge_id,
        -- Derived fields
        CASE
            WHEN status IN ('closed', 'dismissed') THEN 'resolved'
            WHEN status IN ('open', 'pending')     THEN 'active'
            ELSE 'unknown'
        END                                         AS resolution_category,

        CASE
            WHEN case_type IN ('criminal', 'traffic') THEN 'criminal_justice'
            WHEN case_type IN ('civil', 'probate')    THEN 'civil_justice'
            WHEN case_type = 'family'                 THEN 'family_court'
            ELSE 'other'
        END                                         AS court_division,

        DATE_PART('year', filed_date)               AS filing_year,
        DATE_PART('month', filed_date)              AS filing_month,
        DATE_PART('quarter', filed_date)            AS filing_quarter,

        transformed_at                              AS last_updated
    FROM cases
)

SELECT * FROM final
