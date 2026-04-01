-- Staging model: clean_cases → standardized staging layer
-- Applies final type casting and renames for downstream models

WITH source AS (
    SELECT * FROM {{ source('court_raw', 'clean_cases') }}
),

renamed AS (
    SELECT
        case_number                          AS case_number,
        LOWER(TRIM(case_type))               AS case_type,
        filed_date::DATE                     AS filed_date,
        LOWER(TRIM(status))                  AS status,
        court_id::INTEGER                    AS court_id,
        NULLIF(TRIM(judge_id), '')           AS judge_id,
        ssn_hash,
        notes,
        transformed_at
    FROM source
    WHERE case_number IS NOT NULL
)

SELECT * FROM renamed
