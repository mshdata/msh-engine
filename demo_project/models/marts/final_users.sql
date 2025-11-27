-- This runs locally in DuckDB
-- It prepares the final table for consumption

SELECT
    user_id,
    name,
    email,
    -- Add some business logic
    CASE 
        WHEN email LIKE '%@example.com' THEN 'Internal' 
        ELSE 'External' 
    END as user_type,
    CURRENT_DATE as report_date
FROM {{ ref('int_active_users') }}
