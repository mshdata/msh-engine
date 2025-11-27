-- This runs locally in DuckDB
-- It transforms the raw data we just ingested from Postgres

SELECT
    id as user_id,
    LOWER(email) as email,
    name,
    status,
    created_at
FROM {{ ref('stg_postgres_users') }}
WHERE status = 'active'
