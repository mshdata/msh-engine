-- This is a standard SQL model running in DuckDB
-- It reads from the local python model 'stg_users'

SELECT
    id as customer_id,
    name,
    email,
    'active' as status,
    CURRENT_TIMESTAMP as modeled_at
FROM {{ ref('stg_users') }}
WHERE id > 2
