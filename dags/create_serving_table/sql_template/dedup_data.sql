DROP TABLE IF EXISTS stg.{table_name}_temp;
CREATE TABLE IF NOT EXISTS stg.{table_name}_temp AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY created_at DESC) AS rn
    FROM stg.{table_name}
)
SELECT {columns}
FROM ranked WHERE rn = 1;