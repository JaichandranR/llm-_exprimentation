SELECT 
  c.table_schema,
  c.table_name,
  c.column_name,
  c.data_type
FROM apitables.information_schema.columns c
JOIN apitables.information_schema.tables t
  ON c.table_schema = t.table_schema
  AND c.table_name = t.table_name
WHERE c.table_schema != 'information_schema'
  AND t.table_type = 'BASE TABLE'
ORDER BY c.table_schema, c.table_name, c.ordinal_position
LIMIT 1000;
