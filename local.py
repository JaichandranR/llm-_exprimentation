SELECT 
  table_schema,
  table_name,
  column_name,
  data_type
FROM cosmos_nonhcd_iceberg.information_schema.columns
WHERE table_schema != 'information_schema'
ORDER BY table_schema, table_name, ordinal_position;
