import json
from pyspark.sql import SparkSession

# Assuming Spark session is already configured with Iceberg extensions
tables = spark.sql("SHOW TABLES IN cosmos_nonhcd_iceberg.common_data").collect()

for table in tables:
    table_name = table.tableName
    qualified = f"`cosmos_nonhcd_iceberg.common_data.{table_name}`"

    try:
        # Load snapshots
        snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at, operation, parent_id, manifest_list, summary FROM {qualified}$snapshots")
        manifests_df = spark.sql(f"SELECT * FROM {qualified}$manifests")
        files_df = spark.sql(f"SELECT * FROM {qualified}$files")

        # Join snapshots to manifests and files (indirectly via snapshot_id)
        joined_df = snapshots_df.join(
            manifests_df, snapshots_df.snapshot_id == manifests_df.snapshot_id, how="left"
        ).join(
            files_df, manifests_df.path == files_df.data_file, how="left"
        ).withColumn("table_name", lit(table_name))

        # Write to your inventory Iceberg table (create once before this step)
        joined_df.selectExpr(
            "table_name", "snapshot_id", "committed_at", "operation", "parent_id",
            "manifest_list", "summary", "path as manifest_path",
            "partition_spec_id", "partition", "record_count",
            "file_path", "file_size_in_bytes"
        ).writeTo("cosmos_nonhcd_iceberg.common_data.iceberg_metadata_inventory").append()

    except Exception as e:
        print(f"❌ Error processing table {table_name}: {e}")
