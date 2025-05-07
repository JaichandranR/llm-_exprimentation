from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sha2, concat_ws, from_json, col
from boto3 import session
from uuid import uuid4

# --- Configurable Parameters ---
catalog_nm = "cosmos_nonhcd_iceberg"
database_nm = "common_data"
inventory_table_nm = "iceberg_metadata_inventory"
inventory_table_full = f"{catalog_nm}.{database_nm}.{inventory_table_nm}"
warehouse_path = "s3://app-id-90177-dep-id-114232-uu-id-pee895fr5knp/"  # Adjust if needed

# --- Context Setup ---
spark = SparkSession.builder.getOrCreate()
region = session.Session().region_name
run_id = str(uuid4())

# --- Create table if not exists ---
existing_tables = spark.sql(f"SHOW TABLES IN {catalog_nm}.{database_nm}").collect()
existing_table_names = [t.tableName for t in existing_tables]

if inventory_table_nm not in existing_table_names:
    spark.sql(f"""
        CREATE TABLE {inventory_table_full} (
            table_name STRING,
            region STRING,
            run_id STRING,
            snapshot_id BIGINT,
            committed_at TIMESTAMP,
            operation STRING,
            parent_id BIGINT,
            manifest_list STRING,
            manifest_path STRING,
            partition_spec_id INT,
            partition STRUCT<partition_key: STRING>,  -- Adjust for your partition structure
            record_count BIGINT,
            file_path STRING,
            file_size_in_bytes BIGINT,
            table_location STRING,
            summary STRING,
            snapshot_checksum STRING
        )
        PARTITIONED BY (table_name, region)
        TBLPROPERTIES ('format'='iceberg')
    """)
    print(f"✅ Created inventory table: {inventory_table_full}")
else:
    print(f"ℹ️ Inventory table already exists: {inventory_table_full}")

# --- Scan all tables in catalog ---
tables = spark.sql(f"SHOW TABLES IN {catalog_nm}.{database_nm}").collect()

for table in tables:
    table_name = table.tableName
    qualified = f"`{catalog_nm}.{database_nm}.{table_name}`"
    print(f"\n🔍 Processing table: {table_name}")

    try:
        # Step 1: Load snapshot metadata
        snapshots_df = spark.sql(
            f"SELECT snapshot_id, committed_at, operation, parent_id, manifest_list, summary FROM {qualified}$snapshots"
        )

        # Step 2: Load manifests and files
        manifests_df = spark.sql(f"SELECT * FROM {qualified}$manifests")
        files_df = spark.sql(f"SELECT * FROM {qualified}$files")

        # Step 3: Table location
        location_df = spark.sql(f"DESCRIBE TABLE EXTENDED {qualified}")
        table_location = location_df.filter("col_name = 'Location'").collect()[0]['data_type']

        # Step 4: Join and enrich
        joined_df = snapshots_df \
            .join(manifests_df, snapshots_df.snapshot_id == manifests_df.snapshot_id, "left") \
            .join(files_df, manifests_df.path == files_df.file_path, "left") \
            .withColumn("table_name", lit(table_name)) \
            .withColumn("region", lit(region)) \
            .withColumn("run_id", lit(run_id)) \
            .withColumn("table_location", lit(table_location)) \
            .withColumn("snapshot_checksum", sha2(concat_ws("||", 
                col("snapshot_id").cast("string"), 
                col("committed_at").cast("string"), 
                col("operation"), 
                col("manifest_list")
            ), 256))

        # Step 5: Write to inventory table
        joined_df.selectExpr(
            "table_name", "region", "run_id", "snapshot_id", "committed_at", "operation", "parent_id",
            "manifest_list", "path as manifest_path", "partition_spec_id", "partition", "record_count",
            "file_path", "file_size_in_bytes", "table_location", "summary", "snapshot_checksum"
        ).writeTo(inventory_table_full).append()

        print(f"✅ Metadata captured for table: {table_name}")

    except Exception as e:
        print(f"⚠️ Skipped table {table_name} due to error: {e}")
