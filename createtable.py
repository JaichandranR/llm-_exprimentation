from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sha2, concat_ws
from pyspark.sql.types import *
from boto3 import session
from uuid import uuid4

# -----------------------------
# Configurable parameters
# -----------------------------
catalog_nm = "cosmos_nonhcd_iceberg"
database_nm = "common_data"
inventory_table_nm = "iceberg_metadata_inventory"
inventory_table_full = f"{catalog_nm}.{database_nm}.{inventory_table_nm}"

spark = SparkSession.builder.getOrCreate()
region = session.Session().region_name
run_id = str(uuid4())

# -----------------------------
# Define schema and create the inventory table (if not exists)
# -----------------------------
schema = StructType([
    StructField("table_name", StringType()),
    StructField("region", StringType()),
    StructField("run_id", StringType()),
    StructField("snapshot_id", LongType()),
    StructField("committed_at", TimestampType()),
    StructField("operation", StringType()),
    StructField("parent_id", LongType()),
    StructField("manifest_list", StringType()),
    StructField("manifest_path", StringType()),
    StructField("partition_spec_id", IntegerType()),
    StructField("partition", StructType([StructField("partition_key", StringType())])),  # Adjust as needed
    StructField("record_count", LongType()),
    StructField("file_path", StringType()),
    StructField("file_size_in_bytes", LongType()),
    StructField("table_location", StringType()),
    StructField("summary", StringType()),
    StructField("snapshot_checksum", StringType())
])

empty_df = spark.createDataFrame([], schema)

# Create the table if it doesn't exist
empty_df.writeTo(inventory_table_full) \
    .tableProperty("format", "iceberg") \
    .createIfNotExists()

# -----------------------------
# List tables using V2-safe API
# -----------------------------
tables = spark.catalog.listTables(f"{catalog_nm}.{database_nm}")

for tbl in tables:
    table_name = tbl.name
    qualified = f"`{catalog_nm}.{database_nm}.{table_name}`"
    print(f"\n🔍 Processing table: {qualified}")

    try:
        # Read metadata tables
        snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at, operation, parent_id, manifest_list, summary FROM {qualified}$snapshots")
        manifests_df = spark.sql(f"SELECT * FROM {qualified}$manifests")
        files_df = spark.sql(f"SELECT * FROM {qualified}$files")

        # Get table location
        describe_df = spark.sql(f"DESCRIBE TABLE EXTENDED {qualified}")
        table_location = describe_df.filter("col_name = 'Location'").collect()[0]['data_type']

        # Join metadata and enrich
        joined_df = snapshots_df \
            .join(manifests_df, "snapshot_id", "left") \
            .join(files_df, manifests_df.path == files_df.file_path, "left") \
            .withColumn("table_name", lit(table_name)) \
            .withColumn("region", lit(region)) \
            .withColumn("run_id", lit(run_id)) \
            .withColumn("table_location", lit(table_location)) \
            .withColumn("snapshot_checksum", sha2(concat_ws("||",
                snapshots_df.snapshot_id.cast("string"),
                snapshots_df.committed_at.cast("string"),
                snapshots_df.operation,
                snapshots_df.manifest_list
            ), 256))

        # Write to inventory table
        joined_df.selectExpr(
            "table_name", "region", "run_id", "snapshot_id", "committed_at", "operation", "parent_id",
            "manifest_list", "path as manifest_path", "partition_spec_id", "partition", "record_count",
            "file_path", "file_size_in_bytes", "table_location", "summary", "snapshot_checksum"
        ).writeTo(inventory_table_full).append()

        print(f"✅ Metadata recorded for: {table_name}")

    except Exception as e:
        print(f"⚠️ Skipped table {table_name} due to: {e}")
