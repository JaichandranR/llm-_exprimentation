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

# Initialize Spark and metadata
spark = SparkSession.builder.getOrCreate()
region = session.Session().region_name
run_id = str(uuid4())

# -----------------------------
# Step 1: Define schema and create the inventory table
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
    StructField("partition", StructType([StructField("partition_key", StringType())])),  # Adjust if needed
    StructField("record_count", LongType()),
    StructField("file_path", StringType()),
    StructField("file_size_in_bytes", LongType()),
    StructField("table_location", StringType()),
    StructField("summary", StringType()),
    StructField("snapshot_checksum", StringType())
])

empty_df = spark.createDataFrame([], schema)

try:
    empty_df.writeTo(inventory_table_full) \
        .tableProperty("format", "iceberg") \
        .createOrReplace()
    print(f"✅ Inventory table created: {inventory_table_full}")
except Exception as e:
    print(f"⚠️ Could not create table: {e}")

# -----------------------------
# Step 2: Collect metadata for each Iceberg table (V2-safe)
# -----------------------------
tables = spark.catalog.listTables(f"{catalog_nm}.{database_nm}")

for tbl in tables:
    table_name = tbl.name
    qualified = f"`{catalog_nm}.{database_nm}.{table_name}`"
    print(f"\n🔍 Processing table: {qualified}")

    try:
        # Step 2a: Load metadata tables
        snapshots_df = spark.sql(
            f"SELECT snapshot_id, committed_at, operation, parent_id, manifest_list, summary FROM {qualified}$snapshots"
        )
        manifests_df = spark.sql(f"SELECT * FROM {qualified}$manifests")
        files_df = spark.sql(f"SELECT * FROM {qualified}$files")

        # Step 2b: Get table location via Java API (V2-safe)
        table_location = spark._jsparkSession.catalog().getTable(
            f"{catalog_nm}.{database_nm}", table_name
        ).location()

        # Step 2c: Join metadata
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

        # Step 2d: Write to inventory table
        joined_df.selectExpr(
            "table_name", "region", "run_id", "snapshot_id", "committed_at", "operation", "parent_id",
            "manifest_list", "path as manifest_path", "partition_spec_id", "partition", "record_count",
            "file_path", "file_size_in_bytes", "table_location", "summary", "snapshot_checksum"
        ).writeTo(inventory_table_full).append()

        print(f"✅ Metadata recorded for: {table_name}")

    except Exception as e:
        print(f"⚠️ Skipped {table_name} due to: {e}")
