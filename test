import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# -----------------------------
# Configuration
# -----------------------------
catalog_nm = "cosmos_nonhcd_iceberg"
database_nm = "common_data"
table_name = "metadata_table"

s3_warehouse_path = "s3://app-id-90177-dep-id-114232-uu-id-pee895Fr5knp/"
full_table_name = f"{catalog_nm}.{database_nm}.{table_name}"

# Snapshot retention configuration
expire_snapshots_older_than_days = 7

# -----------------------------
# Initialize Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("IcebergTableOptimizationAndSnapshotExpiration")
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", s3_warehouse_path)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .enableHiveSupport()
    .getOrCreate()
)

# -----------------------------
# Main Logic
# -----------------------------
try:
    # ✅ Step 1: Preview top 10 rows
    print(f"\n🔎 Previewing table: {full_table_name}")
    df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10")
    df.show()

    # ✅ Step 2: Optimize entire Iceberg table
    print(f"\n🚀 Optimizing all partitions of: {full_table_name}")
    spark.sql(f"""
        CALL {catalog_nm}.system.optimize('{database_nm}.{table_name}')
    """)
    print("✅ Optimization complete.")

    # ✅ Step 3: Expire old snapshots
    expiration_cutoff = datetime.utcnow() - timedelta(days=expire_snapshots_older_than_days)
    expiration_ts = expiration_cutoff.strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n🧹 Expiring snapshots older than: {expiration_ts}")
    spark.sql(f"""
        CALL {catalog_nm}.system.expire_snapshots(
            '{database_nm}.{table_name}',
            TIMESTAMP '{expiration_ts}'
        )
    """)
    print("✅ Snapshot expiration complete.")

except Exception as e:
    print(f"\n❌ Glue job failed due to error: {str(e)}")
    sys.exit(1)
