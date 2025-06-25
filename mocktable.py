from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
# Spark + Iceberg Setup
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("create_100_hidden_partitions_table")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.warehouse", "s3://your-bucket-path/iceberg/warehouse")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .enableHiveSupport()
    .getOrCreate()
)

catalog   = "cosmos_nonhcd_iceberg_prototype"
database  = "common_data_prototype"
table     = "dummy_common_data_hidden_partition"
full_name = f"{catalog}.{database}.{table}"

# ─────────────────────────────────────────────────────────────
# Generate mock data with 100 partitions × 1000 rows
# ─────────────────────────────────────────────────────────────
base_time = datetime(2025, 6, 1, 0, 0, 0)
rows = []

for p in range(100):  # 100 hourly partitions
    part_time = base_time + timedelta(hours=p)
    for r in range(1000):  # 1000 rows per partition
        ts = part_time + timedelta(milliseconds=r)
        rows.append({
            "time": ts,
            "metadata": {"log_provider": "", "log_version": 1},
            "action": "Allowed",
            "action_id": 1,
            "activity_id": 6,
            "category_name": "Network Activity",
            "category_uid": 4,
            "class_name": "Network Activity",
            "class_uid": 4001,
            "severity_id": 1,
            "status_code": "OK",
            "type_uid": "400,106",
            "start_time": datetime(2005, 3, 18, 1, 58)
        })

df = spark.createDataFrame(rows)
df.createOrReplaceTempView("tmp_large_data")

# ─────────────────────────────────────────────────────────────
# Write using Spark SQL: Iceberg supports TRANSFORM in SQL
# ─────────────────────────────────────────────────────────────
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

spark.sql(f"""
CREATE OR REPLACE TABLE {full_name}
PARTITIONED BY (hours(time))
USING ICEBERG
AS SELECT * FROM tmp_large_data
""")

print(f"✅ Iceberg table {full_name} created with 100 partitions and 100,000 rows.")
spark.stop()
