from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, hour, dayofmonth, month, year

# ───────────────────────────────────────────────────────
# Spark + Iceberg setup for AWS Glue compatibility
# ───────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("glue_iceberg_partition_workaround")
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

# ───────────────────────────────────────────────────────
# Create mock data (100 partitions × 1000 records)
# ───────────────────────────────────────────────────────
base_time = datetime(2025, 6, 1, 0, 0, 0)
rows = []

for p in range(100):
    hour_base = base_time + timedelta(hours=p)
    for r in range(1000):
        ts = hour_base + timedelta(milliseconds=r)
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

# Compute `time_hour` as simulated hidden partition
df = df.withColumn("time_hour", (year("time") * 100000) + (month("time") * 1000) + (dayofmonth("time") * 100) + hour("time"))

# ───────────────────────────────────────────────────────
# Create & write table partitioned by `time_hour`
# ───────────────────────────────────────────────────────
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

df.writeTo(full_name) \
    .partitionedBy("time_hour") \
    .using("iceberg") \
    .createOrReplace()

print(f"✅ Table {full_name} created successfully with manual partition column `time_hour`")

spark.stop()
