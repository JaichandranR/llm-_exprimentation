from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, floor
from datetime import datetime, timedelta

# ───── Spark + Iceberg Setup ─────
spark = (
    SparkSession.builder.appName("recreate_partitioned_vpc_logs_epoch_hour")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.warehouse", "s3://your-bucket-path/iceberg/warehouse")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .enableHiveSupport()
    .getOrCreate()
)

catalog = "cosmos_nonhcd_iceberg_prototype"
database = "common_data_prototype"
table = "dummy_common_data_hidden_partition"
full_table = f"{catalog}.{database}.{table}"

# ───── Drop table if exists ─────
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
spark.sql(f"DROP TABLE IF EXISTS {full_table}")

# ───── Generate 100 hourly partitions × 1000 records ─────
base_time = datetime(2025, 6, 1, 0, 0, 0)
data = []

for h in range(100):  # 100 hours
    hour_base = base_time + timedelta(hours=h)
    for i in range(1000):  # 1000 records per hour
        time_value = hour_base + timedelta(milliseconds=i)
        data.append({
            "time": time_value,
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
            "start_time": datetime(2005, 3, 18, 1, 58),
            "end_time": datetime(2005, 3, 18, 1, 59),
            "cloud": {"provider": "AWS", "account": "111111111111"},
            "src_endpoint": {"ip": "111.11.11.11", "port": "1111"},
            "dst_endpoint": {"ip": "111.11.11.111", "port": "1111"},
            "connection_info": {"protocol_num": 1, "tcp_flags": 18},
            "traffic": {"bytes": 1111, "packets": 1},
            "unmapped": {"flag": True},
            "raw_data": {"meta_account_name": "umebob"}
        })

df = spark.createDataFrame(data)

# ───── Compute true epoch hour (integer partition key) ─────
df = df.withColumn("epoch_hour", floor(unix_timestamp(col("time")) / 3600).cast("int"))

# ───── Write with partitioning, drop the column after write ─────
(
    df.writeTo(full_table)
    .partitionedBy("epoch_hour")
    .using("iceberg")
    .createOrReplace()
)

print(f"✅ Table {full_table} created with 100 partitions and 1000 rows each.")

spark.stop()
