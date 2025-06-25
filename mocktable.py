from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# ───── Spark + Iceberg Setup ─────
spark = (
    SparkSession.builder.appName("recreate_hourly_partitioned_vpc_logs")
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

# ───── Drop Table If Exists ─────
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
spark.sql(f"DROP TABLE IF EXISTS {full_table}")

# ───── Generate Mock Data ─────
base_time = datetime(2025, 6, 1, 0, 0, 0)
records = []

for p in range(100):  # 100 hourly partitions
    hour_start = base_time + timedelta(hours=p)
    for r in range(1000):  # 1000 rows per partition
        ts = hour_start + timedelta(milliseconds=r)
        records.append({
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

df = spark.createDataFrame(records)
df.createOrReplaceTempView("tmp_vpc_logs")

# ───── Create Table with Hidden Partitioning by hours(time) ─────
spark.sql(f"""
CREATE TABLE {full_table}
PARTITIONED BY (hours(time))
USING ICEBERG
AS SELECT * FROM tmp_vpc_logs
""")

print(f"✅ Table {full_table} created with 100 hidden partitions × 1000 records each.")

spark.stop()
