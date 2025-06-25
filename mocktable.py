from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, floor
from datetime import datetime, timedelta

# ─────────────── 1. Spark + Iceberg Glue catalog ───────────────
spark = (
    SparkSession.builder.appName("recreate_vpc_logs_hourly_partitions")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.warehouse",
            "s3://your-bucket-path/iceberg/warehouse")          # ⬅️ change
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .enableHiveSupport()
    .getOrCreate()
)

catalog   = "cosmos_nonhcd_iceberg_prototype"
database  = "common_data_prototype"
table     = "dummy_common_data_hidden_partition"
full_tbl  = f"{catalog}.{database}.{table}"

# ─────────────── 2.  Drop table if it exists ───────────────
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
spark.sql(f"DROP TABLE IF EXISTS {full_tbl}")

# ─────────────── 3.  Build mock rows (100 h × 1000) ───────────────
base_time = datetime(2025, 6, 1, 0, 0, 0)
rows = []

for h in range(100):
    hour_start = base_time + timedelta(hours=h)
    for i in range(1000):
        ts = hour_start + timedelta(milliseconds=i)
        rows.append({
            "time"         : ts,
            "metadata"     : {"log_provider": "", "log_version": 1},
            "action"       : "Allowed",
            "action_id"    : 1,
            "activity_id"  : 6,
            "category_name": "Network Activity",
            "category_uid" : 4,
            "class_name"   : "Network Activity",
            "class_uid"    : 4001,
            "severity_id"  : 1,
            "status_code"  : "OK",
            "type_uid"     : "400,106",
            "start_time"   : datetime(2005, 3, 18, 1, 58),
            "end_time"     : datetime(2005, 3, 18, 1, 59),
            "cloud"        : {"provider": "AWS", "account": "111111111111"},
            "src_endpoint" : {"ip": "111.11.11.11", "port": "1111"},
            "dst_endpoint" : {"ip": "111.11.11.111", "port": "1111"},
            "connection_info": {"protocol_num": 1, "tcp_flags": 18},
            "traffic"      : {"bytes": 1111, "packets": 1},
            "unmapped"     : {"flag": True},
            "raw_data"     : {"meta_account_name": "umebob"}
        })

df = spark.createDataFrame(rows)

# ─────────────── 4.  Add helper column time_hour (epoch-hour) ───────────────
df = df.withColumn(
        "time_hour",
        floor(unix_timestamp(col("time")) / 3600).cast("int")
     )

# ─────────────── 5.  Write to Iceberg, partitioned by time_hour ─────────────
(df.writeTo(full_tbl)
   .partitionedBy("time_hour")        # must exist for the write
   .using("iceberg")
   .createOrReplace())

# ─────────────── 6.  Hide time_hour from the schema ─────────────
# The partition spec stays; only the column disappears from SELECT *
spark.sql(f"ALTER TABLE {full_tbl} DROP COLUMN time_hour")

print("✅ Table recreated: hidden hourly partitions, 100 × 1000 rows.")

spark.stop()
