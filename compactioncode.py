import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext

def get_backlog_partition_hours(spark, full_table, partition_fields, last_hour_done, current_hour, batch_size):
    fields = [f.strip() for f in partition_fields.split(",")]
    df = spark.read.format("iceberg").load(full_table + ".partitions")
    df = df.withColumn("hour_val", df[f"partition.{fields[0]}"].cast("int"))
    if len(fields) > 1:
        df = df.withColumn("protocol_val", df[f"partition.{fields[1]}"].cast("string"))
        result = (
            df.filter((df.hour_val >= last_hour_done) & (df.hour_val < current_hour))
              .select("hour_val", "protocol_val")
              .distinct()
              .orderBy("hour_val", "protocol_val")
              .limit(batch_size)
              .rdd.map(lambda r: (r["hour_val"], r["protocol_val"]))
              .collect()
        )
    else:
        result = (
            df.filter((df.hour_val >= last_hour_done) & (df.hour_val < current_hour))
              .select("hour_val")
              .distinct()
              .orderBy("hour_val")
              .limit(batch_size)
              .rdd.map(lambda r: (r["hour_val"],))
              .collect()
        )
    return result

def main():
    args = getResolvedOptions(sys.argv, [
        'file_size_for_optimization',
        'catalog_nm',
        'table_nm',
        'source_db',
        'expire_snapshots_day',
        'skip_newest_partitions',
        'partition_fields'
    ])

    target_file_size_bytes = int(args['file_size_for_optimization']) * 1024 * 1024
    catalog_nm = args['catalog_nm']
    table_nm = args['table_nm']
    source_db = args['source_db']
    expire_snapshots_day = int(args['expire_snapshots_day'])
    skip_newest_partitions = int(args['skip_newest_partitions'])

    full_table = f"{catalog_nm}.{source_db}.{table_nm}"
    status_table = f"{catalog_nm}.{source_db}.table_compaction_status"

    spark = (SparkSession.builder
        .appName("CompactionJob")
        .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .enableHiveSupport()
        .getOrCreate())

    GlueContext(SparkContext.getOrCreate())

    spark.sql(f"CREATE TABLE IF NOT EXISTS {status_table} (table_name STRING, last_compacted_hour INT) USING iceberg")

    def get_last_compacted_index():
        try:
            df = spark.read.format("iceberg").load(status_table)
            row = df.filter(df.table_name == full_table).select("last_compacted_hour").first()
            return row[0] if row else 0
        except AnalysisException:
            return 0

    def persist_last_compacted_index(hour):
        spark.sql(f"""
            MERGE INTO {status_table} t
            USING (SELECT '{full_table}' AS table_name, {hour} AS last_compacted_hour) s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET last_compacted_hour = s.last_compacted_hour
            WHEN NOT MATCHED THEN INSERT *
        """)

    last_hour_done = get_last_compacted_index()
    current_hour = int((datetime.utcnow() - timedelta(hours=skip_newest_partitions) - datetime(1970,1,1)).total_seconds() // 3600)

    if last_hour_done < current_hour:
        partitions = get_backlog_partition_hours(
            spark, full_table, args['partition_fields'], last_hour_done, current_hour, 100
        )

        fields = [f.strip() for f in args['partition_fields'].split(",")]

        for p in partitions:
            hour = p[0]
            start = datetime(1970,1,1) + timedelta(hours=hour)
            end = start + timedelta(hours=1)
            if len(p) == 2:
                protocol = p[1]
                print(f"Compacting {fields[0]}={hour}, {fields[1]}={protocol}")
                where_clause = f"time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}' AND {fields[1]} = '{protocol}'"
            else:
                print(f"Compacting {fields[0]}={hour}")
                where_clause = f"time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}'"
            try:
                spark.sql(f"""
                    CALL {catalog_nm}.system.rewrite_data_files(
                        table => '{source_db}.{table_nm}',
                        where => "{where_clause}",
                        options => map('target-file-size-bytes', '{target_file_size_bytes}', 'min-input-files', '1')
                    )
                """)
                print(f"✔ Completed compaction for {p}")
            except Exception as e:
                print(f"⚠ Failed compaction for {p}: {e}")

        if partitions:
            persist_last_compacted_index(max(p[0] for p in partitions) + 1)

    spark.stop()
    print("✅ Compaction job finished.")

if __name__ == "__main__":
    main()
