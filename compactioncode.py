from pyspark.sql.functions import lit, concat

def create_metric_table_if_not_exists(spark, metric_table: str):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {metric_table} (
            table_name STRING,
            partition_name STRING,
            record_count_before_compact INT,
            file_count_before_compact INT,
            total_size_before_compact DOUBLE,
            record_count_after_compact INT,
            file_count_after_compact INT,
            total_size_after_compact DOUBLE
        )
    """)

def capture_metrics(spark, full_table: str, metric_table: str, partition_fields: list, current_hour: int):
    df = spark.table(full_table + ".partitions")
    for field in partition_fields:
        df = df.withColumn(f"{field}_val", df[f"partition.{field}"].cast("int"))
    df = df.withColumn("total_size_mb", df["total_data_file_size_in_bytes"] / 1024000)

    before_df = df.filter((df["time_hour_val"] >= current_hour - 24) & (df["time_hour_val"] < current_hour))\
        .withColumnRenamed("record_count", "record_count_before_compact")\
        .withColumnRenamed("file_count", "file_count_before_compact")\
        .withColumnRenamed("total_size_mb", "total_size_before_compact")\
        .withColumn("table_name", lit(full_table))\
        .withColumn("partition_name", concat(lit("time_hour="), df["time_hour_val"]))

    after_df = df.filter((df["time_hour_val"] < current_hour - 192))\
        .withColumnRenamed("record_count", "record_count_after_compact")\
        .withColumnRenamed("file_count", "file_count_after_compact")\
        .withColumnRenamed("total_size_mb", "total_size_after_compact")\
        .withColumn("table_name", lit(full_table))\
        .withColumn("partition_name", concat(lit("time_hour="), df["time_hour_val"]))

    combined_df = before_df.join(after_df, on=["table_name", "partition_name"], how="outer")
    combined_df.createOrReplaceTempView("temp_metrics")

    spark.sql(f"""
        MERGE INTO {metric_table} t
        USING temp_metrics s
        ON t.table_name = s.table_name AND t.partition_name = s.partition_name
        WHEN MATCHED THEN UPDATE SET
            t.record_count_before_compact = s.record_count_before_compact,
            t.file_count_before_compact = s.file_count_before_compact,
            t.total_size_before_compact = s.total_size_before_compact,
            t.record_count_after_compact = s.record_count_after_compact,
            t.file_count_after_compact = s.file_count_after_compact,
            t.total_size_after_compact = s.total_size_after_compact
        WHEN NOT MATCHED THEN INSERT *
    """)



    # Create metric table if not exists
    metric_table = f"{catalog_nm}.{source_db}.table_compaction_metric"
    create_metric_table_if_not_exists(spark, metric_table)

    # Capture compaction metrics
    capture_metrics(spark, full_table, metric_table, partition_fields, current_hour)

