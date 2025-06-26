import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object IcebergPartitionWriter {
  def main(sysArgs: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Iceberg_SmallFiles_128KB")
      .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.warehouse", "s3://your-warehouse-path")
      .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.cosmos_nonhcd_iceberg_prototype.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.shuffle.partitions", "8000")
      .config("spark.sql.files.maxRecordsPerFile", "500")
      .config("spark.rpc.message.maxSize", "256")
      .getOrCreate()

    import spark.implicits._

    val startEpoch = 485000
    val totalPartitions = 1000
    val recordsPerPartition = 10000
    val totalRecords = totalPartitions * recordsPerPartition

    val df = spark.range(totalRecords)
      .withColumn("time", expr(s"timestampadd(HOUR, id / $recordsPerPartition, TIMESTAMP'2025-06-01 00:00:00')"))
      .withColumn("time_hour", (unix_timestamp(col("time")) / 3600).cast("int"))
      .withColumn("metadata", lit("""{"log_provider": "aws"}"""))
      .withColumn("action", lit("Allowed"))
      .withColumn("action_id", lit(1))
      .withColumn("activity_id", lit(6))
      .withColumn("category_name", lit("Network Activity"))
      .withColumn("category_uid", lit(4))
      .withColumn("class_name", lit("Network Activity"))
      .withColumn("class_uid", lit("400,106"))
      .withColumn("severity_id", lit(1))
      .withColumn("status_code", lit("OK"))
      .withColumn("type_uid", lit("400,106"))
      .withColumn("start_time", lit("2005-03-18 01:58:00"))
      .withColumn("end_time", lit("2005-03-18 01:58:31"))
      .withColumn("cloud", lit("{meta_account_name=umebob}"))
      .withColumn("src_endpoint", lit("{port=1111, ip=111.11.11.11}"))
      .withColumn("dst_endpoint", lit("{port=1111, ip=111.11.11.11}"))
      .withColumn("traffic", lit("{bytes=1111, packets=1}"))
      .withColumn("unmapped", lit("{flag=true}"))
      .drop("id")

    val shuffledDF = df.repartition(8000)

    // Drop table if exists
    spark.sql("DROP TABLE IF EXISTS cosmos_nonhcd_iceberg_prototype.common_data_prototype.dummy_common_data_hidden_partition")

    // Create table
    spark.sql(
      """
      CREATE TABLE cosmos_nonhcd_iceberg_prototype.common_data_prototype.dummy_common_data_hidden_partition (
        time TIMESTAMP,
        metadata STRING,
        action STRING,
        action_id INT,
        activity_id INT,
        category_name STRING,
        category_uid INT,
        class_name STRING,
        class_uid STRING,
        severity_id INT,
        status_code STRING,
        type_uid STRING,
        start_time STRING,
        end_time STRING,
        cloud STRING,
        src_endpoint STRING,
        dst_endpoint STRING,
        traffic STRING,
        unmapped STRING
      )
      PARTITIONED BY (hours(time))
      USING ICEBERG
      """
    )

    shuffledDF.writeTo("cosmos_nonhcd_iceberg_prototype.common_data_prototype.dummy_common_data_hidden_partition")
      .append()

    spark.stop()
  }
}
