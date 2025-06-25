import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.io.FileIO
import org.apache.hadoop.conf.Configuration

import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.util.Try

object GlueApp {                       // ← Glue looks for this object

  val catName     = "cosmos_nonhcd_iceberg_prototype"
  val warehouse   = "s3://your-bucket/iceberg/warehouse"   // ← update
  val namespace   = Namespace.of("common_data_prototype")
  val tableName   = "dummy_common_data_hidden_partition"
  val tableId     = TableIdentifier.of(namespace, tableName)

  def main(args: Array[String]): Unit = {

    // ── 1. SparkSession with catalog alias ───────────────────────────────
    val spark = SparkSession.builder()
      .appName("Glue5_VPC_Hourly_Iceberg110")
      .config(s"spark.sql.catalog.$catName", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$catName.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config(s"spark.sql.catalog.$catName.warehouse", warehouse)
      .config(s"spark.sql.catalog.$catName.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    // ── 2. Manually initialise GlueCatalog (Iceberg 1.1.0) ──────────────
    val hadoopConf = new Configuration()
    val glueCatalog = new GlueCatalog()
    val props = new util.HashMap[String, String]()
    props.put("warehouse", warehouse)
    props.put("io-impl",  "org.apache.iceberg.aws.s3.S3FileIO")

    val fileIO: FileIO = new S3FileIO()
    fileIO.initialize(props)

    glueCatalog.setConf(hadoopConf)
    glueCatalog.initialize(catName, props)

    // drop & recreate
    Try(glueCatalog.dropTable(tableId, true))

    // ── 3. Build mock rows (100 h × 1000) ────────────────────────────────
    val base = Instant.parse("2025-06-01T00:00:00Z")

    val rows = (0 until 100).flatMap { h =>
      (0 until 1000).map { i =>
        val ts = base.plusMillis(h * 3600000L + i)
        Row(
          java.sql.Timestamp.from(ts),
          """{"log_provider":"","log_version":1}""",
          "Allowed", 1, 6,
          "Network Activity", 4,
          "Network Activity", 4001,
          1, "OK", "400,106",
          java.sql.Timestamp.valueOf("2005-03-18 01:58:00"),
          java.sql.Timestamp.valueOf("2005-03-18 01:59:00"),
          """{"provider":"AWS","account":"111111111111"}""",
          """{"ip":"111.11.11.11","port":"1111"}""",
          """{"ip":"111.11.11.111","port":"1111"}""",
          """{"protocol_num":1,"tcp_flags":18}""",
          """{"bytes":1111,"packets":1}""",
          """{"flag":true}""",
          """{"meta_account_name":"umebob"}"""
        )
      }
    }

    val schema = StructType(List(
      StructField("time",          TimestampType, false),
      StructField("metadata",      StringType,    false),
      StructField("action",        StringType,    false),
      StructField("action_id",     IntegerType,   false),
      StructField("activity_id",   IntegerType,   false),
      StructField("category_name", StringType,    false),
      StructField("category_uid",  IntegerType,   false),
      StructField("class_name",    StringType,    false),
      StructField("class_uid",     IntegerType,   false),
      StructField("severity_id",   IntegerType,   false),
      StructField("status_code",   StringType,    false),
      StructField("type_uid",      StringType,    false),
      StructField("start_time",    TimestampType, false),
      StructField("end_time",      TimestampType, false),
      StructField("cloud",         StringType,    false),
      StructField("src_endpoint",  StringType,    false),
      StructField("dst_endpoint",  StringType,    false),
      StructField("connection_info", StringType,  false),
      StructField("traffic",       StringType,    false),
      StructField("unmapped",      StringType,    false),
      StructField("raw_data",      StringType,    false)
    ))

    val df = spark.createDataFrame(rows.asJava, schema)

    // ── 4. Iceberg schema + hourly partition spec ────────────────────────
    val icebergSchema: Schema = SparkSchemaUtil.convert(df.schema)
    val spec: PartitionSpec   = PartitionSpec.builderFor(icebergSchema)
                                            .hour("time")           // hidden time_hour
                                            .build()

    glueCatalog.createTable(tableId, icebergSchema, spec)

    // ── 5. Append data via Spark (needs catalog alias) ───────────────────
    df.writeTo(s"$catName.common_data_prototype.$tableName").append()

    println("✅ Table created with hidden hourly partition (hours(time)).")
    spark.stop()
  }
}
