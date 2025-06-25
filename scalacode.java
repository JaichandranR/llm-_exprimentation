// Glue 5.0 + Spark 3.5 + Scala 2.12 + Iceberg 1.1.0 (GlueCatalog-compatible)

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.catalog.CatalogUtil
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.hadoop.conf.Configuration

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

object CreateVpcHourlyGlue {
  def main(args: Array[String]): Unit = {

    // ── 1. Spark Setup ───────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Glue5_VPC_Hourly_Iceberg110")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    // ── 2. Load Iceberg GlueCatalog manually ─────────────────────────────
    val warehousePath = "s3://your-bucket/iceberg/warehouse" // ← CHANGE ME
    val glueConf = new Configuration()

    val catalogImpl = CatalogUtil.loadCatalog(
      classOf[GlueCatalog].getName,
      "cosmos",
      Map(
        "warehouse" -> warehousePath,
        "io-impl"   -> "org.apache.iceberg.aws.s3.S3FileIO",
        "catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog"
      ).asJava,
      glueConf
    )

    val namespace = Namespace.of("common_data_prototype")
    val tableName = "dummy_common_data_hidden_partition"
    val tableId   = TableIdentifier.of(namespace, tableName)

    Try(catalogImpl.dropTable(tableId, true))

    // ── 3. Create mock data (100 × 1000 records) ────────────────────────
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

    // ── 4. Iceberg schema + virtual partition ───────────────────────────
    val icebergSchema = SparkSchemaUtil.convert(df.schema)
    val spec = PartitionSpec.builderFor(icebergSchema)
      .hour("time")    // ← will generate hidden `time_hour`
      .build()

    catalogImpl.createTable(tableId, icebergSchema, spec)
    df.writeTo("cosmos.common_data_prototype." + tableName).append()

    println("✅ Iceberg table created with transform partition: hours(time)")
    spark.stop()
  }
}
