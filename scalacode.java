// Compile & run in AWS Glue 5.0 (Spark 3.5/Scala 2.12) with Iceberg 1.1.0 JAR

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

object CreateVpcHourly {

  def main(args: Array[String]): Unit = {

    // ── 1. Spark + Iceberg Catalog ─────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Glue5_VPC_Hourly_Iceberg110")
      .config("spark.sql.catalog.cosmos"           , "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cosmos.catalog-impl" , "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.cosmos.warehouse"    , "s3://your-bucket/iceberg/warehouse") // <-- CHANGE
      .config("spark.sql.catalog.cosmos.io-impl"      , "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions"                 , "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    val catalog = spark.sessionState.catalogManager
                       .catalog("cosmos")
                       .asInstanceOf[SparkCatalog]

    val namespace  = Namespace.of("common_data_prototype")
    val tableName  = "dummy_common_data_hidden_partition"
    val tableId    = TableIdentifier.of(namespace, tableName)

    // ── 2. Drop table if it exists ─────────────────────────────────────────
    Try(catalog.dropTable(tableId))

    // ── 3. Build mock data (100 hours × 1000 rows) ────────────────────────
    val base  = Instant.parse("2025-06-01T00:00:00Z")

    val rows  = (0 until 100).flatMap { h =>
      (0 until 1000).map { i =>
        val ts = base.plusMillis(h * 3600000L + i)     // 3600000L = 1 hour
        Row(
          java.sql.Timestamp.from(ts),                                 // time
          """{"log_provider":"","log_version":1}""",                   // metadata
          "Allowed", 1, 6,                                             // action, action_id, activity_id
          "Network Activity", 4,                                       // category_name, category_uid
          "Network Activity", 4001,                                    // class_name, class_uid
          1, "OK", "400,106",                                          // severity_id, status_code, type_uid
          java.sql.Timestamp.valueOf("2005-03-18 01:58:00"),           // start_time
          java.sql.Timestamp.valueOf("2005-03-18 01:59:00"),           // end_time
          """{"provider":"AWS","account":"111111111111"}""",           // cloud
          """{"ip":"111.11.11.11","port":"1111"}""",                   // src_endpoint
          """{"ip":"111.11.11.111","port":"1111"}""",                  // dst_endpoint
          """{"protocol_num":1,"tcp_flags":18}""",                     // connection_info
          """{"bytes":1111,"packets":1}""",                            // traffic
          """{"flag":true}""",                                         // unmapped
          """{"meta_account_name":"umebob"}"""                         // raw_data
        )
      }
    }

    // ── 4. Spark schema (matching the Row order) ──────────────────────────
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

    // ── 5. Iceberg schema + transform partition spec ──────────────────────
    val icebergSchema: Schema = SparkSchemaUtil.convert(df.schema)
    val spec: PartitionSpec   = PartitionSpec.builderFor(icebergSchema)
                                            .hour("time")       // ← hidden `time_hour`
                                            .build()

    // ── 6. Create table & append data ─────────────────────────────────────
    catalog.createTable(tableId, icebergSchema, spec)
    df.writeTo(s"cosmos.common_data_prototype.$tableName").append()

    spark.stop()
    println(s"✅ Created $tableId with hidden hourly partitioning (hours(time)).")
  }
}
