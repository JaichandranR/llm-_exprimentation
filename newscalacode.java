// ────────────────────────────────────────────────────────────────
// Glue 5.0 (Spark 3.5 / Scala 2.12) • Iceberg 1.1.0 runtime JAR
// Creates 1 000 hidden‑hour partitions × 10 000 records each, 128 KB files
// Table partition field is hours(time) (virtual), **time_hour does NOT exist
// ────────────────────────────────────────────────────────────────

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.types.Types
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.io.FileIO

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

object GlueApp {                                // Glue entry‑point

  // ── 0.  Parameter area ──────────────────────────────────────────
  val catName   = "cosmos_nonhcd_iceberg_prototype"      // Glue catalog alias
  val warehouse = "s3://YOUR‑WAREHOUSE‑BUCKET/iceberg"   // ← CHANGE
  val ns        = Namespace.of("common_data_prototype")
  val tblName   = "dummy_common_data_hidden_partition"
  val tblId     = TableIdentifier.of(ns, tblName)

  val hoursPartitions      = 1000            // number of hourly partitions
  val rowsPerPartition     = 10000           // ≥ 10 000 rows each
  val shuffleTasks         = 8000            // files ≈ shuffleTasks / partitions
  val maxRecordsPerFile    = 500             // ~128 KB files (depends on row size)
  val rpcMsgSizeMB         = 256             // avoid large‑task RPC errors

  // ── 1. Spark session with catalog alias ─────────────────────────
  val spark: SparkSession = SparkSession.builder()
    .appName("Iceberg_HiddenHour_SmallFiles")
    .config(s"spark.sql.catalog.$catName",            "org.apache.iceberg.spark.SparkCatalog")
    .config(s"spark.sql.catalog.$catName.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(s"spark.sql.catalog.$catName.warehouse",    warehouse)
    .config(s"spark.sql.catalog.$catName.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.shuffle.partitions", shuffleTasks.toString)
    .config("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile.toString)
    .config("spark.rpc.message.maxSize", rpcMsgSizeMB.toString)
    .getOrCreate()

  import spark.implicits._

  // ── 2. Initialise GlueCatalog via Iceberg Java API ──────────────
  private val hConf  = new Configuration()
  private val gCat   = new GlueCatalog()
  private val props  = new java.util.HashMap[String,String]()
  props.put("warehouse", warehouse)
  props.put("io-impl",  "org.apache.iceberg.aws.s3.S3FileIO")
  val fileIO: FileIO = new S3FileIO(); fileIO.initialize(props)
  gCat.setConf(hConf); gCat.initialize(catName, props)
  Try(gCat.dropTable(tblId, true))   // clean slate

  // ── 3. Build Iceberg schema (no time_hour) ──────────────────────
  val icebergSchema = new Schema(
    Types.NestedField.required(1,  "time",           Types.TimestampType.withZone()),
    Types.NestedField.optional(2,  "metadata",       Types.StringType.get()),
    Types.NestedField.optional(3,  "action",         Types.StringType.get()),
    Types.NestedField.optional(4,  "action_id",      Types.IntegerType.get()),
    Types.NestedField.optional(5,  "activity_id",    Types.IntegerType.get()),
    Types.NestedField.optional(6,  "category_name",  Types.StringType.get()),
    Types.NestedField.optional(7,  "category_uid",   Types.IntegerType.get()),
    Types.NestedField.optional(8,  "class_name",     Types.StringType.get()),
    Types.NestedField.optional(9,  "class_uid",      Types.StringType.get()),
    Types.NestedField.optional(10, "severity_id",    Types.IntegerType.get()),
    Types.NestedField.optional(11, "status_code",    Types.StringType.get()),
    Types.NestedField.optional(12, "type_uid",       Types.StringType.get()),
    Types.NestedField.optional(13, "start_time",     Types.StringType.get()),
    Types.NestedField.optional(14, "end_time",       Types.StringType.get()),
    Types.NestedField.optional(15, "cloud",          Types.StringType.get()),
    Types.NestedField.optional(16, "src_endpoint",   Types.StringType.get()),
    Types.NestedField.optional(17, "dst_endpoint",   Types.StringType.get()),
    Types.NestedField.optional(18, "traffic",        Types.StringType.get()),
    Types.NestedField.optional(19, "unmapped",       Types.StringType.get())
  )

  val spec: PartitionSpec = PartitionSpec.builderFor(icebergSchema)
    .hour("time")              // hidden transform field time_hour
    .build()

  gCat.createTable(tblId, icebergSchema, spec)

  // ── 4. Generate big DataFrame (10 000 000 rows) ──────────────────
  val baseInstant  = Instant.parse("2025-06-01T00:00:00Z")
  val totalRows    = hoursPartitions * rowsPerPartition

  val df = spark.range(totalRows).map { id =>
    val partIdx    = (id / rowsPerPartition).toInt
    val millis     = partIdx * 3600000L + (id % rowsPerPartition)
    val ts         = java.sql.Timestamp.from(baseInstant.plusMillis(millis))
    // build Row-like tuple
    (
      ts,
      """{"log_provider":"aws"}""",
      "Allowed",1,6,
      "Network Activity",4,
      "Network Activity","400,106",
      1,"OK","400,106",
      "2005-03-18 01:58:00","2005-03-18 01:58:31",
      "{meta_account_name=umebob}",
      "{port=1111, ip=111.11.11.11}",
      "{port=1111, ip=111.11.11.11}",
      "{bytes=1111, packets=1}",
      "{flag=true}"
    )
  }.toDF(
    "time","metadata","action","action_id","activity_id","category_name","category_uid",
    "class_name","class_uid","severity_id","status_code","type_uid","start_time","end_time",
    "cloud","src_endpoint","dst_endpoint","traffic","unmapped"
  )

  // ── 5. Repartition for small files & write ──────────────────────
  val shuffled = df.repartition(shuffleTasks)

  shuffled.writeTo(s"$catName.common_data_prototype.$tblName")
    .option("fanout-enabled", "true")          // parallel writers per partition (Iceberg 1.1)
    .append()

  println("✅ Completed: 1000 partitions × 10 000 rows with ~128 KB files each.")
  spark.stop()
}
