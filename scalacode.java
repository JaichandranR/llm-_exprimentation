import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.types.Types
import org.apache.iceberg.spark.SparkSchemaUtil

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

object CreateHourlyIcebergTable {
  def main(args: Array[String]): Unit = {

    // ── 1. Spark + Glue Iceberg catalog ─────────────────────────
    val spark = SparkSession.builder()
      .appName("hourly_partition_demo_iceberg110")
      .config("spark.sql.catalog.cosmos", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cosmos.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.cosmos.warehouse", "s3://your-bucket/iceberg/warehouse") // ← change
      .config("spark.sql.catalog.cosmos.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    val catalog = spark.sessionState.catalogManager
                       .catalog("cosmos")
                       .asInstanceOf[SparkCatalog]

    val ns       = Namespace.of("common_data_prototype")
    val tblName  = "dummy_common_data_hidden_partition"
    val tblId    = TableIdentifier.of(ns, tblName)

    // ── 2. Drop table if it exists ──────────────────────────────
    Try(catalog.dropTable(tblId))

    // ── 3. Mock rows: 100 h × 1 000 rows ────────────────────────
    val base   = Instant.parse("2025-06-01T00:00:00Z")
    val rows   = for { h <- 0 until 100; i <- 0 until 1000 }
                 yield Row(java.sql.Timestamp.from(base.plusMillis(h*3600_000L+i)))

    val schema = StructType(Seq(StructField("time", TimestampType, false)))
    val df     = spark.createDataFrame(rows.toList.asJava, schema)

    // ── 4. Build Iceberg schema + hourly partition spec ─────────
    val icebergSchema: Schema = SparkSchemaUtil.convert(df.schema)
    val spec: PartitionSpec  = PartitionSpec.builderFor(icebergSchema)
                                           .hour("time")    // ← virtual “time_hour”
                                           .build()

    // ── 5. Create table & append data via DataFrame API ────────
    catalog.createTable(tblId, icebergSchema, spec)
    df.writeTo("cosmos.common_data_prototype." + tblName).append()

    spark.stop()
    println(s"✅ Created $tblId with hidden hourly partitioning.")
  }
}
