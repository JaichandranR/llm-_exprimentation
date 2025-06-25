import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

object HourlyPartitionIceberg110 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IcebergHourlyPartitionDemo")
      .config("spark.sql.catalog.cosmos", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.cosmos.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.cosmos.warehouse", "s3://your-bucket/iceberg/warehouse") // Change
      .config("spark.sql.catalog.cosmos.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    val catalog = spark.sessionState.catalogManager
      .catalog("cosmos")
      .asInstanceOf[SparkCatalog]

    val ns = Namespace.of("common_data_prototype")
    val tableId = TableIdentifier.of(ns, "dummy_common_data_hidden_partition")
    Try(catalog.dropTable(tableId))

    val base = Instant.parse("2025-06-01T00:00:00Z")
    val rows = (0 until 100).flatMap { h =>
      (0 until 1000).map { i =>
        Row(java.sql.Timestamp.from(base.plusMillis(h * 3600_000L + i)))
      }
    }

    val schema = StructType(Seq(StructField("time", TimestampType, false)))
    val df = spark.createDataFrame(rows.asJava, schema)

    val icebergSchema = SparkSchemaUtil.convert(df.schema)
    val spec = PartitionSpec.builderFor(icebergSchema)
      .hour("time")
      .build()

    catalog.createTable(tableId, icebergSchema, spec)
    df.writeTo("cosmos.common_data_prototype.dummy_common_data_hidden_partition").append()

    spark.stop()
    println("✅ Created Iceberg table with hidden hourly partition.")
  }
}
