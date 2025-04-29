import sys
import traceback
from pyspark.sql import SparkSession

# ---- Configurable Inputs ----
catalog_name = "glue_catalog"
warehouse_path = "s3://your-bucket/warehouse/"  # <-- ✅ REPLACE with your actual S3 path
table_name = "your_db.your_table_name"          # <-- ✅ REPLACE with your Glue Iceberg table name

# ---- Spark Session Setup ----
try:
    spark = (
        SparkSession.builder
        .appName("IcebergUpdatePOC")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    print("✅ Spark session created successfully.")
except Exception as e:
    print("❌ Failed to create Spark session:", e)
    traceback.print_exc()
    sys.exit(1)

# ---- Run ALTER TABLE ----
try:
    print(f"⚙️  Switching to Iceberg catalog: {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

    alter_sql = f"""
    ALTER TABLE {table_name}
    SET TBLPROPERTIES (
        'write.metadata.relative-path' = 'true'
    )
    """

    print(f"🚀 Running SQL:\n{alter_sql.strip()}")
    spark.sql(alter_sql)

    print(f"✅ Successfully updated table: {table_name}")
except Exception as e:
    print(f"❌ Failed to update table {table_name}: {e}")
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
    print("🛑 Spark session stopped.")

