import sys
import traceback
from pyspark.sql import SparkSession

# ---------------- Configurable Inputs ----------------
catalog_name = "glue_catalog"
warehouse_path = "s3://your-bucket/warehouse/"   # ✅ Replace with your actual Iceberg warehouse S3 location
table_name = "your_db.your_table"                # ✅ Example: "common_data.105130_tpc_products"

# ---------------- Spark Session Setup ----------------
try:
    spark = (
        SparkSession.builder
        .appName("IcebergRelativePathPOC")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .getOrCreate()
    )
    print("✅ Spark session created successfully.")
except Exception as e:
    print("❌ Failed to create Spark session.")
    traceback.print_exc()
    sys.exit(1)

# ---------------- Alter Table Properties ----------------
try:
    alter_sql = f"""
    ALTER TABLE {catalog_name}.{table_name}
    SET TBLPROPERTIES ('write.metadata.relative-path' = 'true')
    """
    print(f"🚀 Running SQL: {alter_sql.strip()}")
    spark.sql(alter_sql)
    print(f"✅ Table `{table_name}` updated successfully.")
except Exception as e:
    print(f"❌ ALTER TABLE failed: {e}")
    traceback.print_exc()
    sys.exit(1)

# ---------------- Shutdown ----------------
spark.stop()
