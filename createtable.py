import sys
import traceback
from pyspark.sql import SparkSession

# ---- Configurable Inputs ----
catalog_name = "glue_catalog"
warehouse_path = "s3://app-id-90177-dep-id-114232-uu-id-pee895fr5knp/105130_tpc_products/"
table_name = "common_data.105130_tpc_products"

# ---- Spark Session Setup ----
try:
    spark = (
        SparkSession.builder
        .appName("IcebergUpdatePOC")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    print("✅ Spark session created successfully.")

    # Use correct Iceberg catalog
    spark.sql(f"USE {catalog_name}")

    # ALTER statement
    alter_sql = f"""
    ALTER TABLE {catalog_name}.{table_name}
    SET TBLPROPERTIES ('write.metadata.relative-path' = 'true')
    """
    print(f"Running SQL: {alter_sql}")
    spark.sql(alter_sql)
    print(f"✅ Successfully updated table: {table_name}")

except Exception as e:
    print(f"❌ Failed to run Spark session: {e}")
    traceback.print_exc()
    sys.exit(1)

finally:
    spark.stop()
