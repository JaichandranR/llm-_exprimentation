from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import os

# Set environment variable for AWS profile (optional)
# os.environ["AWS_PROFILE"] = "your-profile-name"

# Load AWS Glue catalog
catalog = load_catalog(
    name="glue",
    properties={
        "type": "glue",
        "warehouse": "s3://mrap-your-mrap-name/warehouse/",
        "s3.endpoint": "https://s3.amazonaws.com",  # Optional, for global endpoint
    }
)

# Replace with your database and table name
database = "your_database"
table_name = "your_table"

# Load Iceberg table
table: Table = catalog.load_table(f"{database}.{table_name}")

# Scan and read table rows
for row in table.scan().to_arrow().to_pandas().itertuples(index=False):
    print(row)
