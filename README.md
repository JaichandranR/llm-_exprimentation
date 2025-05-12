Iceberg Snapshot Inventory Glue Job
📌 Purpose
This Glue PySpark job captures and stores snapshot metadata from all Iceberg tables in a given AWS Glue Catalog.
It helps maintain an up-to-date inventory of data versioning activity across tables and supports:

Auditing snapshot history

Tracking recent changes

Optimizing incremental ingestion or validation pipelines

🏗️ Architecture Overview
plaintext
Copy
Edit
Iceberg Tables (S3-backed) ──> Glue Job ──> Snapshot Inventory Table
                                     └──> Snapshot State Table (latest only)
Iceberg Tables: Tables written via Trino, Spark, or Athena into AWS S3

Glue Job: Extracts metadata using Spark SQL queries

Snapshot Inventory Table: Stores full metadata for all versions (partitioned by table, snapshot)

Snapshot State Table: Stores latest snapshot ID per table to detect changes

⚙️ Configuration
Parameter	Value/Example
catalog_nm	cosmos_nonhcd_iceberg
database_nm	common_data
inventory_table	iceberg_snapshot_inventory
state_table	iceberg_inventory_state
warehouse_path	s3://app-id-90177-dep-id-114232-uu-id-pee895fr5knp/

Ensure the Glue job has the following permissions:

GlueCatalog access

S3ReadWrite for Iceberg metadata

LakeFormation table write access (if enforced)

🔁 Execution Logic
Step 1: List All Iceberg Tables
The job queries:

sql
Copy
Edit
SHOW TABLES IN cosmos_nonhcd_iceberg.common_data
to retrieve all available Iceberg tables.

Step 2: Check/Create Snapshot State Table
Checks if iceberg_inventory_state exists. If not, it is created with this schema:

text
Copy
Edit
table_name      STRING
snapshot_id     STRING
Step 3: Loop Over Each Table
For every table:

Queries table.snapshots to get metadata

Sorts by committed_at timestamp (descending)

Marks the most recent row with is_latest_snapshot = 'Yes'

Compares latest snapshot ID to stored value in state table

If changed:

Appends all metadata to in-memory list

Flags it for update

If unchanged:

Skips table to save processing time

Step 4: Write Output Tables (If Any Updates Found)
Snapshot Inventory Table (iceberg_snapshot_inventory)

Stores all rows from snapshots metadata with is_latest_snapshot flag

Overwrites existing table

Snapshot State Table (iceberg_inventory_state)

Contains only the latest snapshot ID per table

Overwritten with most recent snapshot IDs

📂 Output Table Schemas
🧾 iceberg_snapshot_inventory
Column	Type	Description
table_name	STRING	Name of the Iceberg table
snapshot_id	STRING	Unique snapshot identifier
committed_at	TIMESTAMP	Snapshot creation timestamp
operation	STRING	e.g., append, overwrite, replace
manifest_list	STRING	Location of manifest list in S3
summary	STRING	Summary stats in stringified JSON
is_latest_snapshot	STRING	"Yes" if latest, otherwise "No"

🧾 iceberg_inventory_state
Column	Type	Description
table_name	STRING	Iceberg table name
snapshot_id	STRING	Most recent snapshot ID at runtime

🧪 Sample Use Case
Use this job to:

Compare snapshot history for troubleshooting data changes

Identify recent updates by filtering on is_latest_snapshot = 'Yes'

Track data publishing frequency over time

🧰 Operational Notes
Recommended schedule: every 30–60 minutes

Runtime: ~1–2 minutes for 100+ tables

Supports easy filtering by table group (if needed)

Logs print snapshot changes and skipped tables

Easily extendable to include summary -> user, queryId, or operation metrics

🔒 Security
Ensure this Glue job runs with a role that has:

glue:GetTable, glue:GetTables

s3:GetObject, s3:ListBucket for warehouse path

glue:CreateTable, glue:UpdateTable if inventory tables are managed through Glue catalog
