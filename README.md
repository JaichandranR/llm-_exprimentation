Custom external_table Materialization for Trino + DBT + Iceberg
🔧 Implementation Overview
This materialization solves a common challenge in DBT when using Trino with Apache Iceberg: the inability to define external S3 table locations dynamically through config().

To work around this, we implemented a custom materialization called external_table, which lets us:

Override the default Iceberg S3 location path

Dynamically construct CREATE TABLE statements

Organize tables under meaningful S3 prefixes

Support multi-environment deployments via --vars

✅ Key Features
Feature	Description
prefix support	Organize tables under s3://bucket/schema/prefix/table_name/
dbt_project.yml environment integration	Root S3 path is injected dynamically using --vars
Schema suffix handling	Removes suffixes like _jules from schema name during S3 resolution
Iceberg CREATE TABLE logic	Custom macro emits full CREATE TABLE IF NOT EXISTS ... WITH (location) DDL
Supports parquet format	Easily extendable to ORC, AVRO in future versions

📁 Example: Model Configuration
sql
Copy
Edit
{{ config(
   materialized = 'external_table',
   format = 'parquet',
   prefix = 'cyberflow'
) }}

SELECT
  '2024-01-01'::date AS creation_date_displayname,
  NULL::timestamp AS last_updated_date,
  NULL::varchar AS last_updated_username
WHERE FALSE
📁 Example: dbt_project.yml
yaml
Copy
Edit
vars:
  prod_common_data_prototype_s3location: 's3://app-id-90177-dep-id-114232-uu-id-du8vur5fx06i/cyberflow/'
▶ Example: CLI Run
bash
Copy
Edit
dbt run --vars '{"env": "prod"}'
🚀 Benefits
💡 This approach decouples business logic from infrastructure wiring, while maintaining control over physical table storage in Iceberg.

🔄 Environment Agnostic – Same model can deploy to dev, staging, or prod without code changes

🗂️ Better S3 Organization – Tables can be grouped by project or pipeline using prefix

💻 Fully Compatible with Trino Iceberg Connector

💥 Avoids Manual SQL – DBT model authors don't need to write CTAS statements

🔭 Future Work
1. 🔍 Automate S3 Bucket Detection via SHOW CREATE SCHEMA
Currently, schema-level S3 bucket paths are hardcoded in dbt_project.yml.

If SHOW CREATE SCHEMA access is available in Trino, the macro can be enhanced to dynamically discover the S3 path:

sql
Copy
Edit
SHOW CREATE SCHEMA cosmos_nonhcd_iceberg_prototype.common_data_prototype
From this, the macro can extract:

sql
Copy
Edit
CREATE SCHEMA ... WITH (location = 's3://bucket-name/path/')
🔄 This would make the macro entirely self-healing and environment-agnostic.

2. 📦 Multi-format Support
Right now, the macro assumes:

sql
Copy
Edit
format = 'parquet'
Future enhancements can include:

sql
Copy
Edit
format = 'orc' or 'avro'
3. 🔐 Validation & Safety Checks
Enhancements to consider:

Check if location conflicts with existing table

Validate table name & prefix rules

Auto-append trailing / to prefixes when missing

📝 Summary Statement (for top of Confluence page)
Although direct location configuration in config() was unsupported by DBT, we implemented a workaround by creating a custom materialization external_table, enabling Iceberg external tables to be created dynamically using environment-specific paths. This approach centralizes control in dbt_project.yml, supports multi-env deployments, and will become fully dynamic when SHOW CREATE SCHEMA access is permitted.
