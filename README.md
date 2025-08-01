Here’s a comprehensive, step-by-step upgrade plan for moving from dbt 1.8.0 to 1.9.8 in your environment (Trino 773, AWS S3 backend with Iceberg tables, Glue catalog, Kestra orchestration):

✅ 1. Pre-Upgrade Preparation
Before upgrading, ensure the environment and all dependencies are ready.

1.1 Assess Compatibility
dbt Version: Moving from 1.8.0 → 1.9.8 (minor version upgrade, but includes changes in parsing, adapters, and config).

Adapter: Ensure dbt-trino version supports dbt 1.9.x (latest is usually compatible).

bash
Copy
Edit
pip install --upgrade dbt-trino==1.9.x
Trino Version: 773 → compatible with dbt 1.9.x.

AWS Glue / Iceberg: Confirm Iceberg connector behavior hasn’t changed for schema evolution, external table creation, etc.

Kestra Integration: Check if Kestra jobs/scripts reference dbt CLI flags or environment variables that changed in dbt 1.9.

1.2 Review dbt Release Notes
Check for:

Deprecated configs in dbt_project.yml.

Removed flags or changed CLI behavior.

Macros in dbt-utils or custom macros that depend on internal dbt functions.

Links:

dbt v1.9.0 release notes

dbt v1.9.x changelog

1.3 Environment Backup
dbt Artifacts: manifest.json, run_results.json, catalog.json.

Project Files: Entire dbt repo snapshot (models, macros, profiles.yml).

Kestra Workflows: Export YAML workflows.

Glue Catalog: Backup metadata (optional but good practice).

✅ 2. Upgrade Process
2.1 Create Isolated Upgrade Environment
Use Python Virtual Env or Docker for controlled testing:

bash
Copy
Edit
python -m venv dbt19_env
source dbt19_env/bin/activate
pip install --upgrade pip
pip install dbt-core==1.9.8 dbt-trino==1.9.x
2.2 Update dbt Packages
Upgrade packages to match dbt 1.9.x:

bash
Copy
Edit
dbt deps
Check packages.yml for deprecated packages (like older dbt-utils).

2.3 Update Docker Image (If Used in Prod)
Modify Dockerfile:

dockerfile
Copy
Edit
FROM python:3.11
RUN pip install dbt-core==1.9.8 dbt-trino==1.9.x
Build & tag:

bash
Copy
Edit
docker build -t my-dbt:1.9.8 .
✅ 3. Functional Testing Plan
Testing must ensure parsing, compilation, execution, and artifacts are intact.

3.1 Syntax & Compilation Checks
Run:

bash
Copy
Edit
dbt debug
dbt parse
dbt compile
Validate:

dbt_project.yml structure (check for deprecated keys).

All Jinja macros compile without error.

Sources and seeds compile.

3.2 Dry Runs & Unit-Level Tests
Run specific models with --compile-only or --full-refresh --target dev:

bash
Copy
Edit
dbt run --models +my_model --target dev --compile-only
dbt test --target dev
Validate:

Schema tests pass.

Data tests return same results as before upgrade.

3.3 Integration with Iceberg & Glue
Create a sandbox schema in Glue for testing.

Run:

bash
Copy
Edit
dbt run --target dev --full-refresh
Validate:

Tables registered in Glue with correct Iceberg properties.

Partitioning, table location, and snapshot lineage match expectations.

ANALYZE or SHOW CREATE TABLE in Trino returns expected metadata.

3.4 Incremental Model Behavior
Test incremental models:

bash
Copy
Edit
dbt run --models incremental_model --target dev
Validate:

MERGE or INSERT SQL logic generated is unchanged.

Iceberg snapshots are created correctly (use SELECT * FROM system.snapshots in Trino).

3.5 Snapshot Testing
Run snapshots:

bash
Copy
Edit
dbt snapshot
Validate:

Snapshot queries produce expected change detection.

No changes in snapshot table schema.

3.6 Hooks & Macros Validation
Check pre-hook and post-hook execution for:

Audit logging.

Glue-specific DDL.

Validate custom macros using run-operation:

bash
Copy
Edit
dbt run-operation my_macro
3.7 Kestra Orchestration
Update Kestra job definitions to reference:

New dbt CLI version.

Any flag changes (e.g., --defer remains unchanged).

Test sample run from Kestra to confirm environment variables and configs work.

✅ 4. Regression Testing
Compare outputs before & after upgrade:

Row counts per model.

Schema consistency.

Materialization types (view, table, incremental).

Validate manifest.json structure for downstream tools (e.g., lineage, docs).

✅ 5. Performance Validation
Compare:

Runtime for heavy models.

Incremental runs vs. previous version.

Look for:

Changes in query plans in Trino (EXPLAIN).

Any new optimizer hints or unexpected behavior.

✅ 6. Deployment Plan
After successful testing:

Update Docker image in production.

Roll out upgrade in staging first, then production.

Communicate changes to analytics team.

✅ 7. Rollback Plan
Keep existing dbt 1.8.0 Docker image ready.

Maintain previous virtual environment or pip package cache.

If regression detected:

Switch Kestra back to old image.

Restore backup of manifest.json for lineage tools.

✅ 8. Post-Upgrade Monitoring
Monitor:

Glue catalog updates.

Iceberg snapshot creation & expiration policies.

Kestra job success/failures.

Collect:

dbt logs (logs/dbt.log) for unexpected warnings.

Performance metrics for large models.

🔍 Would you like me to prepare a full detailed checklist document (Excel/Markdown) for all steps with status columns, so your team can track progress during upgrade?
Or should I generate a Kestra workflow snippet for testing dbt 1.9.x?
Or both?
