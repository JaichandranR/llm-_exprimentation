Observation
In the current snapshot analysis from the fnd_fct_tmp$snapshots metadata:

All operations show as overwrite, which means the Iceberg table is rewriting entire partitions (or the full table) during incremental updates.

This behavior occurs because Trino translates MERGE into overwrite logic for partitions when the partition key changes or when only partial data is provided.

Since your partition spec includes STS (a status field), any update to STS or partial incremental load removes older rows from the current snapshot, making them invisible unless time travel is used.

The logical deletions you observed are not from snapshot expiration or post-hook failure—they happen because of overwrite-based snapshot commits during incremental runs.

✅ Recommendation
Create a new Iceberg table:

Use the same schema and storage location pattern.

Keep the existing partition keys except remove STS from the partition spec.

This approach:

Prevents large-scale overwrites caused by status updates.

Reduces unnecessary partition rewrites.

Preserves other partitioning dimensions (FND_TYPE, AGST_TYPE) for query performance.

Keep the existing table intact for safety until validation is complete.

Update dbt incremental logic to point to the new table and disable post-hook updates that move data across partitions.
