import re

def derive_location_from_metadata(metadata_location):
    """Extracts the top 3 levels from metadata_location to construct Location."""
    match = re.match(r'^(s3://[^/]+/[^/]+/[^/]+)', metadata_location)
    return match.group(1) + '/' if match else None

def copy_iceberg_tables():
    """Discovers and copies multiple Iceberg tables from source to target Glue database."""
    paginator = glue_client.get_paginator('get_tables')
    page_iterator = paginator.paginate(DatabaseName=source_db)

    for page in page_iterator:
        for table in page.get('TableList', []):
            table_name = table['Name']
            parameters = table.get('Parameters', {})

            if parameters.get('table_type') == 'ICEBERG':
                print(f"Processing Iceberg table: {table_name}")

                # ✅ Get metadata_location
                metadata_location = parameters.get('metadata_location')

                # ✅ Derive Location from metadata_location if Location is missing
                storage_descriptor = table.get('StorageDescriptor', {})
                location = storage_descriptor.get('Location')

                if not location and metadata_location:
                    location = derive_location_from_metadata(metadata_location)
                    print(f"🟢 Derived Location for {table_name}: {location}")

                # ✅ If Location is still missing, log error and skip
                if not location:
                    print(f"❌ ERROR: Missing Location for {table_name}, skipping table.")
                    continue

                # ✅ Preserve all table parameters from source
                table_parameters = parameters.copy()

                # ✅ Get column schema from source table
                columns = storage_descriptor.get('Columns', []) if storage_descriptor else []

                # ✅ Get storage descriptor with exact Location
                storage_descriptor = {
                    'Location': location,
                    'Columns': columns,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {'serialization.format': '1'}
                    }
                }

                # Extract partition keys
                partition_keys = table.get('PartitionKeys', [])

                # ✅ Debugging log
                print(f"✅ Table {table_name}: Final Location = {location}, Metadata = {metadata_location}")

                # Register or update table
                create_or_update_table(target_db, table_name, storage_descriptor, partition_keys, metadata_location, table_parameters, glue_client)

                # Sync partitions
                sync_table_partitions(source_db, target_db, table_name, glue_client)
