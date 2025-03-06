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

                # ✅ Ensure StorageDescriptor exists before accessing Location
                storage_descriptor = table.get('StorageDescriptor')
                
                if storage_descriptor:
                    location = storage_descriptor.get('Location')
                else:
                    location = None

                # ✅ Retry fetching table if StorageDescriptor is missing
                retry_attempts = 3
                while not location and retry_attempts > 0:
                    print(f"⚠️ Retrying fetch for {table_name}, attempt {4 - retry_attempts}...")
                    table = glue_client.get_table(DatabaseName=source_db, Name=table_name)
                    storage_descriptor = table.get('Table', {}).get('StorageDescriptor')
                    if storage_descriptor:
                        location = storage_descriptor.get('Location')
                    retry_attempts -= 1

                # ✅ Debugging: Print retrieved Location
                print(f"✅ Table {table_name}: Location = {location}")

                # ✅ If Location is still missing, log error and skip
                if not location:
                    print(f"❌ ERROR: Missing Location for {table_name}, skipping table.")
                    continue

                # ✅ Extract metadata_location safely
                metadata_location = parameters.get('metadata_location', f'{location}/metadata/metadata.json')

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
