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

                # ✅ Correctly extract Location
                storage_descriptor = table.get('StorageDescriptor')
                if storage_descriptor and 'Location' in storage_descriptor:
                    location = storage_descriptor['Location']
                else:
                    location = None

                if not location:
                    print(f"⚠️ Warning: Missing Location for {table_name}, skipping table.")
                    continue

                # ✅ Extract metadata_location safely
                metadata_location = parameters.get('metadata_location', f'{location}/metadata/metadata.json')

                # ✅ Ensure all table parameters are copied
                table_parameters = parameters.copy()

                # ✅ Get column schema
                columns = storage_descriptor.get('Columns', []) if storage_descriptor else []

                # ✅ Build storage descriptor correctly
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
                print(f"✅ Table {table_name}: Location = {location}, Metadata = {metadata_location}")

                # Register or update table
                create_or_update_table(target_db, table_name, storage_descriptor, partition_keys, metadata_location, table_parameters, glue_client)

                # Sync partitions
                sync_table_partitions(source_db, target_db, table_name, glue_client)
