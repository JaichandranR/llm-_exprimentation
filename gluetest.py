import boto3
from botocore.exceptions import ClientError

def get_iceberg_storage_descriptor(s3_location, columns):
    """Generate a StorageDescriptor for Iceberg tables with correct columns."""
    return {
        'Location': s3_location,
        'Columns': columns,  # ✅ Include columns from the source table
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters': {'serialization.format': '1'}
        }
    }

def create_or_update_table(database, table_name, storage_descriptor, partition_keys, metadata_location, table_parameters, glue_client):
    """Creates or updates Iceberg tables dynamically in Glue with metadata_location and schema."""
    
    table_parameters['metadata_location'] = metadata_location  # ✅ Ensure metadata_location is included

    table_input = {
        'Name': table_name,
        'TableType': 'EXTERNAL_TABLE',
        'StorageDescriptor': storage_descriptor,
        'PartitionKeys': partition_keys,
        'Parameters': table_parameters  # ✅ Copy all parameters
    }

    try:
        glue_client.get_table(DatabaseName=database, Name=table_name)
        print(f"Updating existing Iceberg table {table_name} in {database}.")
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Creating new Iceberg table {table_name} in {database}.")
            glue_client.create_table(DatabaseName=database, TableInput=table_input)
        else:
            raise

def sync_table_partitions(source_database, target_database, table_name, glue_client):
    """Synchronizes partitions for Iceberg tables in Glue."""
    paginator = glue_client.get_paginator('get_partitions')
    page_iterator = paginator.paginate(DatabaseName=source_database, TableName=table_name)

    source_partitions = set()
    for page in page_iterator:
        for partition in page['Partitions']:
            partition_values = tuple(partition['Values'])
            source_partitions.add(partition_values)
            try:
                glue_client.get_partition(DatabaseName=target_database, TableName=table_name, PartitionValues=partition_values)
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    print(f"Creating partition {partition_values} in table {table_name}.")
                    glue_client.batch_create_partition(
                        DatabaseName=target_database,
                        TableName=table_name,
                        PartitionInputList=[{
                            'Values': partition_values,
                            'StorageDescriptor': partition.get('StorageDescriptor', {}),
                            'Parameters': partition.get('Parameters', {})
                        }]
                    )

    # Remove partitions in target that are not in source
    paginator = glue_client.get_paginator('get_partitions')
    page_iterator = paginator.paginate(DatabaseName=target_database, TableName=table_name)
    for page in page_iterator:
        for partition in page['Partitions']:
            partition_values = tuple(partition['Values'])
            if partition_values not in source_partitions:
                print(f"Deleting partition {partition_values} from table {table_name} in target database.")
                glue_client.delete_partition(DatabaseName=target_database, TableName=table_name, PartitionValues=list(partition_values))

def copy_iceberg_tables(source_database, target_database, s3_bucket, glue_client):
    """Discovers and copies multiple Iceberg tables from source to target Glue database."""
    paginator = glue_client.get_paginator('get_tables')
    page_iterator = paginator.paginate(DatabaseName=source_database)

    for page in page_iterator:
        for table in page['TableList']:
            table_name = table['Name']
            parameters = table.get('Parameters', {})

            # Only process Iceberg tables
            if parameters.get('table_type') == 'ICEBERG':
                print(f"Processing Iceberg table: {table_name}")

                # Extract storage location
                s3_location = parameters.get('location', f's3://{s3_bucket}/{table_name}/')

                # Extract metadata_location (ensuring it exists)
                metadata_location = parameters.get('metadata_location', f'{s3_location}/metadata/metadata.json')

                # Extract table parameters (to ensure all metadata is preserved)
                table_parameters = parameters.copy()  # ✅ Preserve all table properties

                # Extract column schema from source table
                columns = table.get('StorageDescriptor', {}).get('Columns', [])  # ✅ Copy column schema

                # Get storage descriptor with correct columns
                storage_descriptor = get_iceberg_storage_descriptor(s3_location, columns)

                # Extract partition keys
                partition_keys = table.get('PartitionKeys', [])

                # Register or update table with metadata location and schema
                create_or_update_table(target_database, table_name, storage_descriptor, partition_keys, metadata_location, table_parameters, glue_client)

                # Sync partitions
                sync_table_partitions(source_database, target_database, table_name, glue_client)

if __name__ == "__main__":
    source_db = 'common_data'
    target_db = 'local-common-data'
    s3_bucket = 'your-iceberg-bucket-name'
    region = 'us-east-1'

    glue_client = boto3.client('glue', region_name=region)

    copy_iceberg_tables(source_db, target_db, s3_bucket, glue_client)
