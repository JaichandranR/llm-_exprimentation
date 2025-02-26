import boto3
from botocore.exceptions import ClientError

def partitions_are_different(existing_partition, new_partition):
    # Compare relevant fields to determine if the partitions are different
    return existing_partition.get('StorageDescriptor') != new_partition.get('StorageDescriptor')

def sync_table_partitions(source_database, target_database, table_name, glue_client):
    paginator = glue_client.get_paginator('get_partitions')
    page_iterator = paginator.paginate(DatabaseName=source_database, TableName=table_name)

    source_partitions = set()
    for page in page_iterator:
        partitions = page['Partitions']
        for partition in partitions:
            partition_values = partition['Values']
            source_partitions.add(tuple(partition_values))
            try:
                # Check if the partition already exists in the target database
                existing_partition = glue_client.get_partition(
                    DatabaseName=target_database,
                    TableName=table_name,
                    PartitionValues=partition_values
                )['Partition']

                # Compare existing partition with new partition
                if partitions_are_different(existing_partition, partition):
                    print(f"Updating partition {partition_values} in table {table_name}.")
                    glue_client.delete_partition(
                        DatabaseName=target_database,
                        TableName=table_name,
                        PartitionValues=partition_values
                    )
                    glue_client.batch_create_partition(
                        DatabaseName=target_database,
                        TableName=table_name,
                        PartitionInputList=[{
                            'Values': partition_values,
                            'StorageDescriptor': partition.get('StorageDescriptor', {}),
                            'Parameters': {**partition.get('Parameters', {}), "lakeformation.table": "true"}
                        }]
                    )
                else:
                    print(f"Partition {partition_values} in table {table_name} is already up-to-date.")

            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    # If the partition does not exist, create it
                    print(f"Creating partition {partition_values} in table {table_name}.")
                    glue_client.batch_create_partition(
                        DatabaseName=target_database,
                        TableName=table_name,
                        PartitionInputList=[{
                            'Values': partition_values,
                            'StorageDescriptor': partition.get('StorageDescriptor', {}),
                            'Parameters': {**partition.get('Parameters', {}), "lakeformation.table": "true"}
                        }]
                    )
                else:
                    raise

    # Delete partitions in the target that are not in the source
    paginator = glue_client.get_paginator('get_partitions')
    page_iterator = paginator.paginate(DatabaseName=target_database, TableName=table_name)
    for page in page_iterator:
        for partition in page['Partitions']:
            partition_values = tuple(partition['Values'])
            if partition_values not in source_partitions:
                print(f"Deleting partition {partition_values} from table {table_name} in target database.")
                glue_client.delete_partition(
                    DatabaseName=target_database,
                    TableName=table_name,
                    PartitionValues=list(partition_values)
                )

def copy_table_definitions(source_database, target_database, region_name='us-west-2'):
    glue_client = boto3.client('glue', region_name=region_name)

    # Get the list of tables in the source database
    source_tables = set()
    paginator = glue_client.get_paginator('get_tables')
    page_iterator = paginator.paginate(DatabaseName=source_database)
    for page in page_iterator:
        for table in page['TableList']:
            table_name = table['Name']
            source_tables.add(table_name)
            print(f"Processing table: {table_name}")

            # Check if StorageDescriptor exists
            if 'StorageDescriptor' not in table:
                print(f"Warning: Table {table_name} does not have a StorageDescriptor. Skipping.")
                continue

            # Prepare the TableInput dictionary
            table_input = {
                'Name': table_name,
                'TableType': table.get('TableType', 'EXTERNAL_TABLE'),
                'StorageDescriptor': table['StorageDescriptor'],
                'Parameters': {**table.get('Parameters', {}), "lakeformation.table": "true"},
                'PartitionKeys': table.get('PartitionKeys', [])
            }

            try:
                # Check if the table already exists in the target database
                glue_client.get_table(DatabaseName=target_database, Name=table_name)
                # If it exists, update the table
                print(f"Updating table: {table_name}")
                glue_client.update_table(DatabaseName=target_database, TableInput=table_input)
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    # If the table does not exist, create it
                    print(f"Creating table: {table_name}")
                    glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
                else:
                    raise

            # Sync partitions
            sync_table_partitions(source_database, target_database, table_name, glue_client)

if __name__ == '__main__':
    source_db = 'common_data'
    target_db = 'local-common-data'
    region = 'us-east-1'  # e.g., 'us-west-2'

    copy_table_definitions(source_db, target_db, region)
