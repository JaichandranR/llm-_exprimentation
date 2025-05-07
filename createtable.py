import sys
import types
import unittest
from unittest import mock
from botocore.exceptions import ClientError

# Set fake CLI args
sys.argv = [
    "common_data_sync.py",
    "--source_db", "dummy_src",
    "--target_db", "dummy_target",
    "--region", "us-east-1",
    "--s3_bucket", "dummy-bucket",
    "--s3_prefix", "dummy-prefix"
]

# Create mock awsglue and pyspark modules
awsglue = types.ModuleType("awsglue")
awsglue_context = types.ModuleType("awsglue.context")
awsglue_utils = types.ModuleType("awsglue.utils")
pyspark_context = types.ModuleType("pyspark.context")
pyspark = types.SimpleNamespace(SparkContext=mock.MagicMock())

awsglue_context.GlueContext = mock.MagicMock()
awsglue_utils.getResolvedOptions = mock.MagicMock(return_value={
    "source_db": "dummy_src",
    "target_db": "dummy_target",
    "region": "us-east-1",
    "s3_bucket": "dummy-bucket",
    "s3_prefix": "dummy-prefix"
})
pyspark_context.SparkContext = mock.MagicMock()

sys.modules["awsglue"] = awsglue
sys.modules["awsglue.context"] = awsglue_context
sys.modules["awsglue.utils"] = awsglue_utils
sys.modules["pyspark"] = pyspark
sys.modules["pyspark.context"] = pyspark_context

# Create mock Glue client + paginator selector
mock_glue_client = mock.MagicMock()

# get_tables paginator
mock_get_tables_paginator = mock.MagicMock()
mock_get_tables_paginator.paginate.return_value = [
    {
        "TableList": [
            {
                'Name': 'test_table',
                'StorageDescriptor': {
                    'Location': 's3://mock-location/',
                    'Columns': [],
                    'SerdeInfo': {}
                },
                'PartitionKeys': [{'Name': 'year', 'Type': 'string'}],
                'Parameters': {
                    'classification': 'json',
                    'table_type': 'OTHER',
                    'metadata_location': 's3://mock-location/test_table/metadata'
                }
            }
        ]
    }
]

# get_partitions paginator
mock_get_partitions_paginator = mock.MagicMock()
mock_get_partitions_paginator.paginate.return_value = [
    {
        "Partitions": [
            {
                "Values": ["2024"],
                "StorageDescriptor": {"Location": "s3://mock-location/year=2024"},
                "Parameters": {}
            }
        ]
    }
]

# Return correct paginator based on operation name
mock_glue_client.get_paginator.side_effect = lambda op: {
    "get_tables": mock_get_tables_paginator,
    "get_partitions": mock_get_partitions_paginator
}[op]

# Patch boto3 globally
mock_boto3 = mock.MagicMock()
mock_boto3.client.return_value = mock_glue_client
sys.modules["boto3"] = mock_boto3

# Import target after mocking
from src.main.python import common_data_sync

class TestCommonDataSync(unittest.TestCase):

    def setUp(self):
        common_data_sync.summary_table['processed_tables'] = 0
        common_data_sync.summary_table['failed_tables'] = 0
        common_data_sync.summary_table['deleted_tables'] = 0
        common_data_sync.summary_table['errors'] = []

    def test_copy_tables_with_partitions(self):
        with mock.patch("src.main.python.common_data_sync.create_or_update_table") as mock_create, \
             mock.patch("src.main.python.common_data_sync.sync_table_partitions") as mock_sync, \
             mock.patch("src.main.python.common_data_sync.delete_orphan_tables") as mock_delete:

            common_data_sync.copy_tables()

            mock_create.assert_called_once()
            mock_sync.assert_called_once()
            mock_delete.assert_called_once()

        self.assertEqual(common_data_sync.summary_table['processed_tables'], 1)
        self.assertEqual(common_data_sync.summary_table['failed_tables'], 0)
        self.assertEqual(common_data_sync.summary_table['deleted_tables'], 0)
        self.assertEqual(common_data_sync.summary_table['errors'], [])

    def test_delete_orphan_tables_success(self):
        source_tables = ['table1', 'table2']

        mock_page = {
            "TableList": [
                {"Name": "table3"},
                {"Name": "table2"}
            ]
        }

        paginator = mock.MagicMock()
        paginator.paginate.return_value = [mock_page]

        mock_glue = mock.MagicMock()
        mock_glue.get_paginator.return_value = paginator

        with mock.patch("src.main.python.common_data_sync.glue_client", mock_glue):
            common_data_sync.delete_orphan_tables(source_tables)

        mock_glue.delete_table.assert_called_once_with(
            DatabaseName='dummy_target',
            Name='table3'
        )
        self.assertEqual(common_data_sync.summary_table['deleted_tables'], 1)
        self.assertEqual(len(common_data_sync.summary_table['errors']), 0)

    def test_delete_orphan_tables_with_client_error(self):
        source_tables = []

        mock_page = {
            "TableList": [{"Name": "tableX"}]
        }

        paginator = mock.MagicMock()
        paginator.paginate.return_value = [mock_page]

        mock_glue = mock.MagicMock()
        mock_glue.get_paginator.return_value = paginator
        mock_glue.delete_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            operation_name='DeleteTable'
        )

        with mock.patch("src.main.python.common_data_sync.glue_client", mock_glue):
            common_data_sync.delete_orphan_tables(source_tables)

        mock_glue.delete_table.assert_called_once()
        self.assertEqual(common_data_sync.summary_table['deleted_tables'], 0)
        self.assertEqual(len(common_data_sync.summary_table['errors']), 1)
        self.assertIn('tableX', common_data_sync.summary_table['errors'][0]['table_name'])

    def test_create_or_update_table(self):
        table_name = 'my_table'
        metadata_location = 's3://some-location/meta/'
        storage_descriptor = {'Location': metadata_location, 'Columns': []}
        partition_keys = [{'Name': 'year', 'Type': 'string'}]
        table_params = {'param1': 'value1'}

        glue = mock.MagicMock()
        glue.get_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'EntityNotFoundException'}},
            operation_name='GetTable'
        )

        common_data_sync.create_or_update_table(
            'my_db', table_name, storage_descriptor,
            partition_keys, metadata_location, table_params, glue
        )

        glue.create_table.assert_called_once()

    def test_sync_table_partitions(self):
        source_db = 'src'
        target_db = 'tgt'
        table_name = 'test_table'

        mock_partition = {
            'Values': ['2024'],
            'StorageDescriptor': {'Location': 's3://some-location/2024'},
            'Parameters': {}
        }

        src_paginator = mock.MagicMock()
        src_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]

        tgt_paginator = mock.MagicMock()
        tgt_paginator.paginate.return_value = [{'Partitions': []}]

        glue = mock.MagicMock()
        glue.get_paginator.side_effect = lambda op: {
            'get_partitions': src_paginator if op == 'get_partitions' else tgt_paginator
        }[op]

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        glue.batch_create_partition.assert_called_once()

if __name__ == "__main__":
    unittest.main()
