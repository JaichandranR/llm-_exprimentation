...
    def test_sync_table_partitions(self):
        source_db = 'src'
        target_db = 'tgt'
        table_name = 'test_table'

        mock_partition = {
            'Values': ['2024'],
            'StorageDescriptor': {'Location': 's3://some-location/2024'},
            'Parameters': {}
        }

        glue = mock.MagicMock()
        glue.batch_create_partition = mock.MagicMock()

        paginator = mock.MagicMock()

        def paginate_side_effect(**kwargs):
            db_name = kwargs.get('DatabaseName')
            if db_name == source_db:
                return [{'Partitions': [mock_partition]}]
            elif db_name == target_db:
                return [{'Partitions': []}]
            return []

        paginator.paginate.side_effect = paginate_side_effect
        glue.get_paginator.return_value = paginator

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        glue.batch_create_partition.assert_called()

if __name__ == "__main__":
    unittest.main()
