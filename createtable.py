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

        # Mock get_paginator to return different partitions for source and target
        glue.get_paginator.return_value.paginate.side_effect = [
            [{'Partitions': [mock_partition]}],  # Source partitions
            [{'Partitions': []}]                 # Target partitions
        ]

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        # Simple direct assertion
        self.assertTrue(glue.batch_create_partition.called)

if __name__ == "__main__":
    unittest.main()
