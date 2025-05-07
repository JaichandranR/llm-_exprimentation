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

        # Mock paginators for source and target
        source_paginator = mock.MagicMock()
        target_paginator = mock.MagicMock()

        source_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]
        target_paginator.paginate.return_value = [{'Partitions': []}]

        def get_paginator(op):
            paginator = mock.MagicMock()
            paginator.paginate.side_effect = lambda **kwargs: source_paginator.paginate() if kwargs['DatabaseName'] == source_db else target_paginator.paginate()
            return paginator

        glue.get_paginator.side_effect = get_paginator

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        self.assertTrue(glue.batch_create_partition.called)

if __name__ == "__main__":
    unittest.main()
