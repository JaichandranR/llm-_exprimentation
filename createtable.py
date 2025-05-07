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

        src_paginator = mock.MagicMock()
        tgt_paginator = mock.MagicMock()

        src_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]
        tgt_paginator.paginate.return_value = [{'Partitions': []}]

        def get_paginator_side_effect(op):
            if op == 'get_partitions':
                # simulate source and target paginator use
                return mock.MagicMock(paginate=mock.MagicMock(side_effect=lambda **kwargs: [{'Partitions': [mock_partition]}] if kwargs['DatabaseName'] == source_db else [{'Partitions': []}]))
            return mock.MagicMock()

        glue.get_paginator.side_effect = get_paginator_side_effect

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        assert glue.batch_create_partition.call_count == 1

if __name__ == "__main__":
    unittest.main()
