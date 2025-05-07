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

        # Create separate paginator mocks for source and target
        src_paginator = mock.MagicMock()
        tgt_paginator = mock.MagicMock()

        src_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]
        tgt_paginator.paginate.return_value = [{'Partitions': []}]

        # Setup side effect so it returns different paginator based on call
        def get_paginator_side_effect(op):
            return src_paginator if op == 'get_partitions' else tgt_paginator

        glue.get_paginator.side_effect = get_paginator_side_effect

        glue.batch_create_partition = mock.MagicMock()

        # Call the function
        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        # Assert batch_create_partition is called
        glue.batch_create_partition.assert_called_once()
