        glue = mock.MagicMock()

        mock_partition = {
            'Values': ['2024'],
            'StorageDescriptor': {'Location': 's3://some-location/2024'},
            'Parameters': {}
        }

        glue.batch_create_partition = mock.MagicMock()

        def get_paginator_side_effect(op):
            paginator = mock.MagicMock()
            if op == 'get_partitions':
                paginator.paginate.side_effect = lambda **kwargs: (
                    [{'Partitions': [mock_partition]}] if kwargs['DatabaseName'] == source_db
                    else [{'Partitions': []}]
                )
            return paginator

        glue.get_paginator.side_effect = get_paginator_side_effect

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        glue.batch_create_partition.assert_called_once()
