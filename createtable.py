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

    # Patch batch_create_partition with the correct binding
    with patch.object(glue, 'batch_create_partition') as batch_mock:
        source_paginator = mock.MagicMock()
        target_paginator = mock.MagicMock()

        source_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]
        target_paginator.paginate.return_value = [{'Partitions': []}]

        def get_paginator(name):
            return source_paginator if name == 'get_partitions' else target_paginator

        glue.get_paginator.side_effect = lambda op: source_paginator if op == 'get_partitions' else target_paginator

        common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

        batch_mock.assert_called_once()
