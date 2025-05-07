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

    # Create separate paginator mocks
    src_paginator = mock.MagicMock()
    tgt_paginator = mock.MagicMock()

    src_paginator.paginate.return_value = [{'Partitions': [mock_partition]}]
    tgt_paginator.paginate.return_value = [{'Partitions': []}]

    # Return different paginator based on which DB is passed
    def get_paginator(name):
        paginator = mock.MagicMock()
        if name == "get_partitions":
            def paginate(**kwargs):
                if kwargs['DatabaseName'] == source_db:
                    return [{'Partitions': [mock_partition]}]
                else:
                    return [{'Partitions': []}]
            paginator.paginate.side_effect = paginate
        return paginator

    glue.get_paginator.side_effect = get_paginator

    common_data_sync.sync_table_partitions(source_db, target_db, table_name, glue)

    glue.batch_create_partition.assert_called_once()
