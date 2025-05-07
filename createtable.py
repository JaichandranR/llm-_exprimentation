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

        def get_paginator(operation_name):
            paginator = mock.MagicMock()

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

if __name__ == "__main__":
    unittest.main()
