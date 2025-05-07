        glue = mock.MagicMock()

        def paginate_side_effect(**kwargs):
            if kwargs['DatabaseName'] == source_db:
                return [{'Partitions': [mock_partition]}]
            else:
                return [{'Partitions': []}]

        paginator = mock.MagicMock()
        paginator.paginate.side_effect = paginate_side_effect

        glue.get_paginator.return_value = paginator
