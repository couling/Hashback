from datetime import timedelta
from uuid import uuid4

import pytest

from hashback.protocol import ClientConfiguration, ClientConfiguredBackupDirectory, Filter, FilterType


@pytest.fixture(scope='function')
def client_config(tmp_path) -> ClientConfiguration:
    return ClientConfiguration(
        client_name='Test Client',
        client_id=uuid4(),
        backup_granularity=timedelta(days=1),
        backup_directories={
            'test': ClientConfiguredBackupDirectory(
                base_path=str(tmp_path / 'test_root'),
                filters=[Filter(filter=FilterType.EXCLUDE, path='exclude')],
            )
        }
    )
