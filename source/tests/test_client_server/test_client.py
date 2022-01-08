import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from hashback.http_client import ClientSession
from hashback.protocol import ClientConfiguration, InternalServerError
from hashback.server import SERVER_VERSION


def test_login(client: ClientSession, client_config: ClientConfiguration, mock_local_db):
    assert client.client_config == client_config
    assert {'client_id_or_name': str(client_config.client_id)} in \
           (call.kwargs for call in mock_local_db.open_client_session.mock_calls)


def test_server_version(client: ClientSession):
    # pylint: disable=protected-access
    result = asyncio.get_event_loop().run_until_complete(client._client.server_version())
    assert result == SERVER_VERSION
    # This ensures the server version came from the server and NOT the client
    assert result is not SERVER_VERSION


@pytest.mark.parametrize('exc', (ValueError, OSError))
def test_internal_server_error(client: ClientSession, mock_local_db, exc):
    mock_local_db.get_backup = AsyncMock(side_effect=exc)
    with pytest.raises(InternalServerError):
        asyncio.get_event_loop().run_until_complete(client.get_backup(backup_date=datetime.now(timezone.utc)))
