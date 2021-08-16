# pylint: disable=redefined-outer-name

import asyncio
import importlib
from unittest.mock import MagicMock

import pytest
import requests
from fastapi.testclient import TestClient

from hashback.http_client import BasicAuthClient, ClientSession
from hashback.http_protocol import ServerProperties
from hashback.protocol import ClientConfiguration
from hashback.server import app, config, cache


SERVER_PROPERTIES = ServerProperties.parse_url('http://test_user:password@example.com')


@pytest.fixture()
def mock_local_db(mock_session: MagicMock) -> MagicMock:
    mock_db = MagicMock()
    mock_db.open_client_session.return_value = mock_session
    return mock_db

@pytest.fixture()
def mock_session(client_config: ClientConfiguration, mock_backup_session: MagicMock) -> MagicMock:
    session = MagicMock()
    session.start_backup.return_value = mock_backup_session
    session.resume_backup.return_value = mock_backup_session
    session.client_config = client_config
    return session

@pytest.fixture()
def mock_backup_session() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def mock_server(monkeypatch: pytest.MonkeyPatch, mock_local_db: MagicMock) -> TestClient:
    importlib.reload(cache)
    monkeypatch.setattr(config, 'LOCAL_DATABASE', mock_local_db)
    result = TestClient(app.app)
    # The test client has a broken close() method
    TestClient.close = lambda _: None
    return result


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch, mock_server: TestClient) -> ClientSession:
    monkeypatch.setattr(requests, 'Session', lambda: mock_server)
    with BasicAuthClient(SERVER_PROPERTIES) as client:
        yield asyncio.get_event_loop().run_until_complete(ClientSession.create_session(client))


def test_hello(client: ClientSession, client_config: ClientConfiguration, mock_local_db):
    assert client.client_config == client_config
    assert {'client_id_or_name': 'test_user'} in (call.kwargs for call in mock_local_db.open_client_session.mock_calls)
