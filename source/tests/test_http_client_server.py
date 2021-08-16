# pylint: disable=redefined-outer-name

import asyncio
import importlib
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, AsyncMock
from uuid import uuid4

import pytest
import requests
from fastapi.testclient import TestClient

from hashback.http_client import BasicAuthClient, ClientSession
from hashback.http_protocol import ServerProperties
from hashback.protocol import ClientConfiguration, BackupSession, BackupSessionConfig
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
    session.start_backup = AsyncMock(return_value=mock_backup_session)
    session.resume_backup = AsyncMock(return_value=mock_backup_session)
    session.client_config = client_config
    return session


@pytest.fixture()
def mock_backup_session(client_config: ClientConfiguration) -> MagicMock:
    session = MagicMock()
    session.config = BackupSessionConfig(
        client_id=client_config.client_id,
        session_id=uuid4(),
        backup_date=datetime.now(timezone.utc),
        started=datetime.now(timezone.utc),
        allow_overwrite=True,
        description='Something different',
    )
    return session


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


def test_login(client: ClientSession, client_config: ClientConfiguration, mock_local_db):
    assert client.client_config == client_config
    assert {'client_id_or_name': 'test_user'} in (call.kwargs for call in mock_local_db.open_client_session.mock_calls)


@pytest.mark.parametrize('allow_overwrite', (False, True))
def test_start_backup_pass_through(client: ClientSession, allow_overwrite: bool, mock_session):
    backup_date = datetime.now(timezone.utc)
    description = 'a new test backup'
    backup_session: BackupSession = asyncio.get_event_loop().run_until_complete(client.start_backup(
        backup_date=backup_date,
        allow_overwrite=allow_overwrite,
        description='a new test backup',
    ))
    assert backup_session.config.client_id == client.client_config.client_id
    assert {'backup_date':backup_date, 'allow_overwrite': allow_overwrite, 'description': description} in (
        call.kwargs for call in mock_session.start_backup.mock_calls)


@pytest.mark.parametrize('params', ({'backup_date': datetime.now(timezone.utc)}, {'session_id':  uuid4()}),
                         ids=lambda x: str(x))
def test_resume_backup_pass_through(client: ClientSession, params, mock_session):
    backup_session: BackupSession = asyncio.get_event_loop().run_until_complete(client.resume_backup(**params))
    #assert backup_session.config.client_id == client.client_config.client_id
    params.setdefault('backup_date', None)
    params.setdefault('session_id', None)
    assert params in (call.kwargs for call in mock_session.resume_backup.mock_calls)
