# pylint: disable=redefined-outer-name
import asyncio
import importlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock
from uuid import uuid4

import pytest
import requests
from starlette.testclient import TestClient

from hashback.http_client import ClientSession, BasicAuthClient
from hashback.protocol import ClientConfiguration, BackupSessionConfig, BackupSession
from hashback.server import cache, config, app
from tests.test_client_server.constants import SERVER_PROPERTIES


@pytest.fixture()
def mock_local_db(mock_session: MagicMock) -> MagicMock:
    mock_db = MagicMock()
    mock_db.open_client_session.return_value = mock_session
    return mock_db


@pytest.fixture()
def mock_session(client_config: ClientConfiguration, mock_backup_session) -> MagicMock:
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


@pytest.fixture()
def client_backup_session(client: ClientSession) -> BackupSession:
    return asyncio.get_event_loop().run_until_complete(client.start_backup(
        backup_date=datetime.now(timezone.utc),
        allow_overwrite=True,
        description='a new test backup',
    ))