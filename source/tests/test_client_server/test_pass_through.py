# pylint: disable=redefined-outer-name
"""
Note: These tests are all intended to test both the client and server.  Primarily they are pass-through tests which show
that a call on the client will result in a similar call to the underlying database server side.
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock
from uuid import uuid4

import pytest

from hashback import protocol
from hashback.http_client import ClientSession
from hashback.protocol import ClientConfiguration, BackupSession
from tests.test_client_server.constants import EXAMPLE_DIR


def test_login(client: ClientSession, client_config: ClientConfiguration, mock_local_db):
    assert client.client_config == client_config
    assert {'client_id_or_name': 'test_user'} in (call.kwargs for call in mock_local_db.open_client_session.mock_calls)


@pytest.mark.parametrize('allow_overwrite', (False, True))
def test_start_backup(client: ClientSession, allow_overwrite: bool, mock_session):
    backup_date = datetime.now(timezone.utc)
    description = 'a new test backup'
    backup_session: BackupSession = asyncio.get_event_loop().run_until_complete(client.start_backup(
        backup_date=backup_date,
        allow_overwrite=allow_overwrite,
        description='a new test backup',
    ))
    assert backup_session.server_session is client
    assert backup_session.is_open
    assert backup_session.config.client_id == client.client_config.client_id
    assert {'backup_date':backup_date, 'allow_overwrite': allow_overwrite, 'description': description} in (
        call.kwargs for call in mock_session.start_backup.mock_calls)


def test_start_backup_duplicate(client: ClientSession, mock_session):
    mock_session.start_backup = AsyncMock(side_effect=protocol.DuplicateBackup)
    backup_date = datetime.now(timezone.utc)
    with pytest.raises(protocol.DuplicateBackup):
        asyncio.get_event_loop().run_until_complete(client.start_backup(
            backup_date=backup_date,
            allow_overwrite=False,
            description='a new test backup',
        ))


@pytest.mark.parametrize('params', ({'backup_date': datetime.now(timezone.utc)}, {'session_id':  uuid4()}), ids=str)
def test_resume_backup(client: ClientSession, params, mock_session):
    backup_session: BackupSession = asyncio.get_event_loop().run_until_complete(client.resume_backup(**params))
    params.setdefault('backup_date', None)
    params.setdefault('session_id', None)
    assert backup_session.server_session is client
    assert backup_session.is_open
    assert params in (call.kwargs for call in mock_session.resume_backup.mock_calls)


@pytest.mark.parametrize('params', ({'backup_date': datetime.now(timezone.utc)}, {'session_id':  uuid4()}), ids=str)
def test_resume_backup_not_exists(client: ClientSession, params, mock_session):
    mock_session.resume_backup = AsyncMock(side_effect=protocol.NotFoundException)
    with pytest.raises(protocol.NotFoundException):
        asyncio.get_event_loop().run_until_complete(client.resume_backup(**params))


@pytest.mark.parametrize('backup_date', (datetime.now(timezone.utc), None))
def test_get_backup(client: ClientSession, mock_session, backup_date, client_config: ClientConfiguration):
    example_backup = protocol.Backup(
        client_id=client_config.client_id,
        client_name=client_config.client_name,
        backup_date=datetime.now(timezone.utc),
        started=datetime.now(timezone.utc) - timedelta(minutes=20),
        completed=datetime.now(timezone.utc) - timedelta(minutes=10),
        roots={},
        description='example backup',
    )
    mock_session.get_backup = AsyncMock(return_value=example_backup)
    backup: protocol.Backup = asyncio.get_event_loop().run_until_complete(client.get_backup(backup_date))
    if backup_date:
        assert {'backup_date': backup_date} in (call.kwargs for call in mock_session.get_backup.mock_calls)
    else:
        assert (
            ({'backup_date': None} in (call.kwargs for call in mock_session.get_backup.mock_calls))
            or ({} in (call.kwargs for call in mock_session.get_backup.mock_calls))
        )

    assert backup == example_backup
    assert backup is not example_backup


def test_get_backup_not_found(client: ClientSession, mock_session):
    mock_session.get_backup = AsyncMock(side_effect=protocol.NotFoundException)
    with pytest.raises(protocol.NotFoundException):
        asyncio.get_event_loop().run_until_complete(client.get_backup(datetime.now(timezone.utc)))


def test_get_dir(client: ClientSession, mock_session):
    directory_inode = protocol.Inode(
        modified_time=datetime.now(timezone.utc) - timedelta(days=365),
        type = protocol.FileType.DIRECTORY,
        mode=0o755,
        size=599,
        uid=1000,
        gid=1001,
        hash="bbbb",
    )
    mock_session.get_directory = AsyncMock(return_value=EXAMPLE_DIR)
    directory: protocol.Directory = asyncio.get_event_loop().run_until_complete(client.get_directory(directory_inode))
    for call in mock_session.get_directory.mock_calls:
        assert call.kwargs['inode'].hash == directory_inode.hash
        break
    else:
        assert False, "No mock calls"
    assert directory == EXAMPLE_DIR
    assert directory is not EXAMPLE_DIR


@pytest.mark.parametrize('streaming', (True, False))
def test_get_file(client: ClientSession, mock_session, streaming):
    async def read_file():
        with await client.get_file(file_inode) as file:
            assert file.file_size == (None if streaming else len(content))
            assert await file.read() == content

    content = b"this is a test"
    content_reader = iter((content[:4], content[4:], bytes()))
    mock_file = MagicMock()
    mock_file.read = AsyncMock(side_effect=lambda _: next(content_reader))
    mock_file.file_size = None if streaming else len(content)

    file_inode = protocol.Inode(
        modified_time=datetime.now(timezone.utc) - timedelta(days=365),
        type = protocol.FileType.REGULAR,
        mode=0o755,
        size=599,
        uid=1000,
        gid=1001,
        hash=protocol.hash_content(content),
    )

    mock_session.get_file = AsyncMock(return_value=mock_file)
    asyncio.get_event_loop().run_until_complete(read_file())
    assert len(mock_file.close.mock_calls) == 0


@pytest.mark.parametrize('file_type', (protocol.FileType.REGULAR, protocol.FileType.LINK))
@pytest.mark.parametrize('streaming', (True, False))
def test_get_file_restore(client: ClientSession, mock_session, streaming, tmp_path: Path, file_type):
    target_path = tmp_path / 'test_file.txt'
    content = b"this is a test"
    content_reader = iter((content[:4], content[4:], bytes()))
    mock_file = MagicMock()
    mock_file.read = AsyncMock(side_effect=lambda _: next(content_reader))
    mock_file.file_size = None if streaming else len(content)

    modified_time = datetime.now(timezone.utc) - timedelta(days=365)
    modified_time = modified_time.replace(microsecond=0)

    file_inode = protocol.Inode(
        modified_time=modified_time,
        type=file_type,
        mode=0o755,
        size=599,
        uid=1000,
        gid=1001,
        hash=protocol.hash_content(content),
    )
    target_path.parent.mkdir(exist_ok=True, parents=True)
    mock_session.get_file = AsyncMock(return_value=mock_file)
    asyncio.get_event_loop().run_until_complete(client.get_file(
        inode=file_inode,
        target_path=target_path,
        restore_permissions=False,
        restore_owner=False,
    ))
    if file_type is protocol.FileType.REGULAR:
        with target_path.open('rb') as file:
            restored_content = file.read()
        assert restored_content == content
        assert target_path.stat().st_mtime == modified_time.timestamp()
    else:
        restored_content = os.readlink(target_path).encode()
        assert restored_content == content


@pytest.mark.parametrize('expected_result', (protocol.DirectoryDefResponse(missing_ref=uuid4(), missing_files=["aaaa"]),
                                             protocol.DirectoryDefResponse(ref_hash='bbbb'),
                                             protocol.DirectoryDefResponse(missing_files=['aaaa']),))
@pytest.mark.parametrize('replaces', (None, uuid4()))
def test_directory_def(client_backup_session: BackupSession, mock_backup_session, replaces, expected_result):
    mock_backup_session.directory_def = AsyncMock(return_value=expected_result)
    result = asyncio.get_event_loop().run_until_complete(client_backup_session.directory_def(EXAMPLE_DIR, replaces))

    assert expected_result == result
    assert expected_result is not result
