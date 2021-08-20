# pylint: disable=redefined-outer-name
"""
Note: These tests are all intended to test both the client and server.  Primarily they are pass-through tests which show
that a call on the client will result in a similar call to the underlying database server side.
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, AsyncMock
from uuid import uuid4

import pytest

from hashback import protocol
from hashback.http_client import ClientSession
from hashback.protocol import ClientConfiguration, BackupSession
from .constants import EXAMPLE_DIR, EXAMPLE_DIR_INODE


class BaseTestPassThrough:

    mock_backend_session: MagicMock

    def _run_and_check_pass_through(self, method, return_value=None, check_return_value=True, side_effect=None):
        method_name = method.cr_frame.f_code.co_name
        kwargs = {key: value for key, value in method.cr_frame.f_locals.items() if key != 'self'}
        # Setup the mocked server backend to have this method read to respond as requested
        if side_effect is not None:
            setattr(self.mock_backend_session, method_name, AsyncMock(side_effect=side_effect))
        else:
            setattr(self.mock_backend_session, method_name, AsyncMock(return_value=return_value))

        # Run the coroutine
        result = asyncio.get_event_loop().run_until_complete(method)

        # Check the return value was passed through correctly
        if check_return_value:
            if return_value is not None:
                assert return_value == result
                assert return_value is not result
            else:
                assert result is None

        # Check the call made it through the client and server side to server backend (mock)
        assert kwargs in (call.kwargs for call in getattr(self.mock_backend_session, method.__name__).mock_calls), \
            "Call kwargs not passed through correctly"

        return result


class TestPassThroughServerSession(BaseTestPassThrough):

    client_session: ClientSession

    @pytest.fixture(autouse=True)
    def _client_backup_session(self, client, mock_session):
        self.client_session = client
        self.mock_backend_session = mock_session

    @pytest.mark.parametrize('allow_overwrite', (False, True))
    def test_start_backup(self, allow_overwrite: bool):
        backup_date = datetime.now(timezone.utc)
        description = 'a new test backup'
        backup_session: BackupSession = asyncio.get_event_loop().run_until_complete(self.client_session.start_backup(
            backup_date=backup_date,
            allow_overwrite=allow_overwrite,
            description='a new test backup',
        ))
        assert backup_session.server_session is self.client_session
        assert backup_session.is_open
        assert backup_session.config.client_id == self.client_session.client_config.client_id
        assert {'backup_date': backup_date, 'allow_overwrite': allow_overwrite, 'description': description} in (
            call.kwargs for call in self.mock_backend_session.start_backup.mock_calls)

    def test_start_backup_duplicate(self):
        self.mock_backend_session.start_backup = AsyncMock(side_effect=protocol.DuplicateBackup)
        backup_date = datetime.now(timezone.utc)
        with pytest.raises(protocol.DuplicateBackup):
            asyncio.get_event_loop().run_until_complete(self.client_session.start_backup(
                backup_date=backup_date,
                allow_overwrite=False,
                description='a new test backup',
            ))

    @pytest.mark.parametrize('params', ({'backup_date': datetime.now(timezone.utc)}, {'session_id': uuid4()}), ids=str)
    def test_resume_backup(self, params):
        backup_session = asyncio.get_event_loop().run_until_complete(self.client_session.resume_backup(**params))
        params.setdefault('backup_date', None)
        params.setdefault('session_id', None)
        assert backup_session.server_session is self.client_session
        assert backup_session.is_open
        assert params in (call.kwargs for call in self.mock_backend_session.resume_backup.mock_calls)

    @pytest.mark.parametrize('params', ({'backup_date': datetime.now(timezone.utc)}, {'session_id':  uuid4()}), ids=str)
    def test_resume_backup_not_exists(self, params):
        with pytest.raises(protocol.NotFoundException):
            self._run_and_check_pass_through(self.client_session.resume_backup(**params),
                                             side_effect=protocol.NotFoundException)

    @pytest.mark.parametrize('backup_date', (datetime.now(timezone.utc), None))
    def test_get_backup(self, backup_date, client_config: ClientConfiguration):
        self._run_and_check_pass_through(
            self.client_session.get_backup(backup_date),
            return_value=protocol.Backup(
                client_id=client_config.client_id,
                client_name=client_config.client_name,
                backup_date=datetime.now(timezone.utc),
                started=datetime.now(timezone.utc) - timedelta(minutes=20),
                completed=datetime.now(timezone.utc) - timedelta(minutes=10),
                roots={},
                description='example backup',
            )
        )

    def test_get_backup_not_found(self):
        with pytest.raises(protocol.NotFoundException):
            self._run_and_check_pass_through(self.client_session.get_backup(datetime.now(timezone.utc)),
                                             side_effect=protocol.NotFoundException)

    def test_get_dir(self):
        # get_dir does not pass through the inode.  It only passes through the ref_hash from the inode
        self.mock_backend_session.get_directory = AsyncMock(return_value=EXAMPLE_DIR)
        directory = asyncio.get_event_loop().run_until_complete(
            self.client_session.get_directory(EXAMPLE_DIR_INODE))
        for call in self.mock_backend_session.get_directory.mock_calls:
            assert call.kwargs['inode'].hash == EXAMPLE_DIR_INODE.hash
            break
        else:
            assert False, "No mock calls"
        assert directory == EXAMPLE_DIR
        assert directory is not EXAMPLE_DIR

    @pytest.mark.parametrize('streaming', (True, False))
    def test_get_file(self, streaming):
        async def read_file():
            with await self.client_session.get_file(file_inode) as file:
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

        self.mock_backend_session.get_file = AsyncMock(return_value=mock_file)
        asyncio.get_event_loop().run_until_complete(read_file())
        assert len(mock_file.close.mock_calls) == 0

    @pytest.mark.parametrize('file_type', (protocol.FileType.REGULAR, protocol.FileType.LINK))
    @pytest.mark.parametrize('streaming', (True, False))
    def test_get_file_restore(self, streaming, tmp_path: Path, file_type):
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
        self.mock_backend_session.get_file = AsyncMock(return_value=mock_file)
        asyncio.get_event_loop().run_until_complete(self.client_session.get_file(
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


class MockFileReader(protocol.FileReader):

    def __init__(self, parts=(b"this ", b"is ", b" a ", b"test!"), streaming: bool = False):
        self.parts = parts
        self.streaming = streaming
        self._iter = iter(parts)

    def reset(self):
        self._iter = iter(self.parts)

    async def read(self, num_bytes: int = -1) -> bytes:
        try:
            result = next(self._iter)
            assert num_bytes < 0 or len(result) <= num_bytes
            return result
        except StopIteration:
            return b""

    def close(self):
        pass

    @property
    def file_size(self) -> Optional[int]:
        if self.streaming:
            return None
        return sum(len(part) for part in self.parts)


class TestPassThroughBackupSession(BaseTestPassThrough):

    client_session: BackupSession

    @pytest.fixture(autouse=True)
    def _client_backup_session(self, client_backup_session, mock_backup_session):
        self.client_session = client_backup_session
        self.mock_backend_session = mock_backup_session

    @pytest.mark.parametrize('expected_result', (
            protocol.DirectoryDefResponse(missing_ref=uuid4(), missing_files=["aaaa"]),
            protocol.DirectoryDefResponse(ref_hash='bbbb'),
            protocol.DirectoryDefResponse(missing_files=['aaaa']),
    ))
    @pytest.mark.parametrize('replaces', (None, uuid4()))
    def test_directory_def(self, replaces, expected_result):
        self.mock_backend_session.directory_def = AsyncMock(return_value=expected_result)
        self._run_and_check_pass_through(self.client_session.directory_def(EXAMPLE_DIR, replaces),
                                         return_value=expected_result)

    @pytest.mark.parametrize('is_complete', (True, False))
    @pytest.mark.parametrize('file_content', (
        MockFileReader(parts=(b"this ", b"is ", b"a ", b"test!"),streaming=True),
        MockFileReader(parts=(b"this ", b"is ", b"a ", b"test!"), streaming=False),
        b"this is a test!",
    ))
    def test_upload_file(self, is_complete: bool, file_content):
        async def mock_callback(**kwargs):
            if not isinstance(kwargs['file_content'], bytes):
                kwargs['file_content'] = await kwargs['file_content'].read()
            calls.append(kwargs)
            # Note this is_complete is NOT the same as the outer is_complete
            if kwargs['is_complete']:
                return mock_ref_hash
            return None

        calls = []
        mock_ref_hash = "this is a ref hash" if is_complete else None
        resume_id = uuid4()
        self.mock_backend_session.upload_file_content = mock_callback
        if isinstance(file_content, MockFileReader):
            file_content.reset()
        result = asyncio.get_event_loop().run_until_complete(self.client_session.upload_file_content(
            file_content=file_content,
            resume_id=resume_id,
            resume_from=0,
            is_complete=is_complete,
        ))

        assert result == mock_ref_hash
        if isinstance(file_content, MockFileReader):
            assert len(calls) == len(file_content.parts) + (1 if file_content.streaming and is_complete else 0)
            for call, expected_content in zip(calls, file_content.parts):
                assert call['resume_id'] == resume_id
                assert call['file_content'] == expected_content
        else:
            assert len(calls) == 1
            assert calls[0]['file_content'] == file_content

        for call in calls[:-1]:
            assert not call['is_complete']
        assert calls[-1]['is_complete'] == is_complete

    def test_add_root(self):
        self._run_and_check_pass_through(self.client_session.add_root_dir('some_child', EXAMPLE_DIR_INODE))

    def test_check_file_upload_size(self):
        self._run_and_check_pass_through(self.client_session.check_file_upload_size(resume_id=uuid4()),
                                         return_value=10678)

    def test_complete(self):
        result = protocol.Backup(
            client_id=self.client_session.config.client_id,
            client_name=self.client_session.server_session.client_config.client_name,
            backup_date=datetime.now(timezone.utc),
            started=datetime.now(timezone.utc) - timedelta(hours=1),
            completed=datetime.now(timezone.utc),
            roots=EXAMPLE_DIR.children,
            description='Example backup',
        )
        self._run_and_check_pass_through(self.client_session.complete(), return_value=result)

    def test_discard(self):
        self._run_and_check_pass_through(self.client_session.discard())
