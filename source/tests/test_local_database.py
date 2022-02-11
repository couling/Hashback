# pylint: disable=redefined-outer-name

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from hashback import protocol
from hashback.local_database import LocalDatabase, LocalDatabaseServerSession, LocalDatabaseBackupSession
from hashback.local_file_system import BytesReader
from hashback.protocol import ClientConfiguration, Backup, Inode, Directory, FileType, SessionClosed


@pytest.fixture(scope='function')
def local_database_configuration() -> LocalDatabase.Configuration:
    return LocalDatabase.Configuration(
        store_split_count=1,
        store_split_size=2,
    )


@pytest.fixture(scope='function')
def local_database(tmp_path: Path, local_database_configuration, client_config) -> LocalDatabase:
    database = LocalDatabase.create_database(tmp_path / 'database', local_database_configuration)
    database.create_client(client_config)
    return database


@pytest.fixture(scope='function')
def server_session(local_database: LocalDatabase, client_config: ClientConfiguration) -> LocalDatabaseServerSession:
    return local_database.open_client_session(str(client_config.client_id))


@pytest.fixture(scope='function')
def backup_session(server_session) -> LocalDatabaseBackupSession:
    return asyncio.get_event_loop().run_until_complete(
        server_session.start_backup(datetime.now(timezone.utc)),
    )


@pytest.fixture()
def previous_backup(server_session: LocalDatabaseServerSession, client_config: ClientConfiguration, tmp_path: Path
                    ) -> Backup:
    async def upload() -> Backup:
        backup_session = await server_session.start_backup(datetime.now(timezone.utc), description="New Backup")
        ref_hash = await backup_session.upload_file_content(file_content=file_text.encode(), resume_id=uuid4())
        assert ref_hash == file_hash
        response = await backup_session.directory_def(directory)
        assert response.success
        assert response.ref_hash is not None

        await backup_session.add_root_dir(
            root_dir_name=root_name,
            inode=Inode.from_stat(root_path.stat(), response.ref_hash),
        )

        return await backup_session.complete()

    file_name = 'test.txt'
    file_text = "Hello World"
    file_hash = "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"

    root_name, root = next(iter(client_config.backup_directories.items()))

    root_path = Path(root.base_path)
    assert str(tmp_path) in str(root_path)
    root_path.mkdir(exist_ok=True, parents=True)
    with (root_path / file_name).open('w') as file:
        file.write("Hello World")
    for item in root.filters:
        (root_path / item.path).mkdir(parents=True, exist_ok=True)

    directory = Directory(__root__ = {
        file_name: Inode.from_stat((root_path / file_name).stat(), hash_value=file_hash)
    })

    return asyncio.get_event_loop().run_until_complete(upload())


def test_client_provides_client_config(server_session: LocalDatabaseServerSession, client_config: ClientConfiguration):
    assert server_session.client_config == client_config
    # This should be pulled from the database and not somehow echoed back from the fixture
    assert server_session.client_config is not client_config


def test_backup_can_be_retrieved(previous_backup: Backup, server_session: LocalDatabaseServerSession):
    async def check():
        all_backups = await server_session.list_backups()
        assert len(all_backups) == 1
        backup_date, backup_description = all_backups[0]
        assert backup_date == previous_backup.backup_date
        assert backup_description == previous_backup.description

        retrieved_backup = await server_session.get_backup(backup_date)
        assert retrieved_backup == previous_backup
        assert retrieved_backup is not previous_backup

        root_name, root = next(iter(retrieved_backup.roots.items()))
        root_path = Path(server_session.client_config.backup_directories[root_name].base_path)

        for child, inode in (await server_session.get_directory(root)).children.items():
            child_path = (root_path / child)
            assert child_path.exists()
            if child_path.is_file() and not child_path.is_symlink():
                assert inode.type == FileType.REGULAR
                assert inode.size == child_path.stat().st_size

                content = await server_session.get_file(inode)
                retrieved_content = await content.read()

                with child_path.open('rb') as file:
                    assert retrieved_content == file.read()

    asyncio.get_event_loop().run_until_complete(check())


def test_open_session_by_name(local_database: LocalDatabase, server_session: LocalDatabaseServerSession):
    new_session = local_database.open_client_session(server_session.client_config.client_name)
    assert new_session.client_config == server_session.client_config


def test_open_session_failed(local_database: LocalDatabase):
    with pytest.raises(SessionClosed):
        local_database.open_client_session(str(uuid4()))


def test_get_backup_can_return_none(server_session: LocalDatabaseServerSession):
    result = asyncio.get_event_loop().run_until_complete(server_session.get_backup())
    assert result is None


class TestWithBackup:
    server_session: LocalDatabaseServerSession
    backup_session: LocalDatabaseBackupSession

    @pytest.fixture(autouse=True)
    def _initialize_self(self, server_session: LocalDatabaseServerSession, backup_session: LocalDatabaseBackupSession):
        self.server_session = server_session
        self.backup_session = backup_session

    def test_duplicate_backup(self):
        asyncio.get_event_loop().run_until_complete(
            self.backup_session.complete()
        )
        with pytest.raises(protocol.DuplicateBackup):
            asyncio.get_event_loop().run_until_complete(
                self.server_session.start_backup(self.backup_session.config.backup_date)
            )

    def test_resume_backup_by_id(self):
        new_session = asyncio.get_event_loop().run_until_complete(
            self.server_session.resume_backup(session_id=self.backup_session.config.session_id)
        )
        assert new_session is not self.backup_session
        assert new_session.config == self.backup_session.config

    def test_resume_backup_by_date(self):
        new_session = asyncio.get_event_loop().run_until_complete(
            self.server_session.resume_backup(backup_date=self.backup_session.config.backup_date)
        )
        assert new_session is not self.backup_session
        assert new_session.config == self.backup_session.config

    @pytest.mark.asyncio
    async def test_resume_backup_discard_partial(self, backup_session: LocalDatabaseBackupSession):
        resume_id = uuid4()
        # Create a partial file with 0 bytes
        await backup_session.upload_file_content(BytesReader(b'hello'), resume_id=resume_id, is_complete=False)
        # Sanity check that this was successful
        assert await backup_session.check_file_upload_size(resume_id) == 5

        new_session = await self.server_session.resume_backup(
            backup_date=self.backup_session.config.backup_date,
            discard_partial_files=True,
        )
        with pytest.raises(protocol.NotFoundException):
            assert await new_session.check_file_upload_size(resume_id) == 5

    @pytest.mark.asyncio
    @pytest.mark.parametrize('params', ({'discard_partial_files': False}, {}), ids=str)
    async def test_resume_backup_no_discard_partial(self, backup_session: LocalDatabaseBackupSession, params):
        resume_id = uuid4()
        # Create a partial file with 0 bytes
        await backup_session.upload_file_content(BytesReader(b'hello'), resume_id=resume_id, is_complete=False)
        # Sanity check that this was successful
        assert await backup_session.check_file_upload_size(resume_id) == 5

        new_session = await self.server_session.resume_backup(
            backup_date=self.backup_session.config.backup_date,
            **params
        )
        assert await new_session.check_file_upload_size(resume_id) == 5


    def test_list_backup_sessions(self):
        all_sessions = asyncio.get_event_loop().run_until_complete(
            self.server_session.list_backup_sessions()
        )
        assert all_sessions == [self.backup_session.config]

    def test_list_backups(self):
        async def complete_and_list():
            await self.backup_session.complete()
            return await self.server_session.list_backups()

        all_backups = asyncio.get_event_loop().run_until_complete(complete_and_list())
        assert all_backups == [(self.backup_session.config.backup_date, self.backup_session.config.description)]
