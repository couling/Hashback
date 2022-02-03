import random
from pathlib import Path
import os

import pytest

from hashback.protocol import ClientConfiguration, ClientConfiguredBackupDirectory, FileType
from hashback.local_database import LocalDatabase


TEST_CLIENT_NAME = 'test_client'
TEST_ROOT_NAME = 'test-root'
REGULAR_CONTENT = random.randbytes(50)

@pytest.fixture()
def dir_to_backup(tmp_path: Path) -> Path:
    # Setup the path to backup
    to_backup = tmp_path / 'to_backup'
    to_backup.mkdir(exist_ok=False, parents=True)
    regular_path = to_backup / 'regular.txt'
    with regular_path.open('wb') as file:
        file.write(REGULAR_CONTENT)
    link_path = to_backup / 'some_link'
    link_path.symlink_to(regular_path.relative_to(regular_path.parent))
    child_dir = to_backup / 'child_dir'
    child_dir.mkdir(parents=False, exist_ok=False)
    grandchild = child_dir / 'grandchild'
    os.mkfifo(grandchild)
    return to_backup


@pytest.fixture(autouse=True)
def local_db(local_db_path: Path) -> LocalDatabase:
    result = LocalDatabase(local_db_path)
    client_config = ClientConfiguration(client_name=TEST_CLIENT_NAME)
    client_config.backup_directories[TEST_ROOT_NAME] = ClientConfiguredBackupDirectory(base_path="/")
    result.create_client(client_config)
    return LocalDatabase(local_db_path)


@pytest.mark.asyncio
async def test_simple_migration(cli_runner, local_db: LocalDatabase, dir_to_backup: Path):
    # Run the backup migration
    cli_runner('migrate-backup', TEST_CLIENT_NAME, TEST_ROOT_NAME, str(dir_to_backup), '--accept-warning')

    # Check the result
    client_session = local_db.open_client_session(TEST_CLIENT_NAME)
    backup = await client_session.get_backup()
    children = (await client_session.get_directory(backup.roots[TEST_ROOT_NAME])).children
    assert len(children) == 3
    assert children['regular.txt'].type is FileType.REGULAR
    assert children['some_link'].type is FileType.LINK
    assert children['child_dir'].type is FileType.DIRECTORY

    with await client_session.get_file(children['regular.txt']) as result_file:
        saved_content = await result_file.read()
    assert saved_content == REGULAR_CONTENT
    assert len(saved_content) == children['regular.txt'].size

    with await client_session.get_file(children['some_link']) as result_file:
        saved_content = await result_file.read()
    assert saved_content.decode() == 'regular.txt'
    assert len(saved_content) == children['some_link'].size

    children = (await client_session.get_directory(children['child_dir'])).children
    assert len(children) == 1
    assert children['grandchild'].type is FileType.PIPE

    with await client_session.get_file(children['grandchild']) as result_file:
        saved_content = await result_file.read()
    assert len(saved_content) == 0
    assert children['grandchild'].size is None
