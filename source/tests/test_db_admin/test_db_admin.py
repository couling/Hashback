from pathlib import Path

import pytest

from hashback.local_database import LocalDatabase
from hashback.protocol import ClientConfiguration


def test_create_client(cli_runner, local_db_path: Path):
    client_name = 'test-client'
    cli_runner('add-client', client_name)
    database = LocalDatabase(local_db_path)
    all_clients = list(database.iter_clients())
    assert len(all_clients) == 1
    assert all_clients[0].client_name == client_name


def test_create_duplicate_client_fails(cli_runner, local_db_path: Path):
    client_name = 'duplicate'
    cli_runner('add-client', client_name, exit_code=0)
    database = LocalDatabase(local_db_path)
    clients_before = list(database.iter_clients())

    cli_runner('add-client', client_name, exit_code=1)
    clients_after = list(database.iter_clients())
    assert clients_before == clients_after


@pytest.mark.parametrize('refer_by', ["name", "config"])
def test_add_new_root(cli_runner, local_db_path: Path, refer_by: str):
    database = LocalDatabase(local_db_path)
    client_name = 'test_client'
    client_config = ClientConfiguration(
        client_name=client_name,
    )
    database.create_client(client_config)
    reference = client_name if refer_by == 'name' else str(client_config.client_id)

    cli_runner('add-directory', reference, 'some_root', '/foo/bar')

    roots = database.open_client_session(str(client_config.client_id)).client_config.backup_directories

    assert len(roots) == 1
    assert roots['some_root'].base_path == '/foo/bar'
    assert roots['some_root'].filters == []
