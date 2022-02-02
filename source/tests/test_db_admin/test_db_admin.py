from pathlib import Path
from hashback.local_database import LocalDatabase


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
