import json
from datetime import datetime, timedelta, timezone

import pytest_asyncio

from hashback.cmdline import Settings
from hashback.local_database import LocalDatabase
from hashback.protocol import Directory, FileType, Inode


@pytest_asyncio.fixture()
async def existing_backups(configured_client: Settings, local_database: LocalDatabase):
    session = local_database.open_client_session(configured_client.client_id)

    example_backups = (
        (datetime.now(timezone.utc), "Test Backup 1"),
        (datetime.now(timezone.utc) - timedelta(days=7), "Test Backup 2"),
        (datetime.now(timezone.utc) - timedelta(days=14), None)
    )

    for backup_date, backup_description in example_backups:
        backup_session = await session.start_backup(backup_date=backup_date, description=backup_description)
        # Empty directory
        directory_def_result = await backup_session.directory_def(Directory(__root__={}))
        assert directory_def_result.success
        for root in session.client_config.backup_directories.keys():
            await backup_session.add_root_dir(
                root_dir_name=root,
                inode=Inode(type=FileType.DIRECTORY, mode=0, hash=directory_def_result.ref_hash),
            )
        await backup_session.complete()
    return example_backups


def test_json_list_backups_returns_no_results(cli_runner, configured_client: Settings):
    result = cli_runner('list', '--json')
    assert json.loads(result.stdout) == []


def test_json_list_backups_returns_results(cli_runner, configured_client: Settings, existing_backups, local_database):
    result = cli_runner('list', '--json')
    result_backups = json.loads(result.stdout)

    # Dates should be translated from client time into UTC and aligned to the backup granularity as they are created
    # ... then translated into the client's timezone when listed
    client_config = local_database.open_client_session(configured_client.client_id).client_config
    expected_result = [{
        'date_time': client_config.date_string(client_config.normalize_backup_date(backup_date)),
        'description': description
    } for backup_date, description in existing_backups]

    assert sorted(result_backups, key=lambda backup: backup['date_time']) == \
           sorted(expected_result, key=lambda backup: backup['date_time'])

    for result in result_backups:
        assert result['date_time'][-6] == '-'
        assert result['date_time'][:-5] != "00:00"


def test_list_backups_returns_results(cli_runner, configured_client: Settings, existing_backups, local_database):
    result = cli_runner('list')

    # Dates should be translated from client time into UTC and aligned to the backup granularity as they are created
    # ... then translated into the client's timezone when listed
    client_config = local_database.open_client_session(configured_client.client_id).client_config

    for backup_date, description in existing_backups:
        assert client_config.date_string(client_config.normalize_backup_date(backup_date)) in result.stdout
        assert str(description) in result.stdout


def test_list_backups_returns_no_results(cli_runner, configured_client: Settings):
    result = cli_runner('list')
    assert "No backups found!" in result.stdout


def test_describe_listed_backup(cli_runner, configured_client: Settings, existing_backups, local_database):
    result = json.loads(cli_runner('list', '--json').stdout)
    backup_date_time = result[-1]['date_time']

    result = cli_runner('describe', backup_date_time)
    assert backup_date_time in result.stdout
    assert existing_backups[0][1] in result.stdout


def test_describe_listed_backup_json(cli_runner, configured_client: Settings, existing_backups, local_database):
    result = json.loads(cli_runner('list', '--json').stdout)
    backup_date_time = result[-1]['date_time']

    result = json.loads(cli_runner('describe', '--json', backup_date_time).stdout)
    assert result['backup_date'] == backup_date_time
    assert result['description'] == existing_backups[0][1]
