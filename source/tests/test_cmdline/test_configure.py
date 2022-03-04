from pathlib import Path
from typing import Collection
from uuid import uuid4

import pytest

from hashback.cmdline import Settings


def test_configure_minimum(cli_runner, user_config_path: Path):
    client_id = str(uuid4())
    db_url = '/not-exists'

    cli_runner('config', 'set', '--database-url',  db_url, '--client-id', client_id)

    saved_settings = Settings.parse_file(user_config_path)
    assert saved_settings.client_id == client_id
    assert saved_settings.database_url == '/not-exists'


@pytest.mark.parametrize('target', ('--user', '--site'))
def test_configure_saves_to_correct_location(cli_runner, user_config_path: Path, site_config_path: Path, target: str):
    client_id = str(uuid4())
    db_url = '/not-exists'

    cli_runner('config', 'set', '--database-url', db_url, '--client-id', client_id, target)

    if target == '--site':
        correct_path = site_config_path
        incorrect_path = user_config_path
    else:
        correct_path = user_config_path
        incorrect_path = site_config_path

    assert correct_path.exists()
    assert not incorrect_path.exists()


@pytest.mark.parametrize('target', ('--user', '--site'))
def test_configure_credentials(cli_runner, user_config_path: Path, site_config_path: Path, target: str):
    credentials = str(uuid4())
    client_id = str(uuid4())
    db_url = '/not-exists'

    cli_runner('config', 'set', '--database-url', db_url, '--client-id', client_id, target, '--credentials',
               credentials)

    config_path = site_config_path if target == '--site' else user_config_path
    saved_settings = Settings.parse_file(config_path)
    credentials_path = saved_settings.credentials

    # Credentials should be stored in the same directory as the settings
    assert credentials_path.parent == Path('.')
    with (config_path.parent / credentials_path).open('r') as file:
        content = file.read()
    assert content == credentials


@pytest.mark.parametrize('args', [
    (),
    ('--database-url', '/foo/bar'),
    ('--client-id', str(uuid4())),
])
def test_minimum_validation(cli_runner, args: Collection[str]):
    result = cli_runner('config', 'set', exit_code=1, *args)
    assert "validation" in result.stderr and "error" in result.stderr


def test_configure_log_level(cli_runner, user_config_path: Path):
    client_id = str(uuid4())
    database_url = '/foo/bar'
    new_level = 'DEBUG'

    settings = Settings(client_id=client_id, database_url=database_url)
    assert settings.logging.log_level != new_level
    user_config_path.parent.mkdir(parents=True, exist_ok=True)
    with user_config_path.open('w') as file:
        file.write(settings.json(exclude_unset=True, indent=True))

    cli_runner('config', 'set', '--log-level', new_level)

    settings = Settings.parse_file(user_config_path)
    assert settings.logging.log_level == new_level


def test_configure_log_unit_level(cli_runner, user_config_path):
    client_id = str(uuid4())
    database_url = '/foo/bar'

    settings = Settings(client_id=client_id, database_url=database_url)
    # pylint: disable=use-implicit-booleaness-not-comparison
    assert settings.logging.log_unit_levels == {}
    user_config_path.parent.mkdir(parents=True, exist_ok=True)
    with user_config_path.open('w') as file:
        file.write(settings.json(exclude_unset=True, indent=True))

    cli_runner('config', 'set', '--log-unit-level', 'foo=WARNING', '--log-unit-level', 'bar=DEBUG')

    settings = Settings.parse_file(user_config_path)
    assert settings.logging.log_unit_levels == {
        'foo': 'WARNING',
        'bar': 'DEBUG',
    }


def test_updating_levels_leaves_others_in_place(cli_runner, user_config_path):
    client_id = str(uuid4())
    database_url = '/foo/bar'

    settings = Settings(client_id=client_id, database_url=database_url)
    settings.logging.log_unit_levels = {
        'bar': 'DEBUG',
    }
    user_config_path.parent.mkdir(parents=True, exist_ok=True)
    with user_config_path.open('w') as file:
        file.write(settings.json(exclude_defaults=True, indent=True))

    cli_runner('config', 'set', '--log-unit-level', 'foo=WARNING')

    settings = Settings.parse_file(user_config_path)
    assert settings.logging.log_unit_levels == {
        'foo': 'WARNING',
        'bar': 'DEBUG',
    }


def test_deleting_levels(cli_runner, user_config_path):
    client_id = str(uuid4())
    database_url = '/foo/bar'

    settings = Settings(client_id=client_id, database_url=database_url)
    settings.logging.log_unit_levels = {
        'bar': 'DEBUG',
        'foo': 'WARNING',
    }
    user_config_path.parent.mkdir(parents=True, exist_ok=True)
    with user_config_path.open('w') as file:
        file.write(settings.json(exclude_defaults=True, indent=True))

    cli_runner('config', 'set', '--log-unit-level', 'foo=')

    settings = Settings.parse_file(user_config_path)
    assert settings.logging.log_unit_levels == {
        'bar': 'DEBUG',
    }


@pytest.mark.parametrize('args', [('--log-level', 'BLAH'), ('--log-unit-level', 'foo=BLAH')], ids=str)
def test_incorrect_level_name_raises_exception(cli_runner, user_config_path, args):
    client_id = str(uuid4())
    database_url = '/foo/bar'

    settings = Settings(client_id=client_id, database_url=database_url)
    settings.logging.log_level = 'INFO'
    settings.logging.log_unit_levels = {
        'bar': 'DEBUG',
        'foo': 'WARNING',
    }
    user_config_path.parent.mkdir(parents=True, exist_ok=True)
    with user_config_path.open('w') as file:
        file.write(settings.json(exclude_defaults=True, indent=True))

    result = cli_runner('config', 'set', *args, exit_code=1)

    assert 'Unknown level name ' in result.stderr
    settings = Settings.parse_file(user_config_path)
    assert settings.logging.log_level == 'INFO'
    assert settings.logging.log_unit_levels == {
        'foo': 'WARNING',
        'bar': 'DEBUG',
    }
