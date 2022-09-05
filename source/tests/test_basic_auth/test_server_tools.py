# pylint: disable=redefined-outer-name
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

import click.testing
import pytest

from hashback import http_protocol, protocol
from hashback.basic_auth import server
from hashback.basic_auth.basic_auth import BasicAuthDb
from hashback.local_database import LocalDatabase

CLIENT_NAME = 'test_client'


@pytest.fixture()
def run_cli(configuration: Path):
    def _run_cli(*args: str, catch_exceptions: bool = False, return_code: Optional[int] = 0):
        runner = click.testing.CliRunner()
        args = ('--config-path', str(configuration)) + args
        result = runner.invoke(server.main, args, catch_exceptions=catch_exceptions)
        assert return_code is None or result.exit_code == return_code
        return result
    return _run_cli


@pytest.fixture
def configuration(tmp_path: Path, basic_auth_db_path: Path, local_db: LocalDatabase) -> Path:
    assert tmp_path.is_dir()
    config_path = tmp_path / 'server_config.json'

    settings = server.Settings(
        config_path=config_path,
        database_path=local_db.path,
        users_path=basic_auth_db_path,
    )
    with config_path.open('w') as file:
        file.write(settings.json(indent=True))

    return config_path


@pytest.fixture()
def local_db(tmp_path: Path) -> LocalDatabase:
    db_path = tmp_path / 'db'
    LocalDatabase.create_database(db_path, LocalDatabase.Configuration())
    return LocalDatabase(db_path)


@pytest.fixture()
def client_id(local_db: LocalDatabase) -> UUID:
    client_id = uuid4()

    local_db.save_client_config(protocol.ClientConfiguration(
        client_name=CLIENT_NAME,
        client_id=client_id,
    ))

    return client_id


@pytest.fixture(params=[False, True], ids=['client_name', 'client_id'])
def specified_user(request, client_id: UUID) -> str:
    use_id = request.param
    if use_id:
        return str(client_id)
    return CLIENT_NAME


def credentials_from_output(output: str) -> http_protocol.Credentials:
    output = output.split('\n')
    for line in output:
        if line.startswith('{'):
            return http_protocol.Credentials.parse_raw(line)
    raise RuntimeError("Credentials not found in output")


@pytest.mark.parametrize('extra', ('--display-credentials','--hide-credentials'))
@pytest.mark.asyncio
def test_authorize_user(run_cli, basic_auth_db: BasicAuthDb, client_id: UUID, specified_user: str, extra: str):
    result = run_cli('authorize', specified_user, extra)
    # '--hide-credentials' should have no effect since the password is being generated dynamically
    credentials = credentials_from_output(result.output)

    assert credentials.username == str(client_id)
    assert credentials.auth_type == 'basic'

    basic_auth_db.authenticate(credentials.username, credentials.password)


@pytest.mark.asyncio
def test_authorize_user_exp(run_cli, basic_auth_db: BasicAuthDb, client_id: UUID, specified_user: str):
    password = str(uuid4())

    # Just verify we didn't screw up the test; the user should not already exist
    with pytest.raises(http_protocol.AuthenticationFailedException):
        basic_auth_db.authenticate(str(client_id), password)

    run_cli('authorize', specified_user, password)

    # Check the user was authorized
    basic_auth_db.authenticate(str(client_id), password)


@pytest.mark.asyncio
def test_authorize_user_hide_credentials(run_cli, client_id: UUID):
    """
    Test credentials can be hidden when password is specified
    """
    password = str(uuid4())
    result = run_cli('authorize', str(client_id), password, '--hide-credentials')
    assert password not in result.output


@pytest.mark.asyncio
def test_authorize_user_show_credentials(run_cli, client_id: UUID, specified_user:str):
    password = str(uuid4())
    result = run_cli('authorize', specified_user, password, '--display-credentials')
    credentials = credentials_from_output(result.output)
    assert credentials.username == str(client_id)
    assert credentials.auth_type == 'basic'
    assert credentials.password == password


@pytest.mark.asyncio
def test_revoke_user(run_cli, basic_auth_db: BasicAuthDb, client_id: UUID, specified_user: str):
    password = str(uuid4())

    basic_auth_db.register_user(str(client_id), password)
    # Just verify we didn't screw up the test; the user should not already exist
    basic_auth_db.authenticate(str(client_id), password)

    run_cli('revoke', specified_user, catch_exceptions=False)

    # Check the user was removed
    with pytest.raises(http_protocol.AuthenticationFailedException):
        basic_auth_db.authenticate(str(client_id), password)
