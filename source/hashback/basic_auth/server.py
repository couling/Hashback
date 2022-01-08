import functools
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from uuid import uuid4

import click
from pydantic import BaseSettings, root_validator
from uvicorn import run

from . import basic_auth
from ..server import app
from ..http_protocol import DEFAULT_PORT
from ..local_database import LocalDatabase
from ..misc import register_clean_shutdown, merge, SettingsConfig, DEFAULT_LOG_FORMAT


@click.group()
@click.option("--config-path", type=click.Path(exists=True, dir_okay=False, path_type=Path),
                               default=Path('/etc/hashback/basic-server.json'))
@click.pass_context
def main(context: click.Context, config_path: Path):
    context.obj = Settings(config_path=config_path)


@main.command()
@click.option("--host", default=["::1", "127.0.0.1"], multiple=True)
@click.option("--port", type=click.INT, default=8000)
def run_foreground(host: List[str], port: int):
    settings: Settings = click.get_current_context().obj

    log_config = configure_logging(settings)

    database = LocalDatabase(settings.database_path)
    authorizer = basic_auth.BasicAuthenticatorAuthorizer(basic_auth.BasicAuthDb(settings.users_path))
    app.configure(authorizer, database)

    register_clean_shutdown()
    logging.info("Starting up")
    run(f"{app.__name__}:app", access_log=True, log_config=log_config, host=host, port=port)


@main.command()
@click.argument('CLIENT')
@click.argument('CLIENT_PASSWORD', required=False)
@click.option('--display-password/--hide-password', default=False)
def authorize_client(client: str, client_password: Optional[str], display_password:bool):
    settings: Settings = click.get_current_context().obj
    users_db = basic_auth.BasicAuthDb(auth_file=settings.users_path)
    if client_password is None:
        display_password = True
        client_password = str(uuid4())

    device_db = LocalDatabase(settings.database_path)
    client_config = device_db.open_client_session(client_id_or_name=client).client_config
    print(f"Authorizing Client: {client_config.client_name} ({client_config.client_id})")
    if display_password:
        print(f"With Password: {client_password}")
    users_db.register_user(username=str(client_config.client_id), password=client_password)
    print("Done")


class Settings(BaseSettings):
    config_path: Path
    database_path: Path
    users_path: Path
    session_cache_size: int = 128
    port: int = DEFAULT_PORT
    host: str = "localhost"
    logging: Dict[str, Any]

    Config = SettingsConfig

    @root_validator
    def _relative_path(cls, values: Dict[str, Any]):
        for item in 'database_path', 'users_path':
            if not values[item].is_absolute():
                values[item] = values['config_path'].parent / values[item]
        return values


def configure_logging(settings: Settings) -> Dict:
    from uvicorn.config import LOGGING_CONFIG
    from logging.config import dictConfig
    log_config = LOGGING_CONFIG.copy()
    del log_config['loggers']['uvicorn']['handlers']
    log_config = merge(log_config, {
        'root': {'handlers': ["default"], 'level': settings.logging.get('level', 'INFO')},
        'formatters': {'default': {'fmt': DEFAULT_LOG_FORMAT}},
        'loggers': merge(settings.logging.get('loggers', {}), {
            'uvicorn': {'handlers': []}
        })
    })
    log_config['formatters']['default']['fmt'] = DEFAULT_LOG_FORMAT
    dictConfig(log_config)
    return log_config


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
