import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import click
import dateutil.tz
from pydantic import BaseSettings

from .misc import run_then_cancel, register_clean_shutdown, setup_logging, SettingsConfig
from .protocol import ServerSession, DuplicateBackup
from .backup_algorithm import BackupController
from .local_file_system import LocalFileSystemExplorer

logger = logging.getLogger(__name__)

#pylint: disable=duplicate-code,no-value-for-parameter
def main():
    register_clean_shutdown()
    setup_logging()
    click_main()


@click.group()
@click.option("--config-path", default="/etc/hasback/client.json",
              type=click.Path(path_type=Path, exists=True, file_okay=True, dir_okay=False))
def click_main(config_path: Path):
    settings = Settings(config_path=config_path)
    client = create_client(settings)
    context = click.get_current_context()
    context.call_on_close(lambda: run_then_cancel(client.close()))
    context.obj = client

@click_main.command("backup")
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--description")
@click.option("--overwrite/--no-overwrite", default=False)
@click.option("--fast-unsafe/--slow-safe", default=True)
@click.option("--full-prescan/--low-mem", default=False)
def backup(timestamp: datetime, description: Optional[str], fast_unsafe: bool, full_prescan: bool, overwrite: bool):
    server_session: ServerSession = click.get_current_context().obj
    if timestamp is None:
        timestamp = datetime.now(dateutil.tz.gettz())
    elif timestamp.tzinfo is None:
        timestamp = timestamp.astimezone(dateutil.tz.gettz())

    async def _backup():
        try:
            backup_session = await server_session.start_backup(
                backup_date=timestamp,
                allow_overwrite=overwrite,
                description=description,
            )
        except DuplicateBackup as exc:
            raise click.ClickException(f"Duplicate backup {exc}") from None

        logger.info(f"Backup - {backup_session.config.backup_date}")
        backup_scanner = BackupController(LocalFileSystemExplorer(), backup_session)

        backup_scanner.read_last_backup = fast_unsafe
        backup_scanner.match_meta_only = fast_unsafe
        backup_scanner.full_prescan = full_prescan

        try:
            await backup_scanner.backup_all()
            logger.info("Finalizing backup")
            await backup_session.complete()
            logger.info("All done")
        except:
            logger.warning("Discarding session")
            await backup_session.discard()
            raise

    run_then_cancel(_backup())


class Settings(BaseSettings):
    config_path: Path
    database_url: str
    client_id: str
    credentials: Optional[Path] = None
    logging: Dict[str, Any] = {}

    Config = SettingsConfig


def create_client(settings: Settings) -> ServerSession:
    url = urlparse(settings.database_url)
    if url.scheme == '' or url.scheme == 'file':
        return _create_local_client(settings)
    if url.scheme == 'http' or url.scheme =='https':
        return _create_http_client(settings)

    raise ValueError(f"Unknown scheme {url.scheme}")


def _create_local_client(settings: Settings):
    logger.debug("Loading local database plugin")
    # pylint: disable=import-outside-toplevel
    from . import local_database
    return local_database.LocalDatabase(Path(settings.database_url)).open_client_session(settings.client_id)


def _create_http_client(settings: Settings):
    async def _start_session():
        server_version = await client.server_version()
        logger.info(f"Connected to server {server_version.server_type} protocol {server_version.protocol_version}")
        return await ClientSession.create_session(client)

    logger.debug("Loading http client plugin")
    # pylint: disable=import-outside-toplevel
    from . import http_protocol
    from .http_client import ClientSession
    server_properties = http_protocol.ServerProperties.parse_url(settings.database_url)

    if settings.credentials is not None:
        if settings.credentials.is_absolute():
            credentials_path = server_properties.credentials
        else:
            credentials_path = settings.config_path.parent / settings.credentials
        server_properties.credentials = http_protocol.Credentials.parse_file(credentials_path)

    if settings.credentials is None and server_properties.credentials is None:
        from .http_client import RequestsClient
        client = RequestsClient(server_properties)
    else:
        from .basic_auth.client import BasicAuthClient
        client = BasicAuthClient(server_properties)

    session = run_then_cancel(_start_session())
    return session


if __name__ == '__main__':
    main()
