import json
import logging.config
import logging.handlers
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse
from uuid import UUID

import click
import pydantic
from pydantic import BaseSettings

from .algorithms import BackupController
from .local_file_system import LocalFileSystemExplorer
from .log_config import LogConfig, flush_early_logging, setup_early_logging
from .misc import SettingsConfig, cleanup_event_loop, register_clean_shutdown, str_exception, wrapped_async
from .protocol import Backup, DuplicateBackup, ENCODING, NotFoundException, ServerSession

logger = logging.getLogger(__name__)

DATE_FORMAT = click.DateTime(formats=[
    '%Y-%m-%d',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S.%f%z',
])


class Settings(BaseSettings):
    database_url: str
    client_id: str
    credentials: Optional[Path] = None
    logging: LogConfig = LogConfig()

    class Config(SettingsConfig):
        validate_assignment = True
        SETTINGS_FILE_DEFAULT_NAME = 'client.json'

    def credentials_absolute_path(self) -> Optional[Path]:
        if self.credentials is None:
            return None

        if self.credentials.is_absolute():
            return self.credentials

        for config_path in [self.Config.user_config_path, self.Config.site_config_path]:
            parent = config_path().parent
            logger.debug("Looking for %s in %s", self.credentials, parent)
            credentials_path = parent / self.credentials
            if credentials_path.is_file():
                return credentials_path

        raise FileNotFoundError(str(self.credentials))


@click.group()
@click.option("--config", type=click.Path(path_type=Path, exists=True, file_okay=True, dir_okay=False),
              help=f"A configuration file to loads defaults\n\nDefaults to {Settings.Config.user_config_path()}")
def main(config: Optional[Path]):
    """
    Backup client for system or user specified backups
    """
    setup_early_logging()
    register_clean_shutdown()
    context = click.get_current_context()
    context.call_on_close(cleanup_event_loop)
    if context.invoked_subcommand == config_group.name:
        context.obj = config
        return
    settings = Settings(config_path=config)
    _config_logging(settings)
    context.obj = settings


@main.group("config")
def config_group():
    """
    Set / show hashback client configuration
    """

@config_group.command("show")
def show_config():
    settings = Settings(click.get_current_context().obj)
    _config_logging(settings)
    print(settings.json(indent=True))


@config_group.command("set")
@click.option('--site/--user', default=False, help="Configure for the whole machine (default) or just this user")
@click.option("--client-id", help="Client id to backup to on the server")
@click.option("--database-url", help="Database URL or file path")
@click.option("--log-level", help="Default log level: one of DEBUG, INFO, WARNING, ERROR")
@click.option(
    "--log-unit-level",
    multiple=True,
    help="Set a program unit and its children to a different log level to the one defined by --log-level. "
         "Eg: --log-unit-level=hashback.misc=WARNING",
)
@click.option("--credentials", help="Provide credentials string")
def set_config(site: Optional[bool], **options):
    """
    Configure hashback client
    """
    log_level: Optional[str] = options.pop('log_level')
    log_unit_level: List[str] = options.pop('log_unit_level')
    credentials: Optional[str] = options.pop('credentials')

    try:
        new_settings = Settings(**{key: options[key] for key in ('database_url', 'client_id')
                                   if options[key] is not None})
        click.get_current_context().call_on_close(partial(_config_logging, new_settings))

        # Log levels cannot be passed in or they will unintentionally overwrite instead of amend
        # Besides they exist at a different level.
        if log_level is not None:
            if not isinstance(logging.getLevelName(log_level), int):
                raise click.ClickException(f"Unknown level name {log_level}")
            new_settings.logging.log_level = log_level

        for item in log_unit_level:
            log_unit, level = item.split('=', 1)
            if not level:
                new_settings.logging.log_unit_levels.pop(log_unit)
            else:
                if not isinstance(logging.getLevelName(level), int):
                    raise click.ClickException(f"Unknown level name {level}")
                new_settings.logging.log_unit_levels[log_unit] = level

        config_path = Settings.Config.site_config_path() if site else Settings.Config.user_config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)

        if credentials is not None:
            credentials_path = config_path.parent / 'client-credentials.json'
            logger.info(f"Saving credentials to {credentials_path}")
            credentials_path.unlink(missing_ok=True)
            credentials_path.touch(mode=0o600, exist_ok=False)
            with credentials_path.open('w') as file:
                file.write(credentials)
            new_settings.credentials = Path('client-credentials.json')

    except pydantic.ValidationError as ex:
        raise click.ClickException(str_exception(ex)) from ex

    logger.info(f"Saving settings to {config_path}")
    config_path.touch(mode=0o600, exist_ok=True)
    with config_path.open('w', encoding=ENCODING) as file:
        file.write(new_settings.json(indent=True, exclude_defaults=True))


@main.command()
@click.option("--description", help="Description to attach to this backup")
@click.option("--overwrite", is_flag=True, help="Overwrite an existing backup for the same timestamp")
@click.option("--read-every-byte/--match-meta-only", default=True,
              help="Reduces network overhead at the expense of disk IO."
                   "By default hashback will check if each file's metadata against it's metadata in the last backup."
                   "If the metadata is identical then hashback will assume the file has not changed and will not read "
                   "it.  This forces hashback to read the last backup directories which can be a lot of requests to the "
                   "server.  If --read-every-byte is specified, hashback will re-read every file locally and it will "
                   "not read back anything from the last backup on the server.  This reads much more from disk and much"
                   "less from the server.")
@click.option("--root-first/--leaf-first", default=True,
              help="Reduces network overhead at the expense of memory. "
                   "Buy default hashback will submit every directory to the server starting at th leaf nodes and "
                   "working back to the trunk.  If --full-prescan is specified it will read the entire directory tree "
                   "and cache the directory listings in memory for the whole backup before it starts to upload. "
                   "It then submits directories starting from the root.  This costs more memory but can be much "
                   "quicker on large directory trees with very little changing.")
@click.option("--resume", type=click.UUID,
              help="Resume a stopped backup session.  You must specify a UUID for the session")
@wrapped_async
async def backup(description: Optional[str],
                 read_every_byte: Optional[bool],
                 root_first: Optional[bool],
                 overwrite: bool,
                 resume: Optional[UUID]):
    """
    Run a backup now.

    The configuration of what to backup is stored on the server.
    """
    settings: Settings = click.get_current_context().obj
    server_session = await create_client(settings)
    try:
        if resume is not None:
            backup_session = await server_session.resume_backup(session_id=resume)
        else:
            try:
                timestamp = datetime.now()
                backup_session = await server_session.start_backup(
                    backup_date=timestamp,
                    allow_overwrite=overwrite,
                    description=description,
                )
            except DuplicateBackup as exc:
                raise click.ClickException(f"Duplicate backup {exc}") from None

        logger.info(
            f"Backup - %s (%s) - %s (%s)",
            server_session.client_config.client_name,
            server_session.client_config.client_id,
            backup_session.config.backup_date,
            backup_session.config.session_id,
        )
        backup_scanner = BackupController(LocalFileSystemExplorer(), backup_session)

        backup_scanner.read_last_backup = not read_every_byte
        backup_scanner.match_meta_only = not read_every_byte
        backup_scanner.full_prescan = root_first

        await backup_scanner.backup_all()
        logger.info("Finalizing backup")
        await backup_session.complete()
        logger.info("All done")
    finally:
        server_session.close()


@main.command()
@click.argument("SESSION_ID", type=click.UUID)
@wrapped_async
async def discard_session(session_id: UUID):
    """
    Discard an open backup session
    """
    server_session: ServerSession = click.get_current_context().obj
    try:
        backup_session = await server_session.resume_backup(session_id=session_id)
    except NotFoundException as ex:
        raise click.ClickException(f"Session not found {session_id}") from ex
    logger.info(
        "Discarding backup session %s %s %s",
        backup_session.config.session_id,
        server_session.client_config.date_string(backup_session.config.backup_date),
        backup_session.config.description,
    )
    await backup_session.discard()


@main.command()
@click.option("--json/--plain", default=False)
@wrapped_async
async def list_backups(**options):
    """
    List completed backups
    """
    client = await create_client(click.get_current_context().obj)
    logger.info(f"Listing backups for {client.client_config.client_name} ({client.client_config.client_id})")
    tz_info = client.client_config.timezone
    backups = sorted(((backup_date.astimezone(tz_info), description)
                      for backup_date, description in await client.list_backups()), key=lambda item: item[0])
    if options['json']:
        result = [{'date_time': client.client_config.date_string(backup_date), 'description': description}
                  for backup_date, description in backups]
        print(json.dumps(result))
    else:
        if backups:
            print("Backup Date/time\tDescription")
            for backup_date, description in backups:
                print(f"{client.client_config.date_string(backup_date)}\t{description}")
        else:
            print("No backups found!")


@main.command()
@click.argument("BACKUP_DATE", type=DATE_FORMAT)
@click.option("--json/--plain", default=False)
@wrapped_async
async def describe_backup(backup_date: datetime, **options):
    """
    Describe a completed backup
    """
    client = await create_client(click.get_current_context().obj)
    client_timezone = client.client_config.timezone
    if backup_date.tzinfo is None:
        backup_date = backup_date.replace(tzinfo=client_timezone)
    result: Backup = await client.get_backup(backup_date)
    result.backup_date = result.backup_date.astimezone(client_timezone)
    result.started = result.started.astimezone(client_timezone)
    result.completed = result.completed.astimezone(client_timezone)

    if options['json']:
        print(result.json())
    else:
        print(f"{client.client_config.date_string(result.backup_date)}: {result.description}")
        print(f"Started: {client.client_config.date_string(result.started)}")
        print(f"Finished: {client.client_config.date_string(result.completed)}")
        print(f"Roots: {list(result.roots.keys())}")


async def create_client(settings: Settings) -> ServerSession:
    url = urlparse(settings.database_url)
    if url.scheme in ('', 'file'):
        return _create_local_client(settings)
    if url.scheme in ('http', 'https'):
        return await _create_http_client(settings)
    if url.scheme == "s3":
        return await _create_s3_client(settings)
    raise ValueError(f"Unknown scheme {url.scheme}")


def _create_local_client(settings: Settings) -> ServerSession:
    logger.debug("Loading local database plugin")
    # pylint: disable=import-outside-toplevel
    from . import local_database
    return local_database.LocalDatabase(Path(settings.database_url)).open_client_session(settings.client_id)


async def _create_http_client(settings: Settings) -> ServerSession:
    logger.debug("Loading http client plugin")
    # pylint: disable=import-outside-toplevel
    from . import http_protocol
    from .http_client import ClientSession, RequestsClient
    server_properties = http_protocol.ServerProperties.parse_url(settings.database_url)

    credentials_path = settings.credentials_absolute_path()
    if credentials_path is not None:
        server_properties.credentials = http_protocol.Credentials.parse_file(credentials_path)

    if server_properties.credentials is None:
        client = RequestsClient(server_properties)
    else:
        logger.debug("Loading basic auth client plugin")
        from .basic_auth.client import BasicAuthClient
        client = BasicAuthClient(server_properties)

    server_version = await client.server_version()
    logger.info(f"Connected to server {server_version.server_type} protocol {server_version.protocol_version}")
    return await ClientSession.create_session(client)


async def _create_s3_client(settings: Settings) -> ServerSession:
    logger.debug("Loading s3 client")
    # pylint: disable=import-outside-toplevel
    from . import aws_s3_client

    credentials_path = settings.credentials_absolute_path()
    if credentials_path is not None:
        credentials = aws_s3_client.Credentials.parse_file(credentials_path)
    else:
        credentials = aws_s3_client.Credentials()

    url = urlparse(settings.database_url)
    bucket = url.hostname
    path = url.path
    if path.startswith("/"):
        path = path[1:]
    return aws_s3_client.S3Database(bucket_name=bucket, directory=path, credentials=credentials).open_client_session(
        client_id_or_name=settings.client_id,
    )


def _config_logging(settings: Settings):
    logging.config.dictConfig(settings.logging.dict_config())
    flush_early_logging()


if __name__ == '__main__':
    # Pylint does not understand click
    # pylint: disable=no-value-for-parameter
    main()
