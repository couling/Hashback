import logging
import click
import asyncio
from typing import List, Optional
from datetime import datetime
from pathlib import Path

from . import protocol, scanner
from .local_database import LocalDatabase, Configuration


logger = logging.getLogger(__name__)


@click.group()
@click.option("--database", default=".", envvar="DATABASE")
@click.pass_context
def main(ctx: click.Context, database: str):
    if ctx.invoked_subcommand != 'create':
        ctx.obj = LocalDatabase(Path(database))
    else:
        ctx.obj = Path(database)


@main.command('create')
@click.option("--backup-by", type=click.Choice(['date', 'timestamp'], case_sensitive=False), default="date")
@click.option("--friendly-links/--flat", default=True)
@click.option("--store-split-count", type=click.INT, default=2)
@click.pass_obj
def create(database: Path, **db_config):
    config = Configuration(**db_config)
    logger.info("Creating database %s", database)
    LocalDatabase.create_database(base_path=database, configuration=config)


@main.command('add-client')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.pass_obj
def add_client(database: LocalDatabase, client_name: str):
    if not client_name:
        logger.warning("No GROUP_NAME specified.  Nothing to do.")

    config = protocol.ClientConfiguration(
        client_name=client_name,
    )
    database.create_client(config)
    logger.info("Created client %s", config.client_id)


@main.command('add-directory')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('ROOT_NAME')
@click.argument('ROOT_PATH')
@click.option('--include', multiple=True)
@click.option('--exclude', multiple=True)
@click.pass_obj
def add_directory(database: LocalDatabase, client_name: str, root_name: str, root_path: str,
                  include: List[str], exclude: List[str]):
    def normalize(path: str) -> str:
        tmp = Path(path)
        if tmp.is_absolute():
            return str(tmp.relative_to(root_path))
        return path

    # Normalize paths correctly
    root_path = Path(root_path).absolute()
    include = [normalize(path) for path in include]
    exclude = [normalize(path) for path in exclude]
    root_path = str(root_path)

    client = database.open_client_session(client_name=client_name)
    new_dir = protocol.ClientConfiguredBackupDirectory(base_path=root_path)
    for path in include:
        new_dir.filters.append(protocol.Filter(protocol.FilterType.INCLUDE, path))
    for path in exclude:
        new_dir.filters.append(protocol.Filter(protocol.FilterType.EXCLUDE, path))
    client.client_config.backup_directories[root_name] = new_dir
    client.save_config()


@main.command("backup")
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']),
              default=str(datetime.now()))
@click.option("--description")
@click.option("--overwrite/--no-overwrite", default=False)
@click.pass_obj
def backup(database: LocalDatabase,  client_name: str, timestamp: datetime, description: Optional[str],
           overwrite: bool):
    server_session = database.open_client_session(client_name=client_name)

    async def _backup():
        backup_session = await server_session.start_backup(
            backup_date=timestamp,
            allow_overwrite=overwrite,
            description=description,
        )
        logger.info(f"Backup - {backup_session}")

        backup_scanner = scanner.Scanner(backup_session, server_session)
        await backup_scanner.scan_all()
        logger.info("Finalizing backup")
        await backup_session.complete()
        logger.info("All done.")

    asyncio.get_event_loop().run_until_complete(_backup())
