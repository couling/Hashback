import logging
from pathlib import Path

import click

from .. import protocol
from ..local_database import Configuration, LocalDatabase
from ..log_config import setup_logging
from ..misc import register_clean_shutdown


logger = logging.getLogger(__name__)


def main():
    register_clean_shutdown()
    setup_logging()
    # pylint: disable=no-value-for-parameter
    click_main()


@click.group()
@click.argument("DATABASE", type=click.Path(path_type=Path, file_okay=False), envvar="BACKUP_DATABASE")
@click.pass_context
def click_main(ctx: click.Context, database: Path):
    if ctx.invoked_subcommand != 'create':
        ctx.obj = LocalDatabase(database)
    else:
        ctx.obj = database


@click_main.command('create')
@click.option("--store-split-count", type=click.INT, default=2)
@click.pass_obj
def create(database: Path, **db_config):
    config = Configuration(**db_config)
    logger.info("Creating database %s", database)
    try:
        LocalDatabase.create_database(base_path=database, configuration=config)
    except FileExistsError as exc:
        raise click.ClickException(f"Database already exists at {database}") from exc


@click_main.command('add-client')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.pass_obj
def add_client(database: LocalDatabase, client_name: str):
    config = protocol.ClientConfiguration(
        client_name=client_name,
    )
    try:
        database.create_client(config)
    except FileExistsError as exc:
        raise click.ClickException(f"Client '{client_name}' already exists")
    logger.info("Created client %s", config.client_id)


@click_main.command('add-directory')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('ROOT_NAME')
@click.argument('ROOT_PATH')
@click.option('--include', multiple=True)
@click.option('--exclude', multiple=True)
@click.option('--pattern-ignore', multiple=True)
@click.pass_obj
def add_root(database: LocalDatabase, client_name: str, root_name: str, root_path: str, **options):
    client = database.open_client_session(client_id_or_name=client_name)
    new_dir = protocol.ClientConfiguredBackupDirectory(base_path=root_path)
    for path in options['include']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.INCLUDE, path=path))
    for path in options['exclude']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.EXCLUDE, path=path))
    for pattern in options['pattern-ignore']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.PATTERN_EXCLUDE, path=pattern))
    client.client_config.backup_directories[root_name] = new_dir
    client.save_config()
