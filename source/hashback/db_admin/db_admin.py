import logging
from pathlib import Path
from urllib.parse import urlparse
from copy import deepcopy

import click

from .. import protocol
from ..log_config import setup_logging
from ..misc import register_clean_shutdown


logger = logging.getLogger(__name__)


def main():
    register_clean_shutdown()
    setup_logging()
    # pylint: disable=no-value-for-parameter
    click_main()


@click.group()
@click.argument("DATABASE", envvar="BACKUP_DATABASE")
@click.pass_context
def click_main(ctx: click.Context, database: str):
    if ctx.invoked_subcommand != 'create':
        url = urlparse(database)
        if url.scheme in ('', 'file'):
            from ..local_database import LocalDatabase
            ctx.obj = LocalDatabase(Path(url.path))
        elif url.scheme == 's3':
            from ..aws_s3_client import S3Database
            ctx.obj = S3Database(bucket_name=url.hostname, directory=url.path[1:])
    else:
        ctx.obj = database


@click_main.command('create')
@click.option("--store-split-count", type=click.INT, default=2)
@click.pass_obj
def create(database: Path, **db_config):
    from ..local_database import LocalDatabase
    config = LocalDatabase.Configuration(**db_config)
    logger.info("Creating database %s", database)
    try:
        LocalDatabase.create_database(base_path=database, configuration=config)
    except FileExistsError as exc:
        raise click.ClickException(f"Database already exists at {database}") from exc


@click_main.command('add-client')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.pass_obj
def add_client(database: protocol.BackupDatabase, client_name: str):
    config = protocol.ClientConfiguration(client_name=client_name)
    try:
        database.save_client_config(config)
    except FileExistsError as ex:
        raise click.ClickException(f"Client '{client_name}' already exists") from ex
    logger.info("Created client %s", config.client_id)


@click_main.command('add-directory')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('ROOT_NAME')
@click.argument('ROOT_PATH')
@click.option('--include', multiple=True)
@click.option('--exclude', multiple=True)
@click.option('--pattern-ignore', multiple=True)
@click.pass_obj
def add_root(database: protocol.BackupDatabase, client_name: str, root_name: str, root_path: str, **options):
    client_config = database.load_client_config(client_id_or_name=client_name)
    new_dir = protocol.ClientConfiguredBackupDirectory(base_path=root_path)
    for path in options['include']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.INCLUDE, path=path))
    for path in options['exclude']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.EXCLUDE, path=path))
    for pattern in options['pattern_ignore']:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.PATTERN_EXCLUDE, path=pattern))
    new_config = deepcopy(client_config)
    new_config.backup_directories[root_name] = new_dir
    database.save_client_config(new_config)
