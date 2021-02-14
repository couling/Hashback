import logging
import click
from typing import List, Dict
from os import path
from datetime import datetime

from .misc import setup_logging
from .database import Database, DBConfig
from .scanner import Scanner, FileName
from . import datamodel


_log = logging.getLogger(__name__)


@click.group()
@click.option("--database", default=".", envvar="DATABASE")
@click.pass_context
def main(ctx: click.Context, database: str):
    setup_logging()
    if ctx.invoked_subcommand != 'create':
        ctx.obj = Database(database)
    else:
        ctx.obj = database


@main.command('create')
@click.option("--backup-by", type=click.Choice(['date', 'timestamp'], case_sensitive=False), default="date")
@click.option("--friendly-links/--flat", default=True)
@click.option("--store-split-count", type=click.INT, default=2)
@click.option("--compression-type", type=click.Choice(o.value for o in datamodel.CompressionType),
              default=datamodel.CompressionType.none.value)
@click.pass_obj
def create(database: str, **db_config):
    config = DBConfig(**db_config)
    _log.info("Creating database %s", database)
    Database.create(base_dir=database, config=config)


@main.command('add-group')
@click.argument('GROUP_NAME', nargs=-1, envvar="GROUP_NAME")
@click.pass_obj
def add_group(database: Database, group_name: List[str]):
    if not group_name:
        _log.warning("No GROUP_NAME specified.  Nothing to do.")
    for name in group_name:
        database.add_group(name)


@main.command("backup")
@click.argument("DIRECTORIES", nargs=-1)
@click.option("--group-name", required=True, envvar="GROUP_NAME")
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']),
              default=str(datetime.now()))
@click.pass_obj
def backup(database: Database,  group_name: str, directories: List[str], timestamp: datetime):
    def sequence():
        x = iter(directories)
        if len(directories) %2 != 0:
            raise ValueError("Directories must be Name, Path, Name, Path, ...)")
        try:
            while True:
                yield next(x), next(x)
        except StopIteration:
            pass

    _log.info("Saving backup for %s", timestamp)
    directories = [d for d in sequence()]

    group = database.get_group(group_name)

    scanner = Scanner()
    content = {}
    for name, dir_path in directories:
        _log.info("Saving %s", name)
        scan_result = scanner.scan(dir_path)
        content[name] = scan_result.stat
        backup_dir(database, scan_result, dir_path)

    group.add_backup(backup_date=timestamp, content=content)


def backup_dir(database: Database, dir: FileName, dir_path: str):
    for name, child in dir.children.items():
        child_path = path.join(dir_path, name)
        if child.stat.type is datamodel.FileType.REGULAR:
            try:
                with database.open(child.stat.hash, "xb") as target, open(child_path, "rb") as source:
                    bytes_read = source.read(409600)
                    while bytes_read:
                        target.write(bytes_read)
                        bytes_read = source.read(409600)
            except FileExistsError:
                pass
            else:
                _log.debug("File %s as %s", child_path, child.stat.hash)
        elif child.stat.type is datamodel.FileType.DIRECTORY:
            backup_dir(database, child, child_path)
    contents = {name: child.stat for name, child in dir.children.items()}
    try:
        database.create_directory(contents)
    except FileExistsError:
        pass
    else:
        _log.debug("Directory %s as %s", dir_path, dir.stat.hash)


if __name__ == '__main__':
    main()
