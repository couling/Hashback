import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import dateutil.tz

from . import protocol, scanner
from .local_database import LocalDatabase, Configuration
from .misc import run_then_cancel, str_exception, setup_logging, register_clean_shutdown

logger = logging.getLogger(__name__)


def main():
    register_clean_shutdown()
    setup_logging()
    # pylint: disable=no-value-for-parameter
    click_main()


@click.group()
@click.option("--database", type=click.Path(path_type=Path, file_okay=False), default=".", envvar="BACKUP_DATABASE")
@click.pass_context
def click_main(ctx: click.Context, database: Path):
    if ctx.invoked_subcommand != 'create':
        ctx.obj = LocalDatabase(database)
    else:
        ctx.obj = database


@click_main.command('create')
@click.option("--backup-by", type=click.Choice(['date', 'timestamp'], case_sensitive=False), default="date")
@click.option("--friendly-links/--flat", default=True)
@click.option("--store-split-count", type=click.INT, default=2)
@click.pass_obj
def create(database: Path, **db_config):
    config = Configuration(**db_config)
    logger.info("Creating database %s", database)
    LocalDatabase.create_database(base_path=database, configuration=config)


@click_main.command('add-client')
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


@click_main.command('add-directory')
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('ROOT_NAME')
@click.argument('ROOT_PATH', type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option('--include', type=click.Path(path_type=Path), multiple=True)
@click.option('--exclude', type=click.Path(path_type=Path), multiple=True)
@click.pass_obj
def add_directory(database: LocalDatabase, client_name: str, root_name: str, root_path: Path, **options):
    def normalize(path: Path) -> str:
        if path.is_absolute():
            return str(path.relative_to(root_path))
        return str(path)

    # Normalize paths correctly
    root_path = Path(root_path).absolute()
    include = [normalize(path) for path in options['include']]
    exclude = [normalize(path) for path in options['exclude']]
    root_path = str(root_path)

    client = database.open_client_session(client_id_or_name=client_name)
    new_dir = protocol.ClientConfiguredBackupDirectory(base_path=root_path)
    for path in include:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.INCLUDE, path=path))
    for path in exclude:
        new_dir.filters.append(protocol.Filter(filter=protocol.FilterType.EXCLUDE, path=path))
    client.client_config.backup_directories[root_name] = new_dir
    client.save_config()


@click_main.command("migrate-backup")
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('BASE_PATH', type=click.Path(path_type=Path, exists=True, file_okay=False))
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--infer-timestamp/--no-infer-timestamp", default=False)
@click.option("--description")
@click.option("--accept-warning/--no-accept-warning", default=False)
@click.option("--hardlinks/--no-hardlinks", default=False)
@click.option("--fast-unsafe/-safe", default=False)
@click.pass_obj
def migrate_backup(database: LocalDatabase,  client_name: str, base_path: Path, timestamp: datetime,
                   description: Optional[str], **options: bool):
    server_session = database.open_client_session(client_id_or_name=client_name)
    base_path = base_path.absolute()

    if not options['accept_warning']:
        _warn_migrate_backup(base_path, server_session, **options)
        return

    if options['infer_timestamp']:
        for directory in base_path.iterdir():
            timestamp = datetime.fromisoformat(directory.name)
            if timestamp.tzinfo:
                timestamp.astimezone(dateutil.tz.gettz())
            migrate_single_backup(
                server_session=server_session,
                base_path=directory,
                timestamp=timestamp,
                description=description,
                hardlinks=options['hardlinks'],
                fast_unsafe=options['fast_unsafe'],
            )
    else:
        if timestamp.tzinfo is None:
            timestamp = timestamp.astimezone(dateutil.tz.gettz())
        migrate_single_backup(
            server_session=server_session,
            base_path=base_path,
            timestamp=timestamp,
            description=description,
            hardlinks=options['hardlinks'],
            fast_unsafe=options['fast_unsafe'],
        )


def _warn_migrate_backup(base_path: Path, server_session: protocol.ServerSession, **options: bool):
    warning_message = "WARNING! migrate-backup is DANGEROUS! Make sure you understand it first.\n\n"
    if options['hardlinks']:
        warning_message += (
            "--hardlinks ... Changing the migrated files after migration WILL CORRUPT YOUR BACKUP DATABASE. "
            "  Hardlinks are fast but hardlinks are DANGEROUS.\n"
            "To avoid corruption you are advised either to use --no-hardlinks (creating a copy) or consider "
            "deleting the original after migration.\n\n"
        )

    if options['infer_timestamp']:
        try:
            timestamps = ', '.join(sorted(datetime.fromisoformat(path.name).isoformat()
                                          for path in base_path.iterdir()))

        # pylint: disable=broad-except
        # There's too many reasons this can fail.  We're only informing the user of warnings here, not actually
        # doing work so just carry on.
        except Exception as exc:
            logger.error(f"Unable to determine list because of error: {str_exception(exc)}")
            timestamps = f"Unable to determine list because of error: {str_exception(exc)}"

        warning_message += (
            "--infer-timestamp will use an iso formatted timestamp or date in the file path to infer multiple "
            "backup dates.  The full list of backups migrated will be:\n"
        )
        warning_message += timestamps
        warning_message += "\n\n"

    warning_message += "You have configured the following directories to be migrated:\n"

    for directory in server_session.client_config.backup_directories.values():
        if options['infer_timestamp']:
            warning_message += f"{base_path / '<timestamp>' / Path(*Path(directory.base_path).parts[1:])}\n"
        else:
            warning_message += f"{base_path / Path(*Path(directory.base_path).parts[1:])}\n"
    warning_message += "\nThese will be stored in the database as:\n"
    for directory in server_session.client_config.backup_directories.values():
        warning_message += f"{directory.base_path}\n"

    warning_message += (
        "\nTo accept this warning and run the migration, run the same command again with an additional option: "
        "--accept-warning\n\n"
        "Always run WITHOUT --accept-warning first to check the specific warnings.\n\n"
        "The database has NOT been modified."
    )
    logger.warning(warning_message)


def migrate_single_backup(server_session: protocol.ServerSession, base_path: Path, timestamp: datetime,
                          description: str, **options: bool):
    async def _backup():
        backup_session = await server_session.start_backup(
            backup_date=timestamp,
            description=description,
        )

        logger.info(f"Migrating Backup - {backup_session.config.backup_date}")
        backup_directories = {name: offset_base_path(value, base_path)
                              for name, value in server_session.client_config.backup_directories.items()}
        backup_scanner = BackupMigrationScanner(backup_session, options['hardlinks'])
        try:
            await backup_scanner.scan_all(backup_directories, fast_unsafe=options['fast_unsafe'])
            logger.info("Finalizing backup")
            await backup_session.complete()
            logger.info("%s done", timestamp.isoformat())
        except (Exception, asyncio.CancelledError) as exc:
            logger.info("Discarding session due to error (%s)", str_exception(exc))
            await backup_session.discard()
            raise

    run_then_cancel(_backup())


def offset_base_path(scan_spec: protocol.ClientConfiguredBackupDirectory,
                     new_base_path: Path) -> protocol.ClientConfiguredBackupDirectory:
    base_path = Path(scan_spec.base_path)
    assert base_path.is_absolute()
    # Chop off the root of the file system (/ or c:) and replace with self._new_base_path
    base_path = new_base_path / Path(*base_path.parts[1:])
    return protocol.ClientConfiguredBackupDirectory(
        base_path=str(base_path),
        filters=scan_spec.filters,
    )


class BackupMigrationScanner(scanner.Scanner):

    def __init__(self, backup_session: protocol.BackupSession, hardlinks: bool):
        super().__init__(backup_session)
        self.hardlinks = hardlinks

    async def _upload_missing_file(self, path: Path, directory: protocol.Directory, missing_file: str):
        if not self.hardlinks:
            await super()._upload_missing_file(path, directory, missing_file)
            return
        inode = directory.children[missing_file]
        if inode.type == protocol.FileType.REGULAR:
            # This is really cheating, we assume this is a local database session and use a protected field to add the
            # file as a hardlink to the original
            # TODO add a hardlink option or method to local database.  This is likely to be useful elsewhere
            # pylint: disable=protected-access
            target_path = self.backup_session._new_object_path_for(inode.hash)
            try:
                source_path = path / missing_file
                logger.info(f"Creating hardlink '{source_path}' → '{target_path}'")
                source_path.link_to(target_path)
                return
            except OSError as exc:
                logger.error(f"Failed to create hardlink ({exc}) falling back to copying")
                await super()._upload_missing_file(path, directory, missing_file)


if __name__ == '__main__':
    main()
