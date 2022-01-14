import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import click
import dateutil.tz

from .. import backup_algorithm, protocol
from .db_admin import click_main
from ..local_database import LocalDatabase, LocalDatabaseBackupSession
from ..local_file_system import LocalFileSystemExplorer
from ..misc import run_then_cancel, str_exception

logger = logging.getLogger(__name__)


@click_main.command("migrate-backup")
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument('BASE_PATH', type=click.Path(path_type=Path, exists=True, file_okay=False))
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--batch/--single", default=False)
@click.option("--description")
@click.option("--accept-warning/--no-accept-warning", default=False)
@click.option("--hardlinks/--no-hardlinks", default=False)
@click.option("--full-prescan/--no-full-prescan", default=True)
@click.pass_obj
def migrate_backup(database: LocalDatabase,  client_name: str, base_path: Path, timestamp: Optional[datetime],
                   description: Optional[str], **options: bool):
    server_session = database.open_client_session(client_id_or_name=client_name)
    base_path = base_path.absolute()

    if not options['accept_warning']:
        _warn_migrate_backup(base_path, server_session, **options)
        return

    # The local filesystem explorer caches the inodes and indexs them by st_dev, st_ino meaning that when hardlinks
    # of the same file are encountered, there's no need to rescan
    explorer = LocalFileSystemExplorer()
    if options['batch']:
        for directory in base_path.iterdir():
            timestamp = datetime.fromisoformat(directory.name)
            if timestamp.tzinfo:
                timestamp = timestamp.astimezone(dateutil.tz.gettz())
            else:
                timestamp = timestamp.replace(tzinfo=dateutil.tz.gettz())
            migrate_single_backup(
                server_session=server_session,
                base_path=directory,
                timestamp=timestamp,
                description=description,
                explorer=explorer,
                **options,
            )
    else:
        if timestamp.tzinfo is None:
            timestamp = timestamp.astimezone(dateutil.tz.gettz())
        migrate_single_backup(
            server_session=server_session,
            base_path=base_path,
            timestamp=timestamp,
            description=description,
            explorer=explorer,
            **options,
        )


def _warn_migrate_backup(base_path: Path, server_session: protocol.ServerSession, **options: bool):
    warning_message = "WARNING! migrate-backup is DANGEROUS! Make sure you understand it first.\n\n"
    if options['hardlinks']:
        warning_message += (
            "--hardlinks ... Changing the migrated files after migration WILL CORRUPT YOUR BACKUP DATABASE. "
            "  Hardlinks are fast but hardlinks are DANGEROUS.\n"
            "To avoid corruption you are advised either to use --no-hardlinks (creating a copy) or "
            "delete the original after migration.\n\n"
        )

    if options['batch']:
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
            "--batch will use an iso formatted timestamp or date in the file path to infer multiple "
            "backup dates.  The full list of backups migrated will be:\n"
        )
        warning_message += timestamps
        warning_message += "\n\n"

    warning_message += "You have configured the following directories to be migrated:\n"

    for directory in server_session.client_config.backup_directories.values():
        if options['batch']:
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
                          description: str,
                          explorer: Callable[[protocol.ClientConfiguredBackupDirectory], protocol.DirectoryExplorer],
                          **options: bool):
    async def _backup():
        backup_session = await server_session.start_backup(
            backup_date=timestamp,
            description=description,
        )
        logger.info(f"Started Backup Session {backup_session.config.session_id}")

        try:
            logger.info(f"Migrating Backup - {backup_session.config.backup_date}")

            backup_controller = BackupMigrationController(
                file_system_explorer=explorer,
                backup_session=backup_session,
                hardlinks=options.get('hardlinks', False),
            )
            # This is one context where the full prescan might be a lot faster. Use it by default.
            backup_controller.full_prescan = options.get('full_prescan', True)
            backup_controller.read_last_backup = False
            backup_controller.match_meta_only = False

            for root_name, scan_spec in backup_session.server_session.client_config.backup_directories.items():
                await backup_controller.backup_root(
                    root_name=root_name,
                    scan_spec=offset_base_path(new_base_path=base_path, scan_spec=scan_spec)
                )
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


class BackupMigrationController(backup_algorithm.BackupController):

    backup_session: LocalDatabaseBackupSession

    def __init__(self, *args, hardlinks: bool, **kwargs):
        super().__init__(*args, **kwargs)
        self.hardlinks = hardlinks

    async def _upload_file(self, explorer: protocol.DirectoryExplorer, directory: protocol.Directory, child_name: str):

        inode = directory.children[child_name]

        if not self.hardlinks or inode.type is not protocol.FileType.REGULAR:
            return await super()._upload_file(explorer, directory, child_name)

        # This is really cheating, we assume this is a local database session and use a protected field to add the
        # file as a hardlink to the original instead of the super() version which would copy it.
        # pylint: disable=protected-access
        target_path = self.backup_session._new_object_path_for(inode.hash)
        try:
            source_path = Path(explorer.get_path(child_name))
            logger.info(f"Creating hardlink '{source_path}' → '{target_path}'")
            source_path.link_to(target_path)
            return
        except OSError as exc:
            logger.error(f"Failed to create hardlink ({exc}) falling back to copying")
            return await super()._upload_file(explorer, directory, child_name)