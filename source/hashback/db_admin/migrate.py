import asyncio
import logging
import multiprocessing
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Set

import click
import dateutil.tz

from .db_admin import click_main
from .. import protocol
from ..local_database import LocalDatabase, DIR_SUFFIX
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




class Migrator:

    inode_cache: Dict[int, protocol.Inode]
    exists_cache: Set[str]
    database: LocalDatabase
    mp_pool: multiprocessing.Pool

    def __init__(self, server_session: LocalDatabase):
        self.inode_cache = {}
        self.exists_cache = set()
        self.backup_server = server_session

    def backup_dir(self, directory: Path) -> str:
        logger.info(f"Migrating {directory}")
        child: os.DirEntry
        children = {}
        with os.scandir(directory) as scan:
            for child in scan:
                child_inode = self.inode_cache.get(child.inode())
                if child_inode is not None:
                    children[child.name] = child_inode
                    continue

                child_path = directory / child
                child_inode = protocol.Inode.from_stat(child_path.lstat(), None)
                backup_method = self._BACKUP_TYPES.get(child_inode.type)
                if backup_method is None:
                    logger.debug(f"Warning file of type {child_inode.type}: {child_path}")
                    children.pop(child.name)
                    continue

                child_inode.hash = backup_method(self, child_path)
                self.inode_cache[child.inode()] =  child_inode

        directory_content = protocol.Directory(__root__=children).hash()
        ref_hash = directory_content.ref_hash + DIR_SUFFIX
        if ref_hash not in self.exists_cache:
            target_path = self.database.store_path_for(ref_hash=ref_hash)
            if not target_path.exists():
                with target_path.open('wb') as file:
                    file.write(directory_content.content)

            self.exists_cache.add(ref_hash)

        return directory_content.ref_hash

    def backup_regular_file(self, file_path: Path) -> str:
        logger.debug(f"File Backup {file_path}")
        hash_obj = protocol.HashType()
        with file_path.open('rb') as file:
            bytes_read = file.read(protocol.READ_SIZE)
            while bytes_read:
                hash_obj.update(bytes_read)
                bytes_read = file.read(protocol.READ_SIZE)

        ref_hash = hash_obj.hexdigest()
        if ref_hash not in self.exists_cache:
            target_path = self.database.store_path_for(ref_hash)
            if not target_path.exists():
                try:
                    file_path.link_to(target_path)
                except OSError as exc:
                    logger.warning(f"Hardlink Failed {str(exc)}")
                    temp_file_path = target_path.parent / (target_path.name + ".tmp")
                    with temp_file_path.open('wb') as target, file_path.open('rb') as source:
                        bytes_read = source.read(protocol.READ_SIZE)
                        while bytes_read:
                            target.write(bytes_read)
                            bytes_read = source.read(protocol.READ_SIZE)
                    temp_file_path.rename(target_path)

            self.exists_cache.add(ref_hash)

        return ref_hash

    def backup_symlink(self, file_path: Path) -> str:
        content =  os.readlink(file_path)
        ref_hash = protocol.hash_content(content)
        target_path = self.database.store_path_for(ref_hash)
        if not target_path.exists():
            with target_path.open('wb') as file:
                file.write(content)
        return ref_hash

    def backup_pipe(self, file_path: Path) -> str:
        ref_hash = protocol.EMPTY_FILE
        target_path = self.database.store_path_for(ref_hash)
        if not target_path.exists():
            # No content
            target_path.touch()
        return ref_hash

    _BACKUP_TYPES = {
        protocol.FileType.DIRECTORY: backup_dir,
        protocol.FileType.REGULAR: backup_regular_file,
        protocol.FileType.LINK: backup_symlink,
        protocol.FileType.PIPE: backup_pipe,
    }
