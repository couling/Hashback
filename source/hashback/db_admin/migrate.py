import logging
import logging
import multiprocessing
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

import click
import dateutil.tz

from .db_admin import click_main
from .. import protocol
from ..local_database import DIR_SUFFIX, LocalDatabase
from ..local_file_system import LocalFileSystemExplorer
from ..misc import str_exception

logger = logging.getLogger(__name__)


@click_main.command("migrate-backup")
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.argument("ROOT_NAME")
@click.argument('BASE_PATH', type=click.Path(path_type=Path, exists=True, file_okay=False))
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--batch/--single", default=False)
@click.option("--description")
@click.option("--accept-warning/--no-accept-warning", default=False)
@click.option("--hardlinks/--no-hardlinks", default=False)
@click.option("--full-prescan/--no-full-prescan", default=True)
@click.pass_obj
def migrate_backup(database: LocalDatabase,  client_name: str, root_name: str, base_path: Path,
                   timestamp: Optional[datetime], description: Optional[str], **options: bool):
    base_path = base_path.absolute()

    if not options['accept_warning']:
        server_session = database.open_client_session(client_id_or_name=client_name)
        _warn_migrate_backup(base_path, server_session, **options)
        return

    # The local filesystem explorer caches the inodes and indexs them by st_dev, st_ino meaning that when hardlinks
    # of the same file are encountered, there's no need to rescan
    explorer = LocalFileSystemExplorer()
    migrator = Migrator(database, client_name)
    if options['batch']:
        for directory in base_path.iterdir():
            timestamp = datetime.fromisoformat(directory.name)
            if timestamp.tzinfo:
                timestamp = timestamp.astimezone(dateutil.tz.gettz())
            else:
                timestamp = timestamp.replace(tzinfo=dateutil.tz.gettz())
            migrator.migrate_single_backup(
                timestamp=timestamp,
                root_name=root_name,
                base_path=directory,
                description=description,
            )
    else:
        if timestamp is None:
            timestamp = datetime.now(dateutil.tz.gettz())
        elif timestamp.tzinfo is None:
            timestamp = timestamp.astimezone(dateutil.tz.gettz())
        migrator.migrate_single_backup(
            timestamp=timestamp,
            root_name=root_name,
            base_path=base_path,
            description=description,
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


class Migrator:

    inode_cache: Dict[int, protocol.Inode]
    exists_cache: Set[str]
    database: LocalDatabase
    mp_pool: multiprocessing.Pool
    hardlink: bool = False

    def __init__(self, server_session: LocalDatabase, client_id: str):
        self.inode_cache = {}
        self.exists_cache = set()
        self.database = server_session
        self.client_id = client_id

    def migrate_single_backup(self, timestamp: datetime, root_name: str,  base_path: Path, description: str = None):
        session = self.database.open_client_session(self.client_id)
        client_config = session.client_config
        timestamp = client_config.normalize_backup_date(timestamp)

        backup_meta_path = session._path_for_backup_date(timestamp)

        try:
            existing_backup = protocol.Backup.parse_file(backup_meta_path)
            if root_name in existing_backup.roots:
                raise click.ClickException(f"Backup for {client_config.date_string(timestamp)} already exists")
        except FileNotFoundError:
            existing_backup = protocol.Backup(
                client_id=client_config.client_id,
                client_name=client_config.client_name,
                backup_date=client_config.normalize_backup_date(timestamp),
                started=timestamp,
                completed=timestamp,
                roots={},
                description=description,
            )

        logger.info("Migrating Backup - %s as %s in %s", base_path, root_name,
                    client_config.date_string(existing_backup.backup_date))

        root_hash = self.backup_dir(base_path)
        existing_backup.roots[root_name] = protocol.Inode.from_stat(base_path.stat(), root_hash)

        backup_meta_path.parent.mkdir(parents=True)
        with backup_meta_path.open('w') as file:
            file.write(existing_backup.json(indent=True))


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
                    continue

                child_inode.hash = backup_method(self, child_path)
                self.inode_cache[child.inode()] = child_inode
                children[child.name] = child_inode


        directory_content = protocol.Directory(__root__=children).hash()
        ref_hash = directory_content.ref_hash + DIR_SUFFIX
        if ref_hash not in self.exists_cache:
            target_path = self.database.store_path_for(ref_hash=ref_hash)
            if not target_path.exists():
                target_path.parent.mkdir(parents=True)
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
                target_path.parent.mkdir(parents=True)
                try:
                    file_path.link_to(target_path)
                except OSError as exc:
                    logger.warning(f"Hardlink Failed {str(exc)}")
                    temp_file_path = target_path.parent / (target_path.name + ".tmp")
                    with temp_file_path.open('xb') as target:
                        try:
                            with file_path.open('rb') as source:
                                bytes_read = source.read(protocol.READ_SIZE)
                                while bytes_read:
                                    target.write(bytes_read)
                                    bytes_read = source.read(protocol.READ_SIZE)
                            temp_file_path.rename(target_path)
                        except:
                            temp_file_path.unlink()
                            raise

            self.exists_cache.add(ref_hash)

        return ref_hash

    def backup_symlink(self, file_path: Path) -> str:
        content = os.readlink(file_path)
        ref_hash = protocol.hash_content(content)
        target_path = self.database.store_path_for(ref_hash)
        if not target_path.exists():
            target_path.parent.mkdir(parents=True)
            with target_path.open('wb') as file:
                file.write(content.encode())
        return ref_hash

    def backup_pipe(self, file_path: Path) -> str:
        ref_hash = protocol.EMPTY_FILE
        target_path = self.database.store_path_for(ref_hash)

        if not target_path.exists():
            target_path.parent.mkdir(parents=True)
            # No content
            target_path.touch()
        return ref_hash

    _BACKUP_TYPES = {
        protocol.FileType.DIRECTORY: backup_dir,
        protocol.FileType.REGULAR: backup_regular_file,
        protocol.FileType.LINK: backup_symlink,
        protocol.FileType.PIPE: backup_pipe,
    }
