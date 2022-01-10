import logging
import os
from asyncio import gather
from pathlib import Path
from typing import Dict
from typing import Optional, Callable
from uuid import uuid4

from . import protocol
from .misc import str_exception

logger = logging.getLogger(__name__)


class BackupController:

    def __init__(self,
                 file_system_explorer: Callable[[protocol.ClientConfiguredBackupDirectory], protocol.DirectoryExplorer],
                 backup_session: protocol.BackupSession):
        self.all_files = {}
        self.backup_session = backup_session
        self.file_system_explorer = file_system_explorer
        self.read_last_backup = True
        self.match_meta_only = True

    async def backup_all(self, backup_roots: Optional[Dict[str, protocol.ClientConfiguredBackupDirectory]] = None):
        """
        Scan all directories.
        :param backup_roots: The directories to backup.  If not, this will be pulled from the client's server.
        :param fast_unsafe: Compare meta-data to the previous backup stored on the same server.  This can be much faster
            But it is "unsafe" because it does not check the file content.  Theoretically content can change without
            changing the timestamp or size.  It's rare, but it could theoretically happen.
        """
        if backup_roots is None:
            backup_roots = self.backup_session.server_session.client_config.backup_directories

        # Scans are internally parallelized.  Let's not gather() this one so we have some opportunity to understand
        # what it was doing if it failed.
        if self.read_last_backup:
            last_backup = await self.backup_session.server_session.get_backup()
            if last_backup is None:
                logger.warning("No previous backup found. This scan will slow-safe not fast-unsafe")
                last_backup_roots = {}
            else:
                last_backup_roots = last_backup.roots
                logger.warning("Comparing meta data to last backup, will not check content for existing files.")
            for name, scan_spec in backup_roots.items():
                last_backup = last_backup_roots.get(name)
                if last_backup is None:
                    logger.warning(f"Directory '{name}' not in last backup")
                await self.backup_root(root_name=name, scan_spec=scan_spec, last_backup=last_backup.hash)
        else:
            logger.info("Ignoring last backup, will hash every file")
            for name, scan_spec in backup_roots.items():
                await self.backup_root(root_name=name, scan_spec=scan_spec)

    async def backup_root(self, root_name: str, scan_spec: protocol.ClientConfiguredBackupDirectory,
                          last_backup: Optional[str] = None):

        logger.info(f"Backing up '{root_name}' ({scan_spec.base_path})")
        explorer = self.file_system_explorer(scan_spec)
        root_hash = await self._backup_directory(explorer, last_backup)
        root_inode = await explorer.inode()
        root_inode.hash = root_hash
        await self.backup_session.add_root_dir(root_name, root_inode)

    async def _backup_directory(self, explorer: protocol.DirectoryExplorer,
                                last_backup: Optional[protocol.Inode]) -> str:
        """
        Backup a directory, returning the ref-hash
        :param explorer: A DirectoryExplorer attached to the directory to backup.
        :param last_backup: The last backup definition if available.
        """
        directory_definition = await self._scan_directory(explorer, last_backup)
        if last_backup is None or last_backup.hash != directory_definition.hash():
            return await self._upload_directory(explorer, directory_definition)
        else:
            logger.debug(f"Skipping %s directory not changed", explorer.get_path(None))
            return last_backup.hash


    async def _scan_directory(self, explorer: protocol.DirectoryExplorer,
                              last_backup: Optional[protocol.Inode]) -> protocol.Directory:

        if self.read_last_backup and last_backup is not None:
            last_backup_children = await self.backup_session.server_session.get_directory(last_backup)
        else:
            last_backup_children = {}

        children = {}
        for child_name, child_inode in await explorer.iter_children():
            if child_inode.type is protocol.FileType.DIRECTORY:
                child_inode.hash = await self._backup_directory(
                    explorer.get_child(child_name),
                    last_backup_children.get(child_name),
                )

            else:
                if (child_inode.hash is None and self.match_meta_only and last_backup is not None
                        and child_name in last_backup_children):
                    # Try to match on meta only from the last backup
                    child_last_backup = last_backup_children[child_name]
                    child_inode.hash = child_last_backup.hash
                    # After copying the hash across, the inodes will match [only] if the meta matches.
                    if child_inode != child_last_backup:
                        # It didn't match, remove the hash because it's most likely wrong.
                        child_inode.hash = None

                if child_inode.hash is None:
                    # The explorer will correctly handle reading the content of links etc.
                    # Opening a symlink will return a reader to read the link itself, NOT the file it links to.
                    with await explorer.open_child(child_name, mode='r') as file:
                        child_inode.hash = await protocol.async_hash_content(file)

            children[child_name] = child_inode

        return protocol.Directory(__root__=children)


    async def _upload_directory(self, explorer: protocol.DirectoryExplorer, directory: protocol.Directory) -> str:
        """
        Uploads a directory to the server.

        First it uploads the filenames and inode information including hashes for all children.  The server can then
        reject this if any or all children are missing from the server.  If that happens the server will respond
        with a list of missing children...  we then upload all missing children and try again.
        """
        logger.debug(f"Uploading directory {explorer}")
        # The directory has changed.  We send the contents over to the server. It will tell us what else it needs.
        server_response = await self.backup_session.directory_def(directory)
        if not server_response.success:
            logger.debug(f"{len(server_response.missing_files)} missing files in {explorer}")
            await gather(*(self._upload_file(explorer, directory, missing_file)
                           for missing_file in server_response.missing_files))
            # Retry the directory now that all files have been uploaded.
            # We let the server know this replaces the previous request.  Some servers may place a marker on the session
            # preventing us from completing until unsuccessful requests have been replaced.
            server_response = await self.backup_session.directory_def(directory, replaces=server_response.missing_ref)
            if not server_response.success:
                raise protocol.ProtocolError(
                    "Files disappeared server-side while backup is in progress.  "
                    "This must not happen or the backup will be corrupted. "
                    f"{ {name: directory.children.get(name) for name in server_response.missing_files} }",
                )

        logger.debug(f"Server accepted directory {explorer.get_path(None)} as {server_response.ref_hash}")
        return server_response.ref_hash

    async def _upload_file(self, explorer: protocol.DirectoryExplorer, directory: protocol.Directory,
                           child_name: str):
        """
        Upload a file after the server has stated it does not already have a copy.
        """
        file_path = explorer.get_path(child_name)
        logger.info(f"Uploading {file_path}")
        try:
            with await explorer.open_child(child_name, 'r') as missing_file_content:
                resume_id = uuid4()
                new_hash = await self.backup_session.upload_file_content(
                    file_content=missing_file_content,
                    resume_id=resume_id,
                )
                if new_hash != directory.children[child_name].hash:
                    logger.warning(f"Calculated hash for {file_path} ({resume_id}) was "
                                   f"{directory.children[child_name].hash} but server thinks it's {new_hash}.  "
                                   f"Did the file content change?")
                    directory.children[child_name].hash = new_hash
                logger.debug(f"Uploaded {file_path} - {new_hash}")
        except FileNotFoundError:
            logger.error(f"File disappeared before it could be uploaded: {file_path}")
            del directory.children[child_name]
        except OSError as exc:
            logger.error(f"Cannot upload: {file_path} - {str_exception(exc)}", exc_info=True)
            del directory.children[child_name]
