from typing import Optional
import os
import logging
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from asyncio import gather
from typing import NamedTuple, Dict, List, Set
from dataclasses import dataclass, field

from . import protocol


logger = logging.getLogger(__name__)


__all__ = ['Scanner']


class SkipThis(Exception):
    pass


@dataclass
class _NormalizedFilter:
    filter_type: Optional[protocol.FilterType] = field(default=None)
    exceptions: Dict[str, "_NormalizedFilter"] = field(default_factory=dict)


class Scanner:

    def __init__(self, backup_session: protocol.BackupSession, database: Optional[protocol.ServerSession]):
        self.all_files = {}
        self._session = backup_session
        self._database = database

    async def scan_all(self, client_config: protocol.ClientConfiguration = None,
                       ref_last_scan: Optional[datetime] = None):
        """
        Scan all directories.
        :param client_config: Override the client config.  If None use the one stored in the database.
        :param ref_last_scan: Override the last scan date.  If None the database will decide which to use
            (likely the most recent).
        """
        if client_config is None:
            client_config = self._database.client_config
        for name, scan_spec in client_config.backup_directories.items():
            await self.scan_root(name, scan_spec, ref_last_scan)

    async def scan_root(self, root_name: str, scan_spec: protocol.ClientConfiguredBackupDirectory,
                        ref_last_scan: Optional[datetime] = None):
        path = Path(scan_spec.base_path)
        if not path.is_absolute():
            raise ValueError(f"{root_name} path is not absolute: {path}")
        filters = _normalize_filters(scan_spec.filters)
        logger.info(f"Backing up '{root_name}' ({path})")
        if not path.is_dir():
            logger.error(f"Not a valid directory {root_name}: {str(path)}")
        if self._database is not None:
            last_backup = await self._database.get_backup(backup_date=ref_last_scan)
            if last_backup is None or root_name not in last_backup.roots:
                logger.warning(f"No backup found for '{root_name}' executing full scan")
                last_backup = None
            else:
                logger.debug(f"Using backup dated {last_backup.backup_date} completed at {last_backup.completed}")
                last_backup = last_backup.roots[root_name]
        else:
            logger.info("Executing full scan with no attempt to find a last backup")
            last_backup = None
        inode = await self._scan_inode(path, last_backup, filters)
        await self._session.add_root_dir(root_name, inode)

    async def _scan_inode(self, path: Path, last_scan: Optional[protocol.Inode], filters: Optional[_NormalizedFilter]
                          ) -> protocol.Inode:
        """
        Scan an inode:
            - If the inode is a directory all children will be scanned, and the result uploaded to the server if it
                does not match last_scan.  This will also upload any files which do not have matching versions on the
                server.
            - If the inode is not a directory, then it's meta data will be compared with the last_scan version.
                - If there is no last_scan version or the meta data does not match the file will be read and hashed
                - If the meta data does match last_scan then the hash from the previous scan will be used.
        :param path: The file path of the file to scan.
        :param last_scan: The inode object generated the last time this path was scanned.
        :filters: A normalized tree of filters to exclude children from scan.
        """
        if filters is not None and filters.filter_type == protocol.FilterType.EXCLUDE:
            if filters.exceptions:
                # TODO implement inclusions
                raise NotImplementedError(f"unable to process exceptions to exclusions (IE inclusions)")
            raise SkipThis(f"Skipping excluded path {path}")

        last_scan_hash = last_scan.hash if last_scan is not None else None
        inode = protocol.Inode.from_file_path(path, last_scan_hash)

        if inode.type == protocol.FileType.DIRECTORY:
            directory = await self._scan_directory(path, last_scan, filters)
            inode.hash = await self._upload_directory(path, last_scan_hash, directory)

        elif inode.type == protocol.FileType.REGULAR:
            if inode != last_scan:
                logger.debug(f"Hashing {path}")
                with path.open('rb') as file:
                    inode.hash = protocol.hash_content(file)

        elif inode.type == protocol.FileType.LINK:
            inode.hash = protocol.hash_content(os.readlink(path))

        elif inode.type == protocol.FileType.PIPE:
            inode.hash = protocol.EMPTY_FILE

        else:
            raise SkipThis(f"Skipping type type is {inode.type} {path}")

        return inode

    async def _scan_directory(self, dir_path: Path, last_scan: Optional[protocol.Inode],
                              filters: Optional[_NormalizedFilter]) -> protocol.Directory:
        """
        Scan a directory, returning the children inodes
        """
        logger.debug(f"Directory {dir_path}")
        assert dir_path.is_dir()

        # Fetch the last scan
        if last_scan is None:
            last_scan_children = {}
        else:
            logger.debug(f"Fetching last_scan {last_scan.hash} for {dir_path}")
            last_scan_directory = await self._database.read_directory(last_scan)
            last_scan_children = last_scan_directory.children

        children = list(dir_path.iterdir())
        filter_tree = filters.exceptions if filters is not None else {}

        # Create scan task for every child which is not excluded by filter
        scan_tasks = [
            self._scan_inode(child_path, last_scan_children.get(child_path.name), filter_tree.get(child_path.name))
            for child_path in children
        ]

        # Run the tasks
        results = await gather(*scan_tasks, return_exceptions=True)

        # Log any failures
        for child, result in zip(children, results):
            if isinstance(result, Exception):
                if isinstance(result, SkipThis):
                    logger.debug(str(result))
                else:
                    logger.error(f"Could not scan {child}", exc_info=result)

        # Assemble successful scans into a child dictionary
        child_inodes = {child_path.name: child_inode
                        for child_path, child_inode in zip(children, results)
                        if not isinstance(child_inode, Exception)}

        return protocol.Directory(__root__=child_inodes)

    async def _upload_directory(self, path: Path, last_scan_hash: str, directory: protocol.Directory) -> str:
        ref_hash = protocol.hash_content(directory.dump())
        # If the hash matches the last scan then there's no need to hash
        if ref_hash == last_scan_hash:
            logger.debug(f"Matches last backup: {path} as {ref_hash}")
            return ref_hash

        logger.debug(f"Uploading directory {path}")

        # The directory has changed.  We send the contents over to the server. It will tell us what else it needs.
        server_response = await self._session.directory_def(directory)
        if server_response.missing_files:
            for missing_file in server_response.missing_files:
                missing_file_path = path / missing_file
                logger.info(f"Uploading {missing_file_path}")
                try:
                    directory.children[missing_file].hash = await self._session.upload_file_content(
                        file_content=missing_file_path,
                        resume_id=uuid4(),
                    )
                    logger.debug(f"Uploaded {missing_file} - {directory.children[missing_file].hash}")
                except FileNotFoundError:
                    logger.error(f"File disappeared before it could be uploaded: {path / missing_file}")
                    del directory.children[missing_file]
                except OSError as ex:
                    logger.error(f"Cannot upload: {path / missing_file} - {str(ex)}")
            # Retry the directory now that all files have been uploaded.  This should never fail.
            # File hashes could have changed and files could have disappeared, so re-hash the directory
            server_response = await self._session.directory_def(directory, replaces=ref_hash)
            if server_response.missing_files:
                raise RuntimeError("Second attempt to store a directory on the server failed")
            ref_hash = server_response.ref_hash
            logger.debug(f"Complete {path} - {ref_hash}")
        else:
            logger.debug(f"Server already has identical copy of {path} as {server_response.ref_hash}")
        return ref_hash


def _normalize_filters(filters: List[protocol.Filter]) -> _NormalizedFilter:
    """
    Take a list of filters and build them into a tree of filters.
    :param filters: A list of filters
    :return: A _NormalizedFilter tree structure
    """
    result = _NormalizedFilter()
    for filter_item in filters:
        if filter_item.path == '.':
            result.filter_type = filter_item.filter
        else:
            filter_path = Path(filter_item.path)
            position = result
            for directory in filter_path.parts[:-1]:
                if directory not in position.exceptions:
                    position.exceptions[directory] = _NormalizedFilter()
                position = position.exceptions[directory]
            directory = filter_path.name
            if directory in position.exceptions:
                position.exceptions[directory].filter_type = filter_item.filter
            else:
                position.exceptions[directory] = _NormalizedFilter(filter_type=filter_item.filter)
    _prune_redundant_filters(result)
    return result


def _prune_redundant_filters(filters: _NormalizedFilter, parent_type: protocol.FileType = protocol.FilterType.INCLUDE):
    """
    It's perfectly legitimate for a user to have redundant filters such as excluding a directory inside another that
    is already excluded.  It's more performant to remove redundant filters before scanning
    :param filters:  The filters to prune
    :param parent_type: The effective filter type of the parent.  At the root this will be INCLUDE (default)
    """
    to_prune = []
    if filters.filter_type == parent_type:
        # If this filter is just doing the same thing as it's parent then it has no effect.  Change it's filter type
        # to propagate the parent (None).
        filters.filter_type = None
    for name, child in filters.exceptions.items():
        _prune_redundant_filters(child, filters.filter_type if filters.filter_type is not None else parent_type)
        if child.filter_type is None and not child.exceptions:
            # Here the child is propagating the parent and it has no exceptions so it has no effect... it's meaningless
            to_prune.append(name)
    for name in to_prune:
        del filters.exceptions[name]
