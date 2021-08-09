import os
import logging
from typing import Optional
from pathlib import Path
from uuid import uuid4
from asyncio import gather
from typing import Dict, List
from dataclasses import dataclass, field

from . import protocol
from .misc import str_exception


logger = logging.getLogger(__name__)


__all__ = ['Scanner']


class SkipThis(Exception):
    pass


@dataclass
class _NormalizedFilter:
    filter_type: Optional[protocol.FilterType] = field(default=None)
    exceptions: Dict[str, "_NormalizedFilter"] = field(default_factory=dict)


class Scanner:

    def __init__(self, backup_session: protocol.BackupSession):
        self.all_files = {}
        self.backup_session = backup_session

    async def scan_all(self, backup_directories: Optional[Dict[str, protocol.ClientConfiguredBackupDirectory]] = None,
                       fast_unsafe: bool = False):
        """
        Scan all directories.
        :param backup_directories: The directories to backup.  If not, this will be pulled from the client's server.
        :param fast_unsafe: Compare meta-data to the previous backup stored on the same server.  This can be much faster
            But it is "unsafe" because it does not check the file content.  Theoretically content can change without
            changing the timestamp or size.  It's rare, but it could theoretically happen.
        """
        if backup_directories is None:
            backup_directories = self.backup_session.server_session.client_config.backup_directories
        if fast_unsafe:
            last_backup_roots = (await self.backup_session.server_session.get_backup()).roots
        else:
            last_backup_roots = {}

        # Scans are internally parallelized.  Let's not gather() this one so we have some opportunity to understand
        # what it was doing if it failed.
        if fast_unsafe:
            logger.warning("Comparing meta data to last backup, will not check content for existing files.")
            for name, scan_spec in backup_directories.items():
                last_backup = last_backup_roots.get(name)
                if last_backup is None:
                    logger.warning(f"Directory '{name}' not in last backup")
                await self.scan_root(root_name=name, scan_spec=scan_spec, last_backup=last_backup)
        else:
            logger.info("Ignoring last backup, will hash every file")
            for name, scan_spec in backup_directories.items():
                await self.scan_root(root_name=name, scan_spec=scan_spec)

    async def scan_root(self, root_name: str, scan_spec: protocol.ClientConfiguredBackupDirectory,
                        last_backup: Optional[protocol.Directory] = None):
        path = Path(scan_spec.base_path)
        if not path.is_absolute():
            raise ValueError(f"{root_name} path is not absolute: {path}")
        filters = _normalize_filters(scan_spec.filters)
        logger.info(f"Backing up '{root_name}' ({path})")
        if not path.is_dir():
            logger.error(f"Not a valid directory {root_name}: {str(path)}")
        inode = await self._scan_inode(path, last_backup, filters)
        await self.backup_session.add_root_dir(root_name, inode)

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

        file_stat = path.stat()
        inode = protocol.Inode.from_stat(file_stat, last_scan_hash)

        if inode.type == protocol.FileType.DIRECTORY:
            directory = await self._scan_directory(path, last_scan, filters)
            inode.hash = await self._upload_directory(path, last_scan_hash, directory)

        elif inode.type == protocol.FileType.REGULAR:
            if inode != last_scan:
                logger.debug(f"Hashing {path}")
                inode.hash = protocol.hash_content(path)

        elif inode.type == protocol.FileType.LINK:
            inode.hash = protocol.hash_content(os.readlink(path))

        elif inode.type == protocol.FileType.PIPE:
            inode.hash = protocol.EMPTY_FILE

        else:
            raise SkipThis(f"Skipping type type is {inode.type} {path}")

        # If this is not a directory then store it in our database of all inodes.
        # We don't waste memory on storing directories, they cannot be hard linked.
        if inode.type is not protocol.FileType.DIRECTORY:
            self.all_files[(file_stat.st_dev, file_stat.st_ino)] = inode

        return inode

    async def _scan_directory(self, dir_path: Path, last_scan: Optional[protocol.Inode],
                              filters: Optional[_NormalizedFilter]) -> protocol.Directory:
        """
        Scan a directory, returning the children inodes
        """
        logger.debug(f"Directory {dir_path}")

        # Fetch the last scan
        if last_scan is None:
            last_scan_children = {}
        else:
            logger.debug(f"Fetching last_scan {last_scan.hash} for {dir_path}")
            last_scan_directory = await self.backup_session.server_session.get_directory(last_scan)
            last_scan_children = last_scan_directory.children

        filter_tree = filters.exceptions if filters is not None else {}

        # Check every file to see if we've seen it before.  If not create a task to scan it fully.
        child_inodes = {}
        scan_paths = []
        scan_tasks = []
        for child in dir_path.iterdir():
            child_stat = child.stat()
            try:
                child_inodes[child.name] = self.all_files[(child_stat.st_dev, child_stat.st_ino)]
                logger.debug(f"Using previous hardlink for {child}")
            except KeyError:
                # Create a task to scan every child, make a note of it's name as this will not be in the result.
                scan_paths.append(child)
                scan_tasks.append(self._scan_inode(
                    path=child,
                    last_scan=last_scan_children.get(child.name),
                    filters=filter_tree.get(child.name),
                ))

        # Run the scan tasks
        scan_results = await gather(*scan_tasks, return_exceptions=True)

        # Process the results.
        for child_path, result in zip(scan_paths, scan_results):
            # If scanning the child raised an exception then just log it and carry on.
            # The child will just be excluded from the backup
            if isinstance(result, Exception):
                if isinstance(result, SkipThis):
                    logger.debug(str(result))
                else:
                    logger.error(f"Could not scan {child_path}", exc_info=result)
            else:
                # Add the result of the scan to the dictionary of inodes
                child_inodes[child_path.name] = result

        return protocol.Directory(__root__=child_inodes)

    async def _upload_directory(self, path: Path, last_scan_hash: str, directory: protocol.Directory) -> str:
        """
        Uploads a directory to the server.

        First it uploads the filenames and inode information including hashes for all children.  The server can then
        reject this if any or all children are missing from the server.  If that happens the server will respond
        with a list of missing children...  we then upload all missing children and try again.
        """
        ref_hash = directory.hash().ref_hash
        # If the hash matches the last scan then there's no need to go any further.  This is a shortcut, it tells us
        # the server already has an exact copy of the directory.  This will not catch the case where an entire directory
        # moved, only where it was completely unchanged.
        if ref_hash == last_scan_hash:
            logger.debug(f"Matches last backup: {path} as {ref_hash}")
            return ref_hash

        logger.debug(f"Uploading directory {path}")

        # The directory has changed.  We send the contents over to the server. It will tell us what else it needs.
        server_response = await self.backup_session.directory_def(directory)
        if not server_response.success:
            logger.debug(f"{len(server_response.missing_files)} missing files in {path}")
            await gather(*(self._upload_missing_file(path, directory, missing_file)
                           for missing_file in server_response.missing_files))
            # Retry the directory now that all files have been uploaded.
            # We let the server know this replaces the previous request.  Some servers may place a marker on the session
            # preventing us from completing until unsuccessful requests have been replaced.
            server_response = await self.backup_session.directory_def(directory, replaces=server_response.missing_ref)
            if not server_response.success:
                raise protocol.ProtocolError(
                    "Files disappeared server-side while backup is in progress.  "
                    "This must not happen or the backup will be corrupted. %s",
                    {name: directory.children.get(name) for name in server_response.missing_files}
                )
            ref_hash = directory.hash().ref_hash

        logger.debug(f"Server accepted directory {path} as {server_response.ref_hash}")
        return ref_hash

    async def _upload_missing_file(self, path: Path, directory: protocol.Directory, missing_file: str):
        """
        Upload a file after the server has stated it does not already have a copy.
        """
        missing_file_path = path / missing_file
        logger.info(f"Uploading {missing_file_path}")
        try:
            directory.children[missing_file].hash = await self.backup_session.upload_file_content(
                file_content=missing_file_path,
                resume_id=uuid4(),
            )
            logger.debug(f"Uploaded {missing_file} - {directory.children[missing_file].hash}")
        except FileNotFoundError:
            logger.error(f"File disappeared before it could be uploaded: {path / missing_file}")
            del directory.children[missing_file]
        except OSError as ex:
            logger.error(f"Cannot upload: {path / missing_file} - {str_exception(ex)}")


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
