import asyncio
import io
import logging
import os
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import MINYEAR, datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import BinaryIO, Dict, AsyncIterable, List, Optional, Tuple

from . import file_filter, protocol


logger = logging.getLogger(__name__)

# pylint: disable=invalid-name
default_executor: Optional[Executor] = None


def _get_default_executor():
    # pylint: disable=global-statement
    global default_executor
    if default_executor is None:
        default_executor = ThreadPoolExecutor(10)
    return default_executor


class AsyncFile(protocol.FileReader):

    _file: BinaryIO
    _buffer: bytes = bytes()
    _offset: int = 0
    _size: int
    file_path: Path

    def __init__(self, file_path: Path, mode: str, executor = None, **kwargs):
        self.file_path = file_path
        self._executor = executor
        self._file = file_path.open(mode + "b", buffering=False, **kwargs)
        try:
            self._size = os.fstat(self._file.fileno()).st_size
        except:
            self._file.close()
            raise

    @classmethod
    async def open(cls, file_path: Path, mode: str, executor = None, **kwargs):
        if executor is None:
            executor = _get_default_executor()
        return await asyncio.get_running_loop().run_in_executor(
            executor, lambda: AsyncFile(file_path, mode, executor, **kwargs))

    async def read(self, num_bytes: int = -1) -> bytes:
        if num_bytes >= 0:
            if self._buffer:
                next_offset = self._offset + min(num_bytes, len(self._buffer) - self._offset)
                result = self._buffer[self._offset: next_offset]
                if len(self._buffer) == next_offset:
                    del self._buffer
                    self._offset = 0
                else:
                    self._offset = next_offset
                return result

            buffer = await asyncio.get_running_loop().run_in_executor(
                self._executor, self._file.read, protocol.READ_SIZE)
            if len(buffer) > num_bytes:
                self._buffer = buffer
                self._offset = num_bytes
                return self._buffer[:self._offset]
            return buffer

        result = await asyncio.get_running_loop().run_in_executor(self._executor, self._file.read, -1)
        if self._buffer:
            result = self._buffer[self._offset:] + result
            del self._buffer
            self._offset = 0
        return result

    def seek(self, offset: int, whence: int):
        if self._buffer:
            del self._buffer
            self._offset = 0
        self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell() - self._offset

    async def write(self, buffer: bytes):
        await asyncio.get_running_loop().run_in_executor(self._executor, self._file.write, buffer)

    def close(self):
        self._file.close()

    @property
    def file_size(self) -> Optional[int]:
        return self._size


class BytesReader(protocol.FileReader):

    def __init__(self, content: bytes):
        self._reader = io.BytesIO(content)

    async def read(self, num_bytes: int = None) -> bytes:
        return self._reader.read(num_bytes)

    def close(self):
        pass

    @property
    def file_size(self) -> Optional[int]:
        return self._reader.getbuffer().nbytes


async def async_stat(file_path: Path, executor = None):
    if executor is None:
        executor = _get_default_executor()
    return await asyncio.get_running_loop().run_in_executor(executor, file_path.stat)


class LocalDirectoryExplorer(protocol.DirectoryExplorer):

    _EXCLUDED_DIR_INODE = protocol.Inode(
        type=protocol.FileType.DIRECTORY, mode=0, modified_time=datetime(year=MINYEAR, month=1, day=1),
        size=None, uid=0, gid=0, hash=None)

    _INCLUDED_FILE_TYPES = {
        # Here we implicitly ignore device files and sockets as they are not properly supported
        protocol.FileType.DIRECTORY,
        protocol.FileType.REGULAR,
        protocol.FileType.LINK,
        protocol.FileType.PIPE,
    }

    def __init__(self, base_path: Path,
                 filter_node: Optional[file_filter.FilterPathNode],
                 ignore_patterns: List[str],
                 all_files: Dict[Tuple[int, int], protocol.Inode]):

        self._base_path = base_path
        self._all_files = all_files
        self._ignore_patterns = ignore_patterns
        self._filter_node = filter_node
        self._children = {}

    async def iter_children(self) -> AsyncIterable[Tuple[str, protocol.Inode]]:
        if self._filter_node is not None and self._filter_node.filter_type is protocol.FilterType.EXCLUDE:
            # This LocalDirectoryExplorer has been created for an excluded directory, but there may be exceptions.
            logger.debug("Listing %s exceptions for %s ", len(self._filter_node.exceptions), self._base_path)
            for child_name, exception in self._filter_node.exceptions.items():
                child = self._base_path / child_name
                if exception.filter_type is protocol.FilterType.INCLUDE:
                    # Exception to include this child.
                    if not self._should_pattern_ignore(child) and child.exists():
                        yield child_name, self._stat_child(child_name)
                elif child.is_dir():
                    # Looks like there is a child of the child that's the real exception.
                    yield child_name, self._EXCLUDED_DIR_INODE.copy()
                else:
                    logger.warning("Something was included inside %s has been included, but it could not be backed up",
                                    child)
                    yield child_name, self._EXCLUDED_DIR_INODE.copy()

        elif self._filter_node is None or self._filter_node.filter_type is protocol.FilterType.INCLUDE:
            for child in self._base_path.iterdir():
                child_name = child.name


                if self._filter_node is not None and child_name in self._filter_node.exceptions and \
                        self._filter_node.exceptions[child_name].filter_type is protocol.FilterType.EXCLUDE:
                    # If this child is explicitly excluded ...
                    exception_count = len(self._filter_node.exceptions[child_name].exceptions)
                    if exception_count:
                        logger.debug("Skipping %s on filter with %s exceptions", child, exception_count)
                        yield child_name, self._EXCLUDED_DIR_INODE.copy()
                    else:
                        logger.debug("Skipping %s on filter", child)
                    continue

                if self._should_pattern_ignore(child):
                    continue

                inode = self._stat_child(child_name)
                if inode.type not in self._INCLUDED_FILE_TYPES:
                    continue

                yield child.name, inode

        else:
            raise ValueError(f"Normalized filter node had type {self._filter_node.filter_type}.  "
                             f"This should have been one either {protocol.FilterType.INCLUDE} or"
                             f"{protocol.FilterType.EXCLUDE}")

    def _should_pattern_ignore(self, child: Path) -> bool:
        child_name = child.name
        for pattern in self._ignore_patterns:
            if fnmatch(child_name, pattern):
                logger.debug("Skipping %s on pattern %s", child, pattern)
                return True
        return False

    def _stat_child(self, child: str) -> protocol.Inode:
        file_path = self._base_path / child
        inode = self._children.get(child)
        if inode is not None:
            return inode
        file_stat = file_path.lstat()
        inode = self._all_files.get((file_stat.st_dev, file_stat.st_ino))
        if inode is None:
            inode = protocol.Inode.from_stat(file_stat, None)
        self._children[child] = inode
        if inode.type is not protocol.FileType.DIRECTORY:
            self._all_files[(file_stat.st_dev, file_stat.st_ino)] = inode
        return inode

    def __str__(self) -> str:
        return str(self._base_path)

    async def inode(self) -> protocol.Inode:
        if self._filter_node is not None and self._filter_node.filter_type is protocol.FilterType.EXCLUDE:
            return self._EXCLUDED_DIR_INODE.copy()

        stat = self._base_path.lstat()
        inode = protocol.Inode.from_stat(stat, hash_value=None)
        self._all_files[(stat.st_dev, stat.st_ino)] = inode
        return inode

    async def open_child(self, name: str, mode: str) -> protocol.FileReader:
        child_type = self._stat_child(name).type
        child_path = self._base_path / name

        if child_type == protocol.FileType.REGULAR:
            return AsyncFile(child_path, mode)

        if child_type == protocol.FileType.LINK:
            return BytesReader(os.readlink(child_path).encode())

        elif child_type == protocol.FileType.PIPE:
            return BytesReader(bytes(0))

        raise ValueError(f"Cannot open child of type {child_type}")

    def get_child(self, name: str) -> protocol.DirectoryExplorer:
        return type(self)(
            base_path=self._base_path / name,
            filter_node=self._filter_node.exceptions.get(name) if self._filter_node is not None else None,
            ignore_patterns=self._ignore_patterns,
            all_files=self._all_files,
        )

    def get_path(self, name: Optional[str]) -> str:
        if name is None:
            return str(self._base_path)
        return str(self._base_path / name)


class LocalFileSystemExplorer:
    _all_files: Dict[Tuple[int, int], protocol.Inode]

    def __init__(self):
        self._all_files = {}

    def __call__(self, directory_root: protocol.ClientConfiguredBackupDirectory) -> LocalDirectoryExplorer:
        base_path = Path(directory_root.base_path)
        if not base_path.is_absolute():
            raise ValueError(f"Backup path is not absolute: {base_path}")
        if not base_path.is_dir():
            raise ValueError(f"Backup path is not a directory: {base_path}")
        ignore_patterns, root_filter_node = file_filter.normalize_filters(directory_root.filters)
        return LocalDirectoryExplorer(
            base_path=Path(directory_root.base_path),
            ignore_patterns=ignore_patterns,
            filter_node=root_filter_node,
            all_files=self._all_files,
        )
