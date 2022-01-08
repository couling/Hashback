import asyncio
import io
import os
from concurrent.futures import ThreadPoolExecutor, Executor
from pathlib import Path
from typing import BinaryIO
from typing import Optional

from . import protocol, misc

# pylint: disable=invalid-name
default_executor: Optional[Executor] = None


def _get_default_executor():
    # pylint: disable=global-statement
    global default_executor
    if default_executor is None:
        default_executor = ThreadPoolExecutor(10)
    return default_executor


class AsyncFile(protocol.FileReader, misc.ContextCloseMixin):

    _file: BinaryIO
    _buffer: bytes = bytes()
    _offset: int = 0
    _size: int

    def __init__(self, file_path: Path, mode: str, executor = None, **kwargs):
        super().__init__()
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


class BytesIOFileReader(protocol.FileReader):

    def __init__(self, content: bytes):
        super().__init__()
        self._reader = io.BytesIO(content)

    async def read(self, num_bytes: int = None) -> bytes:
        return self._reader.read(num_bytes)

    def close(self):
        pass

    @property
    def file_size(self) -> Optional[int]:
        return len(self._reader.getbuffer())


async def async_stat(file_path: Path, executor = None):
    if executor is None:
        executor = _get_default_executor()
    return await asyncio.get_running_loop().run_in_executor(executor, file_path.stat)
