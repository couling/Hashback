from pydantic import BaseModel, Field, AnyUrl
import enum
from typing import Optional, List, Dict, Union, BinaryIO, TextIO, NamedTuple
import stat
from datetime import datetime
import hashlib
import gzip
import bz2
import json
import os


class FileType(enum.Enum):

    REGULAR = "f"
    DIRECTORY = "d"
    CHARACTER_DEVICE = "c"
    BLOCK_DEVICE = "b"
    SOCKET = "s"
    PIPE = "p"
    LINK = "l"


class Inode(BaseModel):
    modified_time: datetime
    mode: int
    size: int
    uid: int
    gid: int
    hash: Optional[str] = Field(None, )

    _MODE_CHECKS = [
        (FileType.REGULAR, stat.S_ISREG),
        (FileType.DIRECTORY, stat.S_ISDIR),
        (FileType.CHARACTER_DEVICE, stat.S_ISCHR),
        (FileType.BLOCK_DEVICE, stat.S_ISBLK),
        (FileType.SOCKET, stat.S_ISSOCK),
        (FileType.PIPE, stat.S_ISPORT),
        (FileType.LINK, stat.S_ISLNK),
    ]

    @property
    def type(self) -> FileType:
        for file_type, check in self._MODE_CHECKS:
            if check(self.mode):
                return file_type
        raise ValueError(f"No type found for mode {self.mode}")

    @classmethod
    def from_stat(cls, s: os.stat_result, hash_value: str) -> "Inode":
        return Inode(
            mode=s.st_mode,
            size=s.st_size,
            uid=s.st_uid,
            gid=s.st_gid,
            modified_time=datetime.fromtimestamp(s.st_mtime/1000000000),
            hash=hash_value,
        )


class Directory(BaseModel):
    __root__: Dict[str, Inode]

    @property
    def content(self):
        return self.__root__

    @content.setter
    def content(self, value: Dict[str, Inode]):
        self.__root__ = value


def dump_dir(children: Dict[str, Inode]) -> str:
    to_dump = Directory(__root__=children)
    return to_dump.json(sort_keys=True)


def load_dir(source: Union[str, TextIO]) -> Dict[str, Inode]:
    if isinstance(source, str):
        content = json.loads(source)
    else:
        content = json.load(source)
    return {name: Inode(**value) for name, value in content.items()}


def hash_content(content: Union[bytes, str, BinaryIO]) -> str:
    h = hashlib.sha256()
    if isinstance(content, bytes):
        h.update(content)
    elif isinstance(content, str):
        h.update(content.encode("utf-8"))
    else:
        bytes_read = content.read(409600)
        while bytes_read:
            h.update(bytes_read)
            bytes_read = content.read(409600)
    return h.hexdigest()


class Backup(BaseModel):
    generated: Optional[datetime]
    backups: Dict[str, Inode] = Field(default_factory=dict)


class CompressionType(enum.Enum):
    none = "none"
    gzip = "gzip"
    bzip2 = "bzip2"


_COMPRESSION_METHODS = {
    CompressionType.none: open,
    CompressionType.gzip: gzip.open,
    CompressionType.bzip2: bz2.open,
}


def open_compressed(file_path: str, mode: str, compression_type: CompressionType, **kwargs) -> Union[BinaryIO, TextIO]:
    compresson_method = _COMPRESSION_METHODS[compression_type]
    return compresson_method(file_path, mode, **kwargs)


class ConfigCache(BaseModel):
    path: str = Field(default="backup.cache")
    compression_type: CompressionType


class ConfigBackupPath(BaseModel):
    path: str
    excludes: List[str] = Field(default_factory=list)


class ConfigClient(BaseModel):
    url: AnyUrl
    device_id: str
    secret: str


class ConfigBackup(BaseModel):
    paths: Dict[str, ConfigBackupPath] = Field(default_factory=dict, )
    server: ConfigClient
    cache: ConfigCache = Field(default_factory=ConfigCache)
