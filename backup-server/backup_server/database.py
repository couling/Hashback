import os
from os import path
from typing import List, Dict, Union, TextIO, BinaryIO, NamedTuple
import logging
from datetime import datetime
import json

from . import datamodel


_log = logging.getLogger(__name__)


class DBConfig(NamedTuple):
    friendly_links: bool
    backup_by: str
    store_split_count: int
    compression_type: datamodel.CompressionType


class Database:
    base_dir: str
    config: DBConfig

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        with open(path.join(self.base_dir, "db.config"), "r") as file:
            self.config = DBConfig(**json.load(file))
        os.makedirs(self.store_path, exist_ok=True)
        os.makedirs(self.groups_path, exist_ok=True)

    @classmethod
    def create(cls, base_dir: str, config: DBConfig):
        os.makedirs(base_dir, exist_ok=False)
        with open(path.join(base_dir, "db.config"), "x") as file:
            json.dump(config._asdict(), file)
        return cls(base_dir)

    def all_groups(self) -> List["BackupGroup"]:
        groups = []
        entry: os.DirEntry
        with os.scandir(self.base_dir) as scan:
            for entry in scan:
                if entry.is_dir():
                    groups.append(BackupGroup(self, entry.name))
        return groups

    def add_group(self, group_name: str) -> "BackupGroup":
        return BackupGroup(self, group_name, create=True)

    def get_group(self, group_name: str) -> "BackupGroup":
        return BackupGroup(self, group_name)

    def store_path_for(self, ref_hash: str):
        split = (ref_hash[x:x+2] for x in range(0, self.config.store_split_count * 2, 2))
        return path.join(self.store_path, *split, ref_hash)

    def open(self, ref_hash: str, mode: str) -> Union[TextIO, BinaryIO]:
        file_path = self.store_path_for(ref_hash)
        if "r" not in mode:
            os.makedirs(path.dirname(file_path), exist_ok=True)
        return open(file_path, mode)

    def create_directory(self, children: Dict[str, datamodel.Inode]) -> "BackupDirectory":
        return BackupDirectory._create(self, children)

    def _make_friendly_link_dir(self, content: Dict[str, datamodel.Inode], target: str):
        try:
            os.mkdir(target)
            for name, inode in content.items():
                source = path.relpath(start=target, path=self.store_path_for(inode.hash))
                if inode.type is datamodel.FileType.DIRECTORY:
                    source += ".d"
                os.symlink(src=source, dst=path.join(target, name))
        except FileExistsError:
            pass

    @property
    def store_path(self) -> str:
        return path.join(self.base_dir, 'store')

    @property
    def groups_path(self) -> str:
        return path.join(self.base_dir, 'groups')


class BackupGroup:
    def __init__(self, db: Database, group_name: str, create: bool = False):
        self._db = db
        self._group_name = group_name
        if create:
            os.mkdir(self.path)

    def add_backup(self, backup_date: datetime, content: Dict[str, datamodel.Inode]) -> "Backup":
        return Backup._create(self, backup_date, content)

    def get_backup(self, backup_date: datetime) -> "Backup":
        return Backup(self, backup_date)

    @property
    def path(self) -> str:
        return path.join(self._db.groups_path, self._group_name)


class Backup:
    _group: BackupGroup
    _db: Database
    _group_name: str
    _backup_date: str
    _content: Dict[str, datamodel.Inode]

    _DATE_FORMATS = {
        'timestamp': "%Y-%m-%d_%H:%M:%S.%f",
        'date': "%Y-%m-%d",
    }

    def __init__(self, group: BackupGroup, backup_date: datetime):
        self._group = group
        self._backup_date = backup_date.strftime(self._DATE_FORMATS[group._db.config.backup_by])
        backup_path = self.path
        with open(backup_path, "r") as file:
            self._content = json.load(file)

    @classmethod
    def _create(cls, group: BackupGroup, backup_date: datetime, content: Dict[str, datamodel.Inode] = None) -> "Backup":
        date_string = backup_date.strftime(cls._DATE_FORMATS[group._db.config.backup_by])
        backup_path = path.join(group.path, date_string)

        db = group._db
        if db.config.friendly_links:
            db._make_friendly_link_dir(content, backup_path + ".d")

        file_content = datamodel.dump_dir(content)
        with open(backup_path, "x") as file:
            file.write(file_content)

        return cls(group, backup_date)

    @property
    def path(self) -> str:
        return path.join(self._group.path, self._backup_date)


class BackupDirectory:
    _db: Database
    children: Dict[str, datamodel.Inode]

    def __init__(self, db: Database, ref_hash: str):
        self._db = db
        with db.open(ref_hash, "r") as file:
            self.children = datamodel.load_dir(file)

    @classmethod
    def _create(cls, db: Database, children: Dict[str, datamodel.Inode]):
        content = datamodel.dump_dir(children)
        digest = datamodel.hash_content(content)
        file: BinaryIO

        # Technically this isn't necessary but it's nice
        if db.config.friendly_links:
            db._make_friendly_link_dir(children, db.store_path_for(digest) + ".d")

        with db.open(digest, "x") as file:
            file.write(content)

        return cls(db, digest)
