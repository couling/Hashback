import hashlib
import os
import shutil
import logging
from datetime import datetime, timezone
from typing import Optional, Union, BinaryIO
from pydantic import BaseModel, Field
from pathlib import Path
from uuid import UUID, uuid4

from . import protocol
from .protocol import Inode, Directory, Backup

_CONFIG_FILE = 'config.json'
_READ_SIZE = 40960


logger = logging.getLogger(__name__)


class Configuration(BaseModel):
    store_split_count = 1


class BackupSessionConfig(BaseModel):
    client_id: UUID
    backup_date: datetime
    started: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    allow_overwrite: bool
    description: Optional[str] = None


class LocalDatabase:
    config: Configuration

    _CLIENT_DIR = 'client'
    _STORE_DIR = 'store'

    def __init__(self, base_path: Path):
        self._base_path = base_path
        self.config = Configuration.parse_file(base_path / _CONFIG_FILE)

    def save_config(self):
        with (self._base_path / _CONFIG_FILE).open('w') as file:
            file.write(self.config.json(indent=True))

    def open_client_session(self, client_id: Optional[UUID] = None, client_name: Optional[str] = None
                            ) -> "LocalDatabaseServerSession":
        if client_name is not None:
            # TODO change this for pathlib readlink once python 3.9 has bedded in.
            client_id = UUID(os.readlink((self._base_path / self._CLIENT_DIR / client_name)))

        client_path = self._base_path / self._CLIENT_DIR / client_id.hex
        return LocalDatabaseServerSession(self, client_path)

    def store_path_for(self, ref_hash: str) -> Path:
        split = (ref_hash[x:x+2] for x in range(0, self.config.store_split_count * 2, 2))
        return self._base_path.joinpath(self._STORE_DIR, *split, ref_hash)

    def create_client(self, client_config: protocol.ClientConfiguration) -> protocol.ServerSession:
        (self._base_path / self._CLIENT_DIR).mkdir(exist_ok=True, parents=True)
        client_name_path = self._base_path / self._CLIENT_DIR / client_config.client_name
        client_name_path.symlink_to(client_config.client_id.hex)
        client_path = self._base_path / self._CLIENT_DIR / client_config.client_id.hex
        client_path.mkdir(exist_ok=False, parents=True)
        with (client_path / _CONFIG_FILE).open('w') as file:
            file.write(client_config.json(indent=True))
        return LocalDatabaseServerSession(self, client_path)

    @classmethod
    def create_database(cls, base_path: Path, configuration: Configuration) -> "LocalDatabase":
        base_path.mkdir(exist_ok=False, parents=True)
        with (base_path / _CONFIG_FILE).open('w') as file:
            file.write(configuration.json(indent=True))
        (base_path / cls._STORE_DIR).mkdir(exist_ok=False, parents=True)
        (base_path / cls._CLIENT_DIR).mkdir(exist_ok=False, parents=True)
        return cls(base_path)



class LocalDatabaseServerSession(protocol.ServerSession):

    _BACKUPS = 'backup'
    _SESSIONS = 'sessions'
    _TIMESTMAP_FORMAT = "%Y-%m-%d_%H:%M:%S.%f"

    def __init__(self, database: LocalDatabase, client_path: Path):
        self._database = database
        self._client_path = client_path
        with (client_path / _CONFIG_FILE).open('r') as file:
            self.client_config = protocol.ClientConfiguration.parse_raw(file.read())

    def save_config(self):
        with (self._client_path / _CONFIG_FILE).open('w') as file:
            file.write(self.client_config.json(indent=True))

    async def start_backup(self, backup_date: datetime, allow_overwrite: bool = False, description: Optional[str] = None,
                           ) -> protocol.BackupSession:
        backup_date = protocol.normalize_backup_date(backup_date, self.client_config.backup_granularity)

        if not allow_overwrite:
            if self._path_for_backup_date(backup_date).exists():
                raise FileExistsError(f"Backup exists {backup_date.isoformat()}")

        backup_session_id = uuid4()
        backup_session_path = self._path_for_session_id(backup_session_id)
        backup_session_path.mkdir(exist_ok=False, parents=True)
        session_config = BackupSessionConfig(
            client_id=self.client_config.client_id,
            allow_overwrite=allow_overwrite,
            backup_date=backup_date,
            description=description,
        )
        with (backup_session_path / _CONFIG_FILE).open('w') as file:
            file.write(session_config.json(indent=True))

        return LocalDatabaseBackupSession(self, backup_session_path)

    async def resume_backup(self, *, session_id: Optional[UUID] = None,
                            backup_date: Optional[datetime] = None) -> protocol.BackupSession:
        if session_id is not None:
            backup_path = self._path_for_session_id(session_id)
            return LocalDatabaseBackupSession(self, backup_path)

        elif backup_date is not None:
            # TODO  This has not yet been implemented
            raise NotImplementedError("Unable to find backup by date")

        raise ValueError("Either session_id or backup_date must be specified but neither were")

    async def get_backup(self, backup_date: Optional[datetime] = None) -> Optional[Backup]:
        if backup_date is None:
            try:
                backup_path = next(iter(sorted((self._client_path / self._BACKUPS).iterdir(), reverse=True)))
            except (FileNotFoundError, StopIteration):
                logger.warning(f"No backup found for {self.client_config.client_name} ({self.client_config.client_id})")
                return None
        else:
            backup_date = protocol.normalize_backup_date(backup_date, self.client_config.backup_granularity)
            backup_path = self._path_for_backup_date(backup_date)
        with backup_path.open('r') as file:
            return protocol.Backup.parse_raw(file.read())

    async def read_directory(self, inode: Inode) -> Directory:
        if inode.type != protocol.FileType.DIRECTORY:
            raise ValueError(f"Cannot open file type {inode.type} as a directory")
        with self._database.store_path_for(inode.hash).open('r') as file:
            return Directory.parse_raw(file.read())

    async def read_file(self, inode: Inode, target_path: Optional[Path] = None,
                        restore_permissions: bool = False, restore_owner: bool = False) -> Optional[BinaryIO]:
        if inode.type not in (protocol.FileType.REGULAR, protocol.FileType.LINK, protocol.FileType.PIPE):
            raise ValueError(f"Cannot read a file type {inode.type}")
        if target_path is not None:
            if inode.type == protocol.FileType.REGULAR:
                with self._database.store_path_for(inode.hash).open('rb') as source, target_path.open('xb') as target:
                    content = source.read(_READ_SIZE)
                    while content:
                        target.write(content)
                        content = source.read(_READ_SIZE)
            elif inode.type == protocol.FileType.LINK:
                with self._database.store_path_for(inode.hash).open('r') as source:
                    link_target = source.read()
                target_path.symlink_to(link_target)
            else:  # inode.type == protocol.FileType.PIPE:
                if inode.hash != protocol.EMPTY_FILE:
                    raise ValueError(f"File of type {inode.type} must be empty.  But this one is not: {inode.hash}")
                os.mkfifo(target_path)
            if restore_permissions:
                target_path.chmod(inode.permissions)
            if restore_owner:
                os.chown(target_path, uid=inode.uid, gid=inode.gid)
        else:
            return self._database.store_path_for(inode.hash).open('rb')

    def complete_backup(self, meta: protocol.Backup, overwrite: bool):
        backup_path =  self._path_for_backup_date(meta.backup_date)
        backup_path.parent.mkdir(exist_ok=True, parents=True)
        with backup_path.open('w' if overwrite else 'x') as file:
            file.write(meta.json(indent=True))

    def _path_for_backup_date(self, backup_date: datetime) -> Path:
        return self._client_path / self._BACKUPS / (backup_date.strftime(self._TIMESTMAP_FORMAT) + '.json')

    def _path_for_session_id(self, session_id: UUID) -> Path:
        return self._client_path / self._SESSIONS / session_id.hex


class LocalDatabaseBackupSession(protocol.BackupSession):

    _PARTIAL = 'partial'
    _NEW_OBJECTS = 'new_objects'
    _ROOTS = 'roots'

    def __init__(self, client_session: LocalDatabaseServerSession, session_path: Path):
        self._client_session = client_session
        self._session_path = session_path
        with (session_path / _CONFIG_FILE).open('r') as file:
            self._config = BackupSessionConfig.parse_raw(file.read())
        self.is_open = True
        (session_path / self._NEW_OBJECTS).mkdir(exist_ok=True, parents=True)
        (session_path / self._ROOTS).mkdir(exist_ok=True, parents=True)
        (session_path / self._PARTIAL).mkdir(exist_ok=True, parents=True)
        self.backup_date = self._config.backup_date
        self.session_id = UUID(session_path.name)

    async def directory_def(self, definition: protocol.Directory, replaces: Optional[str] = None
                            ) -> protocol.DirectoryDefResponse:

        for name, child in definition.children.items():
            if child.hash is None:
                raise ValueError(f"Child {name} has no hash value")

        content = definition.dump()
        directory_hash = protocol.hash_content(content)
        if self._object_exists(directory_hash):
            return protocol.DirectoryDefResponse(ref_hash=directory_hash)

        missing = []
        for name, inode in definition.children.items():
            if not self._object_exists(inode.hash):
                missing.append(name)

        if missing:
            return protocol.DirectoryDefResponse(ref_hash=None, missing_files=missing)

        tmp_path = self._temp_path()
        with tmp_path.open('xb') as file:
            try:
                file.write(content)
                tmp_path.rename(self._store_path_for(directory_hash))
            except Exception:
                tmp_path.unlink()
                raise
        return protocol.DirectoryDefResponse(ref_hash=directory_hash)

    async def upload_file_content(self, file_content: Union[Path, BinaryIO], resume_id: UUID,
                                  resume_from: int = 0) -> str:
        h = hashlib.sha256()
        temp_file = self._temp_path(resume_id)
        if isinstance(file_content, Path):
            file_content = file_content.open('rb')
        with file_content, temp_file.open('wb') as target:

            # If we are resuming the file
            if resume_from:
                # Seek the source file to the resume position
                file_content.seek(resume_from, os.SEEK_SET)

                # Read partial target file to update the hash
                if file_content.tell() < resume_from:
                    raise ValueError("resume_from is larger than source file")
                while target.tell() < resume_from:
                    bytes_read = target.read(max(_READ_SIZE, resume_from - target.tell()))
                    if not bytes_read:
                        raise ValueError("resume_from is larger tan existing target file")
                    h.update(bytes_read)

            # Write the file content
            bytes_read = file_content.read(_READ_SIZE)
            while bytes_read:
                h.update(bytes_read)
                target.write(bytes_read)
                bytes_read = file_content.read(_READ_SIZE)

        # Move the temporary file to new_objects named as it's hash
        ref_hash = h.hexdigest()
        temp_file.rename(self._new_object_path_for(ref_hash))
        return ref_hash

    async def add_root_dir(self, root_dir_name: str, inode: protocol.Inode) -> None:
        if not self._object_exists(inode.hash):
            raise ValueError(f"Cannot create {root_dir_name} - does not exist: {inode.hash}")
        file_path = self._session_path / self._ROOTS / root_dir_name
        with file_path.open('x') as file:
            file.write(inode.json())

    async def check_file_upload_size(self, resume_id: UUID) -> int:
        return self._temp_path(resume_id).stat().st_size

    async def complete(self) -> None:
        logger.info(f"Committing {self._session_path.name} for {self._client_session.client_config.client_name} "
                    f"({self._client_session.client_config.client_id}) - {self._config.backup_date}")
        for file_path in (self._session_path / self._NEW_OBJECTS).iterdir():
            try:
                target_path = self._client_session._database.store_path_for(file_path.name)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Moving {file_path.name} to store")
                file_path.rename(target_path)
            except FileExistsError:
                pass
        roots = {}
        for file_path in (self._session_path / self._ROOTS).iterdir():
            with file_path.open('r') as file:
                roots[file_path.name] = protocol.Inode.parse_raw(file.read())
        backup_meta = protocol.Backup(
            client_id=self._client_session.client_config.client_id,
            client_name=self._client_session.client_config.client_name,
            backup_date=self._config.backup_date,
            started=self._config.started,
            completed=datetime.now(timezone.utc),
            description=self._config.description,
            roots=roots,
        )
        self._client_session.complete_backup(backup_meta, self._config.allow_overwrite)
        await self.discard()

    async def discard(self) -> None:
        self.is_open = False
        shutil.rmtree(self._session_path)

    def _object_exists(self, ref_hash: str) -> bool:
        return (self._client_session._database.store_path_for(ref_hash).exists()
                or self._store_path_for(ref_hash).exists())

    def _store_path_for(self, ref_hash: str) -> Path:
        return self._session_path / self._NEW_OBJECTS / ref_hash

    def _temp_path(self, resume_id: Optional[UUID] = None) -> Path:
        if resume_id is None:
            resume_id = uuid4()
        return self._session_path / self._PARTIAL / resume_id.hex

    def _new_object_path_for(self, ref_hash: str) -> Path:
        return self._session_path / self._NEW_OBJECTS / ref_hash
