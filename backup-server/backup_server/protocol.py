import enum
import stat
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Protocol, Dict, Optional, Union, BinaryIO, List, NamedTuple
from uuid import UUID, uuid4
from pathlib import Path
from abc import abstractmethod
from pydantic import BaseModel, Field


EMPTY_FILE = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


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
    hash: Optional[str] = None

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

    @property
    def permissions(self) -> int:
        return stat.S_IMODE(self.mode)

    @classmethod
    def from_stat(cls, s, hash_value: Optional[str]) -> "Inode":
        return Inode(
            mode=s.st_mode,
            size=s.st_size,
            uid=s.st_uid,
            gid=s.st_gid,
            modified_time=datetime.fromtimestamp(s.st_mtime),
            hash=hash_value,
        )

    @classmethod
    def from_file_path(cls, path: Path, hash_value: Optional[str] = None) -> "Inode":
        s = path.stat()
        return cls.from_stat(s, hash_value)


class Directory(BaseModel):
    __root__: Dict[str, Inode]

    @property
    def children(self):
        return self.__root__

    @children.setter
    def children(self, value: Dict[str, Inode]):
        self.__root__ = value

    def dump(self) -> bytes:
        return self.json(sort_keys=True).encode()


class Backup(BaseModel):
    client_id: UUID
    client_name: str
    backup_date: datetime
    started: datetime
    completed: datetime
    roots: Dict[str, Inode]
    description: Optional[str]


class DirectoryDefResponse(BaseModel):
    # The new ID of this directory - This is only valid for the session, once complete() the ID may change
    # This may be set Null files are missing.
    ref_hash: Optional[str]
    # Optional list of files which will need to be uploaded before the session can completed.
    # The client MUST retry the request once all missing files have been uploaded.
    missing_files: List[str] = []


class FilterType(enum.Enum):
    INCLUDE = 'include'
    EXCLUDE = 'exclude'


class Filter(NamedTuple):
    filter: FilterType
    path: str


class ClientConfiguredBackupDirectory(BaseModel):
    base_path: str
    filters: List[Filter] = Field(default_factory=list)


class ClientConfiguration(BaseModel):
    # Friendly name for the client, useful for logging
    client_name: str

    # The id of this client
    client_id: UUID = Field(default_factory=uuid4)

    # Typically set to 1 day or 1 hour.
    backup_granularity: timedelta = timedelta(days=1)

    # backup
    backup_directories: Dict[str, ClientConfiguredBackupDirectory] = Field(default_factory=dict)


class BackupSession(Protocol):
    # This ID of this session.  This can be used to resume the session later
    session_id: UUID

    # The reference backup date for this session.
    backup_date: datetime

    # The time this session will be automatically discarded.
    expires: datetime

    # Is the session still open.  Initially True.  Will be set False when the session is completed or discarded
    is_open: bool

    @abstractmethod
    async def directory_def(self, definition: Directory, replaces: Optional[str] = None) -> DirectoryDefResponse:
        """
        Add a directory definition to the server.  The server is ultimately responsible for giving the hash of the
        :param definition: The contents of the directory with the file name as the key and the Inode as the value
        :param replaces: Overwrite an existing directory definition with this one.  This might change the id,
            so the return value must still be read
        :returns: The id of this directory on the server.
        """

    @abstractmethod
    async def upload_file_content(self, file_content: Union[Path, BinaryIO], resume_id: UUID,
                                  resume_from: int = 0) -> str:
        """
        Upload a file, or part of a file to the server.  The server will respond with an ID (the hash) for that file.
        If the upload is interrupted, then the backup can resume where it left off by first calling
        check_file_upload_size and then upload_file_content with the same_resume_id, sending only the remaining part of
        the file.

        Be very careful not to re-use resume_id within the same session!
        :param file_content: Either a Path pointing to a local file or a readable and seekable BinaryIO.  If path is
            specified then restart logic is inferred
        :param resume_id: I locally specified ID to use to resume file upload in the vent it fails part way through.
            WARNING reusing the same resume_id inside the same session will overwrite the previous file.
        :param resume_from: When resuming a failed upload, this specifies how many bytes of the partial upload to keep.
            EG: if this is set to 500 then the first 500 bytes of the partial upload will be kept and all beyond that
            will be overwritten.  Not this will implicitly cause a seek operation on file_content.
        """

    @abstractmethod
    async def add_root_dir(self, root_dir_name: str, inode: Inode) -> None:
        """
        Add a root directory to the backup.  A backup consists of one or more root directory.  Attempting to complete
        a backup with no roots added will result in an error.
        :param root_dir_name: The name for this root directory
        :param inode: the stats about this backup directory including it's hash
        """

    @abstractmethod
    async def check_file_upload_size(self, resume_id: UUID) -> int:
        """
        Checks to see how much of a file was successfully uploaded.
        """

    @abstractmethod
    async def complete(self) -> None:
        """
        Finalize the backup.  Once this has completed, the backup will be visible to other clients and it cannot be
        modified further.
        """

    @abstractmethod
    async def discard(self) -> None:
        """
        Delete this partial backup entirely.  This cannot be undone.  All uploads etc will be discarded from the server.
        """


class ServerSession(Protocol):
    client_config: ClientConfiguration

    @abstractmethod
    async def start_backup(self, backup_date: datetime, replace_okay: bool = False, description: Optional[str] = None
                           ) -> BackupSession:
        """
        Create a new session on the server.  This is used to upload a backup to the server.  Backups happen as a
        transaction.  IE once a session is open, you can upload to that session, but files will not be available
        until the session has been completed.
        :param backup_date: The date/time for this backup.
            This will be rounded based on the configured backup_granularity
        :param replace_okay: If False then an error will be raised if the configured backup already exists.  If True
            The existing backup will be destroyed on complete().
        :param description: User specified description of this backup
        """

    @abstractmethod
    async def resume_backup(self, *, session_id: Optional[UUID] = None, backup_date: Optional[datetime] = None
                            ) -> BackupSession:
        """
        Retrieve a backup session.  It is legitimate to have multiple clients attached to the same backup session.
        However this may actually hurt performance since separate clients may end up uploading the same file each where
        a single client would only upload it once.
        :param session_id: The session id to retrieve
        :param backup_date: The backup date of the session to retrieve
        :return: The session associated with the session_id if not None otherwise the session associated with the
            backup_date
        """

    @abstractmethod
    async def get_backup(self, backup_date: Optional[datetime] = None) -> Optional[Backup]:
        """
        Fetch the details of a completed backup.
        :param backup_date: The backup date of the required backup.  This will be automatically normalized.  If None
            (default) the most recent backup will be retrieved.
        :return: The backup meta data or None if no backup was found.
        """

    @abstractmethod
    async def read_directory(self, inode: Inode) -> Directory:
        """
        Reads a directory
        :param inode: The handle to the directory
        """

    @abstractmethod
    async def read_file(self, inode: Inode, target_path: Optional[Path] = None,
                        restore_permissions: bool = False, restore_owner: bool = False) -> Optional[BinaryIO]:
        """
        Reads a file.
        :param inode: The handle to the file
        :param target_path: The local file path to write the new file. Parent directory must exist, no file at this path
            may exist or a FileExists exception will be raised
        :param restore_permissions: If target_path is specified, restore the original file permissions (mode).
            WARNING this will restore setuid and setgid bits also.
        :param restore_owner: If target_path is specified, restore ownership will not.
        :return: If target_path is None then the file content will be returned as a bytes object.  If target_path is
            not None then None is returned.
        """


def normalize_backup_date(backup_date: datetime, backup_granularity: timedelta):
    """
    Normalize a backup date to the given granularity. EG if granularity is set to 1 day, the backup_date is set to
    midnight of that same day.  If granularity is set to 1 hour, then backup_date is set to the start of that hour.
    """
    assert backup_date.tzinfo is not None
    timestamp = backup_date.timestamp()
    timestamp -= timestamp % backup_granularity.total_seconds()
    return datetime.fromtimestamp(timestamp, timezone.utc)


def hash_content(content: Union[bytes, str, BinaryIO]) -> str:
    """
    Generate an sha256sum for the given content
    """
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
