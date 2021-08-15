import enum
import hashlib
import os
import stat
from abc import abstractmethod
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Protocol, Dict, Optional, Union, BinaryIO, List, NamedTuple, Collection, Tuple
from uuid import UUID, uuid4
import dateutil.tz


import aiofiles.os
from pydantic import BaseModel, Field, validator

# This will either get bumped, or the file will be duplicated and each one will have a VERSION.  In any case this file
# specifies protocol version ...
VERSION = "1.0"


EMPTY_FILE = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
READ_SIZE = 1024**2

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
    type: FileType
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

    @classmethod
    def _type(cls, mode: int) -> FileType:
        # TODO separate this into from_stat() and add a type attribute.
        for file_type, check in cls._MODE_CHECKS:
            if check(mode):
                return file_type
        raise ValueError(f"No type found for mode {mode}")

    @property
    def permissions(self) -> int:
        return stat.S_IMODE(self.mode)

    @classmethod
    def from_stat(cls, s, hash_value: Optional[str]) -> "Inode":
        return Inode(
            mode=stat.S_IMODE(s.st_mode),
            type=cls._type(s.st_mode),
            size=s.st_size,
            uid=s.st_uid,
            gid=s.st_gid,
            modified_time=datetime.fromtimestamp(s.st_mtime, timezone.utc),
            hash=hash_value,
        )


class DirectoryHash(NamedTuple):
    ref_hash: str
    content: bytes


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

    def hash(self) -> DirectoryHash:
        content = self.dump()
        return DirectoryHash(hash_content(content), content)


class Backup(BaseModel):
    client_id: UUID
    client_name: str
    backup_date: datetime
    started: datetime
    completed: datetime
    roots: Dict[str, Inode]
    description: Optional[str]


class FileReader(Protocol):
    @abstractmethod
    async def read(self, n: int = None) -> bytes:
        """
        Read n bytes from the source. If N < 0 read all bytes to the EOF before returning.
        """

    @abstractmethod
    def close(self):
        """
        Close the handle to the underlying source.
        """

    @property
    @abstractmethod
    def file_size(self) -> Optional[int]:
        """
        Get the size of this file. May be Null if the item is a pipe or socket.
        """


class DirectoryDefResponse(BaseModel):
    # This will be set on success but not on error
    ref_hash: Optional[str]

    # If defining the directory could not complete because children were missing, the name (not path) of each missing
    # file will be added to missing_files.
    missing_files: List[str] = []

    # Optionally the server can track all directory def requests which have missing files.  BUT something can happen
    # on the client side in between the two directory_def requests.  Eg: a file could be deleted before the client
    # had chance to upload it.  When that happens the second directory_def request will have a different hash to the
    # first.
    # missing_ref is a server-side reference to the previous failed request.  This does not change with content,
    # but may change each request.
    missing_ref: Optional[UUID] = None

    @property
    def success(self) -> bool:
        # If there were no missing files, then the definition was a success.
        # This structure is NOT used to report errors
        return not self.missing_files


class FilterType(enum.Enum):
    INCLUDE = 'include'
    EXCLUDE = 'exclude'


class Filter(BaseModel):
    filter: FilterType = Field(...)
    path: str = Field(...)


class ClientConfiguredBackupDirectory(BaseModel):
    base_path: str = Field(...)
    filters: List[Filter] = Field(default_factory=list)


class ClientConfiguration(BaseModel):
    # Friendly name for the client, useful for logging
    client_name: str = Field(...)

    # The id of this client
    client_id: UUID = Field(default_factory=uuid4)

    # Typically set to 1 day or 1 hour.
    backup_granularity: timedelta = Field(timedelta(days=1))

    # backup
    backup_directories: Dict[str, ClientConfiguredBackupDirectory] = Field(default_factory=dict)

    # Timezone
    named_timezone: str = Field("Etc/UTC")

    @property
    def timezone(self) -> tzinfo:
        return dateutil.tz.gettz(self.named_timezone)


class BackupSessionConfig(BaseModel):
    client_id: UUID
    session_id: UUID
    backup_date: datetime
    started: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    allow_overwrite: bool
    description: Optional[str] = None

    class Config:
        allow_mutation = False


class BackupSession(Protocol):

    @property
    @abstractmethod
    def config(self) -> BackupSessionConfig:
        """The settings for this backup session"""

    @property
    @abstractmethod
    def server_session(self) -> "ServerSession":
        """The server session this backup session is attached to"""

    @property
    @abstractmethod
    def is_open(self) -> bool:
        """Is the session still open.  Initially True.  Will be set False when the session is completed or discarded"""

    @abstractmethod
    async def directory_def(self, definition: Directory, replaces: Optional[UUID] = None) -> DirectoryDefResponse:
        """
        Add a directory definition to the server.  The server is ultimately responsible for giving the hash of the
        :param definition: The contents of the directory with the file name as the key and the Inode as the value
        :param replaces: Overwrite an existing directory definition with this one.  This might change the id,
            so the return value must still be read
        :returns: The id of this directory on the server.
        """

    @abstractmethod
    async def upload_file_content(self, file_content: Union[Path, BinaryIO], resume_id: UUID,
                                  resume_from: int = 0, is_complete: bool = True) -> Optional[str]:
        """
        Upload a file, or part of a file to the server.  The server will respond with an ID (the hash) for that file.
        If the upload is interrupted, then the backup can resume where it left off by first calling
        check_file_upload_size and then upload_file_content with the same_resume_id, sending only the remaining part of
        the file.

        Clients MUST NOT call upload_file_content in parallel or out of sequence.  This would make it impossible to
        determine how much of a request succeeded with check_file_upload_size.  Servers may raise a ProtocolError if
        this occurs.  However server authors should be mindful that their own locks could cause phantom parallel
        requests avoid tripping up a client unnecessarily.

        Clients may use the resume feature to upload non-zero sections of a partial file.  Thus servers MUST support
        the scenario where a client calls upload_file_content with a resume_from much larger than the existing partial
        file.  Even servers MUST support calling upload_file_content with resume_from > 0 on the first request for that
        file.

        Clients MUST NOT call upload_file_content on a file after it has been successfully completed (with
        complete=True).  Clients may use check_file_upload_size to check if the previous interrupted request completed.
        Here a NotFoundException would infer the previous request completed successfully.

        :param file_content: Either a Path pointing to a local file or a readable and seekable BinaryIO.  If path is
            specified then restart logic is inferred
        :param resume_id: I locally specified ID to use to resume file upload in the vent it fails part way through.
            WARNING reusing the same resume_id inside the same session will overwrite the previous file.
        :param resume_from: When resuming a failed upload, this specifies how many bytes of the partial upload to keep.
            EG: if this is set to 500 then the first 500 bytes of the partial upload will be kept and all beyond that
            will be overwritten.  Not this will implicitly cause a seek operation on file_content.
        :param is_complete: If complete is True an ID will be generated and resume_id will be invalidated.  If complete
            is False, no complete ID
        :return: The ref_hash of the newly uploaded file if complete=True or None if complete=False.  Clients may use
            ref_hash to check the file was not corrupted in transit.
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
        :param resume_id: The resume_id specified in upload_file_content
        :raises: NotFoundException if either an incorrect resume_id was specified, or otherwise the specified file
            has already completed (effectively deleting the resume_id server-side).
        """

    @abstractmethod
    async def complete(self) -> Backup:
        """
        Finalize the backup.  Once this has completed, the backup will be visible to other clients and it cannot be
        modified further.
        """

    @abstractmethod
    async def discard(self) -> None:
        """
        Delete this partial backup entirely.  This cannot be undone.  All uploads etc will be discarded from the server.
        """

    @property
    @abstractmethod
    def server_session(self) -> "ServerSession":
        """
        The server session this is attached to
        """


class ServerSession(Protocol):

    @property
    @abstractmethod
    def client_config(self) -> ClientConfiguration:
        """
        Client confic is stored remotely on the server so that it can be centrally managed for all nodes.
        Clients read this field to discover what they should back up etc.
        """

    @abstractmethod
    async def start_backup(self, backup_date: datetime, allow_overwrite: bool = False, description: Optional[str] = None
                           ) -> BackupSession:
        """
        Create a new session on the server.  This is used to upload a backup to the server.  Backups happen as a
        transaction.  IE once a session is open, you can upload to that session, but files will not be available
        until the session has been completed.
        :param backup_date: The date/time for this backup.
            This will be rounded based on the configured backup_granularity
        :param allow_overwrite: If False then an error will be raised if the configured backup already exists.  If True
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
    async def list_backup_sessions(self) -> List[BackupSessionConfig]:
        """
        Fetch a list of backup sessions.  Since the list of open sessions is generally very small, this will return
        the details of each one.
        """

    @abstractmethod
    async def list_backups(self) -> List[Tuple[datetime, str]]:
        """
        Fetch list of completed backups.  Typically the number of backups can stack up very large so this only returns
        the key for each backup (the datetime) and a name.
        :return: List of backups, keyed by datetime
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
    async def get_directory(self, inode: Inode) -> Directory:
        """
        Reads a directory
        :param inode: The handle to the directory
        """

    @abstractmethod
    async def get_file(self, inode: Inode, target_path: Optional[Path] = None,
                       restore_permissions: bool = False, restore_owner: bool = False) -> Optional[FileReader]:
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


class RequestException(Exception):
    http_status = 400  # Not knowing the cause of this request exception we can only assume it was an internal error


class NotFoundException(RequestException):
    http_status = 404  # Not Found


class InternalServerError(RequestException):
    http_status = 500  # Server did something wong.  Check the server logs.


class SessionClosed(RequestException):
    http_status = 410  # Gone


class DuplicateBackup(RequestException):
    http_status = 409 # Conflict


class ProtocolError(Exception):
    http_status: int = 400  # Bad request


class InvalidArgumentsError(ProtocolError):
    http_status: int = 422  # Unprocessable entity


class InvalidResponseError(ProtocolError):
    http_status: int = 502  # Bad response from backend


# Remote server-client interaction needs a way for the server to raise an exception with the client. Obviously we don't
# want to give the server free reign to raise any exception so anything in this module (or imported into it) can be
# raised by the server by name.
EXCEPTIONS_BY_NAME = {
    name: ex for name, ex in globals().items() if isinstance(ex, type) and issubclass(ex, Exception)
}

EXCEPTIONS_BY_TYPE = {
    ex: name for name, ex in globals().items() if isinstance(ex, type) and issubclass(ex, Exception)
}


class RemoteException(BaseModel):
    name: str
    message: str

    @validator('name')
    def _name_in_exceptions_by_name(cls, name: str) -> str:
        if name not in EXCEPTIONS_BY_NAME:
            raise ValueError(f"Invalid exception name: {name}", RemoteException)
        return name

    @classmethod
    def from_exception(cls, exception: Union[ProtocolError, RequestException]) -> "RemoteException":
        return cls(name=EXCEPTIONS_BY_TYPE[type(exception)], message=str(exception))

    def exception(self) -> Union[RequestException, ProtocolError]:
        exception = EXCEPTIONS_BY_NAME[self.name]
        return exception(self.message)


def normalize_backup_date(backup_date: datetime, backup_granularity: timedelta, client_timezone: tzinfo):
    """
    Normalize a backup date to the given granularity. EG: if granularity is set to 1 day, the backup_date is set to
    midnight of that same day.  If granularity is set to 1 hour, then backup_date is set to the start of that hour.
    """
    assert backup_date.tzinfo is not None
    timestamp = backup_date.timestamp()
    timestamp -= timestamp % backup_granularity.total_seconds()
    return datetime.fromtimestamp(timestamp, timezone.utc)


def hash_content(content: Union[bytes, str, BinaryIO, Path]) -> str:
    """
    Generate an sha256sum for the given content.  Yes this is absolutely part of the protocol!
    Either the server or client can hash the same file and the result MUST match on both sides or things will break.
    """
    h = hashlib.sha256()
    if isinstance(content, bytes):
        h.update(content)
    elif isinstance(content, str):
        h.update(content.encode("utf-8"))
    elif isinstance(content, Path):
        with content.open('rb') as file:
            bytes_read = file.read(READ_SIZE)
            while bytes_read:
                h.update(bytes_read)
                bytes_read = file.read(READ_SIZE)
    else:
        bytes_read = content.read(READ_SIZE)
        while bytes_read:
            h.update(bytes_read)
            bytes_read = content.read(READ_SIZE)
    return h.hexdigest()


async def restore_file(file_path: Path, inode: Inode, content: FileReader, restore_owner: bool, restore_permissions):
    # TODO verify hash as we go
    if inode.type == FileType.REGULAR:
        async with aiofiles.open(file_path, 'xb') as target:
            content = await content.read(READ_SIZE)
            while content:
                await target.write(content)
                content = await content.read(READ_SIZE)
    elif inode.type == FileType.LINK:
        link_target = (await content.read()).decode()
        file_path.symlink_to(link_target)
    else:  # inode.type == protocol.FileType.PIPE:
        if inode.hash != EMPTY_FILE:
            raise ValueError(f"File of type {inode.type} must be empty.  But this one is not: {inode.hash}")
        os.mkfifo(file_path)

    if restore_owner:
        os.chown(file_path, inode.uid, inode.gid)
    if restore_permissions:
        os.chmod(file_path, inode.mode)
    timestamp = inode.modified_time.timestamp()
    os.utime(file_path, (timestamp, timestamp))