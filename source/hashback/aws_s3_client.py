import hashlib
from contextlib import closing
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple, Type, TypeVar, Union
from uuid import UUID, uuid4
import logging
import pydantic

import boto3
import pydantic

from . import misc, protocol
from .protocol import Backup, BackupSession, BackupSessionConfig, ClientConfiguration, Directory, DirectoryDefResponse, \
    FileReader, Inode

_CLIENTS_PATH = "clients"
_CLIENT_REFERENCE = "client-names"
_DIRECTORIES = "directories"
_BACKUPS = "backups"
_BACKUP_SESSIONS = "backup-sessions"
_PARTIAL_UPLOADS = "partial-uploads"
_FILES = "files"


_T = TypeVar("_T")

logger = logging.getLogger(__name__)

class Credentials(pydantic.BaseModel):
    profile_name: Optional[str] = None
    region_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None



class S3Database(protocol.BackupDatabase):

    min_upload_size = min(protocol.READ_SIZE, (1024 ** 2) * 5)

    def __init__(self, bucket_name: str, directory: str = "", credentials: Credentials = Credentials()):
        super().__init__(self)
        self._bucket_name = bucket_name
        self._prefix = directory + "/" if directory and directory[-1] != "/" else directory
        self._client = boto3.Session(**credentials.dict()).client("s3")

    def open_client_session(self, client_id_or_name: str) -> protocol.ServerSession:
        configuration = self.load_client_config(client_id_or_name)
        return S3Session(self, configuration)

    def save_client_config(self, client_config: ClientConfiguration):
        file_location = f"{_CLIENTS_PATH}/{client_config.client_id}.json"
        file_content = client_config.json().encode(protocol.ENCODING)
        self._client.put_object(
            Bucket=self._bucket_name,
            Key=self._prefix + file_location,
            Body=file_content,
        )
        client_ref_path = f"{_CLIENT_REFERENCE}/{client_config.client_name}"
        client_reference = str(client_config.client_id).encode(protocol.ENCODING)
        self._client.put_object(
            Bucket=self._bucket_name,
            Key=self._prefix + client_ref_path,
            Body=client_reference,
        )

    def load_client_config(self, client_id_or_name: str) -> ClientConfiguration:
        try:
            client_id = UUID(client_id_or_name)
        except ValueError:
            reference = self._client.get_object(
                Bucket=self._bucket_name,
                Key=f"{self._prefix}{_CLIENT_REFERENCE}/{client_id_or_name}",
            )['Body']
            with reference:
                client_id = UUID(reference.read().decode(protocol.ENCODING))
        file_location = f"{_CLIENTS_PATH}/{str(client_id)}.json"
        return self.read_json(file_location, ClientConfiguration)

    #######################
    # lower Level functions
    #######################

    def read_json(self, file_key: str, type_class: Type[_T]) -> _T:
        try:
            response = self._client.get_object(Bucket=self._bucket_name, Key=self._prefix + file_key)
            with response['Body']:
                content = response['Body'].read()
            return type_class.parse_raw(content, encoding=protocol.ENCODING)
        except self._client.exceptions.ClientError as ex:
            if getattr(ex, "response", {}).get('Error', {}).get('Code', "") == "404":
                raise protocol.NotFoundException(file_key)
        except pydantic.ValidationError as ex:
            raise protocol.InternalServerError(f"Corrupt corrupt json {file_key}") from ex

    def put_json(self, file_key: str, body):
        self.put_object(file_key, body.json().encode(protocol.ENCODING))

    def put_object(self, file_key: str, body: bytes):
        self._client.put_object(
            Bucket=self._bucket_name,
            Key=self._prefix + file_key,
            Body=body,
        )

    def object_exists(self, file_key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=self._prefix + file_key)
            return True
        except self._client.exceptions.ClientError as ex:
            if getattr(ex, "response", {}).get('Error', {}).get('Code', "") == "404":
                return False
            raise

    def delete_objects(self, *keys: str):
        objects_to_delete = [{'Key': self._prefix + key} for key in keys]
        self._client.delete_objects(
            Bucket=self._bucket_name,
            Delete={'Objects': objects_to_delete},
        )


    def iter_clients(self) -> Iterable[ClientConfiguration]:
        """
        Iterate over the clients configured in this DB
        """
        # TODO
        raise NotImplementedError()


class S3Session(protocol.ServerSession, closing):

    def __init__(self, database: S3Database, client_config: ClientConfiguration):
        super().__init__(self)
        # The wrapper will run any calls in another thread allowing them to be awaited.
        self._database = misc.AsyncThreadWrapper(database)
        self._client_config = client_config
        self._rate_limit = misc.FairSemaphore(2)

    @property
    def client_config(self) -> ClientConfiguration:
        return self._client_config

    async def start_backup(self, backup_date: datetime, allow_overwrite: bool = False,
                           description: Optional[str] = None) -> BackupSession:

        backup_date = self._client_config.normalize_backup_date(backup_date)

        if not allow_overwrite:
            final_location = f"{_BACKUPS}/{self._client_config.client_id}/{backup_date.isoformat()}"
            if await self._database.object_exists(final_location):
                raise protocol.DuplicateBackup(f"Backup exists {backup_date.isoformat()}")

        session_config = protocol.BackupSessionConfig(
            client_id=self.client_config.client_id,
            session_id=uuid4(),
            allow_overwrite=allow_overwrite,
            backup_date=backup_date,
            description=description,
            started=datetime.now(self.client_config.timezone)
        )

        session_location = f"{_BACKUP_SESSIONS}/{self._client_config.client_id}/{session_config.session_id}"
        await self._database.put_json(session_location, session_config)
        return S3BackupSession(self, session_config)

    async def resume_backup(self, *, session_id: Optional[UUID] = None, backup_date: Optional[datetime] = None,
                            discard_partial_files: bool = False) -> BackupSession:
        config = await self._database.read_json(
            f"{_BACKUP_SESSIONS}/{self._client_config.client_id}/{backup_date.isoformat()}",
            BackupSessionConfig,
        )
        return S3BackupSession(self, config)

    async def list_backup_sessions(self) -> List[BackupSessionConfig]:
        # TODO
        raise NotImplementedError()

    async def list_backups(self) -> List[Tuple[datetime, str]]:
        # TODO
        raise NotImplementedError()

    async def get_backup(self, backup_date: Optional[datetime] = None) -> Optional[Backup]:
        try:
            return await self._database.read_json(f"{_BACKUPS}/{self._client_config.client_id}/{backup_date.isoformat()}", Backup)
        except protocol.NotFoundException:
            return None

    async def get_directory(self, inode: Inode) -> Directory:
        assert inode.hash
        return await self._database.read_json(f"{_DIRECTORIES}/{inode.hash}", Directory)

    async def get_file(self, inode: Inode) -> Optional[FileReader]:
        # TODO
        raise NotImplementedError()


class S3BackupSession(protocol.BackupSession):

    def __init__(self, session: S3Session, config: protocol.BackupSessionConfig):
        self._session = session
        self._config = config
        self._is_open = True
        self._partial_uploads = {}
        self._roots = {}

    @property
    def config(self) -> BackupSessionConfig:
        return self._config

    @property
    def server_session(self) -> protocol.ServerSession:
        return self._session

    @property
    def is_open(self) -> bool:
        return self._is_open

    async def directory_def(self, definition: Directory, replaces: Optional[UUID] = None) -> DirectoryDefResponse:
        missing_files = [name for name, inode in definition.children.items() if not await self._inode_exists(inode)]
        if missing_files:
            return DirectoryDefResponse(missing_files=missing_files)
        ref_hash, content = definition.hash()
        await self._session._database.put_object(f"{_DIRECTORIES}/{ref_hash}", content)
        return DirectoryDefResponse(ref_hash=ref_hash)

    async def upload_file_content(self, file_content: Union[FileReader, bytes], resume_id: UUID,
                                  resume_from: Optional[int] = None, is_complete: bool = True) -> Optional[str]:
        async with self._session._rate_limit:
            if resume_id not in self._partial_uploads:
                upload = S3MultipartUpload(self, resume_id)
                self._partial_uploads[resume_id] = upload
            else:
                upload = self._partial_uploads[resume_id]
            if isinstance(file_content, bytes):
                await upload.upload_part(resume_from or 0, file_content)
            else:
                offset = 0
                while bytes_read := await file_content.read(protocol.READ_SIZE):
                    await upload.upload_part((resume_from or 0) + offset, bytes_read)
                    offset += len(bytes_read)
        if is_complete:
            result = await upload.complete()
            del self._partial_uploads[resume_id]
            return result
        else:
            return

    async def add_root_dir(self, root_dir_name: str, inode: Inode) -> None:
        self._roots[root_dir_name] = inode

    async def check_file_upload_size(self, resume_id: UUID) -> int:
        # TODO
        raise NotImplementedError()

    async def complete(self) -> Backup:
        backup_record = Backup(
            client_id=self._config.client_id,
            client_name=self._session._client_config.client_name,
            backup_date=self._config.backup_date,
            started=self._config.started,
            completed=datetime.now(self._session.client_config.timezone),
            roots=self._roots,
            description=self._config.description,
        )
        file_key = f"{_BACKUPS}/{self._config.client_id}/{backup_record.backup_date.isoformat()}"
        if not self._config.allow_overwrite and await self._session._database.object_exists(file_key):
            raise protocol.DuplicateBackup()
        await self._session._database.put_json(file_key, backup_record)
        await self.discard()
        return backup_record

    async def discard(self) -> None:
        backup_session_path = f"{_BACKUP_SESSIONS}/{self._config.client_id}/{self._config.session_id}"
        await self._session._database.delete_objects(backup_session_path)

    async def _inode_exists(self, inode: Inode) -> bool:
        look_in = _DIRECTORIES if inode.type is protocol.FileType.DIRECTORY else _FILES
        file_path = f"{look_in}/{inode.hash}"
        return await self._session._database.object_exists(file_path)


class S3MultipartUpload:
    # Lets make it difficult for someone to shoot themselves in the foot.  AWS has a 5MB minimum limit
    _S3_MIN_LIMIT = (1024 ** 2) * 5

    # We don't want to chop other reads too much
    min_upload_size = (1024 ** 2) * 20
    _cache: Union[bytearray, bytes] = None
    _hash = hashlib.sha256()
    _upload_size: int = 0
    _upload_parts: List[str]
    _upload_id: Optional[str] = None

    def __init__(self, backup_session: S3BackupSession, resume_id: UUID):
        self._database = backup_session._session._database
        self._bucket = self._database._bucket_name
        self._file_key = self._database._prefix + "/".join((
            _PARTIAL_UPLOADS,
            str(backup_session._config.client_id),
            str(backup_session._config.session_id),
            str(resume_id),
        ))
        self._upload_parts = []
        self._client = misc.AsyncThreadWrapper(backup_session._session._database._client)

    async def upload_part(self, position: int, content: bytes):
        total_content = self._upload_size + len(self._cache or bytes())
        if position != total_content:
            raise ValueError(f"Cannot add content out of sequence {total_content} for {self._file_key}")
        new_hash = self._hash.copy()
        new_hash.update(content)

        if self._cache is not None:
            if not isinstance(self._cache, bytearray):
                # Defer conversion from bytes to bytearray until we know for sure we need to combine two blocks
                # This prevents unwanted copying of large volumes of data
                self._cache = bytearray(self._cache)
            self._cache += content
        else:
            self._cache = content

        self._hash = new_hash

        if len(content) >= max(self.min_upload_size, self._S3_MIN_LIMIT):
            await self._flush()

    async def _flush(self):
        if self._upload_id is None:
            response = self._upload_id = await self._client.create_multipart_upload(
                Bucket=self._bucket,
                Key=self._file_key,
            )
            self._upload_id = response['UploadId']

        logger.debug("Uploading part %s - %s", len(self._upload_parts), self._upload_size + len(self._cache))
        response = await self._client.upload_part(
            Bucket=self._bucket,
            Key=self._file_key,
            UploadId=self._upload_id,
            PartNumber=len(self._upload_parts) + 1,
            Body=self._cache,
        )
        self._upload_parts.append(response['ETag'])
        self._upload_size += len(self._cache)
        del self._cache


    async def complete(self) -> str:
        ref_hash = self._hash.hexdigest()
        target_key = f"{_FILES}/{ref_hash}"
        if await self._database.object_exists(target_key):
            await self.abort()
            return ref_hash

        if self._upload_id is None:
            # If we have not started a multipart upload then bypass that procedure and simply upload the object
            await self._database.put_object(target_key, self._cache if self._cache is not None else b"")
            return ref_hash

        if self._cache is not None:
            await self._flush()

        await self._client.complete_multipart_upload(
            Bucket=self._bucket,
            Key=self._file_key,
            UploadId=self._upload_id,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': num,
                        'ETag': etag,
                    }
                    for num, etag in enumerate(self._upload_parts, start=1)
                ]
            },
        )
        await self._client.copy_object(
            Bucket=self._bucket,
            Key=f"{self._database._prefix}{_FILES}/{ref_hash}",
            CopySource={'Bucket': self._bucket, 'Key': self._file_key},
        )
        await self._client.delete_object(
            Bucket=self._bucket,
            Key=self._file_key,
        )
        del self._upload_id
        # Cleanup internal state
        await self.abort()

        return ref_hash

    async def abort(self):
        if self._upload_id is not None:
            await self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=self._file_key,
                UploadId=self._upload_id,
            )
            del self._upload_id
            self._upload_parts = []
            del self._upload_size
            del self._hash
        if self._cache is not None:
            del self._cache


class S3FileReader(protocol.FileReader):

    def __init__(self, body, size: Optional[int]):
        self._body = body
        self._size = size


    async def read(self, num_bytes: int = None) -> bytes:
        return self._body.read(num_bytes)

    def close(self):
        self._body.close()

    @property
    def file_size(self) -> Optional[int]:
        return self._size
