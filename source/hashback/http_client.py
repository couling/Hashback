import abc
import asyncio
from concurrent.futures import Executor, ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from os import SEEK_SET, SEEK_END
from pathlib import Path
from typing import Optional, BinaryIO, Union, Protocol, Any, Dict
from uuid import UUID

import requests.auth

from hashback import protocol, http_protocol
from hashback.protocol import Inode, Directory, DirectoryDefResponse, Backup, BackupSession, ClientConfiguration, \
    BackupSessionConfig


class Client(Protocol):
    @classmethod
    @abc.abstractmethod
    async def login(cls, server: http_protocol.ServerProperties) -> "ClientSession":
        pass

    @abc.abstractmethod
    async def request(self, endpoint: http_protocol.Endpoint, body = None, **params: Any):
        pass


class ClientSession(protocol.ServerSession):

    def __init__(self, client: Client, client_config: http_protocol.USER_CLIENT_CONFIG):
        self._client = client
        self._client_config = client_config

    @classmethod
    async def create_session(cls, client: Client) -> "ClientSession":
        client_config = await client.request(http_protocol.USER_CLIENT_CONFIG)
        return cls(client=client, client_config=client_config)

    @property
    def client_config(self) -> ClientConfiguration:
        return self._client_config

    async def start_backup(self, backup_date: datetime, allow_overwrite: bool = False,
                           description: Optional[str] = None) -> BackupSession:
        params = {
            'backup_date': backup_date,
            'allow_overwrite': allow_overwrite,
        }
        if description is not None:
            params['description'] = description
        return ClientBackupSession(self, await self._client.request(http_protocol.START_BACKUP, params=params))

    async def resume_backup(self, *, session_id: Optional[UUID] = None,
                            backup_date: Optional[datetime] = None) -> BackupSession:
        return await self._client.request(http_protocol.RESUME_BACKUP, session_id=session_id, backup_date=backup_date)


    async def get_backup(self, backup_date: Optional[datetime] = None) -> Optional[Backup]:
        if backup_date is None:
            return await self._client.request(http_protocol.BACKUP_LATEST)
        else:
            return await self._client.request(http_protocol.BACKUP_BY_DATE, backup_date=backup_date)

    async def get_directory(self, inode: Inode) -> Directory:
        if inode.type != protocol.FileType.DIRECTORY:
            raise ValueError("Inode is not a directory")
        result = await self._client.request(http_protocol.GET_DIRECTORY, ref_hash=inode.hash)
        return Directory(__root__=result.children)

    async def get_file(self, inode: Inode, target_path: Optional[Path] = None, restore_permissions: bool = False,
                       restore_owner: bool = False) -> Optional[protocol.FileReader]:
        result = await self._client.request(http_protocol.GET_FILE, restore_permissions=restore_permissions,
                                            restore_owner=restore_owner)
        with result:
            content_length = result.headers.get('Content-Length', None)
            if content_length is not None:
                content_length = int(content_length)
            content = result.content
            content.file_size = content_length
            content.close = lambda: None
        if target_path is None:
            return content
        await protocol.restore_file(target_path, inode, content, restore_owner, restore_permissions)
        return None


class ClientBackupSession(protocol.BackupSession):
    def __init__(self, client_session: ClientSession, config: BackupSessionConfig):
        self._client_session = client_session
        self._config = config
        self._client = client_session._client

    @property
    def config(self) -> BackupSessionConfig:
        return self._config

    @property
    def server_session(self) -> ClientSession:
        return self._client_session

    @property
    def is_open(self) -> bool:
        # TODO figure out how to do this
        return True

    async def _request(self, endpoint: http_protocol.Endpoint, body = None, **params: Any):
        return await self._client.request(endpoint, body, session_id=self._config.session_id, **params)

    async def directory_def(self, definition: Directory, replaces: Optional[UUID] = None) -> DirectoryDefResponse:
        result: DirectoryDefResponse = await self._request(http_protocol.DIRECTORY_DEF, body=definition, replaces=replaces)
        if result.success:
            assert result.missing_ref is None
            # TODO considder adding ref_hash into DirectoryDefResponse
            # assert result.ref_hash is not None
        return result

    async def upload_file_content(self, file_content: Union[Path, BinaryIO], resume_id: UUID, resume_from: int = 0,
                                  is_complete: bool = True) -> Optional[str]:
        # TODO break large files into multiple requests
        # TODO detect sparse files and upload in chunks
        result: http_protocol.UploadFileContentResponse = await self._request(http_protocol.UPLOAD_FILE,
            body=file_content,
            resume_id=resume_id,
            resume_from=resume_from,
            is_complete=is_complete
        )
        if is_complete and result.ref_hash is None:
            raise protocol.InvalidResponseError("Server returned None for complete hashed file")
        return result.ref_hash

    async def add_root_dir(self, root_dir_name: str, inode: Inode) -> None:
        await self._request(http_protocol.ADD_ROOT_DIR, body=inode, root_dir_name=root_dir_name)

    async def check_file_upload_size(self, resume_id: UUID) -> int:
        response = await self._request(http_protocol.FILE_PARTIAL_SIZE, resume_id=resume_id)
        return response.size

    async def complete(self) -> Backup:
        return await self._request(http_protocol.COMPLETE_BACKUP)

    async def discard(self) -> None:
        return await self._request(http_protocol.DISCARD_BACKUP)


class BasicAuthClient(Client):
    _base_url: str
    _auth: requests.auth.AuthBase
    _server_version: http_protocol.ServerVersion
    _executor: Executor
    _http_session: requests.Session

    @classmethod
    async def login(cls, server_properties: http_protocol.ServerProperties) -> ClientSession:
        client = cls(server_properties)
        try:
            return await ClientSession.create_session(client)
        except:
            client.close()
            raise

    def __init__(self, server: http_protocol.ServerProperties):
        self._auth = requests.auth.HTTPBasicAuth(username=server.username, password=server.password)
        server_path = server.copy()
        server_path.username = None
        server_path.password = None
        self._base_url = server_path.format_url()
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._http_session = requests.Session()

    async def server_version(self) -> http_protocol.ServerVersion:
        return await self.request(http_protocol.HELLO)

    async def request(self, endpoint: http_protocol.Endpoint, body = None, **params: Any) -> Any:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._request_object, endpoint, params, body)

    def _request_object(self, endpoint: http_protocol.Endpoint, params: Dict[str, Any], body = None, ):
        url = endpoint.format_url(self._base_url, params)
        stream_response = isinstance(endpoint.result_type, BinaryIO)

        if body is None:
            response = self._send_request(endpoint.method, url, stream_response)
        elif isinstance(body, Path):
            with body.open('rb') as file:
                response = self._send_request(endpoint.method, url, stream_response, files={'file': file})
        elif isinstance(body, bytes):
            response = self._send_request(endpoint.method, url, stream_response, files={'file': body})
        elif hasattr(body, 'json'):
            response = self._send_request(endpoint.method, url, stream_response, data=body.json().encode(),
                                          headers={'Content-Type': 'application/json'})
        else:
            raise ValueError(f"Cannot send body type {type(body).__name__}")

        result = None
        try:
            if endpoint.result_type is None:
                pass  # result = None
            elif isinstance(endpoint.result_type, BinaryIO):
                result = RequestResponse(response, self._executor)
            elif hasattr(endpoint.result_type, 'parse_raw'):
                result = endpoint.result_type.parse_raw(response.content)
            else:
                raise ValueError(f'Cannot parse result type {endpoint.result_type.__name__}')

        finally:
            if not isinstance(result, RequestResponse):
                # If we need to close the result, it might not be a good idea to close without reading the content
                # This results in closing the connection which may slow down future requests.  If the Content-Length
                # header shows less than 10KB, we read the content and leave the connection open by consuming the body.
                # This has not been performance tuned: 10KB is a guess.
                try:
                    if int(response.headers['Content-Length']) <= 10240:
                        _ = response.content
                except Exception:
                    # Honestly we really don't care if / why the above failed.
                    pass
                response.close()
        return result

    def _send_request(self, method: str, url: str, stream_response: bool, **kwargs) -> requests.Response:
        response = self._http_session.request(method, url, stream=stream_response, auth=self._auth, **kwargs)

        if response.status_code >= 400:
            status_code = response.status_code
            content = response.content
            response.close()
            try:
                # Try to parse this as a remote exception.
                remote_exception = protocol.RemoteException.parse_raw(content)
            except ValueError:
                pass
            else:
                # If this was a valid remote exception we can just raise it.
                raise remote_exception.exception() from None


            try:
                message = "\n" + content.decode()
            except UnicodeDecodeError:
                message = ""
            if status_code == 422:
                raise protocol.InvalidArgumentsError(message)
            raise protocol.InvalidResponseError(f"Bad response from server {status_code}: {message}")

        return response

    def close(self):
        self._executor.shutdown(wait=False)
        self._http_session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class RequestResponse(protocol.FileReader):
    def __init__(self, response: requests.Response, executor: Executor):
        self._response = response
        self._content = self._response.iter_content(protocol.READ_SIZE)
        self._cached_content = BytesIO()
        self._executor = executor

    async def read(self, n: int = None) -> bytes:
        if n < 0:
            return await asyncio.get_running_loop().run_in_executor(self._executor, self._read_all)
        return await asyncio.get_running_loop().run_in_executor(self._executor, self._read_partial, n)

    def _read_all(self) -> bytes:
        current_pos = self._cached_content.tell()
        self._cached_content.seek(0, SEEK_END)
        for block in self._content:
            self._cached_content.write(block)
        self._cached_content.seek(current_pos, SEEK_SET)
        return self._cached_content.read(self._cached_content.getbuffer().nbytes - current_pos)

    def _read_partial(self, n: int) -> bytes:
        result = self._cached_content.read(n)
        if not result:
            try:
                self._cached_content = BytesIO(next(self._content))
            except StopIteration:
                return bytes()
            result = self._cached_content.read(n)
        return result

    def close(self):
        self._response.close()

    @property
    def file_size(self) -> Optional[int]:
        try:
            return int(self._response.headers['Content-Length'])
        except KeyError:
            return None