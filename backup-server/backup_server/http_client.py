import abc
from datetime import datetime
from pathlib import Path
from typing import Optional, BinaryIO, Union, Protocol, Dict, Any
from uuid import UUID

import aiohttp

from backup_server import protocol, http_protocol
from backup_server.protocol import Inode, Directory, DirectoryDefResponse, Backup, BackupSession, ClientConfiguration, \
    BackupSessionConfig


class Client(Protocol):
    @classmethod
    @abc.abstractmethod
    async def login(cls, server: http_protocol.ServerProperties) -> "Client":
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
        return await self._client.request(http_protocol.START_BACKUP, params=params)

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
        return await self._client.request(http_protocol.GET_DIRECTORY, ref_hash=inode.hash)

    async def get_file(self, inode: Inode, target_path: Optional[Path] = None, restore_permissions: bool = False,
                       restore_owner: bool = False) -> Optional[protocol.FileReader]:
        result: aiohttp.ClientResponse
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
    def is_open(self) -> bool:
        # TODO figure out how to do this
        return True

    async def _request(self, endpoint: http_protocol.Endpoint, body = None, **params: Any):
        return await self._client.request(endpoint, body, session_id=self._config.session_id, **params)

    async def directory_def(self, definition: Directory, replaces: Optional[UUID] = None) -> DirectoryDefResponse:
        return await self._client.request(http_protocol.DIRECTORY_DEF, body=definition, replaces=replaces)

    async def upload_file_content(self, file_content: Union[Path, BinaryIO], resume_id: UUID, resume_from: int = 0,
                                  is_complete: bool = True) -> Optional[str]:
        # TODO
        pass

    async def add_root_dir(self, root_dir_name: str, inode: Inode) -> None:
        await self._request(http_protocol.ADD_ROOT_DIR, body=inode, root_dir_name=root_dir_name)

    async def check_file_upload_size(self, resume_id: UUID) -> int:
        response = await self._request(http_protocol.FILE_PARTIAL_SIZE, resume_id=resume_id)
        return response.size

    async def complete(self) -> Backup:
        return await self._request(http_protocol.COMPLETE_BACKUP)

    async def discard(self) -> None:
        return await self._request(http_protocol.DISCARD_BACKUP)

    @property
    def server_session(self) -> protocol.ServerSession:
        return self._client_session


class BasicAuthClient(Client):
    _client: aiohttp.ClientSession
    _base_url: str
    _server_properties: http_protocol.ServerProperties

    @classmethod
    async def login(cls, server: http_protocol.ServerProperties) -> "BasicAuthClient":
        basic_auth = aiohttp.BasicAuth(login=server.username, password=server.password)
        server_path = server.copy()
        server_path.username = None
        server_path.password = None
        base_url = server_path.format_url()
        client_session = aiohttp.ClientSession(
            auth=basic_auth,
            json_serialize=lambda o: o.json(),
        )
        try:
            client = cls(base_url=base_url,  client=client_session)
            server_properties = await client.request(http_protocol.HELLO)
            client._server_properties = server_properties
            return client
        except:
            await client_session.close()
            raise

    def __init__(self, base_url: str, client: aiohttp.ClientSession):
        self._base_url = base_url
        self._client = client
        self._server_properties = None

    @property
    def server_properties(self) -> http_protocol.ServerProperties:
        return self._server_properties


    async def request(self, endpoint: http_protocol.Endpoint, body = None, **params: Any):
        url = endpoint.format_url(self._base_url, params)
        response = await self._client.request(
            method=endpoint.method,
            url=url,
            json=body,
        )
        if response.status >= 400:
            reason = protocol.RemoteException.parse_obj(response.json())
            raise reason.exception()
        if hasattr(endpoint.result_type, 'parse_obj'):
            return endpoint.result_type.parse_obj(await response.json())
        return response

    async def close(self):
        await self._client.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()