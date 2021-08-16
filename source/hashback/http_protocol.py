import urllib.parse
from typing import Optional, Dict, List, NamedTuple, Type, BinaryIO, Any

from pydantic import BaseModel

from . import protocol

DEFAULT_PORT = 4649


class Endpoint(NamedTuple):
    method: str
    url_stub: str
    query_params: Optional[set]
    result_type: Optional[Type]

    def format_url(self, base_url: str, kwargs: Dict[str, Any]) -> str:
        if base_url[-1] == '/':
            base_url = base_url[:-1]

        result = self.url_stub
        if kwargs:
            kwargs = {key: urllib.parse.quote_plus(str(value)) for key, value in kwargs.items() if value is not None}
            result = self.url_stub.format(**kwargs)
            if self.query_params:
                query = {key: value for key, value in kwargs.items() if key in self.query_params}
                if query:
                    result = result + "?" + urllib.parse.urlencode(query)
        return base_url + result


class ServerVersion(BaseModel):
    protocol_version: str
    server_type: Optional[str]
    server_version: Optional[str]
    server_authors: Optional[List[str]]


class ServerProperties(BaseModel):

    scheme: str
    hostname: str
    port: int = DEFAULT_PORT
    username: Optional[str] = None
    password: Optional[str] = None
    path: str = "/"

    extended_params: Dict[str, str] = {}

    def format_url(self) -> str:
        # Format netloc manually.  The library won't do this for us.
        if self.port == DEFAULT_PORT:
            netloc = self.hostname
        else:
            netloc = f"{self.hostname}:{self.port}"
        if self.username is not None:
            if self.password is None:
                netloc = f"{urllib.parse.quote_plus(self.username)}@{netloc}"
            else:
                netloc = f"{urllib.parse.quote_plus(self.username)}:{urllib.parse.quote_plus(self.password)}@{netloc}"
        if self.extended_params:
            query = urllib.parse.urlencode(self.extended_params)
            return f"{self.scheme}://{netloc}/?{query}"
        return f"{self.scheme}://{netloc}/"

    @classmethod
    def parse_url(cls, url: str) -> "ServerProperties":
        parts = urllib.parse.urlparse(url)
        params = {
            "hostname": parts.hostname,
            "port": parts.port,
            "username": parts.username,
            "password": parts.password,
            "scheme": parts.scheme,
            "path": parts.path,
        }
        for item in list(params):
            if params[item] in (None, ''):
                del params[item]
        if parts.query != '':
            params['extended_params'] = {
                key: value[-1] for key, value in urllib.parse.parse_qs(parts.query, strict_parsing=True).items()
            }
        return cls.parse_obj(params)


class UploadFileContentResponse(BaseModel):
    __root__: str

    @property
    def ref_hash(self) -> str:
        return self.__root__


class FilePartialSizeResponse(BaseModel):
    __root__: int

    @property
    def size(self) -> int:
        return self.__root__


class GetDirectoryResponse(BaseModel):
    children: Dict[str, protocol.Inode]


HELLO = Endpoint('GET', '/', None, ServerVersion)

# User Session
USER_CLIENT_CONFIG = Endpoint('GET', '/about-me', None, protocol.ClientConfiguration)
BACKUP_LATEST = Endpoint('GET', '/backups/latest', None, protocol.Backup)
BACKUP_BY_DATE = Endpoint('GET', '/backups/{backup_date}', None, protocol.Backup)
GET_DIRECTORY = Endpoint('GET', '/directory/{ref_hash}', None, GetDirectoryResponse)
GET_FILE = Endpoint('GET', "/file/{ref_hash}", None, BinaryIO)

# Backup Session
START_BACKUP = Endpoint('POST', '/backup-session/new', {'backup_date', 'allow_overwrite', 'description'},
                        protocol.BackupSessionConfig)
RESUME_BACKUP = Endpoint('GET', '/backup-session/', {'session_id', 'backup_date'}, protocol.BackupSessionConfig)
DISCARD_BACKUP = Endpoint('DELETE', '/backup-session/{session_id}', None, None)
COMPLETE_BACKUP = Endpoint('POST', '/backup-session/{session_id}/complete', None, protocol.Backup)
DIRECTORY_DEF = Endpoint('POST', '/backup-session/{session_id}/directory', {'replaces'}, protocol.DirectoryDefResponse)
UPLOAD_FILE = Endpoint('POST', '/backup-session/{session_id}/file', {'resume_id', 'resume_from', 'is_complete'},
                       UploadFileContentResponse)
FILE_PARTIAL_SIZE = Endpoint('GET', '/backup-session/{session_id}/file-partial-size', {'resume_id'},
                             FilePartialSizeResponse)
ADD_ROOT_DIR = Endpoint('PUT', '/backup-session/{session_id}/roots/{root_dir_name}', None, None)
