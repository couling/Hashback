from datetime import datetime, timezone
from typing import List, Optional, Tuple, Union
from uuid import UUID, uuid4

from hashback import protocol
from hashback.protocol import Backup, BackupSession, BackupSessionConfig, ClientConfiguration, Directory, \
    DirectoryDefResponse, FileReader, Inode


class MockServerSession(protocol.ServerSession):

    def __init__(self, client_config: ClientConfiguration):
        self._client_config = client_config

    @property
    def client_config(self) -> ClientConfiguration:
        return self._client_config

    async def start_backup(self, backup_date: datetime, allow_overwrite: bool = False,
                           description: Optional[str] = None) -> BackupSession:
        return MockBackupSession(
            server_session=self,
            config=BackupSessionConfig(
                client_id=self._client_config.client_id,
                session_id=uuid4(),
                backup_date=backup_date,
                started=datetime.now(timezone.utc),
                allow_overwrite=allow_overwrite,
                description=description,
            ),
        )

    async def resume_backup(self, *, session_id: Optional[UUID] = None,
                            backup_date: Optional[datetime] = None) -> BackupSession:
        raise NotImplementedError()

    async def list_backup_sessions(self) -> List[BackupSessionConfig]:
        raise NotImplementedError()

    async def list_backups(self) -> List[Tuple[datetime, str]]:
        raise NotImplementedError()

    async def get_backup(self, backup_date: Optional[datetime] = None) -> Optional[Backup]:
        pass

    async def get_directory(self, inode: Inode) -> Directory:
        pass

    async def get_file(self, inode: Inode) -> Optional[FileReader]:
        raise NotImplementedError()


class MockBackupSession(protocol.BackupSession):

    def __init__(self, config: BackupSessionConfig, server_session: protocol.ServerSession):
        self._config = config
        self._server_session = server_session

    @property
    def config(self) -> BackupSessionConfig:
        return self._config

    @property
    def server_session(self) -> protocol.ServerSession:
        return self._server_session

    @property
    def is_open(self) -> bool:
        pass

    async def directory_def(self, definition: Directory, replaces: Optional[UUID] = None) -> DirectoryDefResponse:
        pass

    async def upload_file_content(self, file_content: Union[FileReader, bytes], resume_id: UUID, resume_from: int = 0,
                                  is_complete: bool = True) -> Optional[str]:
        pass

    async def add_root_dir(self, root_dir_name: str, inode: Inode) -> None:
        pass

    async def check_file_upload_size(self, resume_id: UUID) -> int:
        pass

    async def complete(self) -> Backup:
        pass


    async def discard(self) -> None:
        pass
