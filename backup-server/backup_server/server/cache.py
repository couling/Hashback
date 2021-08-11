import functools
from uuid import UUID

import asyncstdlib
import fastapi
from fastapi import Depends

from backup_server import protocol
from backup_server.server import config, security
from backup_server.server.security import AUTHENTICATOR


@functools.lru_cache(maxsize=config.SERVER_SETTINGS.session_cache_size)
def _cached_server_session(client_id_or_name: str) -> protocol.ServerSession:
    return config.LOCAL_DATABASE.open_client_session(client_id_or_name=client_id_or_name)


def user_session(credentials=fastapi.Depends(AUTHENTICATOR)) -> protocol.ServerSession:
    session = _cached_server_session(client_id_or_name=security.get_client_id(credentials))
    return session


@asyncstdlib.lru_cache(maxsize=config.SERVER_SETTINGS.session_cache_size)
async def _cached_backup_session(client_id_or_name: str, backup_session_id: UUID) -> protocol.BackupSession:
    server_session = _cached_server_session(client_id_or_name=client_id_or_name)
    return await server_session.resume_backup(session_id=backup_session_id)


async def backup_session(session_id: UUID, credentials=Depends(security.AUTHENTICATOR)):
    return await _cached_backup_session(client_id_or_name=security.get_client_id(credentials),
                                        backup_session_id=session_id)
