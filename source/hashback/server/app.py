import logging
from datetime import datetime, timezone
from typing import Optional, Union, BinaryIO
from uuid import UUID

import fastapi.responses
from fastapi import Depends

from . import SERVER_VERSION, cache
from .. import protocol, http_protocol, misc
from ..protocol import ServerSession, BackupSession, BackupSessionConfig, ClientConfiguration, Backup

logger = logging.getLogger(__name__)


app = fastapi.FastAPI()

METHOD_MAP = {
    'GET': app.get,
    'POST': app.post,
    'PUT': app.put,
    'DELETE': app.delete,
}


def endpoint(spec: http_protocol.Endpoint, **kwargs):
    """
    Attach the method to the app at the given endpoint
    Annotation which evaluates to get, post, put, delete with a URL stub and response model.
    """
    if spec.result_type is None or spec.result_type == BinaryIO:
        annotation = METHOD_MAP[spec.method](path=spec.url_stub, **kwargs)
    else:
        annotation = METHOD_MAP[spec.method](path=spec.url_stub, response_model=spec.result_type)
    return annotation


@endpoint(http_protocol.HELLO)
async def hello() -> http_protocol.ServerVersion:
    return SERVER_VERSION


@endpoint(http_protocol.USER_CLIENT_CONFIG)
async def about_me(session: ServerSession = Depends(cache.user_session)) -> ClientConfiguration:
    return session.client_config


@endpoint(http_protocol.BACKUP_LATEST)
async def get_backup_latest(session: ServerSession = Depends(cache.user_session)) -> Backup:
    return await session.get_backup()


@endpoint(http_protocol.BACKUP_BY_DATE)
async def get_backup_by_date(backup_date: datetime, session: ServerSession = Depends(cache.user_session)) -> Backup:
    return await session.get_backup(backup_date=backup_date)


@endpoint(http_protocol.GET_DIRECTORY)
async def get_directory(ref_hash: str, session: ServerSession = Depends(cache.user_session)
                        ) -> http_protocol.GetDirectoryResponse:
    inode = protocol.Inode(mode=0, size=0, uid=0, gid=0, hash=ref_hash, type=protocol.FileType.DIRECTORY,
                           modified_time=datetime(year=1970, month=1, day=1))
    result =  await session.get_directory(inode)
    return http_protocol.GetDirectoryResponse(children=result.children)


@endpoint(http_protocol.GET_FILE)
async def get_file(ref_hash: str, session: ServerSession = Depends(cache.user_session)) -> fastapi.Response:
    read_size = 1024*1024
    async def read_content():
        with content:
            bytes_read = await content.read(read_size)
            while bytes_read:
                yield bytes_read
                bytes_read = await content.read(read_size)

    inode = protocol.Inode(mode=0, size=0, uid=0, gid=0, hash=ref_hash, type=protocol.FileType.REGULAR,
                           modified_time=datetime(year=1970, month=1, day=1))
    content = await session.get_file(inode)

    try:
        if content.file_size > 0:
            headers = {'Content-Length': str(content.file_size)}
        else:
            headers = {}
        return fastapi.responses.StreamingResponse(content=read_content(), status_code=200, headers=headers)
    except:
        content.close()
        raise


@endpoint(http_protocol.START_BACKUP)
async def start_backup(session: ServerSession = Depends(cache.user_session), backup_date: Optional[datetime] = None,
                     allow_overwrite: bool = False, description: Optional[str] = None) -> BackupSessionConfig:
    if backup_date is None:
        backup_date = datetime.now(timezone.utc)
    return (await session.start_backup(
        backup_date=backup_date,
        allow_overwrite=allow_overwrite,
        description=description,
    )).config


@endpoint(http_protocol.RESUME_BACKUP)
async def resume_backup(session: ServerSession = Depends(cache.user_session), session_id: UUID = None,
                  backup_date: datetime = None) -> BackupSessionConfig:
    backup_session = await session.resume_backup(session_id=session_id, backup_date=backup_date)
    return backup_session.config


@endpoint(http_protocol.DISCARD_BACKUP)
async def discard_backup(session: BackupSession = Depends(cache.backup_session)) -> fastapi.Response:
    await session.discard()
    return fastapi.Response(status_code=204)


@endpoint(http_protocol.COMPLETE_BACKUP)
async def complete_backup(session: BackupSession = Depends(cache.backup_session)) -> Backup:
    return await session.complete()


@endpoint(http_protocol.DIRECTORY_DEF)
async def directory_definition(definition: protocol.Directory, replaces: Optional[UUID] = None,
                        session: BackupSession = Depends(cache.backup_session)) -> protocol.DirectoryDefResponse:
    return await session.directory_def(definition=definition, replaces=replaces)


@endpoint(http_protocol.UPLOAD_FILE)
async def upload_file_content(resume_id: UUID, file: fastapi.UploadFile = fastapi.File(...),
                              session: BackupSession = Depends(cache.backup_session),
                              resume_from: int = 0, is_complete: bool = True
                              ) -> http_protocol.UploadFileContentResponse:
    ref_hash = await session.upload_file_content(
        file_content=file.file,
        resume_id=resume_id,
        resume_from=resume_from,
        is_complete=is_complete,
    )
    return http_protocol.UploadFileContentResponse(__root__=ref_hash)


@endpoint(http_protocol.FILE_PARTIAL_SIZE)
async def file_partial_size(resume_id: UUID, session: BackupSession = Depends(cache.backup_session)) -> int:
    return await session.check_file_upload_size(resume_id)


@endpoint(http_protocol.ADD_ROOT_DIR)
async def add_root_directory(root_dir_name: str, inode: protocol.Inode,
                             session: BackupSession = Depends(cache.backup_session)):
    await session.add_root_dir(root_dir_name=root_dir_name, inode=inode)
    return fastapi.Response(status_code=204)


@app.exception_handler(protocol.RequestException)
@app.exception_handler(protocol.ProtocolError)
def exception_handler(_: fastapi.Request, exc: Union[protocol.ProtocolError, protocol.RequestException]
                      ) -> fastapi.responses.JSONResponse:
    # Only print stack trace on protocol exception
    logger.error(misc.str_exception(exc), exc_info=isinstance(exc, protocol.ProtocolError))
    response_object = protocol.RemoteException.from_exception(exc)
    return fastapi.responses.JSONResponse(status_code=exc.http_status, content=response_object.dict())


@app.exception_handler(Exception)
def exception_handler_default(request: fastapi.Request, exc: Union[protocol.ProtocolError, protocol.RequestException]
                      ) -> fastapi.responses.JSONResponse:
    # Only print stack trace on protocol exception
    logger.error("Uncaught exception", exc_info=True)
    return exception_handler(request, protocol.InternalServerError(misc.str_exception(exc)))
