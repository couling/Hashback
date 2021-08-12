from pathlib import Path

from pydantic import BaseModel

from .. import http_protocol, local_database


class Settings(BaseModel):
    database_path: str
    session_cache_size: int = 128
    port: int = http_protocol.DEFAULT_PORT
    host: str = "localhost"
    auth_type: str = 'basic'


SERVER_SETTINGS = Settings.parse_file("./settings.json")

LOCAL_DATABASE = local_database.LocalDatabase(Path(SERVER_SETTINGS.database_path))
