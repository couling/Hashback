from .. import http_protocol
from .. import protocol
import importlib.metadata

_SERVER_TYPE='hashback'


try:
    _VERSION = importlib.metadata.version(_SERVER_TYPE)
except importlib.metadata.PackageNotFoundError:
    _VERSION = "unknown"


SERVER_VERSION = http_protocol.ServerVersion(
    protocol_version=protocol.VERSION,
    server_type=_SERVER_TYPE,
    server_version=_VERSION,
    server_authors=["Philip Couling"],
)
