from datetime import datetime, timezone, timedelta

from hashback.http_protocol import ServerProperties
from hashback.protocol import Inode, Directory, FileType

SERVER_PROPERTIES = ServerProperties.parse_url('http://test_user:password@example.com')


EXAMPLE_DIR_INODE = Inode(
    modified_time=datetime.now(timezone.utc) - timedelta(days=365),
    type = FileType.DIRECTORY,
    mode=0o755,
    size=599,
    uid=1000,
    gid=1001,
    hash="bbbb",
)


EXAMPLE_DIR = Directory(__root__={
        'some_file': Inode(
            modified_time=datetime.now(timezone.utc) - timedelta(days=365),
            type = FileType.REGULAR,
            mode=0o755,
            size=599,
            uid=1000,
            gid=1001,
            hash="aaaa",
        )
    })
