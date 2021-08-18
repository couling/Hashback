import asyncio
from unittest.mock import AsyncMock

import pytest

from hashback import protocol
from hashback.http_client import ClientSession
from .constants import EXAMPLE_DIR_INODE


@pytest.mark.parametrize('file_type', [file_t for file_t in protocol.FileType if file_t != protocol.FileType.DIRECTORY])
def test_get_dir_raises_on_invalid_type(client: ClientSession, mock_session, file_type):
    directory_inode = EXAMPLE_DIR_INODE.copy()
    directory_inode.type = file_type
    mock_session.get_directory = AsyncMock(side_effect=RuntimeError('This should not have been called'))
    with pytest.raises(protocol.InvalidArgumentsError):
        _ = asyncio.get_event_loop().run_until_complete(
            client.get_directory(directory_inode)
        )


def test_get_file_raises_for_dir(client: ClientSession):
    file_inode = EXAMPLE_DIR_INODE
    with pytest.raises(protocol.InvalidArgumentsError):
        asyncio.get_event_loop().run_until_complete(client.get_file(file_inode))
