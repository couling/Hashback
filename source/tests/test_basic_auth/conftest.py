# pylint: disable=redefined-outer-name
from pathlib import Path

import pytest

from hashback.basic_auth.basic_auth import BasicAuthDb


@pytest.fixture()
def basic_auth_db_path(tmp_path) -> Path:
    auth_file = tmp_path / 'auth.shadow'
    auth_file.touch(mode=0o600, exist_ok=False)
    return auth_file


@pytest.fixture()
def basic_auth_db(basic_auth_db_path: Path) -> BasicAuthDb:
    return BasicAuthDb(basic_auth_db_path)
