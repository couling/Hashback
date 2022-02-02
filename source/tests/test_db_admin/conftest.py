# pylint: disable=redefined-outer-name
from pathlib import Path
from typing import Optional

import click.testing
import pytest

from hashback.db_admin import db_admin
from hashback.local_database import LocalDatabase


@pytest.fixture()
def cli_runner(local_db_path: Path):
    def _cli_runner(*args: str, catch_exceptions: bool = False, exit_code: Optional[None] = 0):
        runner = click.testing.CliRunner()
        args = (str(local_db_path),) + args
        result = runner.invoke(db_admin.click_main, args, catch_exceptions=catch_exceptions)
        if exit_code is not None:
            assert result.exit_code == exit_code
        return result
    return _cli_runner


@pytest.fixture()
def local_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / 'db'
    LocalDatabase.create_database(db_path, LocalDatabase.Configuration())
    return db_path
