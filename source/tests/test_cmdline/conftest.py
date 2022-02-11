# pylint: disable=redefined-outer-name
from pathlib import Path
from typing import Optional

import click.testing
import pytest

from hashback import cmdline
from hashback.local_database import LocalDatabase


@pytest.fixture(autouse=True)
def user_config_path(tmp_path, monkeypatch) -> Path:
    path = tmp_path / 'user_local' / cmdline.Settings.Config.SETTINGS_FILE_DEFAULT_NAME
    monkeypatch.setattr(cmdline.Settings.Config, 'user_config_path', lambda: path)
    return path


@pytest.fixture(autouse=True)
def site_config_path(tmp_path, monkeypatch) -> Path:
    path = tmp_path / 'site_local' / cmdline.Settings.Config.SETTINGS_FILE_DEFAULT_NAME
    monkeypatch.setattr(cmdline.Settings.Config, 'site_config_path', lambda: path)
    return path


@pytest.fixture()
def cli_runner():
    def _cli_runner(*args: str, catch_exceptions: bool = False, exit_code: Optional[None] = 0):
        runner = click.testing.CliRunner(mix_stderr=False)
        result = runner.invoke(cmdline.main, args, catch_exceptions=catch_exceptions, )
        if exit_code is not None:
            if result.exit_code != exit_code:
                raise RuntimeError(f"Unexpected return code {result.exit_code}, expected {exit_code}. \n"
                                   f"Output was\n{result.stderr}\n{result.stdout}")
        return result
    return _cli_runner


@pytest.fixture()
def local_database(tmp_path: Path) -> LocalDatabase:
    db_path = tmp_path / 'db'
    return LocalDatabase.create_database(db_path)
