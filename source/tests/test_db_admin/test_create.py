from pathlib import Path

import click.testing
import pytest
from hashback.db_admin import db_admin
from hashback.local_database import LocalDatabase, Configuration


def create_db(path: Path, *args: str, exit_code: int = 0):
    runner = click.testing.CliRunner()
    result = runner.invoke(
        cli=db_admin.click_main,
        args=(str(path), 'create',) + args,
        catch_exceptions=False
    )
    assert result.exit_code == exit_code
    return result


@pytest.mark.parametrize('split_count', (0,1,2))
def test_create_db(tmp_path: Path, split_count: int):
    create_db(tmp_path, '--store-split-count', str(split_count))
    config = LocalDatabase(tmp_path).config
    assert config.store_split_count == split_count
    assert config.store_split_size == 2


def test_clobber_existing_fails(tmp_path: Path):
    create_db(tmp_path, '--store-split-count', '0', exit_code=0)
    result = create_db(tmp_path, '--store-split-count', '1', exit_code=1)
    assert LocalDatabase(tmp_path).config.store_split_count == 0
    assert result.output.startswith("Error: Database already exists")
