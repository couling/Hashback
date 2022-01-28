import stat
from pathlib import Path

import pytest

from hashback.basic_auth.basic_auth import BasicAuthDb
from hashback.http_protocol import AuthenticationFailedException

TEST_USER_NAME = 'test_user'
TEST_PASSWORD = '123abc'
TEST_INCORRECT_PASSWORD = 'abc123'


def test_unregistered_user_raises_access_denied(basic_auth_db: BasicAuthDb):
    with pytest.raises(AuthenticationFailedException):
        basic_auth_db.authenticate(TEST_USER_NAME, TEST_PASSWORD)


def test_register_user_allows_login(basic_auth_db: BasicAuthDb):
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)

    # This does nothing on success, but raises an exception on failure
    basic_auth_db.authenticate(TEST_USER_NAME, TEST_PASSWORD)


def test_incorrect_password_causes_access_denied(basic_auth_db: BasicAuthDb):
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)
    with pytest.raises(AuthenticationFailedException):
        basic_auth_db.authenticate(TEST_USER_NAME, TEST_INCORRECT_PASSWORD)


def test_unregistering_user_prevents_login(basic_auth_db: BasicAuthDb):
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)
    basic_auth_db.unregister_user(TEST_USER_NAME)
    with pytest.raises(AuthenticationFailedException):
        basic_auth_db.authenticate(TEST_USER_NAME, TEST_PASSWORD)


def test_change_password(basic_auth_db: BasicAuthDb, basic_auth_db_path: Path):
    basic_auth_db.register_user(TEST_USER_NAME, TEST_INCORRECT_PASSWORD)
    basic_auth_db.authenticate(TEST_USER_NAME, TEST_INCORRECT_PASSWORD)
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)

    with basic_auth_db_path.open('r') as file:
        db_content = list(file)

    # Very important.  Changing the password must REPLACE the existing user not create a second one
    assert len(db_content) == 1

    basic_auth_db.authenticate(TEST_USER_NAME, TEST_PASSWORD)
    with pytest.raises(AuthenticationFailedException):
        basic_auth_db.authenticate(TEST_USER_NAME, TEST_INCORRECT_PASSWORD)


def test_list_users_on_empty_db(basic_auth_db: BasicAuthDb, basic_auth_db_path: Path):
    basic_auth_db_path.unlink(missing_ok=False)
    result = basic_auth_db.list_users()
    assert result == set()


def test_list_users(basic_auth_db: BasicAuthDb):
    users = set(TEST_USER_NAME + f"_{i}" for i in range(3))
    for user in users:
        basic_auth_db.register_user(user, TEST_PASSWORD)

    result = basic_auth_db.list_users()

    assert result == users


def test_missing_file_raises_access_denied(basic_auth_db: BasicAuthDb, basic_auth_db_path: Path):
    """
    In the default setup the sys-admin may not have created a blank db file.
    Correct behaviour here is to behave as if the file exists and is empty.
    We want an AuthenticationFailedException NOT any OSError (FileNotFound).
    """
    basic_auth_db_path.unlink(missing_ok=False)
    with pytest.raises(AuthenticationFailedException):
        basic_auth_db.authenticate(TEST_USER_NAME, TEST_PASSWORD)


def test_auth_db_created_with_correct_permissions(basic_auth_db: BasicAuthDb, basic_auth_db_path: Path):
    basic_auth_db_path.unlink(missing_ok=False)
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)
    file_stat = basic_auth_db_path.stat()
    result = stat.S_IMODE(file_stat.st_mode)
    assert result == 0o600
