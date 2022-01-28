from dataclasses import dataclass, field
from typing import Dict
from uuid import UUID, uuid4

import pytest
from requests.auth import HTTPBasicAuth

from hashback.basic_auth.basic_auth import BasicAuthenticatorAuthorizer
from hashback.http_protocol import AuthenticationFailedException

TEST_USER_NAME = str(uuid4())
TEST_PASSWORD = '123abc'
TEST_INCORRECT_PASSWORD = 'abc123'

TEST_REQUEST_SCOPE = {'type': 'http'}

@dataclass
class MockRequest():
    headers: Dict[str, str] = field(default_factory=dict)


@pytest.fixture()
def authorize(basic_auth_db) -> BasicAuthenticatorAuthorizer:
    basic_auth_db.register_user(TEST_USER_NAME, TEST_PASSWORD)
    return BasicAuthenticatorAuthorizer(basic_auth_db)


@pytest.mark.asyncio
async def test_no_credentials_causes_401(authorize: BasicAuthenticatorAuthorizer):
    try:
        await authorize(MockRequest())
    except AuthenticationFailedException as ex:
        assert ex.http_status == 401
    else:
        raise RuntimeError("No exception raised")


@pytest.mark.asyncio
async def test_authorizer_authenticates_user(authorize: BasicAuthenticatorAuthorizer):
    request = MockRequest()
    add_credentials = HTTPBasicAuth(TEST_USER_NAME, TEST_PASSWORD)
    add_credentials(request)

    result = await authorize(request)
    assert result.client_id == UUID(TEST_USER_NAME)


@pytest.mark.asyncio
async def test_bad_password_causes_401(authorize: BasicAuthenticatorAuthorizer):
    request = MockRequest()
    add_credentials = HTTPBasicAuth(TEST_USER_NAME, TEST_INCORRECT_PASSWORD)
    add_credentials(request)

    with pytest.raises(AuthenticationFailedException):
        await authorize(request)
