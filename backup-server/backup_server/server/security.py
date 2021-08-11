import fastapi.security

from . import config

AUTHENTICATOR = {
    'basic': fastapi.security.HTTPBasic,
    # We can add more here...
}[config.SERVER_SETTINGS.auth_type]()


def get_client_id(credentials: fastapi.security.HTTPBasicCredentials) -> str:
    return credentials.username
