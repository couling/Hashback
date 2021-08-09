import os
from uvicorn import run
from uvicorn.config import LOGGING_CONFIG
from backup_server.misc import register_clean_shutdown, merge, environ_log_levels, DEFAULT_LOG_FORMAT
from backup_server.server import app


if __name__ == '__main__':
    register_clean_shutdown()

    LOGGING_CONFIG['root'] = {"handlers": ["default"], "level": os.environ.get('LOG_LEVEL', 'INFO')}
    LOGGING_CONFIG['formatters']['default']['fmt'] = DEFAULT_LOG_FORMAT
    del LOGGING_CONFIG['loggers']['uvicorn']['handlers']
    run(f"{app.__name__}:app", access_log=True, log_config=merge(LOGGING_CONFIG, {
        'root': {'handlers': ["default"], 'level': os.environ.get('LOG_LEVEL', 'INFO')},
        'formatters': {'default': {'fmt': DEFAULT_LOG_FORMAT}},
        'loggers': merge(environ_log_levels(), {
            'uvicorn': {'handlers': []}
        })
    }))
