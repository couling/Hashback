import os
from typing import List

import click
from uvicorn import run
from uvicorn.config import LOGGING_CONFIG

from . import app
from ..misc import register_clean_shutdown, merge, environ_log_levels, DEFAULT_LOG_FORMAT


@click.command()
@click.option("--host", default=["localhost"], multiple=True)
@click.option("--port", type=click.INT, default=8000)
def main(host: List[str], port: int):
    register_clean_shutdown()
    log_config = merge(LOGGING_CONFIG, {
        'root': {'handlers': ["default"], 'level': os.environ.get('LOG_LEVEL', 'INFO')},
        'formatters': {'default': {'fmt': DEFAULT_LOG_FORMAT}},
        'loggers': merge(environ_log_levels(), {
            'uvicorn': {'handlers': []}
        })
    })
    log_config['formatters']['default']['fmt'] = DEFAULT_LOG_FORMAT
    del log_config['loggers']['uvicorn']['handlers']
    run(f"{app.__name__}:app", access_log=True, log_config=log_config, host=host, port=port)


if __name__ == '__main__':
    main()
