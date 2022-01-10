import asyncio
import collections.abc
import functools
import json
import logging
import os
import signal
from copy import deepcopy
from pathlib import Path
from typing import Any, Collection, Coroutine, Dict, Optional, Union

logger = logging.getLogger(__name__)


DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(default_level: int = logging.INFO):
    """
    Setup logging for the program.  Rather than using program arguments this will interpret two environment variables
    to set log levels.  Both may be blank in which case the program will log at DEBUG level.  Note that some libraries
    such as sqlalchemy set their own fine grained log level and therefore must be enabled through LOG_LEVELS if you need
    their output.

    LOG_LEVEL sets the default.
    LOG_LEVELS is a json string holding a dictionary mapping logger names to log level names.

    Log messages emitted by this method are deliberately from the root logger to make it clear at the start of the
    program run what is supposed to be in the log without it getting switched off by fine grained logging.
    """
    # Find the default log level from environment variable
    try:
        default_level_name = os.environ["LOG_LEVEL"]
        new_default_level = logging.getLevelName(default_level_name)
        if isinstance(default_level, int):
            default_level = new_default_level
        else:
            default_level_name = logging.getLevelName(default_level)
    except KeyError:
        default_level_name = logging.getLevelName(default_level)

    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=default_level)

    # We can't log anything before logging.basicConfig so we have to check it again after and log the message here
    if not isinstance(logging.getLevelName(default_level_name), int):
        logger.warning(f"Unknown log level name {default_level_name} in LOG_LEVEL")
    logger.debug(f"Logging configured to default level {logging.getLevelName(default_level)}")

    for logger_name, level_name in json.loads(os.environ.get('LOG_LEVELS', "{}")).items():
        log_level = logging.getLevelName(level_name)
        if not isinstance(log_level, int):
            logger.warning(f"Unknown log level name {level_name} in LOG_LEVELS")
        else:
            logging.getLogger(logger_name).level = log_level
            logger.debug(f"Logging for '{logger_name}' set to {logging.getLevelName(log_level)}")


def merge(base, update):
    base = deepcopy(base)
    for key, value in update.items():
        if isinstance(value, collections.abc.Mapping):
            base[key] = merge(base.get(key, {}), value)
        else:
            base[key] = value
    return base


def clean_shutdown(num, _):
    """
    Intended to be used as a signal handler
    """
    # pylint: disable=no-member
    logger.error(f"Caught signal '{signal.Signals(num).name}' - Shutting down")
    raise KeyboardInterrupt(f"Signal '{signal.Signals(num).name}'")


def run_then_cancel(future: Optional[Union[asyncio.Future, Coroutine]] = None,
                    loop: Optional[asyncio.BaseEventLoop] = None):
    if loop is None:
        loop = asyncio.get_event_loop()
    try:
        if future is None:
            return loop.run_forever()
        return loop.run_until_complete(future)
    finally:
        # There can still be running tasks on the event loop at this point.
        # Either 'future' has completed but other tasks on the loop have not, or some exception tripped us out of
        # running the loop.
        # Whatever the reason we want to cleanly cancel all remaining tasks (that's the point of this function).
        # To do that we MUST run the event loop after cancelling every task
        all_tasks = asyncio.all_tasks(loop)
        for task in all_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*all_tasks, return_exceptions=True))


# pylint: disable=no-value-for-parameter
def register_clean_shutdown(numbers: Collection[Union[int, signal.Signals]] = (signal.SIGINT, signal.SIGTERM)):
    for num in numbers:
        signal.signal(num, clean_shutdown)


def str_exception(exception: Exception):
    return str(exception) or str(type(exception).__name__)


class SettingsConfig:
    @classmethod
    def customise_sources(cls, init_settings, env_settings, file_secret_settings):
        config_path = init_settings.init_kwargs.get('config_path')
        if config_path is None:
            return (
                init_settings,
                env_settings,
                file_secret_settings,
            )
        return (
            init_settings,
            functools.partial(_load_settings, config_path),
            env_settings,
            file_secret_settings,
        )

def _load_settings(file_path: Path, _) -> Dict[str,Any]:
    with file_path.open('r') as settings_file:
        return json.load(settings_file)


class ContextCloseMixin:

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
