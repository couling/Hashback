import abc
import asyncio
import collections.abc
import functools
import json
import logging
import os
import signal
from copy import deepcopy
from pathlib import Path
from typing import Any, Collection, Deque, Dict, Generic, TypeVar, Union

import appdirs

logger = logging.getLogger(__name__)


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


class CleanEventLoop:
    """
    Context manager to run something in an event loop cleanly.  On exit this will clean up the event loop
    """
    def __init__(self, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()

    def __enter__(self):
        return self._loop

    def __exit__(self, exc_type, exc_val, exc_tb):
        cleanup_event_loop(self._loop)


def cleanup_event_loop(loop=None):
    """
    There can still be running tasks on the event loop.  Either run_until_complete has completed but other tasks on the
    loop have not, or some exception tripped us out of running the loop such as KeyboardInterrupt. Whatever the reason
    we want to cleanly cancel all remaining tasks (that's the point of this function). To do that we MUST run the event
    loop after cancelling every task.
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    all_tasks = asyncio.all_tasks(loop)
    while all_tasks:
        for task in all_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*all_tasks, return_exceptions=True))
        # Theoretically a cancelled task can create new tasks in the finally: or except: clauses so we need to cancel
        # those too.  Yes malicious code could make us hang here. There's no way to avoid that AND  correctly clean up.
        all_tasks = asyncio.all_tasks(loop)


def wrapped_async(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))
    return wrapper


# pylint: disable=no-value-for-parameter
def register_clean_shutdown(numbers: Collection[Union[int, signal.Signals]] = (signal.SIGINT, signal.SIGTERM)):
    for num in numbers:
        signal.signal(num, clean_shutdown)


def str_exception(exception: Exception):
    return str(exception) or str(type(exception).__name__)


class SettingsConfig:
    APP_NAME = 'hashback'
    SETTINGS_FILE_DEFAULT_NAME: str = 'settings.json'

    @classmethod
    def customise_sources(cls, init_settings, env_settings, file_secret_settings):
        """
        Load settings from the site config path if it exists, and override with either the user config_path
        Or the specified config_path
        """
        config_path = init_settings.init_kwargs.pop('config_path', None)
        if config_path is None:
            config_path = cls.user_config_path()

        load_paths = [cls.site_config_path(), config_path]
        logger.debug("Loading settings from '%s'", "' and '".join(str(load_path) for load_path in load_paths))
        loaders = [functools.partial(cls._load_settings, path) for path in load_paths if path.is_file()]

        return (
            init_settings,
            *loaders,
            env_settings,
            file_secret_settings,
        )

    @classmethod
    def user_config_path(cls) -> Path:
        return Path(appdirs.user_config_dir(cls.APP_NAME), cls.SETTINGS_FILE_DEFAULT_NAME)

    @classmethod
    def site_config_path(cls) -> Path:
        if 'XDG_CONFIG_DIRS' not in os.environ:
            os.environ['XDG_CONFIG_DIRS'] = '/etc'
            result = Path(appdirs.site_config_dir(cls.APP_NAME), cls.SETTINGS_FILE_DEFAULT_NAME)
            del os.environ['XDG_CONFIG_DIRS']
        else:
            result = Path(appdirs.site_config_dir(cls.APP_NAME), cls.SETTINGS_FILE_DEFAULT_NAME)
        return result

    @classmethod
    def _load_settings(cls, file_path: Path, context) -> Dict[str,Any]:
        try:
            with file_path.open('r') as settings_file:
                result = json.load(settings_file)
        except OSError as exc:
            logger.warning(f"Could not load {file_path}: ({str_exception(exc)}))")
            result = {}
        return result


class ContextCloseMixin:

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FairSemaphore:
    """
    Semaphore with strictly controlled order.

    By default this will act
    """

    _queue: Deque[asyncio.Future]
    _value: int
    _fifo: bool

    def __init__(self, value: int, fifo=True):
        """
        Initial value of the semaphore
        :param value: Initial value for the semaphore
        :param fifo:
        """
        self._value = value
        self._queue = collections.deque()
        self._fifo = fifo

    def locked(self) -> bool:
        return not self._value

    async def acquire(self):
        if self._value:
            self._value -= 1
        else:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            future = loop.create_future()
            self._queue.append(future)

            try:
                await future
            except:
                # This condition happens when the future's result was set but the task was cancelled
                # In other words another task completed and released this one... but this one got cancelled before it
                # could do anything.  As a result we need to release another.
                if not future.cancelled():
                    self.release()
                # else:
                # But if we were NOT released then we do not have the right to release another.
                raise

    def release(self):
        # Tasks can get cancelled while in the queue.
        # Naively you would expect their _acquire() code to remove them from the queue.  But that doesn't always work
        # because the event loop might not have given them chance execute the CancelledError except clause yet.
        # It's absolutely unavoidable that there could be cancelled tasks waiting on this queue.
        # When that happen the done() state of the future goes to True...
        while self._queue:
            future = self._queue.popleft() if self._fifo else self._queue.pop()
            if not future.done():
                future.set_result(None)
                break
            # ... we discard any task which is already "done" because
        else:
            self._value += 1

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()


async def gather_all_or_nothing(*futures: asyncio.Future):
    try:
        result = await asyncio.gather(*futures)
    except:
        for future in futures:
            future.cancel()
        raise
    return result
