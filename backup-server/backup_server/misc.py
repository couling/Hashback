import logging
import json
import os


def setup_logging(default_level: int):
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

    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=default_level)

    # We can't log anything before logging.basicConfig so we have to check it again after and log the message here
    if not isinstance(logging.getLevelName(default_level_name), int):
        logging.warning(f"Unknown log level name {default_level_name} in LOG_LEVEL")
    logging.info(f"Logging configured to default level {logging.getLevelName(default_level)}")

    # If there's LOG_LEVELS environment variable, use it to set fine grained levels.
    try:
        log_levels = json.loads(os.environ.get("LOG_LEVELS", ""))
    except json.decoder.JSONDecodeError:
        return

    for logger_name, level_name in log_levels.items():
        log_level = logging.getLevelName(level_name)
        if not isinstance(log_level, int):
            logging.warning(f"Unknown log level name {level_name} in LOG_LEVELS")
        else:
            logging.getLogger(logger_name).level = log_level
            logging.info(f"Logging for '{logger_name}' set to {logging.getLevelName(log_level)}")
