import logging
from typing import Dict

import coloredlogs

from cmdproxy.celery_app.config import get_logging_conf

_loggers: Dict[str, logging.Logger] = dict()


def get_logger(name) -> logging.Logger:
    conf = get_logging_conf()

    if name in _loggers:
        logger = _loggers[name]
    else:
        logger = logging.getLogger(name)
        _loggers[name] = logger

        coloredlogs.install(level=conf.loglevel, logger=logger)

    return logger


def init_logging():
    conf = get_logging_conf()
    # logging.basicConfig(level=conf.loglevel)
    coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'yellow'

    # update existing loggers
    for logger in _loggers.values():
        coloredlogs.install(level=conf.loglevel, logger=logger)
