import logging
from typing import Dict

from cmdproxy.celery_app.config import get_logging_conf

_loggers: Dict[str, logging.Logger] = dict()


def get_logger(name) -> logging.Logger:
    if name not in _loggers:
        conf = get_logging_conf()
        logger = logging.getLogger(name)
        logger.setLevel(conf.loglevel)
        _loggers[name] = logger

    return _loggers[name]
