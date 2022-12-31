import dataclasses
import os
import pprint
from pathlib import Path
from typing import List, Optional, Union

import yaml
from autoserde import AutoSerde


@dataclasses.dataclass
class LoggingConfig:
    loglevel: str = 'INFO'


@dataclasses.dataclass
class CeleryConf:
    # url of the celery broker
    broker_url: str

    # url to the celery backend
    backend_url: str

    # queues
    queues: List[str]


@dataclasses.dataclass
class CloudFSConf:
    # url of the mongodb
    mongo_url: str

    # name of the database where stores the cloud files
    mongo_dbname: str

    def db(self):
        return self.mongo_client().get_database(self.mongo_dbname)

    def mongo_client(self):
        import pymongo

        return pymongo.MongoClient(self.mongo_url)

    def grid_fs(self):
        import gridfs

        return gridfs.GridFS(self.db())


@dataclasses.dataclass
class CmdProxyServerConf:
    # logging configuration
    logging: LoggingConfig

    # celery configuration
    celery: CeleryConf

    # transit cloud filesystem
    cloud: CloudFSConf

    # a dict mapping tool names to command paths
    command_palette: dict

    # the path of this config file
    command_palette_path: Optional[Path]


@dataclasses.dataclass
class CmdProxyClientConf:
    # logging configuration
    logging: LoggingConfig

    # celery configuration
    celery: CeleryConf

    # transit cloud filesystem
    cloud: CloudFSConf


_logging_conf: LoggingConfig = LoggingConfig()

_celery_conf: Optional[CeleryConf] = None

_app_server_conf: Optional[CmdProxyServerConf] = None

_app_client_conf: Optional[CmdProxyClientConf] = None


@dataclasses.dataclass
class CmdProxyServerConfFile:
    redis_url: Optional[str] = 'redis://localhost:6379'
    mongo_url: Optional[str] = 'mongodb://localhost:27017'
    mongo_dbname: Optional[str] = 'cmdproxy-db'
    command_palette: Union[str, None] = None
    environments: Union[str, None] = None
    logging_level: Optional[str] = None


@dataclasses.dataclass
class CmdProxyClientConfFile:
    redis_url: Optional[str] = 'redis://localhost:6379'
    mongo_url: Optional[str] = 'mongodb://localhost:27017'
    mongo_dbname: Optional[str] = 'cmdproxy-db'
    logging_level: Optional[str] = None


def init_server_conf(conf_path: Union[str, Path, None] = None, *,
                     redis_url: Optional[str] = None,
                     mongo_url: Optional[str] = None,
                     mongo_dbname: Optional[str] = None,
                     command_palette: Union[str, Path, None] = None,
                     environments: Union[str, Path, None] = None,
                     loglevel: Optional[str] = None,
                     queues: Optional[str] = None):
    global _app_server_conf

    conf_path = conf_path or (Path.home() / '.cmdproxy' / 'server.yaml')
    conf = AutoSerde.deserialize(conf_path, cls=CmdProxyServerConfFile) \
        if os.path.exists(conf_path) else CmdProxyServerConfFile()

    redis_url = redis_url or os.getenv('CMDPROXY_REDIS_URL') or conf.redis_url
    mongo_url = mongo_url or os.getenv('CMDPROXY_MONGO_URL') or conf.mongo_url
    mongo_dbname = \
        mongo_dbname or \
        os.getenv('CMDPROXY_MONGO_DBNAME') or \
        conf.mongo_dbname
    command_palette = \
        command_palette or \
        os.getenv('CMDPROXY_COMMAND_PALETTE') or \
        conf.command_palette
    environments = \
        environments or \
        os.getenv('CMDPROXY_ENVIRONMENTS') or \
        conf.environments
    loglevel = \
        loglevel or \
        os.getenv('CMDPROXY_LOGLEVEL') or \
        conf.logging_level

    logging_conf = __init_logging_conf(loglevel)

    from cmdproxy.logging import get_logger
    logger = get_logger(__name__)

    logger.debug('init server configuration...')

    if command_palette:
        assert os.path.exists(command_palette)
        with open(command_palette) as f:
            command_palette_path = Path(command_palette)
            command_palette = yaml.safe_load(f)
    else:
        command_palette_path = None
        command_palette = dict()

    if environments and os.path.exists(environments):
        with open(environments) as f:
            environ = yaml.safe_load(f)

        for key, val in environ.items():
            os.environ[key] = val

    queues = queues or os.getenv('CMDPROXY_QUEUES')
    queues = queues.split(',') if queues else []
    queues = queues + list(command_palette.keys())

    _app_server_conf = CmdProxyServerConf(
        logging=logging_conf,
        celery=__init_celery_conf(redis_url, mongo_url, queues),
        cloud=CloudFSConf(mongo_url, mongo_dbname),
        command_palette=command_palette,
        command_palette_path=command_palette_path
    )
    logger.debug(f'Server config: \n{pprint.pformat(_app_server_conf)}')
    return _app_server_conf


def init_client_conf(conf_path: Union[str, Path, None] = None, *,
                     redis_url: Optional[str] = None,
                     mongo_url: Optional[str] = None,
                     mongo_dbname: Optional[str] = None,
                     loglevel: Optional[str] = None):
    global _app_client_conf

    conf_path = conf_path or (Path.home() / '.cmdproxy' / 'client.yaml')
    conf = AutoSerde.deserialize(conf_path, cls=CmdProxyClientConfFile) \
        if os.path.exists(conf_path) else CmdProxyClientConfFile()

    redis_url = redis_url or os.getenv('CMDPROXY_REDIS_URL') or conf.redis_url
    mongo_url = mongo_url or os.getenv('CMDPROXY_MONGO_URL') or conf.mongo_url
    mongo_dbname = \
        mongo_dbname or \
        os.getenv('CMDPROXY_MONGO_DBNAME') or \
        conf.mongo_dbname
    loglevel = \
        loglevel or \
        os.getenv('CMDPROXY_LOGLEVEL') or \
        conf.logging_level

    logging_conf = __init_logging_conf(loglevel)

    from cmdproxy.logging import get_logger
    logger = get_logger(__name__)

    _app_client_conf = CmdProxyClientConf(
        logging=logging_conf,
        celery=__init_celery_conf(redis_url, mongo_url, []),
        cloud=CloudFSConf(mongo_url, mongo_dbname),
    )
    logger.debug(f'Client config: \n{pprint.pformat(_app_client_conf)}')
    return _app_client_conf


def get_logging_conf() -> LoggingConfig:
    return _logging_conf


def get_celery_conf() -> CeleryConf:
    if _celery_conf is None:
        raise RuntimeError(
            f'Uninitialized: you must initialize cmdproxy celery config via '
            f'either `:py:func:init_client_conf` or `:py:func:init_server_conf` '
            f'before accessing it.'
        )

    return _celery_conf


def get_server_end_conf() -> CmdProxyServerConf:
    if _app_server_conf is None:
        raise RuntimeError(
            f'Uninitialized: you must initialize cmdproxy server config via '
            f'`:py:func:init_server_conf` before accessing it.'
        )

    return _app_server_conf


def get_client_end_conf() -> CmdProxyClientConf:
    if _app_client_conf is None:
        raise RuntimeError(
            f'Uninitialized: you must initialize cmdproxy client config via '
            f'`:py:func:init_client_conf` before accessing it.'
        )

    return _app_client_conf


def __init_logging_conf(loglevel: str):
    global _logging_conf
    _logging_conf.loglevel = loglevel

    from cmdproxy.logging import init_logging
    init_logging()

    return _logging_conf


def __init_celery_conf(broker_url: str, backend_url: str, queues: List[str]):
    global _celery_conf
    _celery_conf = CeleryConf(broker_url=broker_url, backend_url=backend_url,
                              queues=queues)
    return _celery_conf
