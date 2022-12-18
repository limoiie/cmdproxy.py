import dataclasses
import os
from pathlib import Path
from typing import Optional, Union

import yaml
from autoserde import AutoSerde


@dataclasses.dataclass
class CeleryConf:
    # url of the celery broker
    broker_url: str

    # url to the celery backend
    backend_url: str


@dataclasses.dataclass
class CloudFSConf:
    # url of the mongodb
    mongodb_url: str

    # name of the database where stores the cloud files
    mongodb_name: str

    def db(self):
        return self.mongo_client().get_database(self.mongodb_name)

    def mongo_client(self):
        import pymongo

        return pymongo.MongoClient(self.mongodb_url)

    def grid_fs(self):
        import gridfs

        return gridfs.GridFS(self.db())


@dataclasses.dataclass
class CmdProxyServerConf:
    # celery configuration
    celery: CeleryConf

    # transit cloud filesystem
    cloud: CloudFSConf

    # a dict mapping tool names to command paths
    command_palette: Optional[dict]

    # the path of this config file
    command_palette_path: Optional[Path]


@dataclasses.dataclass
class CmdProxyClientConf:
    # celery configuration
    celery: CeleryConf

    # transit cloud filesystem
    cloud: CloudFSConf


_celery_conf: Optional[CeleryConf] = None

_app_server_conf: Optional[CmdProxyServerConf] = None

_app_client_conf: Optional[CmdProxyClientConf] = None


@dataclasses.dataclass
class CmdProxyServerConfFile:
    redis_url: Optional[str] = 'redis://localhost:6379'
    mongo_url: Optional[str] = 'mongodb://localhost:27017'
    mongodb_name: Optional[str] = 'cmdproxy'
    command_palette: Union[str, None] = None
    environments: Union[str, None] = None


@dataclasses.dataclass
class CmdProxyClientConfFile:
    redis_url: Optional[str] = 'redis://localhost:6379'
    mongo_url: Optional[str] = 'mongodb://localhost:27017'
    mongodb_name: Optional[str] = 'cmdproxy'


def init_server_conf(conf_path: Union[str, Path, None] = None, *,
                     redis_url: Optional[str] = None,
                     mongo_url: Optional[str] = None,
                     mongodb_name: Optional[str] = None,
                     command_palette: Union[str, Path, None] = None,
                     environments: Union[str, Path, None] = None):
    global _app_server_conf

    conf_path = conf_path or (Path.home() / '.cmdproxy' / 'server.yaml')
    conf = AutoSerde.deserialize(conf_path, cls=CmdProxyServerConfFile) \
        if os.path.exists(conf_path) else CmdProxyServerConfFile()

    redis_url = redis_url or os.getenv('CMDPROXY_REDIS_URL') or conf.redis_url
    mongo_url = mongo_url or os.getenv('CMDPROXY_MONGO_URL') or conf.mongo_url
    mongodb_name = \
        mongodb_name or \
        os.getenv('CMDPROXY_MONGODB_NAME') or \
        conf.mongodb_name
    command_palette = \
        command_palette or \
        os.getenv('CMDPROXY_COMMAND_PALETTE') or \
        conf.command_palette
    environments = \
        environments or \
        os.getenv('CMDPROXY_ENVIRONMENTS') or \
        conf.environments

    if command_palette and os.path.exists(command_palette):
        with open(command_palette) as f:
            command_palette_path = Path(command_palette)
            command_palette = yaml.safe_load(f)
    else:
        command_palette_path = None
        command_palette = None

    if environments and os.path.exists(environments):
        with open(environments) as f:
            env = yaml.safe_load(f)

        for key, val in env.items():
            os.putenv(key, val)

    _app_server_conf = CmdProxyServerConf(
        celery=__init_celery_conf(redis_url, mongo_url),
        cloud=CloudFSConf(mongo_url, mongodb_name),
        command_palette=command_palette,
        command_palette_path=command_palette_path
    )
    return _app_server_conf


def init_client_conf(conf_path: Union[str, Path, None] = None, *,
                     redis_url: Optional[str] = None,
                     mongo_url: Optional[str] = None,
                     mongodb_name: Optional[str] = None):
    global _app_client_conf

    conf_path = conf_path or (Path.home() / '.cmdproxy' / 'client.yaml')
    conf = AutoSerde.deserialize(conf_path, cls=CmdProxyClientConfFile) \
        if os.path.exists(conf_path) else CmdProxyClientConfFile()

    redis_url = redis_url or os.getenv('CMDPROXY_REDIS_URL') or conf.redis_url
    mongo_url = mongo_url or os.getenv('CMDPROXY_MONGO_URL') or conf.mongo_url
    mongodb_name = \
        mongodb_name or \
        os.getenv('CMDPROXY_MONGODB_NAME') or \
        conf.mongodb_name

    _app_client_conf = CmdProxyClientConf(
        celery=__init_celery_conf(redis_url, mongo_url),
        cloud=CloudFSConf(mongo_url, mongodb_name),
    )
    return _app_client_conf


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


def __init_celery_conf(broker_url: str, backend_url: str):
    global _celery_conf

    _celery_conf = CeleryConf(broker_url=broker_url, backend_url=backend_url)
    return _celery_conf
