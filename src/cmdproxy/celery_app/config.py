import dataclasses
import pathlib
from typing import Optional, Union

import yaml


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
    cloud_fs: CloudFSConf

    # a dict mapping tool names to command paths
    command_palette: dict

    # the path of this config file
    command_palette_file: pathlib.Path


@dataclasses.dataclass
class CmdProxyClientConf:
    # celery configuration
    celery: CeleryConf

    # transit cloud filesystem
    cloud_fs: CloudFSConf


_celery_conf: Optional[CeleryConf] = None

_app_server_conf: Optional[CmdProxyServerConf] = None

_app_client_conf: Optional[CmdProxyClientConf] = None


def init_server_conf(redis_url: str, mongo_url: str, mongodb_name: str,
                     command_palette_path: Union[str, pathlib.Path]):
    global _app_server_conf

    with open(command_palette_path) as f:
        command_palette = yaml.safe_load(f)

    _app_server_conf = CmdProxyServerConf(
        celery=__init_celery_conf(redis_url, mongo_url),
        cloud_fs=CloudFSConf(mongo_url, mongodb_name),
        command_palette=command_palette,
        command_palette_file=pathlib.Path(command_palette_path)
    )
    return _app_server_conf


def init_client_conf(redis_url: str, mongo_url: str, mongodb_name: str):
    global _app_client_conf

    _app_client_conf = CmdProxyClientConf(
        celery=__init_celery_conf(redis_url, mongo_url),
        cloud_fs=CloudFSConf(mongo_url, mongodb_name),
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
