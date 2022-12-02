import dataclasses
import pathlib
from typing import Optional

import yaml


@dataclasses.dataclass
class CeleryConf:
    # uri to the redis broker
    redis_uri: str

    # uri to the mongodb backend
    mongo_uri: str

    # name of database where stores the remote-fs
    mongodb_name: str

    def mongodb(self):
        return self.mongo_client().get_database(self.mongodb_name)

    def mongo_client(self):
        import pymongo

        return pymongo.MongoClient(self.mongo_uri)

    def grid_fs(self):
        import gridfs

        return gridfs.GridFS(self.mongodb())


@dataclasses.dataclass
class CmdProxyServerConf:
    # celery configuration
    celery: CeleryConf

    # a dict mapping tool names to command paths
    command_palette: dict

    # the path of this config file
    command_palette_file: pathlib.Path


@dataclasses.dataclass
class CmdProxyClientConf:
    # celery configuration
    celery: CeleryConf


_celery_conf: Optional[CeleryConf] = None

_app_server_conf: Optional[CmdProxyServerConf] = None

_app_client_conf: Optional[CmdProxyClientConf] = None


def init_server_end_conf(redis_uri: str, mongo_uri: str, mongodb_name: str,
                         command_palette_path: pathlib.Path):
    global _app_server_conf

    with open(command_palette_path) as f:
        command_palette = yaml.safe_load(f)

    _app_server_conf = CmdProxyServerConf(
        celery=__init_celery_conf(redis_uri, mongo_uri, mongodb_name),
        command_palette=command_palette,
        command_palette_file=command_palette_path
    )
    return _app_server_conf


def init_client_end_conf(redis_uri: str, mongo_uri: str, mongodb_name: str):
    global _app_client_conf

    _app_client_conf = CmdProxyClientConf(
        celery=__init_celery_conf(redis_uri, mongo_uri, mongodb_name))
    return _app_client_conf


def get_celery_conf() -> CeleryConf:
    if _celery_conf is None:
        raise RuntimeError(
            f'Uninitialized: you must initialize proxy config before accessing it.'
        )

    return _celery_conf


def get_server_end_conf() -> CmdProxyServerConf:
    if _app_server_conf is None:
        raise RuntimeError(
            f'Uninitialized: you must initialize proxy config before accessing it.'
        )

    return _app_server_conf


def __init_celery_conf(redis_uri: str, mongo_uri: str, mongodb_name: str):
    global _celery_conf

    _celery_conf = CeleryConf(
        redis_uri=redis_uri,
        mongo_uri=mongo_uri,
        mongodb_name=mongodb_name,
    )
    return _celery_conf
